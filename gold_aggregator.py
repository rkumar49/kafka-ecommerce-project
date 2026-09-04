import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta

SNOWFLAKE_CONFIG = {
    "user": "rkumar49",
    "password": "Apr_2s8h@12345",
    "account": "LQ88322.eu-central-1",
    "warehouse": "COMPUTE_WH",
    "database": "KAFKA_DB",
    "schema": "STREAMING"
}

def aggregate_daily_metrics():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    # Get yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"Aggregating metrics for {yesterday}...")
    
    # Aggregate data from Silver to Gold
    cursor.execute(f"""
        MERGE INTO KAFKA_DAILY_METRICS_GOLD AS target
        USING (
            SELECT 
                DATE(EVENT_TIMESTAMP) AS METRIC_DATE,
                CUSTOMER_ID,
                SUM(CASE WHEN EVENT_TYPE = 'PURCHASE' THEN AMOUNT ELSE 0 END) AS TOTAL_AMOUNT,
                COUNT(CASE WHEN EVENT_TYPE = 'PURCHASE' THEN 1 END) AS TOTAL_ORDERS,
                COUNT(CASE WHEN EVENT_TYPE = 'ADD_TO_CART' THEN 1 END) AS TOTAL_CART_ADDITIONS,
                COUNT(CASE WHEN EVENT_TYPE = 'PAGE_VIEW' THEN 1 END) AS TOTAL_PAGE_VIEWS
            FROM KAFKA_EVENTS_SILVER
            WHERE DATE(EVENT_TIMESTAMP) = '{yesterday}'
            GROUP BY DATE(EVENT_TIMESTAMP), CUSTOMER_ID
        ) AS source
        ON target.METRIC_DATE = source.METRIC_DATE 
        AND target.CUSTOMER_ID = source.CUSTOMER_ID
        WHEN MATCHED THEN UPDATE SET
            target.TOTAL_AMOUNT = source.TOTAL_AMOUNT,
            target.TOTAL_ORDERS = source.TOTAL_ORDERS,
            target.TOTAL_CART_ADDITIONS = source.TOTAL_CART_ADDITIONS,
            target.TOTAL_PAGE_VIEWS = source.TOTAL_PAGE_VIEWS
        WHEN NOT MATCHED THEN INSERT (
            METRIC_DATE, 
            CUSTOMER_ID, 
            TOTAL_AMOUNT, 
            TOTAL_ORDERS, 
            TOTAL_CART_ADDITIONS, 
            TOTAL_PAGE_VIEWS
        ) VALUES (
            source.METRIC_DATE,
            source.CUSTOMER_ID,
            source.TOTAL_AMOUNT,
            source.TOTAL_ORDERS,
            source.TOTAL_CART_ADDITIONS,
            source.TOTAL_PAGE_VIEWS
        )
    """)
    
    rows_affected = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Updated Gold table for {yesterday}. Rows affected: {rows_affected}")

if __name__ == "__main__":
    aggregate_daily_metrics()