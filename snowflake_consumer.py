import json
from kafka import KafkaConsumer
import snowflake.connector
import pandas as pd

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "raw_events"
GROUP_ID = "snowflake-loader"

SNOWFLAKE_CONFIG = {
    "user": "rkumar49",
    "password": "Apr_2s8h@12345",
    "account": "LQ88322.eu-central-1",
    "warehouse": "COMPUTE_WH",
    "database": "KAFKA_DB",
    "schema": "STREAMING"
}

BATCH_SIZE = 10

# Connect to Snowflake
sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
print("✅ Connected to Snowflake")

# Connect to Kafka
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

buffer = []

def flush_to_snowflake(records):
    df = pd.DataFrame(records)
    df.columns = [c.upper() for c in df.columns]
    
    # Write to Snowflake using cursor directly
    cursor = sf_conn.cursor()
    
    # Create insert statement
    values = []
    for _, row in df.iterrows():
        values.append(f"('{row['EVENT_ID']}', '{row['CUSTOMER_ID']}', '{row['EVENT_TYPE']}', {row['AMOUNT']}, '{row['CURRENCY']}', '{row['EVENT_TIMESTAMP']}')")
    
    insert_sql = f"""
    INSERT INTO KAFKA_EVENTS_SILVER (EVENT_ID, CUSTOMER_ID, EVENT_TYPE, AMOUNT, CURRENCY, EVENT_TIMESTAMP)
    VALUES {','.join(values)}
    """
    
    cursor.execute(insert_sql)
    cursor.close()
    print(f"✅ Inserted {len(records)} rows into Snowflake")

print("Starting consumer...")

for message in consumer:
    event = message.value
    
    if not event.get("is_valid", True):
        continue
    
    buffer.append({
        "event_id": event["event_id"],
        "customer_id": event["customer_id"],
        "event_type": event["event_type"],
        "amount": event["amount"],
        "currency": event["currency"],
        "event_timestamp": event["event_timestamp"]
    })
    
    if len(buffer) >= BATCH_SIZE:
        flush_to_snowflake(buffer)
        consumer.commit()
        buffer.clear()