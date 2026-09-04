[README_Kafka_Ecommerce.md](https://github.com/user-attachments/files/31815398/README_Kafka_Ecommerce.md)
# 📊 Kafka E-Commerce Real-Time Data Pipeline

<p align="center">
  <strong>Event Streaming → Data Validation → Snowflake → Analytics</strong><br>
  A real-time e-commerce data engineering pipeline built with Apache Kafka, Python, Docker, and Snowflake.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8%2B-blue?style=for-the-badge&logo=python" alt="Python">
  <img src="https://img.shields.io/badge/Apache%20Kafka-KRaft-black?style=for-the-badge&logo=apachekafka" alt="Kafka">
  <img src="https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=for-the-badge&logo=snowflake" alt="Snowflake">
  <img src="https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker" alt="Docker">
</p>

---

## 📌 Overview

This project implements a **production-oriented real-time e-commerce data pipeline** that processes customer events from generation through streaming, validation, storage, aggregation, and analytics.

The pipeline demonstrates practical data engineering concepts including:

- ⚡ Real-time event streaming with **Apache Kafka**
- 🔀 Multi-partition processing
- 🛡️ Event validation and data quality handling
- 📦 Batched loading into **Snowflake**
- 🥈 **Silver** and 🥇 **Gold** layers using Medallion Architecture
- 🔐 Manual Kafka offset commits
- 🔑 Customer-level ordering through Kafka message keys
- 📅 Daily analytical aggregations
- 👀 Kafka monitoring through **Kafka-UI**

---

## 🏗️ Architecture

```text
                         REAL-TIME E-COMMERCE DATA PIPELINE

┌──────────────┐       ┌────────────────┐       ┌──────────────────┐
│   Producer   │       │  Apache Kafka  │       │     Consumer     │
│  producer.py │──────▶│   raw_events   │──────▶│ snowflake_       │
│              │       │   3 partitions │       │ consumer.py      │
└──────────────┘       └────────────────┘       └────────┬─────────┘
       │                                                  │
       │ Generates                                         │ Validates
       │ events                                            │ + batches
       ▼                                                  ▼
┌──────────────┐                                  ┌─────────────────┐
│ PAGE_VIEW    │                                  │     SILVER      │
│ ADD_TO_CART  │                                  │ KAFKA_EVENTS_   │
│ PURCHASE     │                                  │     SILVER      │
└──────────────┘                                  └────────┬────────┘
                                                          │
                                                          │ Daily
                                                          │ aggregation
                                                          ▼
                                                 ┌─────────────────┐
                                                 │      GOLD       │
                                                 │ KAFKA_DAILY_    │
                                                 │ METRICS_GOLD    │
                                                 └────────┬────────┘
                                                          │
                                                          ▼
                                                 ┌─────────────────┐
                                                 │ Analytics /     │
                                                 │ Dashboard       │
                                                 └─────────────────┘

                           ┌──────────────────┐
                           │     Kafka-UI     │
                           │  localhost:8081  │
                           └──────────────────┘
```

### 🔄 End-to-End Flow

```text
Event Generator
      ↓
Kafka Producer
      ↓
raw_events topic
      ↓
Validation & Processing
      ↓
Batch Loading
      ↓
Snowflake Silver
      ↓
Daily Aggregation
      ↓
Snowflake Gold
      ↓
Analytics / Dashboard
```

---

## ✨ Key Features

| Feature | Description |
|---|---|
| ⚡ **Real-Time Streaming** | Processes e-commerce events continuously through Kafka |
| 📨 **Kafka Messaging** | Uses the `raw_events` topic with 3 partitions |
| 🔑 **Ordered Customer Events** | `customer_id` is used as the Kafka message key |
| 🛡️ **Data Quality** | Invalid events are intentionally generated and filtered |
| 📦 **Batch Processing** | Consumer buffers 10 events before writing to Snowflake |
| ❄️ **Snowflake Integration** | Stores validated events for analytics |
| 🥈 **Silver Layer** | Preserves validated customer events |
| 🥇 **Gold Layer** | Stores daily customer-level metrics |
| 📊 **Analytics Ready** | Includes example business queries |
| 👀 **Kafka Monitoring** | Kafka-UI provides topic and consumer visibility |
| 🐳 **Dockerized Kafka** | Kafka runs in KRaft mode without ZooKeeper |

---

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| **Python** | Event generation and data processing |
| **Apache Kafka** | Real-time event streaming |
| **Kafka-Python** | Kafka producer and consumer implementation |
| **Snowflake** | Cloud data warehouse and analytics |
| **Docker / Docker Compose** | Kafka infrastructure |
| **Kafka-UI** | Kafka monitoring and management |
| **Pandas** | Data processing and transformation |

### 📦 Dependencies

```text
kafka-python==2.0.2
snowflake-connector-python==4.7.3
pandas==3.0.5
```

---

## 📁 Project Structure

```text
kafka-ecommerce-project/
│
├── 📄 producer.py
│   └── Generates e-commerce events at 1 event/second
│
├── 📄 snowflake_consumer.py
│   └── Reads Kafka events, validates them, and loads Snowflake
│
├── 📄 gold_aggregator.py
│   └── Creates daily Silver → Gold aggregations
│
├── 📄 test_connection.py
│   └── Tests Kafka connectivity
│
├── 📄 test_snowflake.py
│   └── Tests Snowflake connectivity
│
├── 🐳 docker-compose.yml
│   └── Kafka + Kafka-UI configuration
│
├── 📄 requirements.txt
│   └── Python dependencies
│
├── 📁 venv/
│   └── Python virtual environment
│
└── 📄 README.md
```

---

## 📦 Data Model

### 🥈 Silver — `KAFKA_EVENTS_SILVER`

Stores validated raw events.

```sql
CREATE TABLE KAFKA_EVENTS_SILVER (
    EVENT_ID VARCHAR(36) PRIMARY KEY,
    CUSTOMER_ID VARCHAR(20),
    EVENT_TYPE VARCHAR(20),
    AMOUNT DECIMAL(10,2),
    CURRENCY VARCHAR(3),
    EVENT_TIMESTAMP TIMESTAMP_NTZ,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 🥇 Gold — `KAFKA_DAILY_METRICS_GOLD`

Stores pre-aggregated daily customer metrics.

```sql
CREATE TABLE KAFKA_DAILY_METRICS_GOLD (
    METRIC_DATE DATE,
    CUSTOMER_ID VARCHAR(20),
    TOTAL_AMOUNT DECIMAL(15,2),
    TOTAL_ORDERS INTEGER,
    TOTAL_CART_ADDITIONS INTEGER,
    TOTAL_PAGE_VIEWS INTEGER,
    PRIMARY KEY (METRIC_DATE, CUSTOMER_ID)
);
```

---

## 🔄 Pipeline Components

### 1️⃣ Producer — `producer.py`

Generates random e-commerce events every second:

```text
PAGE_VIEW
ADD_TO_CART
PURCHASE
```

Approximately **25% of generated events are intentionally invalid** to test data quality.

Events are sent to the `raw_events` Kafka topic using `customer_id` as the message key.

### 🔑 Why use `customer_id` as the Kafka key?

Kafka uses the message key to determine partition placement.

Using `customer_id` means events for the same customer are routed to the same partition, supporting **ordered processing for that customer**.

---

### 2️⃣ Kafka Broker — `docker-compose.yml`

Kafka runs in **KRaft mode**, so ZooKeeper is not required.

Configuration includes:

- `raw_events` topic
- **3 partitions**
- Parallel processing capability
- Kafka-UI on port `8081`

Kafka-UI:

```text
http://localhost:8081
```

---

### 3️⃣ Consumer — `snowflake_consumer.py`

The consumer:

1. Reads events from Kafka
2. Filters invalid events
3. Buffers events
4. Writes batches to Snowflake
5. Commits Kafka offsets manually

The default batch size is **10 events**.

### 📦 Why batch the writes?

Instead of making one Snowflake request per event, events are grouped into batches. This reduces the number of calls and improves loading efficiency.

---

### 4️⃣ Silver Layer

`KAFKA_EVENTS_SILVER` stores validated events.

The Silver layer:

- Preserves customer events
- Supports real-time analytical queries
- Provides trusted input for downstream aggregation

```text
Raw Events
    ↓
  Silver
    ↓
   Gold
```

---

### 5️⃣ Gold Layer — `gold_aggregator.py`

Creates daily customer-level metrics:

- 💰 Revenue
- 🛒 Cart additions
- 🛍️ Orders
- 👀 Page views

Pre-aggregated data makes dashboard queries faster and reduces repeated computation.

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- Snowflake account

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd kafka-ecommerce-project
```

### 2. Create a Virtual Environment

**Windows**

```bash
python -m venv venv
venv\Scriptsctivate
```

**macOS / Linux**

```bash
python -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Start Kafka

```bash
docker-compose up -d
```

Check the containers:

```bash
docker-compose ps
```

Open Kafka-UI:

```text
http://localhost:8081
```

### 5. Start the Producer

```bash
python producer.py
```

### 6. Start the Consumer

Open a second terminal:

```bash
python snowflake_consumer.py
```

### 7. Run the Gold Aggregator

```bash
python gold_aggregator.py
```

### 8. Stop Kafka

```bash
docker-compose down
```

---

## 📊 Business Queries

### ⏱️ Real-Time Monitoring — Last Hour

```sql
SELECT
    COUNT(*) AS EVENTS_LAST_HOUR,
    SUM(CASE
        WHEN EVENT_TYPE = 'PURCHASE' THEN 1
        ELSE 0
    END) AS ORDERS,
    SUM(CASE
        WHEN EVENT_TYPE = 'PURCHASE' THEN AMOUNT
        ELSE 0
    END) AS REVENUE
FROM KAFKA_EVENTS_SILVER
WHERE EVENT_TIMESTAMP >= DATEADD(
    hour,
    -1,
    CURRENT_TIMESTAMP()
);
```

### 🏆 Top Customers by Revenue

```sql
SELECT
    CUSTOMER_ID,
    SUM(AMOUNT) AS TOTAL_SPENT,
    COUNT(*) AS EVENT_COUNT
FROM KAFKA_EVENTS_SILVER
WHERE EVENT_TYPE = 'PURCHASE'
GROUP BY CUSTOMER_ID
ORDER BY TOTAL_SPENT DESC;
```

### 📅 Daily Dashboard

```sql
SELECT
    METRIC_DATE,
    COUNT(DISTINCT CUSTOMER_ID) AS ACTIVE_CUSTOMERS,
    SUM(TOTAL_ORDERS) AS ORDERS,
    SUM(TOTAL_AMOUNT) AS REVENUE
FROM KAFKA_DAILY_METRICS_GOLD
GROUP BY METRIC_DATE
ORDER BY METRIC_DATE DESC;
```

### 💰 Daily Revenue

```sql
SELECT
    METRIC_DATE,
    SUM(TOTAL_AMOUNT) AS DAILY_REVENUE
FROM KAFKA_DAILY_METRICS_GOLD
GROUP BY METRIC_DATE
ORDER BY METRIC_DATE DESC;
```

---

## 🔐 Data Quality & Reliability

The pipeline intentionally generates invalid events so that the validation process can be demonstrated.

```text
                    Kafka Events
                         │
                         ▼
                  ┌──────────────┐
                  │  Validation  │
                  └──────┬───────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
        Valid Events          Invalid Events
              │                     │
              ▼                     ▼
        Snowflake Silver          Filtered
```

The consumer uses **manual offset commits** together with idempotent writes as part of the project's reliability approach.

---

## 💡 Key Engineering Concepts

| Concept | Implementation |
|---|---|
| **Event Streaming** | Kafka with 3 partitions |
| **Partitioning** | `customer_id` used as message key |
| **Data Quality** | Invalid events are flagged and filtered |
| **Batch Loading** | 10-event batches to Snowflake |
| **Offset Management** | Manual Kafka offset commits |
| **Data Architecture** | Silver → Gold Medallion pattern |
| **Real-Time Processing** | Continuous event generation and consumption |
| **Analytics** | SQL queries against Silver and Gold |
| **Monitoring** | Kafka-UI |
| **Containerization** | Docker Compose |

---

## 🔍 Troubleshooting

| Problem | Solution |
|---|---|
| Kafka connection failed | Run `docker-compose ps` and verify Kafka is running |
| Snowflake authentication error | Verify Snowflake credentials/configuration |
| No data in Snowflake | Check consumer logs and batch configuration |
| Topic not found | Create the `raw_events` topic with 3 partitions |
| Kafka-UI unavailable | Confirm the Kafka-UI container is running and port `8081` is available |

### Create the Kafka Topic Manually

```bash
docker-compose exec kafka kafka-topics.sh   --create   --topic raw_events   --bootstrap-server localhost:9092   --partitions 3   --replication-factor 1
```

---

## 📈 Skills Demonstrated

- 📨 Apache Kafka
- ⚡ Real-time data engineering
- 🐍 Python
- 🔀 Kafka partitioning
- 🔐 Consumer offset management
- 🛡️ Data validation
- 📦 Batch processing
- ❄️ Snowflake
- 🥈🥇 Medallion Architecture
- 🗃️ SQL analytics
- 🐳 Docker
- 👀 Kafka monitoring
- 🏗️ Event-driven architecture

---

## 🔮 Future Enhancements

| Enhancement | Description |
|---|---|
| 📊 **Live Dashboard** | Add a real-time analytics dashboard |
| ⚡ **Kafka Streams** | Introduce stream-level transformations |
| ☁️ **Cloud Deployment** | Deploy processing components to cloud infrastructure |
| 🔄 **Schema Management** | Add schema validation and versioning |
| 📈 **Advanced Metrics** | Add conversion rate, customer lifetime value, and retention metrics |
| 🚨 **Alerting** | Add alerts for data-quality or pipeline failures |
| 🧪 **Automated Tests** | Expand integration and end-to-end test coverage |

---

## 🤝 Contributing

Contributions are welcome.

For major changes:

1. Open an issue first.
2. Describe the proposed change.
3. Submit a pull request with the implementation.

---

## 📄 License

This project is licensed under the **MIT License**.

---

## ⭐ Project Summary

**Kafka E-Commerce Real-Time Data Pipeline**

A complete event-driven pipeline demonstrating how e-commerce events can be generated, streamed through Kafka, validated, batch-loaded into Snowflake, transformed into analytical Gold metrics, and queried for business insights.

```text
📤 Python Producer
       ↓
📨 Apache Kafka
       ↓
🔍 Validation & Processing
       ↓
🥈 Snowflake Silver
       ↓
🥇 Snowflake Gold
       ↓
📊 Analytics / Dashboard
```
