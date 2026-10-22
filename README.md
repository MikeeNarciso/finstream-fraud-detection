# FinStream: Real-Time Fraud Detection Lakehouse

FinStream is a streaming Data Lakehouse architecture designed to ingest mock financial transactions, perform stream-side aggregations to detect fraud velocity, and make the data instantly queryable using standard SQL. 

It utilizes a modern big data stack running locally via Docker while bridging securely to AWS S3 and AWS Glue for storage and metadata.

## 🏗 Architecture & Tech Stack

*   **Message Broker:** Apache Kafka (Internal Docker Network)
*   **Stream Processing:** PySpark Structured Streaming (Apache Spark)
*   **Table Format:** Apache Iceberg (Format V2 - Merge on Read)
*   **Storage & Catalog:** AWS S3 & AWS Glue (Secured via AWS SSO)
*   **Query Engine:** Trino (Distributed SQL Engine)
*   **Infrastructure:** Docker

### Data Flow
1. **Event Generation:** A Python producer sends JSON transaction data to a Kafka topic (`transactions`).
2. **Raw Ingestion (`stream_processor.py`):** Spark consumes Kafka, extracts the payload, and appends raw events to an Iceberg table partitioned by `hours(timestamp)`.
3. **Velocity Aggregation (`fraud_velocity_job.py`):** A secondary Spark stream calculates a 1-hour sliding window (updating every 5 minutes) per user. It uses Iceberg V2's `MERGE INTO` command within a `foreachBatch` loop to perform real-time row-level updates (upserts).
4. **Interactive Analytics:** Trino connects to the AWS Glue catalog, allowing analysts to write SQL against the live, updating Parquet files in S3.

## 📂 Project Structure

```text
finstream-fraud-detection/
├── docker-compose.yml              # Infrastructure (Spark, Kafka, Trino)
├── scripts/
│   ├── run_stream.sh               # Runner for raw data ingestion
│   └── run_velocity.sh             # Runner for fraud velocity aggregations
├── src/
│   ├── checkpoints/                # Spark Structured Streaming state tracking
│   └── streaming/
│       ├── stream_processor.py     # Raw ingestion logic
│       └── fraud_velocity_job.py   # Upsert / Aggregation logic
└── trino/
    └── etc/catalog/
        └── iceberg.properties      # Trino catalog configuration for AWS Glue
```

## 🚀 Prerequisites

1. **Docker & Docker Compose** installed.
2. **AWS CLI v2** installed and configured for AWS SSO (`aws sso login`).
3. An active AWS S3 bucket and AWS Glue database/namespace setup.

## 🛠 Setup & Execution Guide

### 1. Start the Infrastructure
Spin up the local Spark cluster, Kafka broker, and Trino coordinator:
```bash
docker compose up -d
```

### 2. Prepare Kafka
Ensure your Kafka topic exists before starting the Spark streams:
```bash
docker exec -it kafka kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --if-not-exists
```
*(Note: Ensure your Python transaction generator is running and publishing messages to this topic).*

### 3. Start the Spark Streaming Pipelines
Since AWS SSO temporary credentials expire, we use shell wrapper scripts to dynamically fetch and inject them into the Docker containers before triggering `spark-submit`.

**Run the Raw Ingestion Stream:**
```bash
./scripts/run_stream.sh
```

**Run the Fraud Velocity Aggregation Stream:**
```bash
./scripts/run_velocity.sh
```
*Note: The velocity job uses Iceberg Format V2 and requires all Catalyst SQL extensions to be defined directly in the `SparkSession.builder` to support `MERGE INTO` within streaming micro-batches.*

### 4. Query the Live Lakehouse with Trino
Trino also requires active AWS credentials to read from Glue and S3. Use the helper script to restart Trino with your latest SSO session, then enter the CLI:

```bash
# Enter the interactive SQL CLI
docker exec -it finstream-trino trino
```

**Example Trino Queries:**
```sql
-- View raw transactions arriving in real-time
SELECT * FROM iceberg.public.transactions ORDER BY timestamp DESC LIMIT 10;

-- Identify users with high transaction velocity in the current sliding window
SELECT * FROM iceberg.public.user_fraud_velocity ORDER BY total_amount DESC LIMIT 10;
```

## 🧹 Maintenance & Optimization

To prevent the "small file problem" common in streaming lakehouses, periodically run these commands in Trino:

```sql
-- Bin-pack small Parquet files into optimized larger files
ALTER TABLE iceberg.public.transactions EXECUTE OPTIMIZE;

-- Delete old snapshots and orphaned files to save S3 storage costs
CALL iceberg.system.expire_snapshots('public', 'transactions', '7d');
CALL iceberg.system.remove_orphan_files('public', 'transactions');
```