import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# 1. Fetch environment variables injected by Docker
KAFKA_BROKER = os.getenv("KAFKA_BROKER_INTERNAL")
TOPIC_NAME = os.getenv("TRANSACTIONS_TOPIC")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

def main():
    # 2. Initialize SparkSession
    spark = SparkSession.builder \
        .appName("FinStream-Iceberg-Lakehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.finstream", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.finstream.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.finstream.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.finstream.warehouse", f"s3://{S3_BUCKET}/iceberg_warehouse") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Ensure the database namespace exists in AWS Glue
    spark.sql("CREATE NAMESPACE IF NOT EXISTS finstream.public")
              
    # 3. Pre-create the Iceberg Table for the streaming job
    spark.sql("""
        CREATE TABLE IF NOT EXISTS finstream.public.transactions (
            transaction_id STRING,
            user_id STRING,
            merchant_id STRING,
            amount DOUBLE,
            timestamp TIMESTAMP,
            lat DOUBLE,
            lon DOUBLE,
            is_flagged BOOLEAN
        )
        USING iceberg
        PARTITIONED BY (days(timestamp))
        TBLPROPERTIES (
        'format-version' = '2',
        'write.merge.mode' = 'merge-on-read'
        )""")

    # 3. Define the Schema
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True), 
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("is_flagged", BooleanType(), True)
    ])

    # 4. Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 5. Parse and Enrich
    parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    enriched_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp")))

    print(f"Starting stream... Writing Iceberg table to s3://{S3_BUCKET}/iceberg_warehouse/public/transactions")

    # 6. Write continuously directly to the Iceberg format!
    checkpoint_dir = "/opt/spark/src/checkpoints/iceberg_transactions"

    query = enriched_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", checkpoint_dir) \
        .toTable("finstream.public.transactions")

    query.awaitTermination()

if __name__ == "__main__":
    main()