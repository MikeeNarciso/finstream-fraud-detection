import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER = os.getenv("KAFKA_BROKER_INTERNAL", "kafka:9092")
TOPIC_NAME = os.getenv("TRANSACTIONS_TOPIC", "transactions")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

def main():
    # 1. Initialize SparkSession
    spark = SparkSession.builder \
        .appName("FinStream-Velocity-Aggregations") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.finstream", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.finstream.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.finstream.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.finstream.warehouse", f"s3://{S3_BUCKET}/iceberg_warehouse") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # 2. Pre-create the Aggregation Iceberg Table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS finstream.public.user_fraud_velocity (
            user_id STRING,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            tx_count BIGINT,
            total_amount DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(window_start))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read'
        )
    """)

    # 3. Read Kafka and Parse
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    enriched_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp")))

    # 4. Stream-side Aggregation
    velocity_df = enriched_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            col("user_id"),
            window(col("timestamp"), "1 hour", "5 minutes")
        ) \
        .agg(
            count("transaction_id").alias("tx_count"),
            sum("amount").alias("total_amount")
        ) \
        .select(
            col("user_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("tx_count"),
            col("total_amount")
        )

    # 5. foreachBatch Upsert Logic
    def merge_to_iceberg(micro_batch_df, batch_id):
        micro_batch_df.cache()
        try:
            micro_batch_df.createOrReplaceTempView("batch_updates")
            
            merge_sql = """
            MERGE INTO finstream.public.user_fraud_velocity t
            USING batch_updates s
            ON t.user_id = s.user_id AND t.window_start = s.window_start
            WHEN MATCHED THEN 
                UPDATE SET t.tx_count = s.tx_count, t.total_amount = s.total_amount
            WHEN NOT MATCHED THEN 
                INSERT (user_id, window_start, window_end, tx_count, total_amount)
                VALUES (s.user_id, s.window_start, s.window_end, s.tx_count, s.total_amount)
            """
            micro_batch_df.sparkSession.sql(merge_sql)
            
        finally:
            micro_batch_df.unpersist()

    # 6. Execute Stream
    checkpoint_dir = "/opt/spark/src/checkpoints/velocity_agg"
    
    query = velocity_df.writeStream \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .foreachBatch(merge_to_iceberg) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()