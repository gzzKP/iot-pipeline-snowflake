"""
Spark Structured Streaming Pipeline
====================================
Reads from Kafka → parses + validates → writes to ClickHouse (real-time)
and Snowflake (batch micro-batch every 30 s).

Schema:
  device_id   STRING
  location    STRING
  device_type STRING
  sensor_type STRING
  value       DOUBLE
  unit        STRING
  timestamp   TIMESTAMP
  quality     STRING
  mqtt_topic  STRING
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

## Config
KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC", "iot-sensor-data")

CH_HOST     = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT     = os.getenv("CLICKHOUSE_PORT", "8123")
CH_USER     = os.getenv("CLICKHOUSE_USER", "iot_user")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "iot_pass")
CH_DB       = os.getenv("CLICKHOUSE_DB", "iot")
CH_TABLE    = "sensor_readings"

SF_URL        = os.getenv("SNOWFLAKE_URL", "")
SF_USER       = os.getenv("SNOWFLAKE_USER", "")
SF_PASSWORD   = os.getenv("SNOWFLAKE_PASSWORD", "")
SF_DB         = os.getenv("SNOWFLAKE_DB", "IOT_DB")
SF_SCHEMA     = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SF_WAREHOUSE  = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_TABLE      = "SENSOR_READINGS"

CHECKPOINT_BASE = "/tmp/spark-checkpoints"

## Schema 
SENSOR_SCHEMA = StructType([
    StructField("device_id",   StringType(),    True),
    StructField("location",    StringType(),    True),
    StructField("device_type", StringType(),    True),
    StructField("sensor_type", StringType(),    True),
    StructField("value",       DoubleType(),    True),
    StructField("unit",        StringType(),    True),
    StructField("timestamp",   TimestampType(), True),
    StructField("quality",     StringType(),    True),
    StructField("mqtt_topic",  StringType(),    True),
])

## Spark session
def build_spark() -> SparkSession:
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "com.clickhouse:clickhouse-jdbc:0.6.0",
    ]
    return (
        SparkSession.builder
        .appName("IoT-Pipeline")
        .master("local[*]")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

## Kafka source
def read_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )

## Transformation 
def transform(raw_df):
    parsed = (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("d"))
        .select("d.*")
    )

    # Data quality filter: only "good" readings with plausible values
    clean = (
        parsed
        .filter(F.col("quality") == "good")
        .filter(F.col("value").isNotNull())
        .filter(
            ((F.col("sensor_type") == "temperature") & F.col("value").between(-40, 125)) |
            ((F.col("sensor_type") == "humidity")    & F.col("value").between(0, 100))   |
            ((F.col("sensor_type") == "pressure")    & F.col("value").between(800, 1200)) |
            ((F.col("sensor_type") == "vibration")   & F.col("value").between(0, 50))
        )
        .withColumn("ingest_time", F.current_timestamp())
    )
    return clean

## ClickHouse sink 
def write_clickhouse(batch_df, batch_id: int):
    """Writes each micro-batch to ClickHouse via JDBC."""
    if batch_df.isEmpty():
        return
    (
        batch_df
        .write
        .format("jdbc")
        .option("url", f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}")
        .option("dbtable", CH_TABLE)
        .option("user", CH_USER)
        .option("password", CH_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode("append")
        .save()
    )
    print(f"[spark] ClickHouse batch {batch_id}: {batch_df.count()} rows written")

## Snowflake sink 
def write_snowflake(batch_df, batch_id: int):
    """Writes each micro-batch to Snowflake (skipped if no credentials)."""
    if not SF_URL or not SF_USER:
        return

    sfOptions = {
        "sfURL":       SF_URL,
        "sfUser":      SF_USER,
        "sfPassword":  SF_PASSWORD,
        "sfDatabase":  SF_DB,
        "sfSchema":    SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE,
        "dbtable":     SF_TABLE,
    }
    (
        batch_df
        .write
        .format("net.snowflake.spark.snowflake")
        .options(**sfOptions)
        .mode("append")
        .save()
    )
    print(f"[spark] Snowflake batch {batch_id}: {batch_df.count()} rows written")

## Window aggregations (written to ClickHouse)
def start_aggregation_stream(clean_df, spark: SparkSession):
    """
    5-minute tumbling windows per device+sensor_type.
    Writes aggregated stats alongside raw readings.
    """
    agg = (
        clean_df
        .withWatermark("timestamp", "2 minutes")
        .groupBy(
            F.window("timestamp", "5 minutes"),
            "device_id",
            "sensor_type",
            "location",
        )
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.stddev("value").alias("stddev_value"),
            F.count("*").alias("reading_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "device_id",
            "sensor_type",
            "location",
            F.round("avg_value", 3).alias("avg_value"),
            F.round("min_value", 3).alias("min_value"),
            F.round("max_value", 3).alias("max_value"),
            F.round("stddev_value", 3).alias("stddev_value"),
            "reading_count",
        )
    )

    def write_agg_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        (
            batch_df
            .write
            .format("jdbc")
            .option("url", f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}")
            .option("dbtable", "sensor_aggregations")
            .option("user", CH_USER)
            .option("password", CH_PASSWORD)
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .mode("append")
            .save()
        )

    return (
        agg.writeStream
        .foreachBatch(write_agg_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/aggregations")
        .trigger(processingTime="30 seconds")
        .start()
    )

## Entry point
def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("[spark] Session started. Waiting for Kafka messages…")

    raw_df   = read_kafka(spark)
    clean_df = transform(raw_df)

    # Stream 1: raw readings → ClickHouse + Snowflake
    raw_query = (
        clean_df.writeStream
        .foreachBatch(lambda df, bid: (write_clickhouse(df, bid), write_snowflake(df, bid)))
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Stream 2: window aggregations → ClickHouse
    agg_query = start_aggregation_stream(clean_df, spark)

    print("[spark] Both streaming queries started.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
