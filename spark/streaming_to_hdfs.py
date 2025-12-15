import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")  # usa "earliest" si quieres probar desde cero
HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:8020/tmp/ecommerce")

spark = (
    SparkSession.builder
    .appName("EcommerceKafkaToHDFS")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("province", StringType(), True),
    StructField("location", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ]), True),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "page_views,cart_events,orders")
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw.select(
        col("topic").cast("string").alias("topic"),
        col("timestamp").alias("kafka_ts"),
        col("value").cast("string").alias("json_str"),
    )
    .select(
        col("topic"),
        from_json(col("json_str"), schema).alias("e")
    )
    .select("topic", "e.*")
    # timestamp viene ISO-8601 con zona (+00:00); Spark lo parsea bien con to_timestamp
    .withColumn("event_ts", to_timestamp(col("timestamp")))
    .withColumn("fecha", to_date(col("event_ts")))
)

def start_sink(df, topic_name: str, out_dir: str):
    topic_df = df.filter(col("topic") == topic_name)

    return (
        topic_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{HDFS_BASE}/{out_dir}")
        .option("checkpointLocation", f"{HDFS_BASE}/_checkpoints/{out_dir}")
        .partitionBy("fecha", "category")
        .start()
    )

q_views = start_sink(parsed, "page_views", "views")
q_cart  = start_sink(parsed, "cart_events", "cart")
q_order = start_sink(parsed, "orders", "orders")

spark.streams.awaitAnyTermination()
