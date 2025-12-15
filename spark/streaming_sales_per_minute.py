import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as Fsum, count as Fcount

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:8020/tmp/ecommerce")

spark = (
    SparkSession.builder
    .appName("StreamingSalesPerMinute")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Lee SOLO el topic de órdenes en streaming
raw_orders = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
)

# Parseo simple (ya sabes que el JSON es válido)
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("category", StringType()),
    StructField("event_type", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", StringType()),
])

orders = (
    raw_orders
    .select(from_json(col("value").cast("string"), schema).alias("e"))
    .select(
        col("e.price").alias("price"),
        col("e.timestamp").alias("timestamp")
    )
    .withColumn("event_ts", col("timestamp").cast("timestamp"))
)

# Ventana de 1 minuto (STREAMING)
sales_per_minute = (
    orders
    .withWatermark("event_ts", "2 minutes")
    .groupBy(
        window(col("event_ts"), "1 minute")
    )
    .agg(
        Fsum("price").alias("total_sales"),
        Fcount("*").alias("orders_count")
    )
    .select(
        col("window.start").alias("minute_start"),
        col("window.end").alias("minute_end"),
        col("total_sales"),
        col("orders_count")
    )
)

query = (
    sales_per_minute.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", f"{HDFS_BASE}/metrics/streaming_sales_per_minute")
    .option("checkpointLocation", f"{HDFS_BASE}/_checkpoints/streaming_sales_per_minute")
    .start()
)

query.awaitTermination()


