import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as Fsum, count as Fcount

HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:8020/tmp/ecommerce")
ORDERS_PATH = f"{HDFS_BASE}/orders"
OUT_PATH = f"{HDFS_BASE}/metrics/sales_per_minute"

spark = (
    SparkSession.builder
    .appName("EcommerceSalesPerMinute")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Lee órdenes ya guardadas en HDFS (Parquet)
orders = spark.read.parquet(ORDERS_PATH)

# Asegura que event_ts exista y sea timestamp (tu streaming_to_hdfs.py ya lo crea)
if "event_ts" not in orders.columns:
    raise RuntimeError("No existe la columna 'event_ts' en orders. Revisa streaming_to_hdfs.py.")

# Ventana de 1 minuto: total ventas + número de órdenes
sales_per_min = (
    orders
    .groupBy(
        col("fecha"),
        window(col("event_ts"), "1 minute").alias("w")
    )
    .agg(
        Fsum(col("price")).alias("total_sales"),
        Fcount("*").alias("orders_count")
    )
    .select(
        col("fecha"),
        col("w.start").alias("minute_start"),
        col("w.end").alias("minute_end"),
        col("total_sales"),
        col("orders_count")
    )
    .orderBy(col("minute_start").asc())
)

# Guarda resultados en HDFS
(
    sales_per_min.write
    .mode("overwrite")
    .partitionBy("fecha")
    .parquet(OUT_PATH)
)

print(f"OK -> Written metrics to: {OUT_PATH}")

spark.stop()

