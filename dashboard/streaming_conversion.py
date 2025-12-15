from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ConversionMetrics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_type", StringType()),
    StructField("timestamp", StringType())
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:19092")
       .option("subscribe", "page_views,orders")
       .load())

parsed = (raw.selectExpr("CAST(value AS STRING) AS json")
          .select(from_json(col("json"), schema).alias("e"))
          .select("e.*")
          .withColumn("event_time", col("timestamp").cast("timestamp"))
)

conv = (parsed
        .groupBy(window(col("event_time"), "1 minute"), col("event_type"))
        .agg(count("*").alias("count"))
        .withColumn("minute_start", col("window.start"))
        .drop("window")
)

(conv.writeStream
 .format("parquet")
 .outputMode("append")
 .option("path", "hdfs://namenode:8020/tmp/ecommerce/metrics/conversion")
 .option("checkpointLocation", "hdfs://namenode:8020/tmp/ecommerce/_checkpoints/conversion")
 .trigger(processingTime="1 minute")
 .start()
 .awaitTermination())
