from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceBasicKafkaReader")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap = "kafka:19092"  # ← INTERNAL listener

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", "page_views")
    .option("startingOffsets", "latest")
    .load()
)

events_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

query = (
    events_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()

