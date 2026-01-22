from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Clickstream-Bronze-Ingestion") \
    .getOrCreate()

raw_df = spark.read.parquet("data/raw/clickstream_events.parquet")

raw_df.write \
    .mode("append") \
    .parquet("data/bronze/clickstream_events")

spark.stop()
