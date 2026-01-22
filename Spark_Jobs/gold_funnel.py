from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

spark = SparkSession.builder \
    .appName("Clickstream-Gold-Funnel") \
    .getOrCreate()

df = spark.read.parquet("data/silver/clickstream_events")

funnel_df = df.groupBy("event_type") \
    .agg(countDistinct("session_id").alias("sessions"))

funnel_df.write \
    .mode("overwrite") \
    .parquet("data/gold/funnel_metrics")

spark.stop()
