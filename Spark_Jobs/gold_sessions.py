from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, count, unix_timestamp
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Clickstream-Gold-Sessions") \
    .getOrCreate()

silver_df = spark.read.parquet("data/silver/clickstream_events")

session_df = silver_df.groupBy("session_id", "user_id") \
    .agg(
        min("event_timestamp").alias("session_start"),
        max("event_timestamp").alias("session_end"),
        count("*").alias("events_per_session")
    ) \
    .withColumn(
        "session_duration_sec",
        unix_timestamp("session_end") - unix_timestamp("session_start")
    )

session_df.write \
    .mode("overwrite") \
    .parquet("data/gold/session_metrics")

spark.stop()
