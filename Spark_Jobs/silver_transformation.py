from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder \
    .appName("Clickstream-Silver-Transformation") \
    .getOrCreate()

# schema
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_timestamp", TimestampType(), False)
])

bronze_df = spark.read.schema(schema) \
    .parquet("data/bronze/clickstream_events")

valid_events = ["page_view", "product_view", "add_to_cart", "checkout", "purchase"]

silver_df = bronze_df \
    .filter(col("event_type").isin(valid_events)) \
    .filter(col("event_timestamp").isNotNull()) \
    .dropDuplicates(["event_id"])

silver_df.write \
    .mode("overwrite") \
    .parquet("data/silver/clickstream_events")

spark.stop()
