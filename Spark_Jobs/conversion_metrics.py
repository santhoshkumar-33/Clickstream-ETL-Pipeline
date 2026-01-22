from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, when

spark = SparkSession.builder \
    .appName("Clickstream-Gold-Product-Metrics") \
    .getOrCreate()

df = spark.read.parquet("data/silver/clickstream_events")

product_metrics = df.groupBy("product_id") \
    .agg(
        countDistinct(when(df.event_type == "product_view", df.session_id)).alias("views"),
        countDistinct(when(df.event_type == "add_to_cart", df.session_id)).alias("adds"),
        countDistinct(when(df.event_type == "purchase", df.session_id)).alias("purchases")
    )

product_metrics.write \
    .mode("overwrite") \
    .parquet("data/gold/product_metrics")

spark.stop()
