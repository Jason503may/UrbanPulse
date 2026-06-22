from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckAlerts")
    .getOrCreate()
)

df = spark.read.parquet(
    "data/gold/alerts"
)

df.show(
    truncate=False
)


