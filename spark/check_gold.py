from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckGold")
    .getOrCreate()
)

df = spark.read.parquet(
    "data/gold/city_metrics"
)

df.show(
    truncate=False
)


