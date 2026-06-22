from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckStress")
    .getOrCreate()
)

df = spark.read.parquet(
    "data/gold/stress_index"
)

df.show(
    truncate=False
)


