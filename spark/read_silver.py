# spark/read_silver.py

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadSilver")
    .getOrCreate()
)

df = spark.read.parquet(
    "data/silver/weather"
)

df.printSchema()

df.show(
    truncate=False
)


