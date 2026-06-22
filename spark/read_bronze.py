# spark/read_bronze.py

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadBronze")
    .getOrCreate()
)

df = spark.read.parquet(
    "data/bronze/city_events"
)

df.printSchema()

df.show(
    truncate=False
)


