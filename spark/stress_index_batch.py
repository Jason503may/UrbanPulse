from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    round
)

spark = (
    SparkSession.builder
    .appName("StressIndexBatch")
    .getOrCreate()
)

weather_df = spark.read.parquet(
    "data/silver/weather"
)

air_df = spark.read.parquet(
    "data/silver/air_quality"
)

traffic_df = spark.read.parquet(
    "data/silver/traffic"
)

weather_avg = (
    weather_df
    .groupBy("city")
    .agg(
        avg("temperature")
        .alias("avg_temperature")
    )
)

air_avg = (
    air_df
    .groupBy("city")
    .agg(
        avg("pm25")
        .alias("avg_pm25")
    )
)

traffic_avg = (
    traffic_df
    .groupBy("city")
    .agg(
        avg("traffic_density")
        .alias("avg_traffic_density")
    )
)

stress_df = (
    weather_avg
    .join(
        air_avg,
        "city"
    )
    .join(
        traffic_avg,
        "city"
    )
    .withColumn(
        "stress_index",
        round(
            (
                col("avg_temperature") * 0.3
                +
                col("avg_pm25") * 0.4
                +
                col("avg_traffic_density") * 0.3
            ),
            2
        )
    )
)

stress_df.write.mode(
    "overwrite"
).parquet(
    "data/gold/stress_index"
)




