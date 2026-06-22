from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = (
    SparkSession.builder
    .appName("UrbanPulseGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

weather = spark.read.parquet(
    "data/silver/weather"
)

air = spark.read.parquet(
    "data/silver/air_quality"
)

traffic = spark.read.parquet(
    "data/silver/traffic"
)

weather_metrics = (
    weather
    .groupBy("city")
    .agg(
        avg("temperature")
        .alias("avg_temperature")
    )
)

air_metrics = (
    air
    .groupBy("city")
    .agg(
        avg("pm25")
        .alias("avg_pm25")
    )
)

traffic_metrics = (
    traffic
    .groupBy("city")
    .agg(
        avg("traffic_density")
        .alias("avg_traffic")
    )
)

gold = (
    weather_metrics
    .join(
        air_metrics,
        "city",
        "outer"
    )
    .join(
        traffic_metrics,
        "city",
        "outer"
    )
)

gold.show(
    100,
    truncate=False
)

gold.write.mode(
    "overwrite"
).parquet(
    "data/gold/city_metrics"
)

print(
    f"Saved {gold.count()} cities"
)

