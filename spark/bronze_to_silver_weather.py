from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("WeatherSilver")
    .getOrCreate()
)

bronze_df = spark.read.parquet(
    "data/bronze/city_events"
)

weather_df = (
    bronze_df
    .filter(
        col("event_type") == "weather"
    )
    .select(
        col("event_id"),
        col("event_timestamp"),
        col("city"),
        col("source"),

        col("location.latitude").alias(
            "latitude"
        ),

        col("location.longitude").alias(
            "longitude"
        ),

        col("payload.temperature")
            .cast("double")
            .alias("temperature"),

        col("payload.humidity")
            .cast("double")
            .alias("humidity"),

        col("payload.wind_speed")
            .cast("double")
            .alias("wind_speed")
    )
)

weather_df.printSchema()

weather_df.write.mode(
    "overwrite"
).parquet(
    "data/silver/weather"
)

print(
    f"Saved {weather_df.count()} rows"
)


