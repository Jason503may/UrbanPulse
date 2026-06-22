from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("AirSilver")
    .getOrCreate()
)

bronze_df = spark.read.parquet(
    "data/bronze/city_events"
)

air_df = (
    bronze_df
    .filter(
        col("event_type") == "air_quality"
    )
    .select(
        col("event_id"),
        col("event_timestamp"),
        col("city"),

        col("location.latitude").alias(
            "latitude"
        ),

        col("location.longitude").alias(
            "longitude"
        ),

        col("payload.pm25")
            .cast("double")
            .alias("pm25"),

        col("payload.pm10")
            .cast("double")
            .alias("pm10"),

        col("payload.carbon_monoxide")
            .cast("double")
            .alias("carbon_monoxide"),

        col("payload.nitrogen_dioxide")
            .cast("double")
            .alias("nitrogen_dioxide")
    )
)

air_df.write.mode(
    "overwrite"
).parquet(
    "data/silver/air_quality"
)

print(
    f"Saved {air_df.count()} rows"
)


