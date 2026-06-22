from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("TrafficSilver")
    .getOrCreate()
)

bronze_df = spark.read.parquet(
    "data/bronze/city_events"
)

traffic_df = (
    bronze_df
    .filter(
        col("event_type") == "traffic"
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

        col("payload.traffic_density")
            .cast("double")
            .alias("traffic_density")
    )
)

traffic_df.write.mode(
    "overwrite"
).parquet(
    "data/silver/traffic"
)

print(
    f"Saved {traffic_df.count()} rows"
)


