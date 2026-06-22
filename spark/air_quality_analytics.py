from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    avg
)

from spark.schemas import urbanpulse_schema

spark = (
    SparkSession.builder
    .appName("AirQualityAnalytics")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:29092"
    )
    .option(
        "subscribe",
        "city_events"
    )
    .load()
)

json_df = raw_df.selectExpr(
    "CAST(value AS STRING) as json_event"
)

events = (
    json_df
    .select(
        from_json(
            col("json_event"),
            urbanpulse_schema
        ).alias("event")
    )
    .select("event.*")
)

air_quality = (
    events
    .filter(
        col("event_type") == "air_quality"
    )
)

analytics = (
    air_quality
    .groupBy("city")
    .agg(
        avg(
            col("payload.pm25").cast("double")
        ).alias("avg_pm25"),

        avg(
            col("payload.pm10").cast("double")
        ).alias("avg_pm10"),

        avg(
            col("payload.nitrogen_dioxide").cast("double")
        ).alias("avg_no2")
    )
)

query = (
    analytics.writeStream
    .outputMode("complete")
    .format("console")
    .start()
)

query.awaitTermination()


