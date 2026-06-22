from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    avg,
    to_timestamp
)

from spark.schemas import urbanpulse_schema

spark = (
    SparkSession.builder
    .appName("WeatherWindowAnalytics")
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

weather = (
    events
    .filter(
        col("event_type") == "weather"
    )
)

weather = (
    weather
    .withColumn(
        "event_time",
        to_timestamp(
            col("event_timestamp")
        )
    )
)

windowed = (
    weather
    .withWatermark(
        "event_time",
        "2 minutes"
    )
    .groupBy(
        window(
            col("event_time"),
            "5 minutes"
        ),
        col("city")
    )
    .agg(
        avg(
            col("payload.temperature")
            .cast("double")
        ).alias("avg_temperature")
    )
)

query = (
    windowed.writeStream
    .outputMode("update")
    .format("console")
    .option(
        "truncate",
        False
    )
    .start()
)

query.awaitTermination()



