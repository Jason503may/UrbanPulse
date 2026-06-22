from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    avg
)

from spark.schemas import urbanpulse_schema

spark = (
    SparkSession.builder
    .appName("StressIndexAnalytics")
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
    .select(
        "city",
        col("payload.temperature")
        .cast("double")
        .alias("temperature")
    )
)

air = (
    events
    .filter(
        col("event_type") == "air_quality"
    )
    .select(
        "city",
        col("payload.pm25")
        .cast("double")
        .alias("pm25")
    )
)

traffic = (
    events
    .filter(
        col("event_type") == "traffic"
    )
    .select(
        "city",
        col("payload.traffic_density")
        .cast("double")
        .alias("traffic_density")
    )
)

weather_avg = (
    weather.groupBy("city")
    .agg(
        avg("temperature")
        .alias("avg_temperature")
    )
)

air_avg = (
    air.groupBy("city")
    .agg(
        avg("pm25")
        .alias("avg_pm25")
    )
)

traffic_avg = (
    traffic.groupBy("city")
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
        (
            col("avg_temperature") * 0.3
            +
            col("avg_pm25") * 0.4
            +
            col("avg_traffic_density") * 0.3
        )
    )
)

query = (
    stress_df.writeStream
    .outputMode("complete")
    .format("console")
    .start()
)

query.awaitTermination()


