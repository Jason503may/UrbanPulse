from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    avg
)

from spark.schemas import urbanpulse_schema

spark = (
    SparkSession.builder
    .appName("TrafficAnalytics")
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

traffic = (
    events
    .filter(
        col("event_type") == "traffic"
    )
)

analytics = (
    traffic
    .groupBy("city")
    .agg(
        avg(
            col("payload.traffic_density")
            .cast("double")
        ).alias("avg_traffic_density")
    )
)

query = (
    analytics.writeStream
    .outputMode("complete")
    .format("console")
    .start()
)

query.awaitTermination()


