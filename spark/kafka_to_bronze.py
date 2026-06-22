from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json
)

from spark.schemas import urbanpulse_schema


spark = (
    SparkSession.builder
    .appName("UrbanPulseBronze")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
            "org.apache.kafka:kafka-clients:3.7.1"
        ])
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
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
    .option(
        "startingOffsets",
        "earliest"
    )
    .option(
        "failOnDataLoss",
        "false"
    )
    .load()
)

raw_events = df.selectExpr(
    "CAST(value AS STRING) as json_event"
)

parsed_events = (
    raw_events
    .select(
        from_json(
            col("json_event"),
            urbanpulse_schema
        ).alias("event")
    )
    .select("event.*")
)

query = (
    parsed_events.writeStream
    .format("parquet")
    .option(
        "path",
        "data/bronze/city_events"
    )
    .option(
        "checkpointLocation",
        "data/checkpoints/bronze_v2"
    )
    .outputMode("append")
    .start()
)

query.awaitTermination()


