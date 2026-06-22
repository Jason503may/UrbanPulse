from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    round
)

import time
import os

spark = (
    SparkSession.builder
    .appName("UrbanPulseLiveGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

CITY_METRICS_PATH = "data/gold/city_metrics"
STRESS_PATH = "data/gold/stress_index"
HISTORY_PATH = "data/gold/city_metrics_history"

while True:

    try:

        weather = spark.read.parquet(
            "data/silver/weather"
        )

        air = spark.read.parquet(
            "data/silver/air_quality"
        )

        traffic = spark.read.parquet(
            "data/silver/traffic"
        )

        if (
            weather.count() == 0
            or air.count() == 0
            or traffic.count() == 0
        ):
            print(
                "Waiting for silver data..."
            )
            time.sleep(30)
            continue

        weather_avg = (
            weather
            .groupBy("city")
            .agg(
                avg("temperature")
                .alias("avg_temperature")
            )
        )

        air_avg = (
            air
            .groupBy("city")
            .agg(
                avg("pm25")
                .alias("avg_pm25")
            )
        )

        traffic_avg = (
            traffic
            .groupBy("city")
            .agg(
                avg("traffic_density")
                .alias("avg_traffic")
            )
        )

        city_metrics = (
            weather_avg
            .join(
                air_avg,
                "city"
            )
            .join(
                traffic_avg,
                "city"
            )
        )

        city_metrics = city_metrics.select(
            "city",
            round(
                col("avg_temperature"),
                2
            ).alias(
                "avg_temperature"
            ),
            round(
                col("avg_pm25"),
                2
            ).alias(
                "avg_pm25"
            ),
            round(
                col("avg_traffic"),
                2
            ).alias(
                "avg_traffic"
            )
        )

        (
            city_metrics
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(
                CITY_METRICS_PATH
            )
        )

        stress_df = (
            city_metrics
            .withColumn(
                "stress_index",
                round(
                    (
                        col("avg_temperature")
                        * 0.3
                        +
                        col("avg_pm25")
                        * 0.4
                        +
                        col("avg_traffic")
                        * 0.3
                    ),
                    2
                )
            )
        )

        (
            stress_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(
                STRESS_PATH
            )
        )

        history_df = (
            stress_df
            .withColumn(
                "timestamp",
                current_timestamp()
            )
        )

        if not os.path.exists(
            HISTORY_PATH
        ):

            (
                history_df
                .coalesce(1)
                .write
                .mode("overwrite")
                .parquet(
                    HISTORY_PATH
                )
            )

        else:

            (
                history_df
                .coalesce(1)
                .write
                .mode("append")
                .parquet(
                    HISTORY_PATH
                )
            )

        print(
            f"Gold Updated | Cities={city_metrics.count()}"
        )

    except Exception as e:

        print(
            f"Gold Engine Error: {e}"
        )

    time.sleep(60)

