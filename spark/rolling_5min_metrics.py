from pyspark.sql import SparkSession
from pyspark.sql.functions import (
avg
)

import time

spark = (
SparkSession.builder
.appName("UrbanPulseRolling")
.getOrCreate()
)

while True:


  try:

    history = spark.read.parquet(
        "data/gold/city_metrics_history"
    )

    rolling = (
        history
        .groupBy("city")
        .agg(
            avg("stress_index")
            .alias("rolling_stress"),
            avg("avg_temperature")
            .alias("rolling_temp"),
            avg("avg_pm25")
            .alias("rolling_pm25"),
            avg("avg_traffic")
            .alias("rolling_traffic")
        )
    )

    (
        rolling.write
        .mode("overwrite")
        .parquet(
            "data/gold/rolling_metrics"
        )
    )

    print("Rolling metrics updated")

  except Exception as e:

    print(e)

time.sleep(60)


