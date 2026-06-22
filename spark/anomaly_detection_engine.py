from pyspark.sql import SparkSession
from pyspark.sql.functions import (
avg,
abs,
col
)

import time

spark = (
SparkSession.builder
.appName("UrbanPulseAnomalies")
.getOrCreate()
)

while True:


  try:

    history = spark.read.parquet(
        "data/gold/city_metrics_history"
    )

    baseline = (
        history
        .groupBy("city")
        .agg(
            avg("stress_index")
            .alias("avg_stress")
        )
    )

    latest = spark.read.parquet(
        "data/gold/stress_index"
    )

    anomalies = (
        latest
        .join(
            baseline,
            "city"
        )
        .withColumn(
            "deviation",
            abs(
                col("stress_index")
                -
                col("avg_stress")
            )
        )
        .filter(
            col("deviation") > 10
        )
    )

    (
        anomalies.write
        .mode("overwrite")
        .parquet(
            "data/gold/anomalies"
        )
    )

    print("Anomalies updated")

  except Exception as e:

    print(e)

time.sleep(60)


