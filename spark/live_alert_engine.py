from pyspark.sql import SparkSession
from pyspark.sql.functions import (
col,
current_timestamp,
lit
)

import time

spark = (
SparkSession.builder
.appName("UrbanPulseAlerts")
.getOrCreate()
)

while True:


  try:

    df = spark.read.parquet(
        "data/gold/stress_index"
    )

    alerts = (
        df
        .filter(
            (col("avg_pm25") > 35)
            |
            (col("avg_traffic") > 80)
            |
            (col("stress_index") > 60)
        )
        .withColumn(
            "generated_at",
            current_timestamp()
        )
    )

    (
        alerts.write
        .mode("overwrite")
        .parquet(
            "data/gold/alerts"
        )
    )

    print("Alerts updated")

  except Exception as e:

    print(e)

time.sleep(60)



