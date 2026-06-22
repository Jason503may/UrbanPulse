from pyspark.sql import SparkSession
from pyspark.sql.functions import (
col,
round
)

import time

spark = (
SparkSession.builder
.appName("UrbanPulseForecast")
.getOrCreate()
)

while True:


  try:

    df = spark.read.parquet(
        "data/gold/stress_index"
    )

    forecast = (
        df
        .withColumn(
            "forecast_1h",
            round(
                col("stress_index")
                * 1.05,
                2
            )
        )
        .withColumn(
            "forecast_6h",
            round(
                col("stress_index")
                * 1.10,
                2
            )
        )
    )

    (
        forecast.write
        .mode("overwrite")
        .parquet(
            "data/gold/forecasts"
        )
    )

    print("Forecast updated")

  except Exception as e:

    print(e)

time.sleep(60)


