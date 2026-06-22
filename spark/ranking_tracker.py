from pyspark.sql import SparkSession
from pyspark.sql.functions import (
dense_rank
)
from pyspark.sql.window import Window

import time

spark = (
SparkSession.builder
.appName("UrbanPulseRanking")
.getOrCreate()
)

while True:

  try:

    df = spark.read.parquet(
        "data/gold/stress_index"
    )

    rankings = (
        df
        .withColumn(
            "rank",
            dense_rank().over(
                Window.orderBy(
                    df.stress_index.desc()
                )
            )
        )
    )

    (
        rankings.write
        .mode("overwrite")
        .parquet(
            "data/gold/rankings"
        )
    )

    print("Rankings updated")

  except Exception as e:

    print(e)

time.sleep(60)


