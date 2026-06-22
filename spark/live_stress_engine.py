import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

spark = (
    SparkSession.builder
    .appName("LiveStress")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

while True:

    try:

        df = spark.read.parquet(
            "data/gold/city_metrics"
        )

        stress = (
            df
            .withColumn(
                "stress_index",
                round(
                    (
                        col("avg_temperature") * 0.3
                        +
                        col("avg_pm25") * 0.4
                        +
                        col("avg_traffic") * 0.3
                    ),
                    2
                )
            )
        )

        (
            stress.write
            .mode("overwrite")
            .parquet(
                "data/gold/stress_index"
            )
        )

        print("Stress updated")

    except Exception as e:

        print(e)

    time.sleep(30)



