from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

spark = (
    SparkSession.builder
    .appName("StressIndex")
    .getOrCreate()
)

df = spark.read.parquet(
    "data/gold/city_metrics"
)

stress_df = (
    df
    .withColumn(
        "stress_index",
        round(
            (
                (col("avg_temperature") * 0.3)
                +
                (col("avg_pm25") * 0.4)
                +
                (col("avg_traffic") * 0.3)
            ),
            2
        )
    )
)

stress_df.show()

(
    stress_df.write
    .mode("overwrite")
    .parquet(
        "data/gold/stress_index"
    )
)

print("Stress index generated")

