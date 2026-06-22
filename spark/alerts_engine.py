from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when,
    current_timestamp
)

spark = (
    SparkSession.builder
    .appName("Alerts")
    .getOrCreate()
)

stress = spark.read.parquet(
    "data/gold/stress_index"
)

alerts = (
    stress
    .withColumn(
        "alert",
        when(
            stress.stress_index > 50,
            "HIGH"
        ).otherwise(
            "NORMAL"
        )
    )
    .withColumn(
        "generated_at",
        current_timestamp()
    )
)

alerts.show()

(
    alerts.write
    .mode("overwrite")
    .parquet(
        "data/gold/alerts"
    )
)

print("Alerts generated")

