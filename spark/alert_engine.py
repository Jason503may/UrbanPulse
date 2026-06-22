from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    current_timestamp
)

spark = (
    SparkSession.builder
    .appName("UrbanPulseAlertEngine")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------------
# Load Stress Index
# -----------------------------------

df = spark.read.parquet(
    "data/gold/stress_index"
)

# -----------------------------------
# Alert Logic
# -----------------------------------

alerts = (
    df
    .withColumn(
        "alert_level",
        when(
            col("stress_index") > 70,
            "HIGH"
        )
        .when(
            col("stress_index") > 40,
            "MEDIUM"
        )
        .otherwise(
            "LOW"
        )
    )
    .withColumn(
        "generated_at",
        current_timestamp()
    )
)

# -----------------------------------
# View Alerts
# -----------------------------------

alerts.show(
    truncate=False
)

# -----------------------------------
# Save
# -----------------------------------

(
    alerts.write
    .mode("overwrite")
    .parquet(
        "data/gold/alerts"
    )
)

print(
    "Alert layer created successfully"
)


