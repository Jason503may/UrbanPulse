import time
import pandas as pd

from datetime import datetime

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CityMetricsHistory")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

OUTPUT_PATH = "data/gold/city_metrics_history"

while True:

    try:

        df = spark.read.parquet(
            "data/gold/city_metrics"
        )

        pdf = df.toPandas()

        pdf["timestamp"] = datetime.now()

        try:

            old = pd.read_parquet(
                OUTPUT_PATH
            )

            pdf = pd.concat(
                [old, pdf],
                ignore_index=True
            )

        except:

            pass

        pdf.to_parquet(
            OUTPUT_PATH,
            index=False
        )

        print(
            f"History updated "
            f"{datetime.now()}"
        )

    except Exception as e:

        print(e)

    time.sleep(30)

