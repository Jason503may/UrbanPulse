# spark/test_kafka_connector.py

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaTest")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    )
    .getOrCreate()
)

print("Spark Started")
print(spark.version)

spark.stop()


