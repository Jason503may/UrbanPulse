from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckSilver")
    .getOrCreate()
)

tables = [
    "weather",
    "air_quality",
    "traffic"
]

for table in tables:

    print("\n" + "=" * 50)
    print(table.upper())
    print("=" * 50)

    df = spark.read.parquet(
        f"data/silver/{table}"
    )

    print(
        f"Rows: {df.count()}"
    )

    df.show(
        5,
        truncate=False
    )


