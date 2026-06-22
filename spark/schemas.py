from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    MapType
)

urbanpulse_schema = StructType([

    StructField(
        "event_id",
        StringType(),
        True
    ),

    StructField(
        "event_type",
        StringType(),
        True
    ),

    StructField(
        "event_timestamp",
        StringType(),
        True
    ),

    StructField(
        "city",
        StringType(),
        True
    ),

    StructField(
        "source",
        StringType(),
        True
    ),

    StructField(
        "location",
        StructType([
            StructField(
                "latitude",
                DoubleType(),
                True
            ),
            StructField(
                "longitude",
                DoubleType(),
                True
            )
        ]),
        True
    ),

    StructField(
        "payload",
        MapType(
            StringType(),
            StringType()
        ),
        True
    )
])


