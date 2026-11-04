

from pyspark.sql import SparkSession

def test_duplicate_removal():
    spark = SparkSession.builder.master("local[1]").appName("EnergyETLTest").getOrCreate()

    try:
        data = [
            ("2026-01-01 10:00:00", 2.5),
            ("2026-01-01 10:00:00", 2.5),
            ("2026-01-01 11:00:00", 3.0),
        ]

        df = spark.createDataFrame(data, ["reading_ts", "active_power_kw"])
        cleaned = df.dropDuplicates(["reading_ts"])

        assert cleaned.count() == 2
    finally:
        spark.stop() 

def test_timestamp_parsing():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_timestamp, col, concat_ws

    spark = SparkSession.builder.master("local[1]").appName("TimestampTest").getOrCreate()

    try:
        data = [("16/12/2006", "17:24:00")]
        df = spark.createDataFrame(data, ["Date", "Time"])

        result = df.withColumn(
            "timestamp",
            to_timestamp(
                concat_ws(" ", col("Date"), col("Time")),  
                "d/M/yyyy H:mm:ss"
            )
        )

        assert result.filter(col("timestamp").isNotNull()).count() == 1
    finally:
        spark.stop()
