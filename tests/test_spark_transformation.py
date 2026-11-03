

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
