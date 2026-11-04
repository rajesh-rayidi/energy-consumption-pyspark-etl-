

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

def test_missing_power_readings():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when

    spark = SparkSession.builder.master("local[1]").appName("MissingValuesTest").getOrCreate()

    try:
        data = [
            ("2.5",),
            ("?",),
            ("3.0",),
        ]

        df = spark.createDataFrame(data, ["power"])

        cleaned = df.withColumn(
            "power_kw",
            when(col("power") == "?", None)
            .otherwise(col("power").cast("double"))
        )

        assert cleaned.filter(col("power_kw").isNull()).count() == 1
        assert cleaned.filter(col("power_kw").isNotNull()).count() == 2
    finally:
        spark.stop()

def test_daily_energy_aggregation():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as spark_sum

    spark = SparkSession.builder.master("local[1]").appName("DailyEnergyTest").getOrCreate()

    try:
        data = [
            ("2026-01-01", 2.0),
            ("2026-01-01", 3.0),
            ("2026-01-01", 4.0),
        ]

        df = spark.createDataFrame(data, ["date", "power_kw"])

        daily = df.groupBy("date").agg(
            (spark_sum(col("power_kw")) / 60).alias("energy_kwh")
        )

        result = daily.first()

        assert abs(result["energy_kwh"] - 0.15) < 0.000001
    finally:
        spark.stop()

def test_clean_readings_function():
    from src.cleaning import clean_readings

    spark = SparkSession.builder.master("local[1]").getOrCreate()
    try:
        data = [
            ("16/12/2006", "17:24:00", "2.5", "230"),
            ("16/12/2006", "17:24:00", "2.5", "230"),
            ("16/12/2006", "17:25:00", "?", "231"),
        ]
        columns = ["Date", "Time", "Global_active_power", "Voltage"]

        raw = spark.createDataFrame(data, columns)
        clean = clean_readings(raw)

        assert clean.count() == 2
        assert clean.filter("active_power_kw IS NULL").count() == 1
    finally:
        spark.stop()

def test_aggregate_daily_function():
    from src.aggregation import aggregate_daily
    from pyspark.sql import functions as F

    spark = SparkSession.builder.master("local[1]").getOrCreate()

    try:
        data = [
            ("2026-01-01 10:00:00", 2.0, 230.0),
            ("2026-01-01 10:01:00", 4.0, 232.0),
        ]

        df = spark.createDataFrame(
            data,
            ["timestamp_text", "active_power_kw", "voltage_v"]
        )

        df = df.withColumn(
            "reading_ts",
            F.to_timestamp("timestamp_text")
        )

        result = aggregate_daily(df).first()

        assert result["total_readings"] == 2
        assert result["valid_power_readings"] == 2
        assert result["avg_active_power_kw"] == 3.0
        assert result["peak_active_power_kw"] == 4.0
        assert result["avg_voltage_v"] == 231.0
        assert result["missing_power_readings"] == 0
    finally:
        spark.stop()
