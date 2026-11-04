
from pyspark.sql import functions as F

def clean_readings(raw):
    return (
        raw
        .withColumn(
            "reading_ts",
            F.to_timestamp(
                F.concat_ws(" ", F.col("Date"), F.col("Time")),
                "d/M/yyyy H:mm:ss"
            )
        )
        .withColumn(
            "active_power_kw",
            F.col("Global_active_power").cast("double")
        )
        .withColumn(
            "voltage_v",
            F.col("Voltage").cast("double")
        )
        .filter(F.col("reading_ts").isNotNull())
        .dropDuplicates(["reading_ts"])
    )
