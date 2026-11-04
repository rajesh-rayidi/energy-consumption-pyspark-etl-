
from pyspark.sql import functions as F


def aggregate_daily(clean):
    return (
        clean.withColumn("date", F.to_date("reading_ts"))
        .groupBy("date")
        .agg(
            F.count("*").alias("total_readings"),
            F.count("active_power_kw").alias("valid_power_readings"),
            F.avg("active_power_kw").alias("avg_active_power_kw"),
            F.max("active_power_kw").alias("peak_active_power_kw"),
            F.avg("voltage_v").alias("avg_voltage_v"),
        )
        .withColumn(
            "missing_power_readings",
            F.col("total_readings") - F.col("valid_power_readings"),
        )
        .orderBy("date")
    )
