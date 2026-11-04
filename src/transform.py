"""Local PySpark pipeline for UCI household electricity measurements.

Input: data/household_power_consumption.txt (semicolon-delimited)
Outputs: outputs/daily_energy/ (Parquet), outputs/quality_report/ (CSV)
"""
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from cleaning import clean_readings
from aggregation import aggregate_daily

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / 'data' / 'household_power_consumption.txt'
OUTPUT = ROOT / 'outputs'

def main():
    if not INPUT.exists():
        raise FileNotFoundError(f"Missing {INPUT}. Download the UCI dataset and place the extracted .txt file in data/.")

    spark = (SparkSession.builder.master('local[*]')
             .appName('EnergyConsumptionETL').getOrCreate())
    try:
        spark.sparkContext.setLogLevel('WARN')
        raw = spark.read.option('header', True).option('sep', ';').csv(str(INPUT))
        clean = (raw
            .withColumn('reading_ts', F.to_timestamp(F.concat_ws(' ', F.col('Date'), F.col('Time')), 'd/M/yyyy H:mm:ss'))
            .withColumn('active_power_kw', F.col('Global_active_power').cast('double'))
            .withColumn('voltage_v', F.col('Voltage').cast('double'))
            .filter(F.col('reading_ts').isNotNull())
            .dropDuplicates(['reading_ts']))

        daily = aggregate_daily(clean)
        quality = (clean.agg(
            F.count('*').alias('total_rows_after_dedup'),
            F.count('active_power_kw').alias('rows_with_valid_power'),
            F.sum(F.when(F.col('active_power_kw').isNull(), 1).otherwise(0)).alias('rows_missing_power')))

        (daily.write.mode('overwrite').parquet(str(OUTPUT / 'daily_energy')))
        (quality.coalesce(1).write.mode('overwrite').option('header', True).csv(str(OUTPUT / 'quality_report')))
        daily.show(10, truncate=False)
        quality.show(truncate=False)
        print('Saved Parquet daily summaries and CSV quality report under outputs/')
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
