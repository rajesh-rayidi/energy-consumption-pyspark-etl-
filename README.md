# Energy Consumption Data Engineering Pipeline

**Status:** Starter project — local PySpark ETL implemented. Airflow, BigQuery, and dashboards are planned, not yet implemented.

This learning project processes household electricity usage measurements using Apache Spark (PySpark), performs timestamp parsing and data-quality checks, and writes daily Parquet summaries and a CSV quality report.

## Dataset

UCI Individual Household Electric Power Consumption, by Georges Hebrail and Alice Berard, CC BY 4.0: https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption

The original dataset is not committed to this repository. Download it and extract `household_power_consumption.txt` into `data/`.

## Windows setup (PowerShell)

Install **Python 3.11** and **Java 17 (JDK)**. Confirm `python --version` and `java -version` work in a new terminal.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

> If PowerShell execution policy blocks activation, run `.\.venv\Scripts\python.exe -m pip install -r requirements.txt`, then use that executable in place of `python` below.

## Test with a tiny included sample

```powershell
Copy-Item data\household_power_consumption_SAMPLE.txt data\household_power_consumption.txt
python src\transform.py
```

You should see a daily summary table and a quality report. Then replace the sample file with the full UCI dataset, and rerun:

```powershell
python src\transform.py
```

## Processing steps

1. Read raw semicolon-separated meter readings.
2. Parse timestamps and cast power/voltage values.
3. Remove duplicate timestamps and reject rows with invalid timestamps.
4. Calculate daily average and peak active power and counts of missing power readings.
5. Save `outputs/daily_energy/` as Parquet and `outputs/quality_report/` as CSV.

## Notes

- The sample in `data/` is only for getting started; measurements are from the first rows of the UCI dataset, shared with attribution.
- This project is about **one household**, not Tesla fleet telemetry.
- Daily average power (kW) is not daily energy (kWh). A future enhancement will compute kWh only after validating coverage and sampling frequency.
- Apache Airflow, GCP/BigQuery and a dashboard are *next milestones*, not claimed features.

## Planned milestones

- [x] Local Spark transformation and data quality summary
- [ ] Add reusable validation tests
- [ ] Add Apache Airflow scheduling
- [ ] Export curated data to BigQuery
- [ ] Create dashboard of usage and missing readings
- [ ] Benchmark and document pipeline runtime
