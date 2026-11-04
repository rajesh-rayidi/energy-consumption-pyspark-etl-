
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="energy_consumption_etl",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Los_Angeles"),
    schedule="0 8 * * *",
    catchup=False,
    tags=["energy", "pyspark"],
) as dag:
    run_etl = BashOperator(
        task_id="run_pyspark_etl",
        retries = 2,
        retry_delay = pendulum.duration(minutes=5),
        execution_timeout = pendulum.duration(minutes=15),
        bash_command=(
            'PROJECT_DIR="${ENERGY_PROJECT_DIR:-$HOME/projects/energy-consumption-pipeline}"; '
            'export PYSPARK_PYTHON="$PROJECT_DIR/.venv/bin/python"; '
            '"$PROJECT_DIR/.venv/bin/python" "$PROJECT_DIR/src/transform.py"'
        ),
    )



    upload_gcs = BashOperator(
        task_id="upload_to_gcs",
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
        bash_command=(
            'PROJECT_DIR="${ENERGY_PROJECT_DIR:-$HOME/projects/energy-consumption-pipeline}"; '
            'gcloud storage cp "$PROJECT_DIR"/outputs/daily_energy/part-*.parquet '
            'gs://energy-etl-rajesh-2026/latest_energy/current.parquet'
        ),
    )

    load_bigquery = BashOperator(
        task_id="load_to_bigquery",
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
        bash_command=(
            "bq load --replace "
            "--source_format=PARQUET "
            "--location=us-west1 "
            "energy_analytics.daily_energy_consumption "
            "gs://energy-etl-rajesh-2026/latest_energy/current.parquet"
        ),
    )

    run_etl >> upload_gcs >> load_bigquery
