
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="energy_consumption_etl",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["energy", "pyspark"],
) as dag:
    run_etl = BashOperator(
        task_id="run_pyspark_etl",
        bash_command=(
            "export PYSPARK_PYTHON=/home/trive/projects/energy-consumption-pipeline/.venv/bin/python; "
            "/home/trive/projects/energy-consumption-pipeline/.venv/bin/python "
            "/home/trive/projects/energy-consumption-pipeline/src/transform.py"
        ),
    )
