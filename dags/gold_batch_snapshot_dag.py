import os
import sys

print("DAG working directory:", os.getcwd())
print("DAG sys.path:", sys.path)
print("DAG PYTHONPATH env:", os.getenv('PYTHONPATH'))
sys.path.insert(0, '/opt/airflow/')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from datetime import timedelta, datetime
from src.batch.gold_batch import process_gold_tables

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="gold_snapshot_dag",
    start_date=datetime(2025, 7, 10),
    schedule_interval=timedelta(hours=4),
    catchup=False,
    default_args=default_args,
    tags=['gold']
) as dag:

    wait_for_silver_dag = ExternalTaskSensor(
        task_id='wait_for_silver_dag',
        external_dag_id='transform_to_silver_dag',
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode='poke',
        poke_interval=60,
        execution_delta=None,  # Aligns the same execution_date
        timeout=600,
        soft_fail=False,  # if False, this task fails on timeout
        retries=0
    )

    gold_snapshot_task = PythonOperator(
        task_id='gold_snapshot_task',
        python_callable=process_gold_tables,
        retries=3
    )

    wait_for_silver_dag >> gold_snapshot_task