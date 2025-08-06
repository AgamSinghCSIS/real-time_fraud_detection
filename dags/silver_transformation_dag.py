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
from src.batch.batch_transformation import transform_dims_to_silver

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="transform_to_silver_dag",
    start_date=datetime(2025, 7, 10),
    schedule_interval=timedelta(hours=4),
    catchup=False,
    default_args=default_args,
    tags=['silver']
) as dag:

    wait_for_ingestion_dag = ExternalTaskSensor(
        task_id='wait_for_ingestion_dag',
        external_dag_id='dimstore_to_raw_ingestion_dag',
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

    transform_silver = PythonOperator(
        task_id='transform_to_silver_layer',
        python_callable=transform_dims_to_silver,
        retries=3
    )

    wait_for_ingestion_dag >> transform_silver