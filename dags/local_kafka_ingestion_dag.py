import os
import sys

print("DAG working directory:", os.getcwd())
print("DAG sys.path:", sys.path)
print("DAG PYTHONPATH env:", os.getenv('PYTHONPATH'))
sys.path.insert(0, '/opt/airflow/')

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='restart_streaming_job',
    start_date=datetime(2025,6,27),
    catchup=False
) as dag:

    run_streaming_job = BashOperator(
        task_id='run_local_spark_ingestion',
        bash_command="cd /opt/airflow && make run_stream",
        dag=dag
    )