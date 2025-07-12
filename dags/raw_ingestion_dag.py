import os
import sys

print("DAG working directory:", os.getcwd())
print("DAG sys.path:", sys.path)
print("DAG PYTHONPATH env:", os.getenv('PYTHONPATH'))
sys.path.insert(0, '/opt/airflow/')


#os.environ["LOGGER_NAME"] = 'BATCH_LOGGER'

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
#from src.common.logger import get_logger
from src.batch.batch_ingestion import ingest_dim_store



with DAG(
    dag_id="dimstore_to_raw_ingestion_dag",
    start_date=datetime(2025,6,27),
    schedule_interval=timedelta(hours=4),
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="run_ingestion_logic",
        python_callable=ingest_dim_store
    )