import os
import sys

print("DAG working directory:", os.getcwd())
print("DAG sys.path:", sys.path)
print("DAG PYTHONPATH env:", os.getenv('PYTHONPATH'))
sys.path.insert(0, '/opt/airflow/')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
from src.common.logger import get_logger
from src.common.config_loader import load_database_config

def test_import():
    logger = get_logger()
    logger.info("Test import successful")

def say_hi():
    print("Hello this is a test dag!")
    log = {
        "function" : "say_hi",
        "start_time" : f"{datetime.utcnow()}",
        "status" : "Completed"
    }
    logging.info(log)
    return log

with DAG(
    dag_id ="test_dag",
    start_date=datetime(2025,6,28),
    schedule_interval="@daily",
    catchup=False) as dag:

    task = PythonOperator(
        task_id="test_hello_task",
        python_callable=say_hi
    )


