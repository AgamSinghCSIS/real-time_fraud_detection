#!/bin/bash
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"
export PYSPARK_PYTHON=/usr/local/bin/python3.10
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.10

# Verify Python executable
if ! command -v /usr/local/bin/python3.10 &> /dev/null; then
    echo "[ERROR] Python 3.10 not found at /usr/local/bin/python3.10" >> /opt/airflow/logs/spark_submit.log
    exit 1
fi
echo "[INFO] Python 3.10 found at /usr/local/bin/python3.10" >> /opt/airflow/logs/spark_submit.log /usr/local/bin/python3.10 --version >> /opt/airflow/logs/spark_submit.log 2>&1
# Ensure logs dir exists
mkdir -p /opt/airflow/logs

# Initialize Airflow DB
airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true


# Start scheduler in background
#airflow scheduler &
/entrypoint airflow scheduler &

# Skip Spark job until Kafka is available
echo "[INFO] Skipping Spark job (Kafka not running)..." >> /opt/airflow/logs/spark_submit.log
# Uncomment the following to run Spark job when Kafka is ready
# cd /opt/airflow || exit 1
# make run_stream >> /opt/airflow/logs/spark_submit.log 2>&1 &

# Start Airflow webserver
echo "[INFO] Starting Airflow webserver..." >> /opt/airflow/logs/spark_submit.log
exec /entrypoint airflow webserver --port 8080