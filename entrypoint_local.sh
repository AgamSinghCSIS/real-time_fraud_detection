#!/bin/bash
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
#mkdir -p /opt/airflow/logs

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
airflow scheduler &

echo "[INFO] Booting container and launching Spark job once..." >> /opt/airflow/logs/spark_submit.log
cd /opt/airflow || exit 1

# Run Spark job once
make run_stream >> /opt/airflow/logs/spark_submit.log 2>&1

# Optional: tail the output so it's visible in `docker logs`
echo "[INFO] Spark job completed. Starting Airflow webserver..."
#exec /entrypoint airflow webserver


# Start webserver
exec airflow webserver
