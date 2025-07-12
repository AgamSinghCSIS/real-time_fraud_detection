#!/bin/bash
echo ">>> ENTRYPOINT.SH EXECUTED <<<"

echo "Current working directory:"
pwd

echo "Contents of .env file:"
cat /opt/airflow/.env || echo "Failed to read .env"

echo "Python version:"
python --version

echo "Installed packages:"
pip list | grep -E 'sqlalchemy|psycopg2|pyspark|databricks-connect'

echo "PYTHONPATH:"
echo $PYTHONPATH

echo "Contents of /opt/airflow"
ls -lR /opt/airflow

echo "Contents of /opt/airflow/src:"
ls -lR /opt/airflow/src


echo "Python sys.path:"
python -c "import sys; print(sys.path)"

echo "Testing src import:"
python -c "import sys; sys.path.insert(0, '/opt/airflow/src'); from src.common.logger import get_logger; print('Import successful')" || echo "src import failed"

echo "Testing test_dag import:"
python /opt/airflow/dags/test_dag.py || echo "Test DAG import failed"

echo "Testing raw_ingestion_dag import:"
python /opt/airflow/dags/raw_ingestion_dag.py || echo "Raw ingestion DAG import failed"

echo "Running databricks setup script..."
python /opt/airflow/databricks_setup.py || echo "Databricks setup failed"


export PYTHONPATH=/opt/airflow/src:$PYTHONPATH

echo "PYTHONPATH in subprocess:"
bash -c "echo \$PYTHONPATH"
echo "Working directory in subprocess:"
bash -c "pwd"

# Update airflow.cfg only if necessary
echo "Checking Airflow config for pythonpath..."
if ! grep -q "\[core\]" /opt/airflow/airflow.cfg; then
  echo "Adding [core] section to airflow.cfg..."
  echo -e "[core]\npythonpath = /opt/airflow:/opt/airflow/src" >> /opt/airflow/airflow.cfg
elif ! grep -q "pythonpath = /opt/airflow:/opt/airflow/src" /opt/airflow/airflow.cfg; then
  echo "Updating pythonpath in [core] section..."
  sed -i '/\[core\]/a pythonpath = /opt/airflow:/opt/airflow/src' /opt/airflow/airflow.cfg
else
  echo "pythonpath already set in airflow.cfg."
fi

# Show Airflow config
echo "Airflow config:"
cat /opt/airflow/airflow.cfg

# Initialize Airflow DB if not already done
if [ ! -f "/opt/airflow/airflow_initialized" ]; then
  echo "Airflow DB not initialized. Running 'airflow db init'..."
  airflow db init

  echo "Creating Airflow admin user..."
  airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

  touch /opt/airflow/airflow_initialized
else
  echo "Airflow DB already initialized."
fi

echo "Launching airflow with command: airflow $@"
exec airflow "$@"