FROM apache/airflow:2.8.1-python3.10

ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    && apt-get clean

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./configs $AIRFLOW_HOME/configs
COPY ./dags $AIRFLOW_HOME/dags
COPY ./logs $AIRFLOW_HOME/logs
COPY ./src $AIRFLOW_HOME/src
COPY ./.env $AIRFLOW_HOME/.env
COPY ./databricks/databricks_setup.py $AIRFLOW_HOME/databricks_setup.py

# Clear __pycache__ and set ownership
USER root
RUN find $AIRFLOW_HOME/src -name '__pycache__' -type d -exec rm -rf {} + && \
    chown -R airflow:0 $AIRFLOW_HOME && \
    chmod -R 775 $AIRFLOW_HOME
#---------

ENV PYTHONPATH=$AIRFLOW_HOME/src:$PYTHONPATH
ENV AIRFLOW__CORE__PYTHONPATH=$AIRFLOW_HOME/src

COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow
WORKDIR $AIRFLOW_HOME
ENTRYPOINT ["/entrypoint.sh"]