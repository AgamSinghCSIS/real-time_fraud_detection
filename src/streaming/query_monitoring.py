import os
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import (
    QueryStartedEvent,
    QueryIdleEvent,
    QueryProgressEvent,
    QueryTerminatedEvent)

import requests
from src.common.logger import get_logger


class QueryMonitoring(StreamingQueryListener):
    def __init__(self):
        self.logger = get_logger(name=os.environ.get("LOGGER_NAME"))
        self.active_queries = {}  # Track active queries by query ID

    def onQueryStarted(self, event: "QueryStartedEvent") -> None:
        self.logger.info(f"QUERY MONITORING: New Streaming Query Started Successfully!\nQuery Name: {event.name} \nQuery Id: {event.id}")

    def onQueryProgress(self, event: "QueryProgressEvent") -> None:
        progress = event.progress
        self.logger.info(
            f"QUERY MONITORING: Query Made Progress\n"
            f"Query Name: {progress.name}\n"
            f"Input Rows: {progress.numInputRows}\n"
            f"Processed Rows/sec: {progress.processedRowsPerSecond:.2f}\n"
            f"Sources: {progress.sources}\n"
            f"Sinks: {progress.sink}\n"
            f"Timestamps: {progress.timestamp}"
        )

    def onQueryIdle(self, event: "QueryIdleEvent") -> None:
        self.logger.info(
            f"QUERY MONITORING: Query ID: {event.id} is now IDLE since: {event.timestamp}"
        )

    def onQueryTerminated(self, event: "QueryTerminatedEvent") -> None:
        exception = event.exception or "No exception reported"
        self.logger.error(
            f"QUERY MONITORING: Streaming Query Terminated\n"
            f"Query Id: {event.id}\n"
            f"With Exception: {exception}"
        )

        self.logger.error(f"QUERY MONITORING: Preparing for Restart Dag call...\nStopping all active queries. ")
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark:
            for query_id, query_name in list(self.active_queries.items()):
                if query_id != event.id:  # Skip the terminated query
                    try:
                        query = spark.streams.get(query_id)
                        if query and query.isActive:
                            query.stop()
                            self.logger.info(f"Stopped active query: {query_name} (ID: {query_id})")
                    except Exception as e:
                        self.logger.error(f"Failed to stop query {query_name} (ID: {query_id}): {str(e)}")
            # Remove all queries from tracking
            self.active_queries.clear()

        self.logger.info("Triggering Airflow DAG to restart job...")
        trigger_stream_restart_dag(self.logger)



def trigger_stream_restart_dag(logger):
    url = os.getenv("AIRFLOW_API_URL", "http://airflow_spark_local:8080/api/v1/dags/restart_streaming_job/dagRuns")
    dag_conf = {"conf": {"reason": "streaming_job_failed"}}
    auth = (os.getenv("AIRFLOW_USER", "admin"), os.getenv("AIRFLOW_PWD", "admin"))
    try:
        response = requests.post(url, json=dag_conf, auth=auth, timeout=30)
        if response.status_code == 200:
            logger.info(f"AIRFLOW DAG TRIGGERED: restart_streaming_job")
        else:
            logger.error(f"FAILED TO TRIGGER DAG: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"EXCEPTION WHEN TRIGGERING DAG: {str(e)}")