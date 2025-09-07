import os
import time

from dotenv import load_dotenv

import src.common.spark_utils

load_dotenv()

from src.common.logger import get_logger

import requests
from pyspark.sql import SparkSession, DataFrame
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    push_to_gateway
)

logger = get_logger(name=os.environ.get("LOGGER_NAME"))

PUSH_GATEWAY_URL = os.environ.get("PROMETHEUS_PUSH_GATEWAY_URL")
METRICS = None

def init_metrics(layer : str, registry : CollectorRegistry):
    global METRICS
    if METRICS is None:
        METRICS = define_metrics(layer=layer, registry=registry)
    else:
        print("Metrics already initialized")


def define_metrics(layer : str, registry : CollectorRegistry):
    metrics = {}

    metrics[f"{layer}_batch_runs_counter"] = Counter(
        name=f"{layer}_batch_runs_counter",
        documentation=f"Tracks {layer} table process runs",
        labelnames=["pipeline"],
        registry=registry
    )

    metrics[f"{layer}_batch_processed_records"] = Counter(
        name=f"{layer}_records_processed_counter",
        documentation=f"Counts records processed into the {layer} layer",
        labelnames=["pipeline", "job_name"],
        registry=registry
    )

    metrics[f"{layer}_batch_records_processed_per_second"] = Gauge(f"{layer}_batch_records_processed_per_second",
    f"Rate of the records being processed every second",
    ["pipeline", "job_name"],
    registry=registry)

    metrics[f"{layer}_spark_startup_time"] = Gauge(f"{layer}_spark_startup_time",
    "Time it took for Spark to start in seconds",
    ["pipeline"],
    registry=registry)

    metrics[f"{layer}_executor_count"] = Gauge(
        name=f"{layer}_spark_executor_count",
        documentation=f"Total number of executors for the {layer} layer",
        labelnames=["pipeline", "job_name"],
        registry=registry
    )

    metrics[f"{layer}_executor_memory_total"] = Gauge(
        name=f"{layer}_spark_executor_total_memory_bytes",
        documentation=f"Total Memory per executors for the {layer} layer",
        labelnames=["pipeline", "job_name"],
        registry=registry
    )

    metrics[f"{layer}_executor_memory_remaining"] = Gauge(
        name=f"{layer}_spark_executor_remaining_memory_bytes",
        documentation=f"Remaining Memory per executors for the {layer} layer",
        labelnames=["pipeline", "job_name"],
        registry=registry
    )

    metrics[f"{layer}_batch_processing_time"] = Gauge(
        name=f"{layer}_batch_processing_time",
        documentation=f"Tracks {layer} layer table processing time",
        labelnames=["pipeline", "job_name"],
        registry=registry
    )

    metrics[f"{layer}_batch_total_tasks"] = Gauge(
        name=f"{layer}_batch_total_tasks",
        documentation=f"Tracks {layer} layer total tasks created",
        labelnames=["pipeline"],
        registry=registry
    )

    metrics[f"{layer}_batch_failed_tasks"] = Gauge(
        name=f"{layer}_batch_failed_tasks",
        documentation=f"Tracks {layer} layer failed tasks created",
        labelnames=["pipeline"],
        registry=registry
    )

    metrics[f"{layer}_batch_duration"] = Gauge(
        name=f"{layer}_batch_duration",
        documentation=f"Tracks {layer} layer total duration",
        labelnames=["pipeline"],
        registry=registry
    )

    logger.info(f"Initiated metrics: {metrics.keys()}")
    return metrics

def get_spark_executor_metrics(spark_ui_host : str = "http://localhost:4040"):

    app_id_url = f"{spark_ui_host}/api/v1/applications"
    response = requests.get(app_id_url)
    response.raise_for_status()
    apps = response.json()
    app_id = apps[0]['id'] if apps else None
    print("App id: ", app_id)
    if app_id is None:
        return False

    url = f"{spark_ui_host}/api/v1/applications/{app_id}/executors"
    response = requests.get(url)
    response.raise_for_status()
    status = response.json()

    print(status)
    return {
        "executor_count": len(status),
        "failed_tasks": [s['failedTasks'] for s in status],
        "total_tasks": [s['totalTasks'] for s in status],
        "total_duration_ms": [s['totalDuration'] for s in status],
        "garbage_collection_time": [s['totalGCTime'] for s in status],
    }


def collect_and_push_metrics(spark : SparkSession, df: DataFrame, pipeline_layer : str, job_name : str ,registry: CollectorRegistry, spark_startup_time, processing_duration):
    # 1. define the metrics to be defined and return as a dict
    global METRICS
    if len(list(registry.collect())) == 0:
        print("New Registry")
        metrics_dict = define_metrics(layer=pipeline_layer, registry=registry)
        METRICS = metrics_dict
        #init_metrics(layer=pipeline_layer, registry=registry)
    else:
        print("registry is Already defined")
        metrics_dict = METRICS
    #global METRICS
    #metrics_dict = METRICS
    # 2. Extract the metrics
    run_counter = metrics_dict[f"{pipeline_layer}_batch_runs_counter"]
    records_processed_counter = metrics_dict[f"{pipeline_layer}_batch_processed_records"]
    startup_time_gauge = metrics_dict[f"{pipeline_layer}_spark_startup_time"]
    records_per_second_gauge = metrics_dict[f"{pipeline_layer}_batch_records_processed_per_second"]
    records_processing_time_gauge = metrics_dict[f"{pipeline_layer}_batch_processing_time"]
    executor_count_gauge = metrics_dict[f"{pipeline_layer}_executor_count"]
    failed_task_gauge = metrics_dict[f"{pipeline_layer}_batch_failed_tasks"]
    total_task_gauge = metrics_dict[f"{pipeline_layer}_batch_total_tasks"]


    # 3. Set the variables for the metrics
    spark_port = os.environ.get("SPARK_JOB_PORT")
    print("Spark Port from os env var:", spark_port)
    spark_ui_url = f"http://localhost:{spark_port}"
    print(spark_ui_url)

    spark_metrics = get_spark_executor_metrics(spark_ui_url)
    print(spark_metrics)
    executor_count = spark_metrics['executor_count']
    if executor_count == 1:
        failed_tasks_count = spark_metrics['failed_tasks'][0]
        total_tasks_count = spark_metrics['total_tasks'][0]
    else:
        print("Distributed environment")
        failed_tasks_count = sum(spark_metrics['failed_tasks'])
        total_tasks_count = sum(spark_metrics['total_tasks'])
        print(total_tasks_count, failed_tasks_count)

    df_count = df.count()
    records_per_second = df_count / processing_duration

    # 2. Set the metrics
    run_counter.labels(pipeline_layer).inc()
    records_processed_counter.labels(pipeline_layer, job_name).inc(df_count)
    startup_time_gauge.labels(pipeline_layer).set(spark_startup_time)
    records_processing_time_gauge.labels(pipeline_layer, job_name).set(processing_duration)
    records_per_second_gauge.labels(pipeline_layer, job_name).set(records_per_second)
    executor_count_gauge.labels(pipeline_layer, job_name).set(executor_count)
    total_task_gauge.labels(pipeline_layer).set(total_tasks_count)
    failed_task_gauge.labels(pipeline_layer).set(failed_tasks_count)

    job_name = pipeline_layer + '_' + job_name
    push_to_gateway(gateway=PUSH_GATEWAY_URL, job=job_name, registry=registry)


if __name__ == "__main__":
    registry = CollectorRegistry()
    layer = "test"
    x = list(registry.collect())
    print(x)
    for i in  registry.collect():
        print(i)
    print("Registry checking done")

    """x = define_metrics(layer=layer, registry=registry)
    print(x)
    val = x['bronze_batch_counter']
    print(type(x.values()))
    print(type(val))
    for z in x.items():
    print(z)"""



    t = time.time()
    spark = src.common.spark_utils.local_get_spark()
    spark_time = time.time() - t
    df = spark.read.option("headers", "true").csv("../../data/credit_cards.csv")
    df.repartition(3)
    job_name = "test_credit_cards"
    processing_time = time.time() - t
    os.environ['SPARK_JOB_PORT'] = "4040"
    collect_and_push_metrics(spark, df, layer, job_name, registry, spark_time, processing_time)
    x = list(registry.collect())
    print(x, "\n", len(x))












