import os
from dotenv import load_dotenv
load_dotenv()
from src.common.logger import get_logger
logger = get_logger(os.environ.get("LOGGER_NAME"))

from pyspark.sql import SparkSession

def local_get_spark():
    spark = (SparkSession.builder
        .appName("LocalKafkaIngestion")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
        .master("local[*]")
        .getOrCreate())
    return spark