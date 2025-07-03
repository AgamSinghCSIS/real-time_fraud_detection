import os
from dotenv import load_dotenv
from pyspark.errors import AnalysisException

load_dotenv()
from src.common.logger import get_logger
logger = get_logger(os.environ.get("LOGGER_NAME"))

from pyspark.sql import SparkSession, DataFrame
from databricks.connect import DatabricksSession

import time

def get_dbx_session(retries=3, delay=10):
    logger.info(f"Trying to acquire Remote Databricks Session")
    for i in range(retries):
        try:
            spark = (DatabricksSession.builder
                     .getOrCreate())
            logger.info(f"Session Successfully Acquired after {i} Retries!")
            return spark
        except Exception as e:
            logger.error(f"[ATTEMPT{i+1} FAILED]: {e} \n Retries left: {retries-i-1}")
            time.sleep(delay)
    raise RuntimeError("Enable to create Session")


def safe_get_spark(retries=3, delay=10):
    for i in range(retries):
        try:
            logger.info(f"ACQUIRE DBX SPARK: Trying to acquire Remote Databricks Session")
            spark = DatabricksSession.builder.getOrCreate()
            spark.sql("SELECT 1")  # Probe to validate session
            logger.info(f"ACQUIRE DBX SPARK: Session Successfully Acquired after {i} Retries!")
            return spark
        except Exception as e:
            logger.error(f"ACQUIRE DBX SPARK: [ATTEMPT{i + 1} FAILED]: {e} \n Retries left: {3 - i - 1}")
            print(f"ACQUIRE DBX SPARK: Retry {i+1} due to: {e}")
            time.sleep(delay)
    raise RuntimeError("ACQUIRE DBX SPARK: Unable to establish Spark session")


def dbx_execute_query(spark : SparkSession, query : str, response_expected : bool = False):
    logger.info(f"Trying to execute query {query}...")
    try:
        if response_expected:
            response = spark.sql(query).collect()[0][0]
            logger.info("Successfully received a response")
            return response
        else:
            spark.sql(query)
            logger.info(f"Query successfully submitted")

    except Exception as e:
        logger.error(f"Error while executing query: {e}")
        return e


def upload_df_to_dbx(df : DataFrame, dbx_table_name : str, output_mode : str = "append"):
    logger.info(f"Trying to Write dataframe to table: {dbx_table_name}, with output mode: {output_mode.upper()}")

    if df.isEmpty() or df is None:
        logger.error("Write Failed because Dataframe is Empty or None")
        return False

    try:
        writer = (df.write.format("delta").mode(output_mode))
        writer.saveAsTable(dbx_table_name)
        logger.info(f"Successfully written DataFrame to {dbx_table_name}")
        return True

    except Exception as e:
        logger.error(f"Failed uploading dataframe to databricks with error: {e}")
        return False


def spark_kafka_consumer():
    pass
