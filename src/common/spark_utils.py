import os
from dotenv import load_dotenv
load_dotenv()
from src.common.logger import get_logger
logger = get_logger(os.environ.get("LOGGER_NAME"))

from pyspark.sql import SparkSession

def local_get_spark():
    sa = os.environ.get("AZURE_STORAGE_ACCOUNT")
    access_key = os.environ.get("AZURE_ACCESS_KEY")
    ivy_path = os.path.expanduser("~/.ivy2").replace("\\", "/")

    spark = (SparkSession.builder
             .appName("LocalKafkaIngestion")
             .master("local[2]")
             .config("spark.driver.memory", "6g")
             .config("spark.executor.memory", "6g")
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-azure:3.3.6,org.apache.hadoop:hadoop-common:3.3.6") # hadoop azure
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.jars.ivy", ivy_path)  # Explicit Ivy cache path
             .config("fs.azure.account.key.{}.dfs.core.windows.net".format(sa), access_key) # Azure
             .config("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")  # Azure
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel("WARN")
    # Log classpath for verification
    """for jar in spark._jvm.java.lang.System.getProperty("java.class.path").split(";"):
        logger.info(f"Classpath JAR: {jar}")"""

    return spark