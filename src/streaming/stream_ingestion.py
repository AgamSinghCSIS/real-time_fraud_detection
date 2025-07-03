import os
from dotenv import load_dotenv
load_dotenv()

os.environ['LOGGER_NAME'] = "KAFKA_INGESTION"
from src.common.logger import init_logger
logger = init_logger(os.environ.get("LOGGER_NAME"), logfile='ingestion.log')

from src.common.spark_utils import local_get_spark
from src.common.dbx_utils import safe_get_spark
from src.common.config_loader import load_ingestion_configs

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_json, schema_of_json, lit, col, from_json, to_date


def stream_kafka():
    sources = load_ingestion_configs(pipeline='streaming', source='kafka')
    if sources is False:
        logger.critical(f"KAFKA INGESTION: Cannot be triggered because loading configs failed!")
        logger.error(f"KAFKA INGESTION: Failed!")
        exit(100)
    else:
        logger.info(f"KAFKA INGESTION: Configs successfully loaded")
        print(sources)

    kafka_host = os.environ.get("BOOTSTRAP_SERVERS")
    kafka_user = os.environ.get("KAFKA_API_KEY")
    kafka_pass = os.environ.get("KAFKA_API_SECRET")

    if kafka_host == "localhost:9092":
        logger.info("SPARK: Acquiring Local Spark with Kafka jars")
        spark = local_get_spark()
    else:
        logger.info(f"SPARK: Acquiring Remote Databricks Spark")
        spark = safe_get_spark()


    for source in sources:
        topic = source['topic_name']
        sink = source['sink_table']
        sample_event = source['sample_event']
        if not topic or not sink:
            logger.error(f"KAFKA INGESTION: Config file is corrupted! Configs not as expected.\nSkipping Source: {source}")
            continue
        logger.info(f"KAFKA INGESTION: Processing topic {topic}")

        if not sample_event:
            logger.error(f"KAFKA INGESTION: Sample Event needed for schema inference! topic: {topic}")
            continue

        success = ingest_stream(spark=spark, kafka_host=kafka_host, kafka_user=kafka_user, kafka_pass=kafka_pass, topic_name=topic, sink_name=sink, sample_string=sample_event)
        if success is False:
            logger.critical(f"KAFKA INGESTION: Failed for topic: {topic}")
            continue



def ingest_stream(spark : SparkSession, kafka_host : str, kafka_user : str, kafka_pass : str, topic_name : str, sink_name : str, sample_string : str):

    logger.info("INGESTING STREAM: ...")

    logger.info(f"INGESTING STREAM: Extracting schema for topic: {topic_name}!")
    schema = extract_schema_from_sample(spark=spark, sample_str=sample_string)

    if schema is False:
        logger.error(f"INGESTING STREAM: Failed Schema inference, Returning False")
        return False

    print(f"Schema: {schema}")

    kafka_params = get_kafka_params(kafka_user=kafka_user, kafka_pass=kafka_pass, kafka_topic=topic_name)
    if not kafka_params:
        logger.error(f"INGESTING STREAM: Failed Extracting Kafka Parameter Dict!")
        return False

    print(f"Kafka params: {kafka_params}")

    stream_binary_df = trigger_stream(spark=spark, kafka_host=kafka_host,  params=kafka_params)
    if stream_binary_df is False:
        logger.error(f"INGESTING STREAM: Failed to Trigger Stream!")
        return False


    parsed_df = parse_df(df=stream_binary_df, schema=schema)
    if parsed_df is False:
        logger.error(f"INGESTING STREAM: Failed to Parse Streaming dataframe")
        return False

    # Add test to check transaction is different than the schema, then push the record to DLQ



    # Add metadata to the streaming dataframe
    # What about watermarking? Reprocessing strat? DLQ?



def extract_schema_from_sample(spark : SparkSession, sample_str : str):
    logger.info(f"STREAM EXTRACTING SCHEMA: ...")
    try:
        schema = spark.range(1).select(schema_of_json(lit(sample_str)).alias("schema")).collect()[0][0]
        logger.info(f"STREAM EXTRACTING SCHEMA: Successful")
        return schema

    except Exception as e:
        logger.error(f"STREAM EXTRACTION SCHEMA: Failed with error: {e}")
        return False


def add_metadata(df: DataFrame):
    pass


def trigger_stream(spark : SparkSession, kafka_host : str, params : dict):
    logger.info(f"STREAM TRIGGERING: Trying to trigger ")
    try:
        if 'kafka.sasl.mechanism' not in params:
            logger.info(f"STREAM TRIGGERING: Local Kafka Stream from topic: {params['subscribe']}")
            df = (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .options(**params)
                    .load())

        else:
            df = (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafka_host)
                    .option('kafka.security.protocol', "SASL_SSL")
                    .options(**params)
                    .load()
            )
        if not df:
            logger.error(f"STREAM TRIGGERING: Failed! df is not df")
            return False
        else:
            logger.info(f"STREAM TRIGGERING: Returning the streaming dataframe")
            return df

    except Exception as e:
        logger.error(f"STREAM TRIGGERING: Failed with error {e}")
        logger.error(f"STREAM TRIGGERING: Defaulting to return False ")
        return False




def parse_df(df : DataFrame, schema):
    logger.info(f"STREAM JSON PARSING: ...")
    try:

        sample_df = (
            df
            .withColumn("ingested_ts", col("timestamp"))
            .withColumn("json_data", from_json(col("value").cast("string"), schema))
            .select(
                "json_data.*",  # flatten the struct
                col("topic").alias("kafka_topic"),  # Kafka metadata
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                "ingested_ts"
            )
            #.withColumn("is_chargeback", lit(False))
            .withColumn("date", to_date(col("timestamp")))
        )

        logger.info(f"STREAM JSON PARSING: Decoding Dataframe")
        encoded_df = df.selectExpr(f"CAST(value AS STRING) as 'json_data'")

        logger.info(f"STREAM JSON PARSING: Applying Schema to json Dataframe")
        json_df = encoded_df.select(to_json(col("json_data"), schema).alias("data"))

        logger.info(f"STREAM JSON PARSING: Splitting Json Dataframe")
        parsed_df = json_df.select("data.*")

        logger.info(f"STREAM JSON PARSING: Dataframe successfully Parsed. Returning...")
        return parsed_df

    except Exception as e:
        logger.error(f"STREAM JSON PARSING: Failed with error: {e}")
        return False




def get_kafka_params(kafka_user : str, kafka_pass : str, kafka_topic : str) -> dict:
    if kafka_user == '' or kafka_pass == '':
        kafka_params = {
            "subscribe": kafka_topic
        }
    else:
        kafka_params = {
            "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_user}' password='{kafka_pass}';",
            "kafka.sasl.mechanism": "PLAIN",
            "subscribe": kafka_topic
        }

    return kafka_params



"""How will streaming jobs be run?: They will simply be triggered by this script"""

if __name__ == '__main__':
    stream_kafka()
