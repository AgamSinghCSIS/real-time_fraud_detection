import sys
import os

# Add the project root folder (one level above 'src') to PYTHONPATH
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Running script from working dir: {os.getcwd()}")
print(f"sys.path: {sys.path}")

from dotenv import load_dotenv
load_dotenv()

os.environ['LOGGER_NAME'] = "KAFKA_INGESTION"
from src.common.logger import init_logger
logger = init_logger(os.environ.get("LOGGER_NAME"), logfile='ingestion.log')

from src.common.spark_utils import local_get_spark
from src.common.config_loader import load_ingestion_configs
from src.streaming.query_monitoring import QueryMonitoring

import time
import json
import tempfile
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_date, array, array_except, map_keys, lit, expr, when, size
from pyspark.sql.types import StructType, MapType, StringType
from pyspark.sql.streaming.query import StreamingQuery

def local_stream_kafka():
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

    if kafka_host == "localhost:9092" or kafka_host == 'host.docker.internal:9092':
        logger.info("SPARK: Acquiring Local Spark with Kafka jars")
        spark = local_get_spark()
        spark.streams.addListener(QueryMonitoring())

    else:
        logger.critical(f"Script only meant to be run for local setup! Failing...")
        logger.info(f"Run: fraud_detection/src/streaming/stream_ingestion.py for confluent host")
        exit(100)

    queries = []
    for source in sources:
        topic = source['topic_name']
        sink = source['sink_table']
        sample_event = source['sample_event']
        checkpoint = source['checkpoint']

        if not topic or not sink:
            logger.error(f"KAFKA INGESTION: Config file is corrupted! Configs expect topic_name, sink_table and sample_event.\nSkipping Source: {source}")
            continue
        logger.info(f"KAFKA INGESTION: Processing topic {topic}")

        if not sample_event:
            logger.error(f"KAFKA INGESTION: Sample Event needed for schema inference! topic: {topic}")
            continue
        logger.info(f"KAFKA INGESTION: Sample event: {sample_event}")

        success = ingest_stream(spark=spark, kafka_host=kafka_host, kafka_user=kafka_user, kafka_pass=kafka_pass, topic_name=topic, sink_name=sink, checkpoint_location=checkpoint, sample_string=sample_event)
        if success is False:
            logger.critical(f"KAFKA INGESTION: Failed for topic: {topic}")
            continue

        elif isinstance(success, StreamingQuery):
            queries.append(success)
            logger.info(f"Successfully Started Streaming Query for topic: {topic}. Query: {success}")

        else:
            logger.error(f"Unexpected Behavior! Value returned was of type: {type(success)}, Value: {success}")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.warning("KeyboadInterrupt caught! Stopping query gracefully...")
        spark.streams.stop()


def ingest_stream(spark : SparkSession, kafka_host : str, kafka_user : str, kafka_pass : str, topic_name : str, sink_name : str, checkpoint_location : str, sample_string : str):

    logger.info("INGESTING STREAM: ...")
    logger.info(f"INGESTING STREAM: Extracting schema for topic: {topic_name}!")
    schema = extract_schema_from_sample(spark=spark, sample_event=sample_string)
    if schema is False:
        logger.error(f"INGESTING STREAM: Failed Schema inference, Returning False")
        return False

    logger.info(f"INGESTING STREAM: Building Kafka Parameter dictionary!")
    kafka_params = get_kafka_params(kafka_user=kafka_user, kafka_pass=kafka_pass, kafka_topic=topic_name)
    if not kafka_params:
        logger.error(f"INGESTING STREAM: Failed Extracting Kafka Parameter Dict!")
        return False
    logger.info(f"INGESTING STREAM: Building Kafka Parameters Successful! {kafka_params}")
    print(f"Kafka params: {kafka_params}")

    logger.info(f"INGESTING STREAM: Triggering ReadStream From Kafka!")
    stream_binary_df = trigger_stream(spark=spark, kafka_host=kafka_host,  params=kafka_params)
    if stream_binary_df is False:
        logger.error(f"INGESTING STREAM: Failed to Trigger Stream!")
        return False
    logger.info(f"INGESTING STREAM: Triggering ReadStream Successful!")

    logger.info(f"INGESTING STREAM: Parsing + Adding Metadata to the DataFrame")
    parsed_df = parse_df(df=stream_binary_df, expected_schema=schema)
    if parsed_df is False:
        logger.error(f"INGESTING STREAM: Failed to Parse Streaming dataframe")
        return False
    logger.info(f"INGESTING STREAM: Paring + Metadata Successful!")

    # Start loading to sink location
    if topic_name == 'transactions':
        parsed_df = parsed_df.withColumn("is_chargeback", lit(False))

    query_name = f'local_kafka_ingestion_from_{topic_name}_query'

    write_query = load_dataframe(spark=spark, df=parsed_df, sink_location=sink_name, checkpoint_location=checkpoint_location, query_name=query_name)
    if write_query is False:
        logger.error(f"INGESTING STREAM: Loading Failed")
        return False
    logger.info(f"INGESTING STREAM: WriteStream Successfully started")
    return write_query
    # What about watermarking? Reprocessing strat? DLQ?



def extract_schema_from_sample(spark : SparkSession, sample_event : str) -> StructType:
    logger.info(f"STREAM EXTRACTING SCHEMA: ...")
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            temp_file.write(sample_event)
            temp_file_path = temp_file.name

        # Use spark.read.json to infer the schema from the temporary file
        json_df = spark.read.json(temp_file_path)

        # Get the StructType schema
        schema = json_df.schema

        # Return the inferred StructType schema
        logger.info(f"STREAM EXTRACTING SCHEMA: Successful")
        return json_df.schema

    except Exception as e:
        logger.error(f"STREAM EXTRACTION SCHEMA: Failed with error: {e}")
        return False

    finally:
        # Clean up the temporary file
        if 'temp_file_path' in locals():
            try:
                os.unlink(temp_file_path)
                logger.info(f"STREAM EXTRACTION SCHEMA: Deleted temporary file")
            except Exception as e:
                print(f"STREAM EXTRACTION SCHEMA: Error deleting temporary file: {str(e)}")



def trigger_stream(spark : SparkSession, kafka_host : str, params : dict):
    logger.info(f"STREAM TRIGGERING: Trying to trigger ")
    try:
        kafka_options = {
            "kafka.bootstrap.servers": kafka_host,
            "security.protocol": "PLAINTEXT",
            **params
        }
        logger.info(f"SPARK-SUBMIT MAKEFILE DEBUG: TOTAL KAFKA PARAMS: {kafka_options}")
        df = (spark.readStream
                .format("kafka")
                .options(**kafka_options)
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


def parse_df(df : DataFrame, expected_schema : StructType):
    """
    This function
        1. Castes the binary dataframe string.
        2. Extracts the metadata fields sent by kafka.
        3. Parses the dataframe based on the provided schema,
            as well as preserves the original payload.
    """
    logger.info(f"STREAM JSON PARSING: ...")

    try:
        expected_keys_array = array(*[lit(k) for k in expected_schema.fieldNames()])
        print("expected keys: ", expected_keys_array)
        unexpected_keys = array_except(map_keys(col("raw_map")), expected_keys_array)
        print("unexpected keys", unexpected_keys)

        logger.info(f"STREAM JSON PARSING: Decoding Dataframe")
        decoded_df = (df
            .withColumn("ingested_ts", col("timestamp"))
            .withColumn("raw_payload", col("value").cast("string"))
            .withColumn("raw_map", from_json(col("raw_payload"), MapType(StringType(), StringType())))
            .withColumn("json_data", from_json(col("raw_payload"), expected_schema))
            .withColumn("date", to_date(col("timestamp")))
        )

        logger.info(f"STREAM JSON PARSING: Filtering & Parsing Dataframe")

        filtered_df = (decoded_df.select(
                        "json_data.*",
                        col("topic").alias("kafka_topic"),  # Kafka metadata
                        col("partition").alias("kafka_partition"),
                        col("offset").alias("kafka_offset"),
                        "ingested_ts",
                        "date",
                        "raw_payload",
                        "raw_map"
                    )
            )

        expr_unparsed_fields = """
          map_filter(raw_map, (k, v) -> NOT array_contains(array({}, ''), k))
            """.format(",".join(f"'{k}'" for k in expected_schema.fieldNames()))

        logger.info(f"STREAM JSON PARSING: Extracting unparsed_fields / New columns")
        parsed_df = (filtered_df
                     .select("*")
                     .withColumn("unparsed_fields", expr(expr_unparsed_fields))
                     .withColumn("schema_version", when(size(col("unparsed_fields")) == 0, lit("1.0")).otherwise(lit("1+")))
                     .drop("raw_map")
        )

        logger.info(f"STREAM JSON PARSING: Dataframe successfully Parsed. Returning...")
        return parsed_df

    except Exception as e:
        logger.error(f"STREAM JSON PARSING: Failed with error: {e}")
        return False



def load_dataframe(spark : SparkSession, df : DataFrame, sink_location : str, checkpoint_location : str, query_name : str = None) -> StreamingQuery:
    # Local Script, so cannot use unity catalog location, but will instead need a adls path
    try:
        logger.info(f"STREAM LOADING: Writing microBatch to {sink_location}")
        logger.info(f"STREAM LOADING: checkpoint location: {checkpoint_location}")

        query = (df.writeStream
                    .format("delta")
                    .outputMode("append")
                    .option("checkpointLocation",checkpoint_location)
                    .queryName(query_name)
                    .start(sink_location)
        )
        logger.info(f"STREAM LOADING: WriteStream Started with StreamingQuery: {query}")
        return query

    except Exception as e:
        logger.error(f"STREAM LOADING: Failed to load microBatch to {sink_location} with error: {e}")
        return False


def get_kafka_params(kafka_user : str, kafka_pass : str, kafka_topic : str) -> dict:
    local_kafka_params = {
        "subscribe": kafka_topic,
        "failOnDataLoss": "false",
        "startingOffsets": "earliest"
    }
    return local_kafka_params


def monitor_queries(queries: list[StreamingQuery], log_interval_sec: int = 30):
    try:
        while True:
            for query in queries:
                name = query.name or query.id
                status = query.status  # dict with {isTriggerActive, message, etc.}
                progress = query.lastProgress  # dict (or None if no batch yet)

                logger.info(f"[Query: {name}] ACTIVE: {query.isActive}, STATUS: {status.get('message')}, TRIGGER ACTIVE: {status.get('isTriggerActive')}")

                if progress:
                    logger.info(f"[Query: {name}] Last Progress: {json.dumps(progress, indent=2)}")
                    logger.info(f"Processed {progress['numInputRows']} records in batch {progress['batchId']} (Query: {query.name or query.id})")
                else:
                    logger.info(f"[Query: {name}] No progress yet — still initializing or waiting for data.")

            if not all(q.isActive for q in queries):
                logger.error("One or more queries have stopped unexpectedly. Exiting...")
                break

            time.sleep(log_interval_sec)

        # Gracefully stop remaining active queries
        for q in queries:
            if q.isActive:
                logger.warning(f"Stopping query {q.name or q.id}")
                q.stop()

    except KeyboardInterrupt:
        logger.warning("Received keyboard interrupt. Stopping all queries...")
        for q in queries:
            if q.isActive:
                q.stop()


"""How will streaming jobs be run?: They will simply be triggered by this script"""

if __name__ == '__main__':
    local_stream_kafka()
