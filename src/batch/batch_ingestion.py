import datetime
from dotenv import load_dotenv
from pyspark.sql.types import MapType, StringType

load_dotenv()
import os
from src.common.logger import init_logger
os.environ["LOGGER_NAME"] = 'batch_logger'
logger = init_logger(os.environ.get("LOGGER_NAME"), logfile='logs/ingestion.log')

from src.common.config_loader import load_ingestion_configs, load_column_list
from src.batch.pg_utils import get_engine, execute_query
from src.common.dbx_utils import get_dbx_session, dbx_execute_query, safe_get_spark, upload_df_to_dbx
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, current_date, to_json, struct, col, create_map, max, to_utc_timestamp
from sqlalchemy.engine import Engine


def ingest_dim_store():
    # Step 1: Get your connections Database and Databricks

    engine = get_engine(
        host=os.environ.get("DIMSTORE_HOST"),
        username=os.environ.get("DIMSTORE_USER"),
        password=os.environ.get("DIMSTORE_PASS"),
        database=os.environ.get("DIMSTORE_DB")
    )

    spark = safe_get_spark()

    configs = load_ingestion_configs(pipeline="batch", source='DIM_Store')
    table_list = configs['tables']
    watermark_table = configs['watermark_table']

    for table in table_list:
        source_schema = table['source_schema'] # test (just cuz schema exists in the config)
        source_table = table['source_table']
        sink_table = table['sink_table']
        replication_key = table['replication_column']
        logger.info(f"Starting Ingestion for {source_table} into Raw Layer table: {sink_table}")
        ingest_table(engine=engine, spark=spark, source_table=source_table, sink_table=sink_table, repl_col=replication_key)



def ingest_table(engine : Engine, spark : SparkSession, source_table : str, sink_table : str, repl_col : str):

    # A : Find out the last_modified_ts for this table in the raw layer table
    last_mod_ts = get_last_modified_ts(engine=engine, object=source_table, key=repl_col)

    print(f"Last Mod ts: {last_mod_ts}")

    if not last_mod_ts:
        logger.critical("Stopping Ingestion because watermark Extraction failed")
        logger.error(f"Stopping Ingestion for now until rds is up. Run manually again")
        exit(100)


    # B : Extract all the new data from the source
    cdc_query = f"SELECT * FROM {source_table} WHERE {repl_col} > '{last_mod_ts}'"
    cdc_df = spark_jdbc_reader(spark=spark, query=cdc_query)

    if cdc_df.isEmpty():
        # Load other tables where data may have been added
        logger.warning(f"Received Dataframe is empty! No new Data?\nSkipping Ingestion for table: {source_table}")
        return

    print("SOURCE DF:")
    cdc_df.show()


    #C : Add Metadata to the Dataframe
    enriched_df = enrich_metadata(raw_df=cdc_df, source_table=source_table)

    print("ENRICHED_DF:")
    enriched_df.show()

    # C : Upload All the latest data as append mode into bronze layer, Append only for raw layer

    current_max_ts = cdc_df.select(to_utc_timestamp(max(col("last_modified_at")), "PST")).collect()[0][0]
    print(f"Current max timestamp is: {current_max_ts}")
    watermark_success = update_watermark_table(engine=engine, source_table=source_table, max_ts=current_max_ts)

    if watermark_success:
        logger.info(f"Watermark table updated Successfully! Starting Upload to Raw Layer table: {sink_table}")
        ingestion_success = upload_df_to_dbx(df=enriched_df, dbx_table_name=sink_table, output_mode="append")
        if ingestion_success:
            logger.info(f"INGESTION : SUCCESSFUL for {source_table}")
        else:
            logger.error(f"INGESTION : FAILED for {source_table}")
            logger.info(f"INGESTION : RESETTING WATERMARK to last known value before ingestion iteration")
            watermark_success = update_watermark_table(engine=engine, source_table=source_table, max_ts=last_mod_ts)
            if watermark_success:
                logger.info(f"INGESTION : RESETTING WATERMARK -> Successful")
                return
            else:
                logger.critical(f"INGESTION FAILURE: Data upload failed BUT Watermark table Could not be reverted!")
    else:
        logger.error(f"Not uploading Data to Raw Lake because watermarking failed!")
        logger.info(f"IMPLEMENT RETRIES")

    # D : Maybe add fail-safe in place as well? Raw layer so no need of any schema
    # No need of great Expectations for schema



def get_last_modified_ts(engine : Engine, object: str, key : str) :
    # NEED TO MODIFY SOURCE AND RAW LAYER
    watermark_table = 'core.ingestion_watermarks' # This needs to be in config
    ts_query = f"SELECT {key} FROM {watermark_table} WHERE table_name = '{object}'"
    try:
        logger.info(f"WATERMARK EXTRACTION: Trying to get {key} for {object}...")
        ts = execute_query(engine=engine, query=ts_query, response_expected=True)

        if ts is False:
            logger.error(f"WATERMARK EXTRACTION FAILED: Received Exception Flag from Query! Exiting ingestion")
            return False

        print(f"Inside ts: Timestamp returned is: {ts}, {type(ts)}")

        if not ts or not isinstance(ts, list) or not ts[0] or ts[0][0] is None:
            logger.warning("WATERMARK EXTRACTION: No valid timestamp found. Returning default backfill date.")
            return datetime.datetime(2000, 1, 1)

        else:
            logger.info(f"WATERMARK EXTRACTION: Successfully received a timestamp: {ts}")
            return ts[0][0]

    except Exception as e:
        logger.error(f"WATERMARK EXTRACTION EXCEPTION: Could not get last modified timestamp with error: {e}")
        logger.info(f"WATERMARK EXTRACTION EXCEPTION: Returning False")
        return False


def update_watermark_table(engine : Engine, source_table : str, max_ts):
    watermark_table = 'core.ingestion_watermarks'
    update_query = f"""
        INSERT INTO {watermark_table} (table_name, last_modified_at, updated_at)
        VALUES (:table_name, :last_modified_at, NOW())
        ON CONFLICT (table_name)
        DO UPDATE SET 
            last_modified_at = EXCLUDED.last_modified_at,
            updated_at = NOW();
    """
    try:
        logger.info(f"UPDATING WATERMARK: Trying to update watermark for {source_table}")
        execute_query(engine=engine, query=update_query, response_expected=False, values={"table_name": source_table, "last_modified_at": max_ts})
        logger.info(f"UPDATING WATERMARK: Watermark Update successful for {source_table} to {max_ts}")
        return True

    except Exception as e:
        logger.error(f"UPDATING WATERMARK EXCEPTION: Failed with error: {e}")
        return False



def spark_jdbc_reader(spark : SparkSession, query : str):
    logger.info(f"SPARK JDBC READER: Reading Dimstore host using query: {query}")
    url = f"jdbc:postgresql://{os.environ.get('DIMSTORE_HOST')}:5432/{os.environ.get('DIMSTORE_DB')}"
    prop = {
        "user": os.environ.get("DIMSTORE_USER"),
        "password": os.environ.get("DIMSTORE_PASS"),
        "driver": "org.postgresql.Driver"
    }
    wrapped_query = f"({query}) AS subquery"
    df = (spark.read
                .format("jdbc")
                .option("url", url)
                .option("dbtable", wrapped_query)
                .options(**prop)
                .load()
    )
    logger.info(f"SPARK JDBC READER: Successfully executed query {wrapped_query} on host. Returning Dataframe")
    #print(df.select(to_utc_timestamp(max(col("last_modified_at")), "PST")).collect()[0][0])
    return df



def enrich_metadata(raw_df : DataFrame, source_table : str):
    """
    Adds Metadata columns such as:
        - source_db, source_schema, source_table_name, schema_version
        - ingested_time, date (for partitioning)
        - raw payload (json)
        - unparsed_fields (For schema drift and versioning)

    Also parses any unexpected column data into unparsed_fields
    """
    db = os.environ.get("DIMSTORE_DB")
    schema_name, table_name = source_table.split(".")
    schema_version = "1.0" # Hardcoded for now, later on through config

    # Cast dataframe with business entities only to string
    #print(f"Before Casting: {raw_df.printSchema()}")
    logger.info(f"INGESTION METADATA: CASTING -> Business columns to STRING")
    casted_df = cast_to_string(df=raw_df, except_columns=["last_modified_at"])  # Dont Cast control column to string
    logger.info(f"INGESTION METADATA: CASTING -> Completed")
    #print(f"After Casting: {casted_df.printSchema()}")

    # parse Unexpected upstream columns for raw layer
    logger.info(f"INGESTION METADATA: SCHEMA PARSING -> LOADING : Trying to load business columns from config")
    expected_cols = load_column_list(source_table)

    if not expected_cols:
        logger.error(f"INGESTION METADATA: SCHEMA PARSING -> LOADING : Failed loading columns list for table: {source_table}")
        logger.info(f"INGESTION METADATA: SCHEMA PARSING -> PARSING : Skipping parsing for new columns")
        parsed_df = casted_df # Assigning over the same raw_df to continue flow
    else:
        logger.info(f"INGESTION METADATA: SCHEMA PARSING -> LOADING : Complete")
        logger.info(f"INGESTION METADATA: SCHEMA PARSING -> PARSING : Trying to parse new columns")
        parsed_df = parse_new_columns(unparsed_df=casted_df, expected_cols=expected_cols)
        logger.info(f"INGESTION METADATA: SCHEMA PARSING -> PARSING : Complete")

    logger.info(f"INGESTION METADATA: ADDING METADATA ")
    metadata_df = (parsed_df.withColumn("source_db", lit(db))
        .withColumn("source_schema", lit(schema_name))
        .withColumn("source_table", lit(table_name))
        .withColumn("schema_version", lit(schema_version))
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("date", current_date())
        .withColumn("raw_payload", to_json(struct([parsed_df[col] for col in parsed_df.columns])))
    )
    logger.info(f"INGESTION METADATA: ADDING METADATA : Complete")

    return metadata_df


def parse_new_columns(unparsed_df : DataFrame, expected_cols : list):
    if len(expected_cols) == 0:
        logger.error(f"INGESTION METADATA: SCHEMA PARSING -> PARSING FAILED: Expected Columns list is empty")
        logger.info(f"INGESTION METADATA: SCHEMA PARSING -> PARSING FAILED: Returning Unparsed Dataframe")
        return unparsed_df  # Return the same dataframe as it is

    elif unparsed_df.limit(1).count() == 0:
        logger.error(f"INGESTION METADATA: SCHEMA PARSING -> PARSING FAILED: Dataframe is Empty")
        logger.info(f"INGESTION METADATA: SCHEMA PARSING -> PARSING FAILED: Returning Unparsed Dataframe")
        return unparsed_df   # Return the same dataframe as it is

    else:
        unexpected_cols = [c for c in unparsed_df.columns if c not in expected_cols]

        if not unexpected_cols:
            logger.info(f"INGESTION METADATA: SCHEMA PARSING -> NO SCHEMA DRIFT DETECTED")
            return unparsed_df.withColumn("unparsed_fields", create_map().cast(MapType(StringType(), StringType())))

        else:
            logger.warning(f"INGESTION METADATA: SCHEMA PARSING -> SCHEMA DRIFT DETECTED! New columns: {unexpected_cols}")
            unparsed_kv = []
            for c in unexpected_cols:
                unparsed_kv += [lit(c), col(c)]

            df_with_unparsed = unparsed_df.withColumn("unparsed_fields", create_map(*unparsed_kv))
            parsed_df = df_with_unparsed.drop(*unexpected_cols)

        return parsed_df


def cast_to_string(df : DataFrame, except_columns : list):
    logger.info(f"Casting Dataframe columns: {df.columns} to STRING ")
    casted_df = df.select([col(c).cast("string").alias(c) if c not in except_columns else col(c) for c in df.columns])

    return casted_df


if __name__ == "__main__":
    ingest_dim_store()
    """engine = get_engine(host=os.environ.get("DIMSTORE_HOST"), username=os.environ.get("DIMSTORE_USER"), password=os.environ.get("DIMSTORE_PASS"), database=os.environ.get("DIMSTORE_DB"))
    tbl = "test"
    tx = datetime.datetime(2000, 1, 1)

    flag = upload_watermark_table(engine, tbl, tx)"""








