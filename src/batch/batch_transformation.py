# This file is supposed to process the database records from raw layer
# 1. De-Deuplify the records
# 2. Implement an SCD Type
# 3. Check Data modelling (Always star schema)
# 4. Mask all PII and sensitive data : Reversible or non-reversible
# 5. Handle Nulls
# Column Standardization	Normalize column names (e.g., snake_case), formats (timestamps in UTC), datatypes (int vs string for IDs)
# Bad record handling when data
"""
Q: Creates standardized dimension tables with surrogate keys and consistent formats
Q: Tracks changes using is_deleted, effective_from, effective_to, current_flag etc isnt it the same as SCD?
Q: Why surrogate keys, and not PKs
"""

import os
from dotenv import load_dotenv
load_dotenv()

os.environ["LOGGER_NAME"] = 'DIMSTORE_TRANSFORMATION'
from src.common.logger import init_logger

logger = init_logger(os.environ.get("LOGGER_NAME"), logfile='transformation.log')

from src.common.config_loader import load_transformation_configs
from src.common.dbx_utils import safe_get_spark

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    max, col, current_date, datediff, floor, concat,
    regexp_extract, lit, substring_index, length, substring,
    when, md5, concat_ws, current_timestamp
)
from delta.tables import DeltaTable
from datetime import datetime


def transform_dims_to_silver():

    configs = load_transformation_configs()
    logger.info(f"TRANSFORMATION: Configs were loaded Successfully! ")

    target_catalog = configs['catalog_name']
    target_layer = configs['destination_layer']
    tables = configs['objects']
    logger.info(f"TRANSFORMATION: Silver layer transformation job started from {configs['ingestion_layer']} layer to {target_layer} for catalog: {target_catalog}")

    logger.info(f"TRANSFORMATION: Trying to Acquire Databricks Remote Spark Session!!")
    spark = safe_get_spark()

    for table in tables:
        print(table)
        table_name = table['table_name']
        logger.info(f"TRANSFORMATION: Processing table: {table_name}")

        source_name = table['source_table']
        sink_name = target_catalog + "." + target_layer + "." + table_name
        table_schema = table['schema']

        try:
            pii_fields = table['pii_fields']
        except:
            pii_fields = False
            logger.info(f"{table_name} Table has no Derived Columns")

        try:
            derived_cols = table['derived_columns']
        except:
            derived_cols = False
            logger.info(f"{table_name} Table has no Derived Columns")

        business_key = table['business_key']


        logger.info(f"TRANSFORMATION -> WATERMARK EXTRACTION for {sink_name}...")
        ts = get_watermark_timestamp(spark=spark, target_table=sink_name)

        if ts is False:
            logger.info(f"TRANSFORMATION -> WATERMARK EXTRACTION Failed! False Returned")
            logger.info(f"SKIPPING TABLE: {sink_name}")
            continue
            #exit(100)

        logger.info(f"TRANSFORMATION -> WATERMARK EXTRACTION Successful")
        print(ts)

        logger.info(f"TRANSFORMATION -> LOADING Bronze table {source_name}...")
        bronze_df = load_bronze_data(spark=spark, source_table=source_name, ts=ts)

        if bronze_df is False:
            logger.info(f"TRANSFORMATION -> LOADING Failed! False Returned!")
            logger.info(f"SKIPPING TABLE: {sink_name}")
            #exit(100)
            continue
        logger.info(f"TRANSFORMATION -> LOADING Bronze table Successful!")


        logger.info(f"TRANSFORMATION -> FILTERING Dataframe...")
        filtered_df = filter_df_with_schema(df=bronze_df, schema=table_schema)
        if filtered_df is False:
            logger.info(f"TRANSFORMATION -> FILTERING Failed! False Returned!")
            logger.info(f"SKIPPING TABLE: {sink_name}")
            # exit(100)
            continue
        logger.info(f"TRANSFORMATION -> FILTERING Successful!")

        business_columns = list(filtered_df.columns)

        if pii_fields is not False:
            logger.info(f"TRANSFORMATION -> PII MASKING Dataframe...")
            masked_df = mask_pii_fields(df=filtered_df, masked_fields=pii_fields)
            if masked_df is False:
                logger.error(f"TRANSFORMATION -> PII MASKING FAILED: for {sink_name}")
                logger.info(f"SKIPPING TABLE: {sink_name}")
                continue
            logger.info(f"TRANSFORMATION -> PII MASKING Successful!")

        else:
            logger.info(f"TRANSFORMATION -> SKIPPING PII MASKING for table: {table_name}")
            masked_df = filtered_df


        if derived_cols is not False:
            logger.info(f"TRANSFORMATION -> DERIVING COLUMNS from Dataframe...")
            derived_df = add_derived_columns(df=masked_df, derived_cols=derived_cols)
            if derived_df is False:
                logger.error(f"TRANSFORMATION -> DERIVATION FAILED: for {sink_name}")
                logger.info(f"SKIPPING TABLE: {sink_name}")
                continue
            logger.info(f"TRANSFORMATION -> DERIVATION Successful!")

        else:
            logger.info(f"TRANSFORMATION -> SKIPPING DERIVED COLUMNS for table: {table_name}")
            derived_df = masked_df

        logger.info(f"TRANSFORMATION -> COMPUTE RECORD HASH...")
        hashed_df = compute_hash(df=derived_df, business_cols=business_columns)
        if hashed_df is False:
            logger.error(f"TRANSFORMATION -> COMPUTE RECORD HASH FAILED: for {sink_name}")
            logger.info(f"SKIPPING TABLE: {sink_name}")
            continue
        logger.info(f"TRANSFORMATION -> COMPUTE RECORD HASH Successful!")


        logger.info(f"TRANSFORMATION -> UPSERT PREPARATION...")
        prepped_df = prepare_for_upsert(df=hashed_df)
        if prepped_df is False:
            logger.error(f"TRANSFORMATION -> UPSERT PREPARATION FAILED: for {sink_name}")
            logger.info(f"SKIPPING TABLE: {sink_name}")
            continue
        logger.info(f"TRANSFORMATION -> UPSERT PREPARATION Successful!")

        prepped_df.show()

        logger.info(f"TRANSFORMATION -> UPSERT START...")
        success = upsert_dataframe(spark=spark, source_df=prepped_df, sink_table=sink_name, business_key=business_key)
        if success is False:
            logger.error(f"TRANSFORMATION -> UPSERT FAILED: for {sink_name}")
            logger.info(f"SKIPPING TABLE: {sink_name}")
            continue
        
        elif success is True:
            logger.info(f"TRANSFORMATION -> UPSERT Successful!")


def load_bronze_data(spark : SparkSession, source_table: str, ts : datetime):
    try:
        logger.info(f"LOADING: Table: {source_table}...")
        query = f"ingestion_ts > '{ts}'"
        df = (
            spark.read
                .format("delta")
                .table(source_table)
                .filter(query)
        )
        logger.info(f"LOADING: Table {source_table} Successfully!")
        return df

    except Exception as e:
        logger.exception(f"LOADING: Failed! Table {source_table} failed with error: {e}")
        logger.info(f"Returning False")
        return False


def get_watermark_timestamp(spark : SparkSession, target_table : str):
    """
    This function queries the silver table, and returns the max timestamp to be queried from the
    Bronze tables
    """
    try:
        logger.info(f"WATERMARK EXTRACTION: Trying to extract watermark from {target_table}")
        ts = (spark.read.format("delta")
              .table(target_table)
              .agg(max("effective_from"))).collect()[0][0]

        if ts is None:
            logger.info(f"WATERMARK EXTRACTION: Null timestamp returned, Defaulting to backfill date")
            return datetime(2000, 1, 1)

        else:
            return ts

    except Exception as e:
        logger.exception(f"WATERMARK EXTRACTION: Failed with error:")
        logger.info(f"WATERMARK EXTRACTION: Returning False")
        return False


def filter_df_with_schema(df : DataFrame, schema: dict):
    try:
        logger.info(f"FILTERING SCHEMA: Trying to Filter expected columns from the dataframe")
        print(schema)
        expected_cols = list(schema.keys())
        logger.info(f"FILTERING SCHEMA: Expected Columns: {expected_cols}")
        for column in schema.items():
            column_name = column[0]
            source_column = column[1]['derived_from']
            datatype = column[1]['type']

            df = df.withColumn(column_name, col(source_column).cast(datatype))

        filtered_df = df.select(*expected_cols)
        logger.info(f"FILTERING SCHEMA: Returning Filtered Dataframe")
        return filtered_df

    except Exception as e:
        logger.exception(f"FILTERING SCHEMA: Filtering failed with error: ")
        logger.info(f"FILTERING SCHEMA: Returning False")
        return False


def add_derived_columns(df : DataFrame, derived_cols : dict):
    try:
        logger.info(f"DERIVED COLUMNS: Adding Derived Columns into the Dataframe")
        for col in derived_cols:
            logger.info(f"DERIVED COLUMNS: Adding Column: {col}")
            source_col = col['derived_from']
            target_col = col['column']
            func = col['derived_using']

            if func == 'calculate_age':
                logger.info(f"DERIVED COLUMN: Function -> Calculate_age for Column: {target_col}")
                df = datediff_from_now(df=df, from_col=source_col, to_col=target_col)

            else:
                logger.error(f"DERIVED COLUMN: Failed!! Function: {func} is not configured yet!")
                logger.info(f"Returning False")
                return False
        return df

    except Exception as e:
        logger.exception(f"DERIVED COLUMN: Failed due to error:")
        return False


def datediff_from_now(df : DataFrame, from_col : str, to_col : str):
    return df.withColumn(to_col, floor(datediff(current_date(), col(from_col)) / 365.25))


def mask_pii_fields(df : DataFrame, masked_fields : list):
    try:
        logger.info(f"PII MASKING: ...")
        for field in masked_fields:
            logger.info(f"PII MASKING: Field: {field}")

            if field not in df.columns:
                logger.warning(f"PII MASKING: Field '{field}' not in DataFrame columns. Skipping.")
                continue

            if "email" in field.lower():
                print("Trying email masking")
                logger.info(f"PII MASKING: executing email masking for Field: {field}")
                df = df.withColumn(field, concat(
                    regexp_extract(col(field), r"^(.).*@", 1),  # ➤ first letter before @
                    lit("***@"),                                  # ➤ literal masking
                    substring_index(col(field), "@", -1)        # ➤ domain after @
                ))
                print("Done")

            elif "phone" in field.lower():
                logger.info(f"PII MASKING: executing phone number masking for Field: {field}")
                print("Trying phone masking")
                df = df.withColumn(
                    field,
                    when(
                        col(field).isNotNull(),
                        concat(
                            lit("***-***-"),
                            substring(col(field), length(col(field)) - 3, 4)
                        )
                    ).otherwise(None)
                )
                print("done")

            elif "card" in field.lower():
                print("Trying card masking")
                logger.info(f"PII MASKING: Executing Credit card number masking for Field: {field}")
                df = df.withColumn(
                    field,
                    when(
                        col(field).isNotNull(),
                        concat(
                            lit("***-***-"),
                            substring(col(field), length(col(field)) - 3, 4)
                        )
                    ).otherwise(None)
                )
                print("done")

            logger.info(f"PII MASKING: Completed masking for field: {field}")

        return df

    except Exception as e:
        logger.exception("PII MASKING: Failed with error:")
        return False


def compute_hash(df : DataFrame, business_cols : list):
    try:
        logger.info(f"HASHING: Trying to Hash the Dataframe")
        df = (df.withColumn("record_hash", md5(concat_ws("||", *[col(c) for c in business_cols]))))
        return df

    except Exception as e:
        logger.exception(f"HASHING: Failed with error:")
        logger.info(f"Returning False")
        return False


def upsert_dataframe(spark : SparkSession, source_df : DataFrame, sink_table : str, business_key : str):
    try:
        logger.info(f"UPSERTING DATA: Loading Target Object from: {sink_table}")
        target_df = DeltaTable.forName(sparkSession=spark, tableOrViewName=sink_table)

        merge_condition = f"tgt.{business_key} = src.{business_key} AND tgt.current_flag = true"
        value_expr = {col_name: f"src.{col_name}" for col_name in source_df.columns}

        logger.info(f"UPSERTING DATA: Dynamic Value Expression Generated: {value_expr}")

        target_df.alias("tgt").merge(
            source=source_df.alias("src"), condition=merge_condition
        ).whenMatchedUpdate(
            condition="tgt.record_hash <> src.record_hash",
            set={
                "effective_to": "current_timestamp()",
                "current_flag": "false"
            }
        ).whenNotMatchedInsert(
            values=value_expr
        ).execute()

        logger.info(f"UPSERTING DATA: Upsert Complete to Target")
        return True

    except Exception as e:
        logger.exception(f"UPSERTING DATA: Failed with error:")
        logger.info(f"Returning False")
        return False


def prepare_for_upsert(df : DataFrame):
    try:
        logger.info(f"UPSERT PREP: Starting prep for Upsert")
        df = (df.withColumn("effective_from", current_timestamp())
                .withColumn("effective_to", lit("3000-1-1 00:00:00").cast("timestamp"))
                .withColumn("current_flag", lit(True))
        )
        logger.info(f"UPSERT PREP: Done")
        return df

    except Exception as e:
        logger.exception(f"UPSERT PREP: Failed due to error:")
        logger.info(f"Returning False")
        return False


if __name__ == "__main__":
    transform_dims_to_silver()



