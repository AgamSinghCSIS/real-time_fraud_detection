import os
from dotenv import load_dotenv
load_dotenv()

os.environ['LOGGER_NAME'] = "KAFKA_TRANSFORMATION"
from src.common.logger import init_logger
logger = init_logger(os.environ.get("LOGGER_NAME"), logfile='transformation.log')

import geoip2.database
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    col, udf, broadcast, collect_list, struct
)
from pyspark.sql.functions import (
    explode, col, size, collect_set, sum,
    min, max, count, expr, when, array_contains,
    size, lit, struct
)
from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType, LongType
GEOIP_DB_PATH = '/Volumes/fraud_catalog/silver/geoip2_volume/GeoLite2-Country.mmdb'

from src.common.config_loader import load_stream_transformation_configs
from src.common.dbx_utils import safe_get_spark
from src.streaming.query_monitoring import QueryMonitoring
#from src.rules_engine.rule_suite import get_context_aggregated_df, apply_fraud_rules

def transform_streams():
    logger.info(f"STREAM TRANSFORMATION: Starting the Stream job!")

    logger.info(f"STREAM TRANSFORMATION: Loading Job configs")
    streams = load_stream_transformation_configs()

    logger.info(f"STREAM TRANSFORMATION: Register Remote Spark Session -> Databricks")
    spark = safe_get_spark()
    queries = []
    for stream in streams:
        stream_name = stream['name']
        stream_source = stream['source_table']
        stream_sink = stream['sink_table']
        stream_checkpoint = stream['checkpoint_location']

        schema = stream['schema']

        try:
            lookups =  stream['lookups']
        except:
            logger.info(f"NO Lookups for stream: {stream_name}")
            lookups = False

        try:
            joins =  stream['joins']
        except:
            logger.info(f"NO Join tables for stream: {stream_name}")
            joins = False

        logger.info(f"STREAM TRANSFORMATION -> Triggering Bronze Readstream...")
        bronze_df = trigger_bronze_readstream(spark=spark, table_name=stream_source)
        if bronze_df is False:
            logger.critical(f"STREAM TRANSFORMATION -> Triggering Bronze Readstream FAILED")
            exit(100)
        logger.info(f"STREAM TRANSFORMATION -> Triggering Bronze Readstream SUCCESSFUL")

        logger.info(f"STREAM TRANSFORMATION -> Filtering Streaming Dataframe...")
        filtered_df = filter_dataframe_using_schema(df=bronze_df, schema=schema)
        if filtered_df is False:
            logger.critical(f"STREAM TRANSFORMATION -> Filtering Streaming Dataframe FAILED")
            exit(100)
        logger.info(f"STREAM TRANSFORMATION -> Filtering Streaming Dataframe SUCCESSFUL")

        if lookups is not False:
            logger.info(f"STREAM TRANSFORMATION -> Adding Lookups...")
            lookup_df = handle_lookups(df=filtered_df, lookup_list=lookups)
            if lookup_df is False:
                logger.critical(f"STREAM TRANSFORMATION -> FAILED Adding Lookups ")
                exit(100)
        else:
            logger.info(f"LOOKUPS: No Lookups, so carrying Filtered Df Forward")
            lookup_df = filtered_df

        if joins is not False:
            logger.info(f"STREAM TRANSFORMATION -> Doing Joins...")
            joined_df = handle_joins(spark=spark, source_df=lookup_df, join_dict=joins)
            if joined_df is False:
                logger.critical(f"STREAM TRANSFORMATION -> FAILED Joins")
                exit(100)
        else:
            logger.info(f"JOINS: No joins, so carrying joined Df Forward")
            joined_df = lookup_df

        if stream_name == 'transactions':
            spark.table("fraud_catalog.silver.transactions").printSchema()

            logger.info(f"STREAM TRANSFORMATION -> Processing Stream with Fraud Logic")
            print("IN TRansactions block")
            query = (joined_df.writeStream
                            .foreachBatch(run_fraud_logic)
                            .option("checkpointLocation", stream_checkpoint)
                            .queryName(queryName=f"{stream_name}_StreamingQuery")
                            .start()
            )
            queries.append(query)

        else:
            query = (joined_df.writeStream
                     .format("delta")
                     .outputMode("append")
                     .option("checkpointLocation", stream_checkpoint)
                     .queryName(queryName=f"{stream_name}_StreamingQuery")
                     .toTable(stream_sink)
            )
            queries.append(query)

    for q in queries:
        q.awaitTermination()


def trigger_bronze_readstream(spark : SparkSession, table_name : str):
    try:
        logger.info(f"STREAM TRIGGER: Adding Streaming Dataframe from {table_name}")
        df = (spark.readStream
                    .format("delta")
                    .option("readChangeFeed", "false")
                    .option("ignoreDeletes", "true")
              .table(tableName=table_name)
        )
        logger.info(f"STREAM TRIGGER: Returning Streaming Dataframe")
        return df

    except Exception as e:
        logger.exception(f"STREAM TRIGGER: Failed with error:")
        logger.info(f"Returning False")
        return False


def filter_dataframe_using_schema(df : DataFrame, schema : dict):
    try:
        business_cols = list(schema.keys())
        logger.info(f"FILTERING ST. DATAFRAME: Business Columns: {business_cols}")
        for column in schema.items():
            column_name = column[0]
            source_column = column[1]['derived_from']
            datatype = column[1]['type']
            df = df.withColumn(column_name, col(source_column).cast(datatype))

        filtered_df = df.select(*business_cols)
        logger.info(f"FILTERING ST. DATAFRAME: Returning Filtered Dataframe")
        return filtered_df

    except Exception as e:
        logger.exception(f"FILTERING ST. DATAFRAME: Failed with error:")
        logger.info(f"Returning False")
        return False


def handle_lookups(df : DataFrame, lookup_list : list):
    try:
        logger.info(f"LOOKUPS: Processing Lookups: {lookup_list}")
        for lookup in lookup_list:
            using_column = lookup['column']
            to_column    = lookup['new_column']

            if lookup['udf'] == 'ip_to_country':
                logger.info(f"LOOKUPS: Calling UDF {lookup['udf']} with Column: {using_column}")
                ip_to_country_udf = udf(ip_to_country, StringType())
                df = df.withColumn(to_column, ip_to_country_udf(col(using_column)))
                logger.info(f"LOOKUPS: Added Column {to_column}")

            else:
                logger.error(f"LOOKUPS: FAILED, Lookup Function has not been defined yet. Failing...")
                exit(100)
        return df

    except Exception as e:
        logger.exception(f"LOOKUPS: FAILED with error:")
        logger.info(f"Returning False")
        return False


def ip_to_country(ip):
    if not hasattr(ip_to_country, "reader"):
        ip_to_country.reader = geoip2.database.Reader(GEOIP_DB_PATH)

    try:
        return ip_to_country.reader.country(ip).country.name
    except:
        return "Unknown"


def handle_joins(spark : SparkSession, source_df : DataFrame, join_dict : dict):

    try:
        logger.info(f"JOINS: Processing Joins")
        #start_count = source_df.count()
        #logger.info(f"JOINS: SOURCE COUNT Is: {start_count}")

        for table in join_dict:
            table_name = table['table']
            join_key = table['key']
            broadcast_flag = table['broadcast']
            columns = table['columns']
            logger.info(f"JOINS: Joining Table {table_name} using key: {join_key}")

            select_expr = [join_key]
            for column in columns.items():
                to_col = column[0]
                from_col = column[1]['derived_from']
                expr = f"{from_col} AS {to_col}"
                select_expr.append(expr)

            df = (spark.read
                  .format("delta")
                  .table(tableName=table_name)
                  .selectExpr(*select_expr)
                  .dropDuplicates([join_key])
            )
            #df.show()

            if broadcast_flag is True:
                logger.info(f"JOINS: Broadcasting Dataframe {table_name}")
                df = broadcast(df)

            logger.info(f"JOINS: Attempting the Streaming Join")
            source_df = source_df.alias("src").join(df.alias("tgt"), join_key, how="left")

        #end_count = source_df.count()
        #logger.info(f"JOINS: FINAL COUNT Is: {end_count}")

        """if end_count != start_count:
            return
        else:"""
        return source_df

    except Exception as e:
        logger.exception(f"JOINS: FAILED with error:")
        logger.info(f"Returning False")
        return False


def run_fraud_logic(batch_df, batch_id):
    print("Getting context df:...")
    context_df = get_context_aggregated_df(batch_df.sparkSession)

    if context_df is False:
        logger.warning(f"FRAUD LOGIC: Skipping batch {batch_id} due to context error")
        return

    print(f"Batch columns: {batch_df.columns}")
    print(f"Context columns: {context_df.columns}")

    context_df.show()

    print("joining the dataframe with context ")
    enriched_df = batch_df.join(context_df, on="card_id", how="left")

    print(f"Apply fraud rules bruh")
    # Apply rule logic
    flagged_df = apply_fraud_rules(enriched_df)

    context_cols_to_drop = [
        "ip_country_set",
        "txn_count_last_10_min",
        "total_spent_last_hour",
        "first_seen_device_ts",
        "agg_card_limit"
    ]

    flagged_df = (flagged_df
                  .drop(*context_cols_to_drop)
                  .withColumn("amount", col("amount").cast("double"))
                  .withColumn("is_chargeback", col("is_chargeback").cast("boolean"))
                  .withColumn("merchant_risk_score", col("merchant_risk_score").cast("int"))
    )
    flagged_df.printSchema()

    # Write flagged data to sink
    (flagged_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("fraud_catalog.silver.transactions")
    )

def apply_fraud_rules(df: DataFrame) -> DataFrame:
    """
    Apply fraud rules to the enriched transaction DataFrame.

    Assumes `df` has:
    - current_txn fields (card_id, amount, device_id, ip_country, timestamp, etc.)
    - context fields (card_limit, txn_count_last_10_min, ip_countries_set, etc.)
    """

    # RULE 1: Sudden IP country change (not in past IPs)
    print("rule1")
    rule_1 = ~array_contains(col("ip_country_set"), col("ip_country"))
    print("rule2")
    # RULE 2: More than 5 txns in 10 mins
    rule_2 = col("txn_count_last_10_min") > 5
    print("rule3")
    # RULE 3: High value volume > 60% of card limit in last 1 hour
    rule_3 = (col("total_spent_last_hour") / col("agg_card_limit")) > 0.6

    print("rule4")
    # RULE 4: New device (registered < 4 hrs ago) + txn > 50% of card limit
    rule_4 = (
        (col("timestamp").cast("long") - col("first_seen_device_ts").cast("long")) < 14400  # 4 hrs = 14400 secs
    ) & ((col("amount") / col("agg_card_limit")) > 0.5)

    print("Write the flag")
    # Assign flags and rule labels
    df = df.withColumn(
        "fraud_flag",
        when(rule_1, lit(True))
        .when(rule_2, lit(True))
        .when(rule_3, lit(True))
        .when(rule_4, lit(True))
        .otherwise(lit(False))
    )

    print("write which rule")
    df = df.withColumn(
        "rule_triggered",
        when(rule_1, lit("Sudden IP Country Change"))
        .when(rule_2, lit("High Txn Volume (10 min)"))
        .when(rule_3, lit("High Value Spending (1 hr)"))
        .when(rule_4, lit("New Device + High Amount"))
        .otherwise(lit(None))
    )

    return df


def get_context_aggregated_df(spark : SparkSession):
    try:
        table_name='fraud_catalog.silver.transactions'
        logger.info("FETCHING CONTEXT:Trying to get context Dataframe")
        print("Spark read")

        context_cols_to_keep = [
            "card_id",
            "ip_country_set",
            "txn_count_last_10_min",
            "total_spent_last_hour",
            "first_seen_device_ts",
            "agg_card_limit"
        ]

        context_df = (spark.read
                            .format("delta")
                            .table(tableName=table_name)
                            .filter("timestamp >= now() - interval 1 hour")
        )

        if context_df.limit(1).count() == 0:
            logger.warning("FETCHING CONTEXT: No recent transactions in past 1 hour")
            empty_schema = StructType([
                StructField("card_id", StringType(), True),
                StructField("ip_country_set", ArrayType(StringType()), True),
                StructField("txn_count_last_10_min", LongType(), True),
                StructField("total_spent_last_hour", DoubleType(), True),
                StructField("first_seen_device_ts", LongType(), True),
                StructField("agg_card_limit", DoubleType(), True)
            ])

            return spark.createDataFrame([], empty_schema)

        logger.info(f"FETCHING CONTEXT: Grouping the Context Dataframe")
        print(f"Doing the aggregate")
        aggregated_df = (
            context_df.groupBy("card_id")
            .agg(
                collect_set("ip_country").alias("ip_country_set"),
                count(
                    expr("CASE WHEN timestamp >= now() - interval 10 minutes THEN 1 END"
                         ).alias("txn_count_last_10_min")),
                count(
                    expr("CASE WHEN amount / card_limit > 0.6 THEN 1 END")
                        ).alias("high_value_txn_count"),
                sum("amount").alias("total_spent_last_hour"),
                min("device_registered_at").alias("first_seen_device_ts"),
                max("card_limit").alias("agg_card_limit")
            )

        )

        aggregated_df = aggregated_df.select(*context_cols_to_keep)
        aggregated_df.show()
        return aggregated_df

    except Exception as e:
        logger.exception(f"FETCHING CONTEXT: FAILED with error:")
        logger.info(f"Returning False")
        return False


if __name__ == "__main__":
    transform_streams()