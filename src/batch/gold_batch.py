import os
import sys
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, '~/PycharmProjects/local_fdp_project/')

os.environ['LOGGER_NAME'] = "GOLD_BATCH"
os.environ['SPARK_JOB_PORT'] = "4040"

from src.common.logger import init_logger
logger = init_logger(os.environ.get("LOGGER_NAME"), logfile='gold.log')

from src.common.config_loader import load_local_batch_gold_configs
from src.common.spark_utils import local_get_spark
from src.batch.batch_metrics import collect_and_push_metrics

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, broadcast, expr
from prometheus_client import CollectorRegistry
from time import perf_counter

LAYER = "GOLD"
PIPELINE_NAME = "GOLD_BATCH_OVERWRITE"


def process_gold_tables():

    logger.info(f"GOLD BATCH: Starting Process...")

    configs = load_local_batch_gold_configs()
    if configs:
        logger.info(f"GOLD BATCH: configs loaded Successfully")
    else:
        logger.critical(f"GOLD BATCH: configs FAILED")
        exit(999)

    spark_get_start = perf_counter()
    spark = local_get_spark()
    spark_get_end = perf_counter()

    spark_startup_time = spark_get_end - spark_get_start

    logger.info(f"GOLD BATCH: Spark Session Successfully obtained")

    for config in configs:
        processing_time_start = perf_counter()
        REGISTRY = CollectorRegistry()

        batch_name         = config['job_name']
        batch_source_table = config['source_table']
        batch_sink_table   = config['output_table']
        logger.info(f"GOLD BATCH: Processing batch: {batch_name}")
        print("Gold start for ", batch_name)

        try:
            batch_source_filters = [config['filter']]
            logger.info(f"GOLD BATCH: BATCH {batch_name} has filters...")
        except Exception as e:
            logger.info(f"GOLD BATCH: No Filters received for BATCH {batch_name}")
            batch_source_filters = False

        try:
            batch_joins = config['joins']
            logger.info(f"GOLD BATCH: BATCH {batch_name} has joins...")
        except:
            logger.info(f"GOLD BATCH: No Filters received for BATCH {batch_name}")
            batch_joins = False

        source_cols = config['columns']

        df = read_batch(spark=spark, source=batch_source_table, filters=batch_source_filters)

        if df is False:
            logger.exception("GOLD batch: Df trigger returned False")
            logger.info(f"GOLD batch: Skipping batch Triggering for source: {batch_name}")
            continue
        logger.info(f"GOLD batch: Filtering Source Columns...")
        schema_applied_df = apply_schema_to_dataframe(df=df, schema=source_cols)

        if df is False:
            logger.exception("GOLD batch: Filtering Source Columns Failed")
            logger.info(f"GOLD batch: Skipping Gold Processing for source: {batch_name}")
            continue
        logger.info(f"GOLD batch: Filtering Source Columns Successfully!!!")

        if batch_joins is not False:
            logger.info(f"GOLD batch: JOINING...")
            joined_df = handle_joins(spark=spark, source_df=schema_applied_df, join_dict=batch_joins)

        else:
            logger.info(f"GOLD batch: SKIPPING JOINS...")
            joined_df = schema_applied_df

        sink_write(df=joined_df, sink_table=batch_sink_table)
        processing_time = perf_counter() - processing_time_start
        collect_and_push_metrics(spark=spark, df=df, pipeline_layer=LAYER, job_name=batch_name,
                                 spark_startup_time=spark_startup_time, processing_duration=processing_time,
                                 registry=REGISTRY)
    print("ALL SUCCESSFUL RUNS")


def read_batch(spark : SparkSession, source : str, filters : list = None):
    logger.info(f"TRIGGERING batch: for source {source} with filters {filters}")
    try:
        if filters is not False:
            df = (spark.read
                        .format("delta")
                  .table(tableName=source)
                  .filter(*filters)
            )
        else:
            df = (spark.read
                  .format("delta")
                  .table(tableName=source)
                  )
        logger.info(f"TRIGGERING batch: batch Started from {source}")
        return df
    except Exception as e:
        logger.exception(f"TRIGGERING batch: ERROR Triggering batch")
        return False

def apply_schema_to_dataframe(df : DataFrame, schema : dict):
    logger.info(f"FILTERING SOURCE:...")
    try:
        business_cols = list(schema.keys())
        logger.info(f"FILTERING SOURCE:: Business Columns: {business_cols}")
        for column in schema.items():
            column_name = column[0]
            source_column = column[1]['derived_from']
            datatype = column[1]['type']
            df = df.withColumn(column_name, col(source_column).cast(datatype))

        filtered_df = df.select(*business_cols)
        logger.info(f"FILTERING DATAFRAME: Returning Filtered Dataframe")
        return filtered_df

    except Exception as e:
        logger.exception(f"FILTERING DATAFRAME: Failed with error:")
        logger.info(f"Returning False")
        return False


def handle_joins(spark : SparkSession, source_df : DataFrame, join_dict : dict):
    try:
        logger.info(f"JOINS: Processing Joins")

        for table in join_dict:

            table_name = table['table']
            join_key = table['key']
            print("Joining ", table_name)
            try:
                broadcast_flag = table['broadcast']
            except:
                broadcast_flag = False

            try:
                group_by = [table['group_by']]
            except:
                group_by = False

            columns = table['columns']
            logger.info(f"JOINS: Joining Table {table_name} using key: {join_key}")

            select_expr = [join_key]
            for column in columns.items():
                to_col = column[0]
                from_col = column[1]['derived_from']
                type = column[1]['type']
                exp = f"CAST({from_col} AS {type}) AS {to_col}"
                select_expr.append(exp)

            if group_by is not False:
                logger.info(f"JOINS: GroupBY Clause detected for {table_name}")
                select_expr.remove(join_key)
                df = (spark.read
                      .format("delta")
                      .table(tableName=table_name)
                      .groupBy(*group_by)
                      .agg(*[expr(e) for e in select_expr])
                )

                renamed_df = df.withColumnRenamed(join_key, f"{join_key}_drop")
                if broadcast_flag is True:
                    logger.info(f"JOINS: Broadcasting Dataframe {table_name}")
                    renamed_df = broadcast(renamed_df)
                renamed_df.printSchema()
                source_df = source_df.alias("src").join(renamed_df.alias("tgt"), source_df[join_key] == renamed_df[f"{join_key}_drop"], how="left")
                source_df = source_df.drop(f"{join_key}_drop")
                fill_defaults = {
                    "total_transactions": 0,
                    "total_amount_spent": 0.0,
                    "avg_transaction_amount": 0.0,
                    "fraud_transaction_count": 0,
                    # timestamp column intentionally skipped
                }
                source_df = source_df.fillna(fill_defaults)

            else:
                df = (spark.read
                      .format("delta")
                      .table(tableName=table_name)
                      .selectExpr(*select_expr)
                      .dropDuplicates([join_key])
                )

                if broadcast_flag is True:
                    logger.info(f"JOINS: Broadcasting Dataframe {table_name}")
                    df = broadcast(df)

                logger.info(f"JOINS: Attempting the batching Join")
                df.printSchema()
                source_df = source_df.alias("src").join(df.alias("tgt"), join_key, how="left")

        return source_df

    except Exception as e:
        logger.exception(f"JOINS: FAILED with error:")
        logger.info(f"Returning False")
        return False

def sink_write(df : DataFrame, sink_table : str):
    try:
        (df.write
             .format("delta")
             .mode("overwrite")
             .saveAsTable(sink_table)
        )
    except Exception as e:
        logger.exception(f"SINK WRITE: FAILED with error:")
        logger.info(f"Returning False")
        return False


if __name__ == "__main__":

    process_gold_tables()