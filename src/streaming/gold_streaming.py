import os
import sys
from dotenv import load_dotenv
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.util import join_condition

load_dotenv()

sys.path.insert(0, '~/PycharmProjects/local_fdp_project/')

os.environ['LOGGER_NAME'] = "KAFKA_GOLD"
from src.common.logger import init_logger
logger = init_logger(os.environ.get("LOGGER_NAME"), logfile='gold.log')

from src.common.config_loader import load_local_stream_gold_configs, load_local_batch_gold_configs
from src.common.spark_utils import local_get_spark
from src.streaming.query_monitoring import QueryMonitoring

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, broadcast, expr
from deltalake import DeltaTable

def process_gold_tables():
    logger.info(f"GOLD STREAM: Starting Process...")

    configs = load_local_stream_gold_configs()
    if configs:
        logger.info(f"GOLD STREAM: configs loaded Successfully")
    else:
        logger.critical(f"GOLD STREAM: configs FAILED")
        exit(999)

    spark = local_get_spark()
    #spark.streams.addListener(QueryMonitoring)
    logger.info(f"GOLD STREAM: Spark Session Successfully obtained")

    for config in configs:
        print(config)
        stream_name         = config['job_name']
        stream_source_table = config['source_table']
        stream_sink_table   = config['output_table']
        logger.info(f"GOLD STREAM: Processing Stream: {stream_name}")

        try:
            stream_source_filters = [config['filter']]
            print("Filters are: ", stream_source_filters)
            logger.info(f"GOLD STREAM: Stream {stream_name} has filters...")
        except Exception as e:
            logger.info(f"GOLD STREAM: No Filters received for stream {stream_name}")
            stream_source_filters = False

        try:
            stream_joins = config['joins']
            logger.info(f"GOLD STREAM: Stream {stream_name} has joins...")
        except:
            logger.info(f"GOLD STREAM: No Filters received for stream {stream_name}")
            stream_joins = False

        source_cols = config['columns']

        print(stream_source_filters)
        df = trigger_stream(spark=spark, source=stream_source_table, filters=stream_source_filters)
        if df is False:
            logger.exception("GOLD STREAM: Df trigger returned False")
            logger.info(f"GOLD STREAM: Skipping Stream Triggering for source: {stream_name}")
            continue


        logger.info(f"GOLD STREAM: Filtering Source Columns...")
        schema_applied_df = apply_schema_to_dataframe(df=df, schema=source_cols)
        if df is False:
            logger.exception("GOLD STREAM: Filtering Source Columns Failed")
            logger.info(f"GOLD STREAM: Skipping Gold Processing for source: {stream_name}")
            continue
        logger.info(f"GOLD STREAM: Filtering Source Columns Successfully!!!")

        if stream_joins is not False:
            logger.info(f"GOLD STREAM: JOINING...")
            joined_df = handle_joins(spark=spark, source_df=schema_applied_df, join_dict=stream_joins)

        else:
            logger.info(f"GOLD STREAM: SKIPPING JOINS...")
            joined_df = schema_applied_df

        s = (joined_df.writeStream.format("console").start())
        s.awaitTermination(timeout=30)

        break



def trigger_stream(spark : SparkSession, source : str, filters : list = None):
    logger.info(f"TRIGGERING STREAM: for source {source} with filters {filters}")
    print("Filters in the trigger func:", filters)
    try:
        if filters is not False:
            df = (spark.readStream
                        .format("delta")
                        .option("readChangeFeed", "false")
                        .option("ignoreDeletes", "true")
                  .table(tableName=source)
                  .filter(*filters)
            )
        else:
            df = (spark.readStream
                  .format("delta")
                  .option("readChangeFeed", "false")
                  .option("ignoreDeletes", "true")
                  .table(tableName=source)
                  )
        logger.info(f"TRIGGERING STREAM: Stream Started from {source}")
        return df
    except Exception as e:
        logger.exception(f"TRIGGERING STREAM: ERROR Triggering stream")
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
        logger.info(f"FILTERING ST. DATAFRAME: Returning Filtered Dataframe")
        return filtered_df

    except Exception as e:
        logger.exception(f"FILTERING ST. DATAFRAME: Failed with error:")
        logger.info(f"Returning False")
        return False


def handle_joins(spark : SparkSession, source_df : DataFrame, join_dict : dict):
    try:
        logger.info(f"JOINS: Processing Joins")

        for table in join_dict:
            print(table)
            table_name = table['table']
            join_key = table['key']
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
                exp = f"{from_col} AS {to_col}"
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

                logger.info(f"JOINS: Attempting the Streaming Join")
                df.printSchema()
                source_df = source_df.alias("src").join(df.alias("tgt"), join_key, how="left")

        return source_df

    except Exception as e:
        logger.exception(f"JOINS: FAILED with error:")
        logger.info(f"Returning False")
        return False

def sink_streams(df : DataFrame, sink_table : str, checkpoint_loc : str, key):
    try:
        configs = {
            "sink": sink_table,
            "merge_key": key
        }
        logger.exception(f"SINK STREAMS: ...")
        streaming_query = (df.writeStream
             .foreachBatch(upsert_to_gold_stream_wrapper(configs))
             .outputMode("update")
             .option("checkpointLocation", checkpoint_loc)
             .start()
        )
        return streaming_query

    except Exception as e:
        logger.exception(f"SINK STREAMS: FAILED with error:")
        logger.info(f"Returning False")
        return False


def upsert_to_gold_stream_wrapper(configs):
    # These can be configurized for multiple streams
    time_col = "timestamp"
    sink = configs['sink']
    key = configs['merge_key']

    def upsert_to_gold(batch, batchId):
        gold_table = DeltaTable.forName(batch.sparkSession, sink)
        join_cond = f"t.{key} = s.{key}"

        (gold_table.alias("t")
         .merge(batch.alias("s"), join_cond)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

    return upsert_to_gold








if __name__ == "__main__":
    process_gold_tables()