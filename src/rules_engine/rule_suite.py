import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode, col, size, collect_set, sum,
    min, max, count, expr, when, array_contains,
    size, lit, struct
)

from pyspark.sql.types import StructType

from src.common.logger import get_logger
logger =  get_logger(os.environ.get("LOGGER_NAME"))


def apply_fraud_rules(df: DataFrame) -> DataFrame:
    """
    Apply fraud rules to the enriched transaction DataFrame.

    Assumes `df` has:
    - current_txn fields (card_id, amount, device_id, ip_country, timestamp, etc.)
    - context fields (card_limit, txn_count_last_10_min, ip_countries_set, etc.)
    """

    # RULE 1: Sudden IP country change (not in past IPs)
    rule_1 = ~array_contains(col("ip_countries_set"), col("ip_country"))

    # RULE 2: More than 5 txns in 10 mins
    rule_2 = col("txn_count_last_10_min") > 5

    # RULE 3: High value volume > 60% of card limit in last 1 hour
    rule_3 = (col("total_spent_last_hour") / col("agg_card_limit")) > 0.6

    # RULE 4: New device (registered < 4 hrs ago) + txn > 50% of card limit
    rule_4 = (
        (col("timestamp").cast("long") - col("first_seen_device_ts").cast("long")) < 14400  # 4 hrs = 14400 secs
    ) & ((col("amount") / col("agg_card_limit")) > 0.5)

    # Assign flags and rule labels
    df = df.withColumn(
        "fraud_flag",
        when(rule_1, lit(True))
        .when(rule_2, lit(True))
        .when(rule_3, lit(True))
        .when(rule_4, lit(True))
        .otherwise(lit(False))
    )

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
        context_df = (spark.read
                            .format("delta")
                            .table(tableName=table_name)
                            .filter("timestamp >= now() - interval 1 hour")
        )

        if context_df.limit(1).count() == 0:
            logger.warning("FETCHING CONTEXT: No recent transactions in past 1 hour")
            return spark.createDataFrame([], schema=context_df.schema)

        logger.info(f"FETCHING CONTEXT: Grouping the Context Dataframe")
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
                max("card_limit").alias("card_limit")
            )

        )

        aggregated_df.show()
        return aggregated_df

    except Exception as e:
        logger.exception(f"FETCHING CONTEXT: FAILED with error:")
        logger.info(f"Returning False")
        return False



