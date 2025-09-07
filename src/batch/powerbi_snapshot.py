import os
from src.common.spark_utils import local_get_spark

# === Configuration ===
DELTA_TABLE_PATH = "file:///C:/fraud_detection_project/gold/user_fraud_summary"  # path to your delta table
EXPORT_FORMAT = "parquet"  # can also be "csv"
EXPORT_PATH = f"file:///C:/fraud_detection_project/powerbi/user_fraud_summary/"

os.environ['SPARK_JOB_PORT'] = "4040"


def main():
    # Initialize Spark with Delta Lake support
    spark = local_get_spark()

    # Read the Delta table
    df = spark.read.format("delta").load(DELTA_TABLE_PATH)

    # Export to Parquet or CSV
    if EXPORT_FORMAT == "parquet":
        # Coalesce to 1 file to simplify Power BI loading
        df.coalesce(1).write.mode("overwrite").option("header", True).parquet(EXPORT_PATH)
    elif EXPORT_FORMAT == "csv":
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(EXPORT_PATH)

    else:
        print(f"Unsupported export format: {EXPORT_FORMAT}")
        return

    print(f"Delta table exported to {EXPORT_PATH} as {EXPORT_FORMAT.upper()}")

    spark.stop()


if __name__ == "__main__":
    main()
