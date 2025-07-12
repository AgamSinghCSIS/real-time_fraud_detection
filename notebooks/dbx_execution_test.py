import os
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
from databricks.connect import DatabricksSession

#pdf = pd.read_csv("../data/customers.csv")

#print(os.environ.get("DATABRICKS_HOST"), os.environ.get("DATABRICKS_TOKEN"), os.environ.get("DATABRICKS_CLUSTER_ID"))
spark = DatabricksSession.builder.getOrCreate()

#df = spark.createDataFrame(pdf)

"""(df.write
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("outputMode", "append")
      .save('abfss://raw@safrauddetection.dfs.core.windows.net/test/test_load'))
"""
df2 = (spark.read
       .option("header","true")
       .csv('abfss://raw@safrauddetection.dfs.core.windows.net/test/test_load'))

df2.show()
