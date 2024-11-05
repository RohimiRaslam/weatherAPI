# Databricks notebook source
from pyspark.sql.functions import *
import pandas as pd

spark.conf.set("fs.azure.account.key.adlskotaksakti1.dfs.core.windows.net",
                dbutils.secrets.get(scope="kotak-sakti-scope-111", key="accesskey-adls-adlskotaksakti1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data into variable
# MAGIC - df_2022
# MAGIC - df_2024

# COMMAND ----------

### 2022-01 Data
df_2022 = pd.read_parquet('https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet')
if 'date' in df_2022.columns: df_2022['date'] = pd.to_datetime(df_2022['date'])

# COMMAND ----------

### 2024-06 Data
df_2024 = pd.read_parquet('https://storage.data.gov.my/pricecatcher/pricecatcher_2024-06.parquet')
if 'date' in df_2024.columns: df_2024['date'] = pd.to_datetime(df_2024['date'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save into Delta Lake format

# COMMAND ----------

# MAGIC %md
# MAGIC enableChangeDataFeed = True
# MAGIC
# MAGIC This is to enable us to see historical data based on "batch data load version".

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# Save 2022 data
df_spark_22 = spark.createDataFrame(df_2022)

df_spark_22.write.mode("append").format("delta").save("abfss://incoming@adlskotaksakti1.dfs.core.windows.net/raw_dosm")

# COMMAND ----------

# Save 2024 data
df_spark_24 = spark.createDataFrame(df_2024, schema=df_spark_22.schema)

df_spark_24.write.mode("append").format("delta").save("abfss://incoming@adlskotaksakti1.dfs.core.windows.net/raw_dosm")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from Delta Lake table

# COMMAND ----------

df_lake = spark.read.format("delta").load("abfss://incoming@adlskotaksakti1.dfs.core.windows.net/raw_dosm")
spark.sql("""
          SELECT DISTINCT year(date), month(date) 
          FROM {df}
          """
          , df = df_lake).display()

# COMMAND ----------

df_history = (spark.read.format("delta")
                    .option("readChangeFeed", "true").option("startingVersion", 0)
                    .load("abfss://incoming@adlskotaksakti1.dfs.core.windows.net/raw_dosm")
                    .where("premise_code = 3178 and item_code = 1")
                    .orderBy("_commit_version")
            )
df_history.display()