# Databricks notebook source
# MAGIC %md
# MAGIC # Writing Python

# COMMAND ----------

print("Hello")

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing SQL.
# MAGIC
# MAGIC Put %sql in the first line.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Hello'

# COMMAND ----------

# MAGIC %md
# MAGIC # Shell commands.
# MAGIC
# MAGIC Put %sh at the first line.

# COMMAND ----------

# MAGIC %sh
# MAGIC ls ..

# COMMAND ----------

# MAGIC %md
# MAGIC #Magic Commands
# MAGIC
# MAGIC Run %lsmagic to list down all available magic commends.

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

# MAGIC %connect_info  

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Utils (DB Utils)
# MAGIC
# MAGIC https://docs.databricks.com/en/dev-tools/databricks-utils.html

# COMMAND ----------

# Creating a widget
dbutils.widgets.dropdown('my_dropdownnn', 
                         choices=['choice1', 'choice2', 'choice3'], 
                         label="TEST", 
                         defaultValue='choice1')

# COMMAND ----------

# Retrieving widget value
env_var = dbutils.widgets.get('env_var')

upload_to_db(param1 = env_var)

# COMMAND ----------

# MAGIC %md
# MAGIC # Installing Libraries - Notebook Level

# COMMAND ----------

# MAGIC %pip install pytest-playwright

# COMMAND ----------

# MAGIC %md
# MAGIC # Database, Schema, & Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists hive_metastore.dosm;

# COMMAND ----------

import pandas as pd
df_pandas = pd.read_parquet('https://storage.data.gov.my/pricecatcher/pricecatcher_2022-11.parquet')
if 'date' in df_pandas.columns: df_pandas['date'] = pd.to_datetime(df_pandas['date'])


# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

df_spark = spark.createDataFrame(df_pandas)

df_spark = df_spark.withColumn("ETL_datetime", lit(datetime.now().isoformat()))

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table hive_metastore.dosm.prices

# COMMAND ----------

df_spark.write.mode("overwrite").saveAsTable("hive_metastore.dosm.prices")

# COMMAND ----------

df_spark.display()