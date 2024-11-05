# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hive_metastore.etl_test

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

now_datetime = datetime.now()

curr_date = now_datetime.date().isoformat()
curr_hr = now_datetime.hour + 8
curr_min = now_datetime.minute


# COMMAND ----------

df = spark.createDataFrame([(curr_date, curr_hr, curr_min)]
                      , ["current_date", "current_hour", "current_minute"])

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode("append").saveAsTable("hive_metastore.etl_test.raw")