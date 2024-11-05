# Databricks notebook source
# MAGIC %run ./template

# COMMAND ----------

df = (spark.read
      .option('header' , 'true')
      .option('inferSchema' , 'true')
      .option('delimiter' , '\t')
      .csv('abfss://sampah@rohimiadls.dfs.core.windows.net/output.parquet'))

# COMMAND ----------

type(df)

# COMMAND ----------

df.limit(5).toPandas()