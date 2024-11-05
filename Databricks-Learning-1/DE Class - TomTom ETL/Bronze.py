# Databricks notebook source
# MAGIC %run "./Initialize Storage Credentials"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Change Data Feed = Enables a historical view of our data change over the time (deleted/inserted/updated etc)
# MAGIC 1. Brief overview https://docs.databricks.com/en/delta/delta-change-data-feed.html
# MAGIC
# MAGIC 2. This file format (delta lake instead of parquet/csv) enables us to store historical change https://docs.delta.io/latest/delta-change-data-feed.html

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable change data feed
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# Load data from a simple API using requests, then load into Pandas dataframe (not Spark dataframe)
import requests
import pandas as pd
data = requests.get("https://api.midway.tomtom.com/ranking/liveHourly/MYS_kuala-lumpur").json()['data']
pd.json_normalize(data)

# COMMAND ----------

# Convert into Pandas Dataframe and parse the UpdateTime column
df = pd.json_normalize(data)
df.head()

# COMMAND ----------

# Convert into Spark Dataframe
df = spark.createDataFrame(df)
# Print the first 10 rows
df.limit(10).display()

# COMMAND ----------

# Import Pyspark functions (https://api-docs.databricks.com/python/pyspark/latest/pyspark.sql/functions.html)
from pyspark.sql.functions import *

# 'expr' enables us to write SQL-like code inside the brackets
# Otherwise, we have to use Python-like code to transform the columns. Both are equivalent and depends on personal preference
df_cleaned = (df
    .withColumn("UpdateTime", expr("CAST(from_unixtime(updatetime/1000) as TIMESTAMP)"))
    .withColumn("UpdateTimeWeekAgo", expr("CAST(from_unixtime(UpdateTimeWeekAgo/1000) as TIMESTAMP)"))
    .withColumn("date", expr("date(UpdateTime)"))
    .withColumn("hour", expr("hour(UpdateTime)"))
    .withColumn("minute", expr("minute(UpdateTime)"))
)

df_cleaned.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC NOTE: To understand how Databricks call database/schema/catalog
# MAGIC
# MAGIC ![](./Naming conventions.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Option 1) Using default database (hive_metastore), we create a new schema
# MAGIC -- CREATE SCHEMA IF NOT EXISTS hive_metastore.tomtom;
# MAGIC
# MAGIC -- Option 2) We also can create our own database like KOTAKSAKTI or something. It will appear in our "managed location" that was created during Databricks creation
# MAGIC CREATE DATABASE IF NOT EXISTS kotaksakti;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show catalogs 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving Data
# MAGIC Now its time to save our data. There are 3 methods.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A) In Catalog - Managed Table
# MAGIC
# MAGIC
# MAGIC - When we create a table in database, of course there will be underlying data stored somewhere
# MAGIC - The storage location is managed by Databricks inside its own Azure resource group in our case
# MAGIC - https://docs.databricks.com/en/tables/index.html#managed-tables
# MAGIC
# MAGIC Pros
# MAGIC - we dont have to manually specify data location
# MAGIC - shorter codes
# MAGIC
# MAGIC Cons 
# MAGIC - if we drop the table, the underlying data also are gone

# COMMAND ----------

# This will create MANAGED TABLE at specified database and schema

df_cleaned.write.format("delta")\
    .mode("append") \
    .saveAsTable("kotaksakti.bronze")
    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### B) In Catalog - External Table
# MAGIC
# MAGIC - When we create a tabke in database, of course there will be underlying data stored somewhere
# MAGIC - The storage location is set by us inside a cloud storage (Azure ADLS/Amazon S3 etc)
# MAGIC - https://docs.databricks.com/en/tables/index.html#external-tables
# MAGIC
# MAGIC Pros
# MAGIC - if we drop the table, the underlying data are still there
# MAGIC - we can control data access at ADLS side
# MAGIC
# MAGIC Cons 
# MAGIC - just a few lines extra code

# COMMAND ----------

## First step: Create the schema under hive_metastore
## We cannot reuse the databricks_kotak_sakti_dev1 catalog because its already tied to a managed external location
spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.kotaksakti;")

## Second step: Create table under the created schema above
df_cleaned.write.format("delta")\
    .mode("append") \
    .option("path", "abfss://incoming@adlskotaksakti1.dfs.core.windows.net/tomtom_demo/bronze_ext/")\
    .saveAsTable("kotaksakti.bronze_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kotaksakti.bronze_ext

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### C) In Cloud Storage Only
# MAGIC - just store the data in cloud storage such as Azure ADLS/Amazon S3
# MAGIC - we dont create tables in Databricks catalog
# MAGIC - if we want to query the data, we have to use 
# MAGIC   ```df = spark.read.format(...)...```
# MAGIC - https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html
# MAGIC
# MAGIC Pros
# MAGIC - lesser stuff to take care of
# MAGIC - useful if we access data from somewhere else instead of using Databricks Catalog
# MAGIC
# MAGIC Cons
# MAGIC - when variety of data gets bigger, we tend to lose track of what kind of data that we have

# COMMAND ----------

# Writing Spark dataframe into a specific folder in Azure Data Lake
df_cleaned.write.format("delta")\
    .mode("append") \
    .save("abfss://incoming@adlskotaksakti1.dfs.core.windows.net/tomtom_demo/bronze_storage_only/")
