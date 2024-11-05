# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import io


# COMMAND ----------

# to list out the scopes used to allow databricks to access storage account
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('kotak-sakti-scope-111')

# COMMAND ----------

#to list the key inside a particular scope
#storage_account_key = dbutils.secrets.list('kotak-sakti-scope-111')
# dbutils.secrets.list('kotak-sakti-scope-111')

# COMMAND ----------

#storage_account_key = dbutils.secrets.get(scope = 'kotak-sakti-scope-111' , key = 'rohimiadlskey')
#spark.conf.set('fs.azure.account.key.rohimiadls.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="rohimiadlskey")
spark.conf.set("fs.azure.account.key.rohimiadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# pull data from data lake into datalake (because we specify adls)
#spark.conf.set('fs.azure.account.key.adlskotaksakti1.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

# move data from datalake into the incoming folder
df = (spark.read
      .option('header' , 'true')
      .option('inferSchema' , 'true')
      .option('delimiter' , '\t')
      .csv('abfss://incoming@adlskotaksakti1.dfs.core.windows.net/marketing_campaign.csv'))

# COMMAND ----------

df.write.mode("overwrite").parquet("abfss://incoming@syafeeqadls.dfs.core.windows.net/marketing_campaign_output")

# COMMAND ----------

# display our data
df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2024-10-01 saving dataframe

# COMMAND ----------

df.head()

# COMMAND ----------

abfss://ingoing@adam98adls.dfs.core.windows.net/marketing_campaign_output

# COMMAND ----------

df.write.mode('overwrite').parquet('abfss://ingoing@rohmiadls.dfs.core.windows.net/marketing_campaign_output/')