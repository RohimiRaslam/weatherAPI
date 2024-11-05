# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import io

# COMMAND ----------

dbutils.secrets.listScopes()
dbutils.secrets.list('kotak-sakti-scope-111')


# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="rohimiadlskey")
spark.conf.set("fs.azure.account.key.rohimiadls.dfs.core.windows.net", storage_account_key)



# COMMAND ----------

df = (spark.read
      .option('header' , 'true')
      .option('inferSchema' , 'true')
      .option('delimiter' , '\t')
      .csv('abfss://incoming@adlskotaksakti1.dfs.core.windows.net/marketing_campaign.csv'))

# COMMAND ----------

df.limit(5).toPandas()

# COMMAND ----------

df.select('ID').distinct().count()

# COMMAND ----------


df.write.mode("overwrite").parquet("abfss://incoming@rohimiadls.dfs.core.windows.net/marketing_campaign_output")

# COMMAND ----------

df.write.mode('overwrite').partitionBy('Marital_Status').parquet('abfss://incoming@rohimiadls.dfs.core.windows.net/marketing_campaign_output_by_marital_status')

# COMMAND ----------

df.write.mode('overwrite').partitionBy('Marital_Status' , 'Education' , 'Response').parquet('abfss://incoming@rohimiadls.dfs.core.windows.net/marketing_campaign_output_by_multi_partition')

# COMMAND ----------

read_df = spark.read.option('mergeSchema' , 'true').parquet('abfss://incoming@rohimiadls.dfs.core.windows.net/marketing_campaign_output_by_multi_partition/Marital_Status=Divorced/Education=Graduation/Response=1')

# COMMAND ----------

read_df.limit(5).toPandas()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

