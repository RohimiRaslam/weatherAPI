# Databricks notebook source
# MAGIC %md
# MAGIC run this notebook from another notebook as a template to import essential libraries and adls authentication using the following command:  
# MAGIC `%run ./template`

# COMMAND ----------

from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import io
from datetime import date

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="rohimiadlskey")
spark.conf.set("fs.azure.account.key.rohimiadls.dfs.core.windows.net", storage_account_key)