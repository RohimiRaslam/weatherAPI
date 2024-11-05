# Databricks notebook source
# MAGIC %run ./template

# COMMAND ----------

today = date.today()
bronze_input = f'abfss://bronze@rohimiadls.dfs.core.windows.net/marketing_campaign/marketing_campaign_output_{today}'
df2 = spark.read.parquet(bronze_input)

# COMMAND ----------

# MAGIC %md
# MAGIC #Do some cleaning here in this column

# COMMAND ----------

filename = f'silver_marketing_output__{today}'
silver_data_directory = f'abfss://silver@rohimiadls.dfs.core.windows.net/marketing_campaign/{filename}'
df2.write.mode('append').parquet(silver_data_directory)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

