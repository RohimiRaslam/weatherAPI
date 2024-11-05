# Databricks notebook source
# MAGIC %run ./template

# COMMAND ----------

today = date.today()
silver_input = f'abfss://bronze@rohimiadls.dfs.core.windows.net/marketing_campaign/marketing_campaign_output_{today}'
df3 = spark.read.parquet(silver_input)

# COMMAND ----------

filename = f'gold_marketing_output__{today}'
gold_data_directory = f'abfss://gold@rohimiadls.dfs.core.windows.net/marketing_campaign/{filename}'
df3.write.mode('append').parquet(gold_data_directory)

# COMMAND ----------

