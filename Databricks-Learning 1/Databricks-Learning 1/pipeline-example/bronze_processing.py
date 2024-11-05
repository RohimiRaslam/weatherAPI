# Databricks notebook source
# MAGIC %run ./template

# COMMAND ----------

df = (spark.read
      .option('header' , 'true')
      .option('inferSchema' , 'true')
      .option('delimiter' , '\t')
      .csv('abfss://incoming@adlskotaksakti1.dfs.core.windows.net/marketing_campaign.csv'))


# COMMAND ----------

type(df)

# COMMAND ----------

today = date.today()
output_folder_path = f'abfss://bronze@rohimiadls.dfs.core.windows.net/marketing_campaign/marketing_campaign_output_{today}'
print(output_folder_path)

# COMMAND ----------

df.write.mode('overwrite').parquet(output_folder_path)

# COMMAND ----------



# COMMAND ----------

