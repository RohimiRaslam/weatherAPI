# Databricks notebook source
# MAGIC %md
# MAGIC ## job to ingest data into adls

# COMMAND ----------

# MAGIC %run ./../template

# COMMAND ----------

url = 'https://api.weatherapi.com/v1/current.json?key=98078c8de2274791b03161315240410&q=kuala lumpur&aqi=yes'
# timezone = pytz.timezone('Asia/Kuala_Lumpur')
# current_datetime_kl = datetime.now(timezone).strftime('%Y-%m-%d_%H:%M')

# COMMAND ----------

def fetch_data_from_API():
    raw_data = requests.get(url).json()
    pandas_df = pd.json_normalize(raw_data)
    return pandas_df
df1 = fetch_data_from_API()
df1

# COMMAND ----------

spark_df = spark.createDataFrame(df1)

# COMMAND ----------

bronze_path = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/1_bronze'
spark_df.write.format('parquet').mode('append').save(bronze_path)