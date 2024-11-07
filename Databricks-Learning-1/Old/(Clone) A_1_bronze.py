# Databricks notebook source
dct = {'Johor': 'Johor Bahru', 'Kedah': 'Alor Setar', 'Kelantan': 'Kota Bharu', 'Melaka': 'Malacca City', 'Negeri Sembilan': 'Seremban',
'Pahang': 'Kuantan', 'Pulau Pinang': 'George Town', 'Perak': 'Ipoh', 'Perlis': 'Kangar',
'Sabah': 'Kota Kinabalu', 'Sarawak': 'Kuching', 'Selangor': 'Shah Alam', 'Terengganu': 'Kuala Terengganu'}

States = [name for name in dct.keys()]
# States
location = [name for name in dct.values()]
# location


# COMMAND ----------

base_url = 'http://api.weatherapi.com/v1'
api_key = '98078c8de2274791b03161315240410'

# COMMAND ----------

def get_current_weather(api_key, location, date):
    base_url = base_url
    history_weather_url = base_url + "/history.json"
    params = {"key": api_key, "q": location}
    response = requests.get(history_weather_url, params=params)
    return response.json()

# COMMAND ----------

def run_current(api_key , cities):
    get_current_weather(api_key , cities)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## job to ingest data into adls

# COMMAND ----------

# MAGIC %run ./../template

# COMMAND ----------

# url = 'https://api.weatherapi.com/v1/current.json?key=98078c8de2274791b03161315240410&q=kuala lumpur&aqi=no'

base_url = 'http://api.weatherapi.com/v1'
api_key = '98078c8de2274791b03161315240410'

def get_current_weather(api_key, location, date):
    base_url = base_url
    history_weather_url = base_url + "/history.json"
    params = {"key": api_key, "q": location}
    response = requests.get(history_weather_url, params=params)
    return response.json()

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

bronze_path = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/A_1_bronze'
spark_df.write.format('parquet').mode('append').save(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # End

# COMMAND ----------

bronze_path = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/A_1_bronze'
df_dummy = spark.read.parquet(bronze_path)
df_dummy.sort('`location.localtime`' , ascending=True).limit(5).toPandas()

# COMMAND ----------

list = [f'`{df_dummy.columns[i]}`' for i in range(len(df_dummy.columns))]

for col_name in list:
    # print(f"Distinct values in {col_name}:")
    type(df_dummy.select(col_name).distinct().count())

# COMMAND ----------

list = [f'`{df_dummy.columns[i]}`' for i in range(len(df_dummy.columns))]
list

# COMMAND ----------

from pyspark.sql import functions as F

for name in list:
    # Assuming df is your DataFrame
    df = df_dummy.select(F.sum(F.col(name).isNull().cast("int")).alias(name))
    df.show()


# COMMAND ----------

from pyspark.sql import functions as F

for name in list:
    # Assuming df is your DataFrame
    df2 = df_dummy.select(F.sum(F.col(name).isNotNull().cast("int")).alias(name))
    df2.show()


# COMMAND ----------

# df_dummy.select('`location.localtime`').distinct().sort('`location.localtime`' , ascending = True).display()
df_dummy.filter(None)