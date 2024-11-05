# Databricks notebook source
# MAGIC %md
# MAGIC # Push to snowflake

# COMMAND ----------

# MAGIC %run ./../template

# COMMAND ----------

silver_path = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/2_silver'

# COMMAND ----------

# move data from datalake into the incoming folder
df = (spark.read
      .option('header' , 'true')
      .option('inferSchema' , 'true')
      .option('delimiter' , '\t')
      .parquet(silver_path))

# COMMAND ----------

df.toPandas().tail()

# COMMAND ----------

options = {
  "sfUrl": "https://isocbxh-er59203.snowflakecomputing.com",
  "sfUser": 'PROJECTSMITH',
  "sfPassword": 'Mrohimi95',
  "sfDatabase": "WEATHERAPI",
  "sfSchema": "RAW",
  "sfWarehouse": "COMPUTE_WH",
  "sfRole": "ACCOUNTADMIN"
}

# COMMAND ----------

df.write.format("snowflake").options(**options).option("dbtable", "weatherapi").mode("overwrite").save()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

gold_path_imperial = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/A_3_gold/imperial'
df = spark.read.parquet(gold_path_imperial)
df.count()

# COMMAND ----------

df.display().

# COMMAND ----------

import datetime
def get_last_3days():
    today = datetime.datetime.now()
    last_3days = [ (today - datetime.timedelta(days=x)).strftime("%Y-%m-%d") for x in range(3) ]
    return last_3days

get_last_3days()

# COMMAND ----------

def get_cities_to_weather():
    brasilian_states   = [ 'Acre', 'Alagoas', 'Amapá', 'Amazonas', 'Bahia', 'Ceará', 'Espírito Santo', 'Goiás', 'Maranhão', 'Mato Grosso', 'Mato Grosso do Sul', 'Minas Gerais', 'Pará', 'Paraíba', 'Paraná', 'Pernambuco', 'Piauí', 'Rio de Janeiro', 'Rio Grande do Norte', 'Rio Grande do Sul', 'Rondônia', 'Roraima', 'Santa Catarina', 'São Paulo', 'Sergipe', 'Tocantins' ]
    brasilian_capitals = [ 'Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador', 'Fortaleza',  'Vitória', 'Goiânia', 'São Luís', 'Cuiabá', 'Campo Grande', 'Belo Horizonte', 'Belém', 'João Pessoa', 'Curitiba', 'Recife', 'Teresina', 'Rio de Janeiro', 'Natal', 'Porto Alegre', 'Porto Velho', 'Boa Vista', 'Florianópolis', 'São Paulo', 'Aracaju', 'Palmas' ]
    cities = [ x + ", " + y + ', Brazil' for x, y in zip(brasilian_capitals, brasilian_states) ]
    cities_unaccented = [ x.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u').replace('ã', 'a').replace('õ', 'o').replace('ç', 'c') for x in cities]
    cities_upper = [ x.upper() for x in cities_unaccented ]
    cities = cities_upper
    return cities
get_cities_to_weather()

# COMMAND ----------

import pandas as pd

save_path = ""

def create_current_weather_file(cities,save_path = save_path):
    current_df = pd.DataFrame()
    infos = ['query']
    for a in cities:
        print(a)
        # record_id = uuid.uuid4()

create_current_weather_file(get_cities_to_weather())