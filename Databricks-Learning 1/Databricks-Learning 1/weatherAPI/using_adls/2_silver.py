# Databricks notebook source
# MAGIC %md
# MAGIC # Transform data

# COMMAND ----------

# MAGIC %run ./../template

# COMMAND ----------

bronze_path = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/1_bronze'
df = spark.read.parquet(bronze_path)
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract the following columns:  
# MAGIC 1. location name 
# MAGIC 2. location local time
# MAGIC 3. current temperature in c
# MAGIC 4. current humidity
# MAGIC 5. current feels like in c

# COMMAND ----------

select = ['location.name' , 'location.localtime' , 'current.temp_c' , 'current.humidity' , 'current.feelslike_c']
select_backtick = [f"`{name}`" for name in select]
new_select = ['location name' , 'local time' , 'current temp in celcius' , 'current humidity in %' , 'feels like in celcius']
dct = {select[i] : new_select[i] for i in range(len(select))}

transformed_df = df.select(select_backtick)\
    .withColumnsRenamed(dct)

transformed_df = transformed_df \
    .withColumn('local time' , date_format('local time' , 'yyyy-MM-dd HH:mm')).sort('local time' , ascending=True)

transformed_df.limit(5).toPandas()

# COMMAND ----------

f'{transformed_df.count()} : {df.count()}'

# COMMAND ----------

silver_path = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/2_silver'
transformed_df.write.format('parquet').mode('overwrite').save(silver_path)

# COMMAND ----------

