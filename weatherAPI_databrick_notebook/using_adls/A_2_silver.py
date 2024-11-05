# Databricks notebook source
# MAGIC %md
# MAGIC ### this could be the actual silver process

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform data

# COMMAND ----------

# MAGIC %run ./../template

# COMMAND ----------

bronze_path = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/A_1_bronze'
df_original = spark.read.parquet(bronze_path)

# COMMAND ----------

# save column names as list
df_columns_original = df_original.columns
# remove . and _ from each elements of the list
df_columns_modified = [i.replace('.' , ' ').replace('_' , ' ') for i in df_columns_original]
# create a dictionary of old and new list
df_columns_modified_dict = {df_columns_original[i] : f'{df_columns_modified[i]}.' for i in range(len(df_columns_original))}
# rename the columns using created dictionary
df_staging = df_original.withColumnsRenamed(df_columns_modified_dict)

# fix the name of the columns
df_staging = df_staging \
    .withColumnRenamed('location lon.' , 'location longitude.') \
    .withColumnRenamed('location lat.' , 'location latitude.') \
    .withColumnRenamed('location tz id.' , 'location timezone id.') \
    .withColumnRenamed('current temp c.' , 'current temperature c.') \
    .withColumnRenamed('current temp f.' , 'current temperature f.') \
    .withColumnRenamed('current is day.' , 'current is day or night.') \
    .withColumnRenamed('current wind kph.' , 'current wind speed kph.') \
    .withColumnRenamed('current wind mph.' , 'current wind speed mph.') \
    .withColumnRenamed('current wind dir.' , 'current wind direction.') \
    .withColumnRenamed('current precip mm.' , 'current precipitation mm.') \
    .withColumnRenamed('current precip in.' , 'current precipitation in.') \
    .withColumnRenamed('current humidity.' , 'current humidity %.') \
    .withColumnRenamed('current cloud.' , 'current cloud condition.') \
    .withColumnRenamed('current pressure mb.' , 'current wind pressure mb.') \
    .withColumnRenamed('current pressure in.' , 'current wind pressure in.')

df_staging_columns = df_staging.columns
# # braces the metric unit
df_staging_columns1 = [i.replace(' c.' , ' (c).') for i in df_staging_columns]
df_staging_columns2 = [i.replace(' kph.' , ' (kph).') for i in df_staging_columns1]
df_staging_columns3 = [i.replace(' mb.' , ' (mb).') for i in df_staging_columns2]
df_staging_columns4 = [i.replace(' mm.' , ' (mm).') for i in df_staging_columns3]
df_staging_columns5 = [i.replace(' km.' , ' (km).') for i in df_staging_columns4]
df_staging_columns6 = [i.replace(' %.' , ' (%).') for i in df_staging_columns5]

# # braces the imperial unit
df_staging_columns7 = [i.replace(' f.' , ' (f).') for i in df_staging_columns6]
df_staging_columns8 = [i.replace(' mph.' , ' (mph).') for i in df_staging_columns7]
df_staging_columns9 = [i.replace(' in.' , ' (in).') for i in df_staging_columns8]
df_staging_columns10 = [i.replace(' miles.' , ' (miles).') for i in df_staging_columns9]

df_staging_columns_dct = {df_staging_columns[i] : df_staging_columns10[i] for i in range(len(df_staging_columns))}

# rename the columns using created dictionary
df = df_staging.withColumnsRenamed(df_staging_columns_dct).dropna()
df.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC # saving to adls

# COMMAND ----------

silver_path = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/A_2_silver'
df.write.format('parquet').mode('overwrite').save(silver_path)