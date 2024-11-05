# Databricks notebook source
# MAGIC %run ./../template

# COMMAND ----------

#read from silver storage
silver_path = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/A_2_silver'
df_original = spark.read\
      .option('header' , 'true')\
      .option('inferSchema' , 'true')\
      .option('delimiter' , '\t')\
      .parquet(silver_path)

# COMMAND ----------

# remove unnecessary columns
df_original_columns = df_original.columns
df_modified = df_original.drop(
    'location longitude.',
    'location latitude.',
    'location localtime epoch.',
    'current last updated epoch.',
    'current last updated.',
    'current condition icon.',
    'current condition code.',
    'current wind degree.',
    'current cloud condition.',
    'current windchill (c).',
    'current windchill (f).',
    'current heatindex (c).',
    'current heatindex (f).',
    'current dewpoint (c).',
    'current dewpoint (f).',
    'current vis (km).',
    'current vis (miles).',
    'current uv.',
)

# rename a few columns
df_modified_columns = df_modified.columns
df_modified_columns_new = [i.replace('location ' , '').replace('current ' , '') for i in df_modified_columns]
df_modified = df_modified \
    .withColumnsRenamed({df_modified_columns[i] : df_modified_columns_new[i] for i in range(len(df_modified_columns))}) \
    .withColumnRenamed('name.', 'location.') \
    .withColumnRenamed('condition text.', 'condition.')

# convert datatype of timestamp
df_modified = df_modified.withColumn('localtime.', col('`localtime.`').cast(TimestampType())).sort('`localtime.`' , ascending=True)

# create new columns for year, month, day, hour, minute
df_modified = df_modified \
    .withColumn('year.' , year(col('`localtime.`'))) \
    .withColumn('month.' , month(col('`localtime.`'))) \
    .withColumn('day.' , day(col('`localtime.`'))) \
    .withColumn('hour.' , hour(col('`localtime.`'))) \
    .withColumn('minute.' , minute(col('`localtime.`'))) \
    .withColumn('is day or night.',when(col('`is day or night.`') == 1, 'Day').otherwise('Night'))

# reorder columns 
df = df_modified.select(
    '`location.`',
    '`region.`',
    '`country.`',
    '`timezone id.`',
    '`localtime.`',
    '`year.`',
    '`month.`',
    '`day.`',
    '`hour.`',
    '`minute.`',
    '`is day or night.`',
    '`condition.`',
    '`temperature (c).`',
    '`temperature (f).`',
    '`humidity (%).`',
    '`feelslike (c).`',
    '`feelslike (f).`',
    '`precipitation (mm).`',
    '`precipitation (in).`',
    '`wind speed (kph).`',
    '`wind speed (mph).`',
    '`wind pressure (mb).`',
    '`wind pressure (in).`',
    '`gust (kph).`',
    '`gust (mph).`',
    '`wind direction.`',
)


df.limit(5).toPandas()

# COMMAND ----------

# to do: create metric and imperial df, push to snowflake

# COMMAND ----------

# create metric df by dropping imperial units
metric_df = df.drop(
    'temperature (f).',
    'feelslike (f).',
    'precipitation (in).',
    'wind speed (mph).',
    'wind pressure (in).',
    'gust (mph).'
)
len(metric_df.columns)

# COMMAND ----------

df.columns

# COMMAND ----------

# create metric df by dropping imperial units
imperial_df = df.drop(
    'temperature (c).',
    'feelslike (c).',
    'precipitation (mm).',
    'wind speed (kph).',
    'wind pressure (mb).',
    'gust (kph).',
)
imperial_df.limit(10).toPandas()

# COMMAND ----------

gold_path_metric = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/A_3_gold/metric'
metric_df.write.format('parquet').mode('overwrite').save(gold_path_metric)

gold_path_imperial = f'abfss://weatherapi@rohimiadls.dfs.core.windows.net/A_3_gold/imperial'
imperial_df.write.format('parquet').mode('overwrite').save(gold_path_imperial)

# COMMAND ----------

# MAGIC %md
# MAGIC # Push to snowflake

# COMMAND ----------

options = {
  "sfUrl": "https://isocbxh-er59203.snowflakecomputing.com",
  "sfUser": 'PROJECTSMITH',
  "sfPassword": 'Mrohimi95',
  "sfDatabase": "WEATHERAPI2",
  "sfSchema": "RAW",
  "sfWarehouse": "COMPUTE_WH",
  "sfRole": "ACCOUNTADMIN"
}

# COMMAND ----------

metric_df.write.format("snowflake").options(**options).option("dbtable", "metric_table").mode("overwrite").save()
imperial_df.write.format("snowflake").options(**options).option("dbtable", "imperial_table").mode("overwrite").save()

# COMMAND ----------

df.toPandas().tail()