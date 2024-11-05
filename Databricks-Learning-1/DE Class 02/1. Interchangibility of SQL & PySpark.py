# Databricks notebook source
from pyspark.sql.functions import *
import pandas as pd

# COMMAND ----------

# Fill your storage account name & account key
spark.conf.set("fs.azure.account.key.adlskotaksakti1.dfs.core.windows.net",
                dbutils.secrets.get(scope="kotak-sakti-scope-111", key="accesskey-adls-adlskotaksakti1")
                )

# COMMAND ----------

# MAGIC %md
# MAGIC ## From PySpark into SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1:
# MAGIC ```
# MAGIC Read data from a source, then store as variable
# MAGIC ---------------------------------------------------------------------------------
# MAGIC df_python = spark.read.... 
# MAGIC
# MAGIC Use SQL to read that variable above
# MAGIC ---------------------------------------------------------------------------------
# MAGIC spark.sql("""
# MAGIC           SELECT * FROM {df}
# MAGIC           """, df = df_python)
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC Documentation: https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html
# MAGIC
# MAGIC More examples: https://sparkbyexamples.com/spark/spark-read-options/

# COMMAND ----------

# Read from a data source
df_python = (spark.read
      .option("inferSchema", "true")
      .parquet("abfss://incoming@adlskotaksakti1.dfs.core.windows.net/stolen_cars.parquet")
      )

# Similar to pandas --> df.head()
df_python.limit(5).display()

# COMMAND ----------

# Lazy to write Python code? use SQL
df_sql = spark.sql("""
          SELECT vehicle_type, count(date_stolen) as total_stolen
          FROM {df} 
          GROUP BY vehicle_type         
          """, df = df_python) #<<------- Here we reference the word in curly bracket with the variable in cell above

# COMMAND ----------

df_sql.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC ### Pattern 2:
# MAGIC
# MAGIC Read from data source, store into a variable, same like pattern 1
# MAGIC ---------------------------------------------------------------------------------
# MAGIC df_python = spark.read.... 
# MAGIC
# MAGIC Store it as a "view" that only exist during this session (temporary view) 
# MAGIC ---------------------------------------------------------------------------------
# MAGIC df_python.createOrReplaceTempView(viewName)
# MAGIC
# MAGIC Use SQL to select from the view
# MAGIC ---------------------------------------------------------------------------------
# MAGIC spark.sql("""
# MAGIC           SELECT *
# MAGIC           FROM viewName
# MAGIC
# MAGIC         """)
# MAGIC ```

# COMMAND ----------

df_python = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("abfss://incoming@adlskotaksakti1.dfs.core.windows.net/mudah-apartment-kl-selangor.csv")
      )

df_python.createOrReplaceTempView("propView")

# COMMAND ----------

df_sql_2 = spark.sql("SELECT monthly_rent, location, rooms FROM propView")

df_sql_2.limit(5).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT monthly_rent, location, rooms FROM propView

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## From SQL into Python
# MAGIC
# MAGIC Have to load into temporary view, then only can load into Python DataFrame.
# MAGIC
# MAGIC ```
# MAGIC CREATE OR REPLACE TEMP VIEW someView as 
# MAGIC SELECT * 
# MAGIC FROM read_files(...)
# MAGIC ------------------------------
# MAGIC
# MAGIC df_python = spark.sql("SELECT * FROM someView")
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW propViewSQL as 
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM read_files(
# MAGIC                 'abfss://incoming@adlskotaksakti1.dfs.core.windows.net/mudah-apartment-kl-selangor.csv',
# MAGIC                 format => 'csv',
# MAGIC                 header => true)

# COMMAND ----------

df_python_2 = spark.sql("SELECT * FROM propViewSQL")
df_python_2.select("monthly_rent").display()