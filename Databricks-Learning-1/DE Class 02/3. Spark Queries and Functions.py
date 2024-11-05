# Databricks notebook source
# MAGIC %md
# MAGIC We can pre-load the same external notebook to authenticate ADLS connection.

# COMMAND ----------

# MAGIC %run "./Set ADLS Credentials"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Data
# MAGIC
# MAGIC We will use a data uploaded in ADLS.
# MAGIC
# MAGIC Data Source: https://www.kaggle.com/datasets/syedanwarafridi/vehicle-sales-data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data From ADLS

# COMMAND ----------

# Load data using PySpark
df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("abfss://containername@storageName.dfs.core.windows.net/folderName/fileName.csv"))
# Data is not loaded yet (lazy evaluation) so it will just show the columns
df

# COMMAND ----------

# Display data (this will start loading of data)

# df.display() 
# or 
display(df)

# COMMAND ----------

# Select certain columns using pyspark, then print only 5 rows
df.select("year",
          "model",
          "sellingprice").limit(5).display()

# COMMAND ----------

# Select certain columns using SQL
spark.sql("SELECT year, model, sellingprice, color from {df} LIMIT 5"
          ,df = df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change column names

# COMMAND ----------

# Using pyspark - Method 1
from pyspark.sql.functions import col

# This will select all and the additional 3 columns as selected below
df.withColumn("tahun", col("year")) \
  .withColumn("modellll", col("model")) \
  .withColumn("harga", col("sellingprice"))\
  .limit(5).display()

# COMMAND ----------

# Using pyspark - Method 2
df.select(col("year").alias('tahun'),
          col("model").alias('modelllll'),
          col("sellingprice").alias('harga'))\
          .limit(5).display()

# COMMAND ----------

# Using SQL
# selectExpr enable us to run sql-like expression but using python
df.selectExpr("year as tahun",
              "model as modellll", 
              "sellingprice as harga") \
  .limit(5).display()

# COMMAND ----------

# 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting schema of a Dataframe

# COMMAND ----------

df.describe()

# COMMAND ----------

df.schema

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converting Data Types

# COMMAND ----------

# Using pyspark - lets convert "sellingprice" from integer into float
# Pyspark - Method 1
df.select("*",   col("sellingprice").cast("float")
          ).printSchema()

#   This way, you have to specify the columns that you need.

# COMMAND ----------

# Pyspark - Method 2
df.withColumn( "sellingprice" ,col("sellingprice").cast("float")
          ).printSchema()


# COMMAND ----------

# Using SQL
spark.sql("select *, cast(sellingprice as float) as sellingprice from {df} LIMIT 5"
          , df=df).display()

# this method also will print all column together with new casted column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filters

# COMMAND ----------

# Pyspark - Method 1
df.where("   color = 'white'    ").limit(10).display()

# COMMAND ----------

df.where("color = 'white'   AND     sellingprice >= 50000   ").limit(10).display()

# COMMAND ----------

# Pyspark - Method 2
from pyspark.sql.functions import *
df.where(col("color") == 'white').limit(10).display()

# COMMAND ----------

df.where(  (col("color") == 'white') & (col("sellingprice") >= 50000  )  ).limit(10).display()

# COMMAND ----------

# Using SQL
spark.sql("""
          select * 
          from {df}
          where color = 'white' and sellingprice >= 50000
          limit 10
          """, df = df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Nulls

# COMMAND ----------

# Pyspark - use isNotNull() or isNull() also can

df.where(   (col("color").isNotNull()) &
            (col("sellingprice").isNotNull())
          ).display()

# COMMAND ----------

df.where("color is not null and sellingprice is not null").limit(7).display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Handling Date & Time

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
# https://docs.databricks.com/en/sql/language-manual/sql-ref-datetime-pattern.html

# Using Pyspark
df.withColumn("saledate1", to_timestamp_ntz("saledate")   )\
  .withColumn("saledate_extract", substring("saledate", 5, 11))   \
  .withColumn("saledate2", to_date(substring("saledate", 5, 11), "MMM dd yyyy")    )   \
  .withColumn("yearr", year(to_date(substring("saledate", 5, 11), "MMM dd yyyy"))    )   \
            .limit(10).display()

# Substring selects nth string until specified length
# Tue Dec 16 2014 12:30:00 GMT-0800 (PST)
# \\\\X\\\\\\\\\X
# \\\\5\\\\\\\\\11

# COMMAND ----------

# Using SQL
spark.sql("""
          SELECT *,
                  substring(saledate, 5, 11) as saledate_extract,
          to_date(substring(saledate, 5, 11), "MMM dd yyyy") as saledate2
          FROM {df}
          LIMIT 5
          
          """, df = df).createOrReplaceTempView("df_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC saledate2, 
# MAGIC year(saledate2), month(saledate2), day(saledate2)
# MAGIC FROM df_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation

# COMMAND ----------

# Pyspark
df.groupBy("make").agg(
                        avg("sellingprice").alias("avg_sellprice"), \
                        round(avg("sellingprice"),0).alias("avg_sellprice_rounded") , \
                        min("odometer")
                        )\
                  .orderBy("avg_sellprice_rounded", ascending=False).display()

# COMMAND ----------

# SQL
spark.sql("""
          SELECT 
            make, 
            avg(sellingprice) as avg_sellingprice, 
            round(avg(sellingprice),0) as avg_sellingprice_rounded,
            min(odometer) as min_odometer
          FROM {df}
          GROUP BY make
          ORDER BY avg_sellingprice_rounded DESC
          
          """, df = df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logics
# MAGIC - CASE WHEN

# COMMAND ----------

# Store the previous aggregation into a "view"
spark.sql("""
          SELECT 
            TRIM(UPPER(make)) as make, 
            avg(sellingprice) as avg_sellingprice, 
            round(avg(sellingprice),0) as avg_sellingprice_rounded,
            min(odometer) as min_odometer
          FROM {df}
          GROUP BY TRIM(UPPER(make))
          ORDER BY avg_sellingprice_rounded DESC
          
          """, df = df).createOrReplaceTempView("df_agg")

# COMMAND ----------

# MAGIC %md
# MAGIC Using Pyspark to perform CASE WHEN

# COMMAND ----------

# lets classify "cheap", "median" and "expensive" based on avg selling price
spark.table("df_agg").withColumn("classification", when(col("avg_sellingprice_rounded") < 10000, "cheap")
                                                   .when(col("avg_sellingprice_rounded") < 20000, "mid")
                                                   .otherwise("espensive!")                              
                                 
                                 ).drop("min_odometer").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Using SQL to perform CASE WHEN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df_agg LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lets classify "cheap", "median" and "expensive" based on avg selling price
# MAGIC SELECT
# MAGIC   make,
# MAGIC   avg_sellingprice_rounded,
# MAGIC   CASE 
# MAGIC     WHEN avg_sellingprice_rounded < 10000 THEN 'cheap'
# MAGIC     WHEN avg_sellingprice_rounded < 30000 THEN 'mid'
# MAGIC     ELSE 'expensive'
# MAGIC     END as classification
# MAGIC FROM df_agg
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins

# COMMAND ----------

from pyspark.sql.functions import *

# Import other data (car brand ranking)
rank = spark.read.options(**{"inferSchema":"true", 
                    "header":"true"})\
          .csv("abfss://carprices@azwanadlsdev01.dfs.core.windows.net/raw/brand_ranking.csv")

rank = rank.withColumn("Brand", upper(col("Brand")))
rank.display()

# COMMAND ----------

# Join using pyspark
df_agg = spark.table("df_agg")

joined_df = df_agg.join(
            rank, 
            df_agg['make'] == rank['Brand'],
            "inner")


joined_df.select("make", "avg_sellingprice_rounded", "Position").display()

# COMMAND ----------

# %sql
# SELECT .... INNER JOIN ....
# ON .....