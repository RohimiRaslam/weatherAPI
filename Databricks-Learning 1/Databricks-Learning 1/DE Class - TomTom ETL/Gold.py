# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.kotaksakti.silver

# COMMAND ----------

# MAGIC %md
# MAGIC Create a "metric" using the non-duplicate latest data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Managed Table
# MAGIC CREATE OR REPLACE TABLE hive_metastore.kotaksakti.gold AS
# MAGIC SELECT
# MAGIC   avg(traveltimelive) as avg_time_per_km,
# MAGIC   avg(jamslength) as avg_jam_length_km,
# MAGIC   hour
# MAGIC FROM hive_metastore.kotaksakti.silver
# MAGIC GROUP BY hour
# MAGIC ORDER BY hour

# COMMAND ----------

spark.sql("drop table hive_metastore.kotaksakti.gold")

# COMMAND ----------

# Or using Python 
df = spark.table("hive_metastore.kotaksakti.silver")

import pyspark.sql.functions as F

df_to_save = (df.groupBy("hour").agg(F.avg("traveltimelive").alias("avg_time_per_km"),
                        F.avg("jamslength").alias("avg_jam_length_km")
                        )
                    .sort("hour")
            )


(df_to_save
    .write
    .mode("overwrite")
    .saveAsTable("hive_metastore.kotaksakti.gold")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.kotaksakti.gold