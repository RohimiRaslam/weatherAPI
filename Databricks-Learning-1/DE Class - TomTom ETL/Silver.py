# Databricks notebook source
# MAGIC %sql
# MAGIC -- We need to run this in each notebook unless we have a cluster wide setting
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog databricks_kotak_sakti_dev1

# COMMAND ----------

# Read data from Catalog and store into variable
df = spark.table("hive_metastore.kotaksakti.bronze_ext")

# COMMAND ----------

### If first time run, we execute the following code manually, otherwise our silver table is empty

# (df.write
#     .format("delta")
#     .mode("overwrite")
#     .option("path", "abfss://incoming@adlskotaksakti1.dfs.core.windows.net/tomtom_demo/silver")
#     .saveAsTable("hive_metastore.kotaksakti.silver")
# )

### Comment out after run once


# COMMAND ----------

# MAGIC %md
# MAGIC In this Silver layer, we just de-duplicate data from the previous stage (bronze or raw). We don't apply any other transformation to make it simple.
# MAGIC
# MAGIC Perform merge/upsert: https://learn.microsoft.com/en-us/azure/databricks/delta/merge

# COMMAND ----------

from delta.tables import *

# Get a target location, which is the data that we saved previously
target = DeltaTable.forPath(spark, "abfss://incoming@adlskotaksakti1.dfs.core.windows.net/tomtom_demo/silver")
source = df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Using the target, we perform UPSERT/MERGE operation
# MAGIC The reason we want to merge is we want to
# MAGIC - only insert new non overlapping data
# MAGIC - remove duplicate
# MAGIC - update old rows if applicable
# MAGIC
# MAGIC

# COMMAND ----------

output_status = (target.alias("target")
            .merge(source.alias("source")     , "source.date = target.date and source.hour = target.hour and source.minute = target.minute")
            .whenNotMatchedInsertAll()
            .execute()
 )

print('num_inserted_rows:', output_status.collect()[0]['num_inserted_rows'], '\n',
      'num_affected_rows', output_status.collect()[0]['num_affected_rows'], '\n',
      'num_deleted_rows', output_status.collect()[0]['num_deleted_rows'], '\n',
      'num_updated_rows', output_status.collect()[0]['num_updated_rows'])