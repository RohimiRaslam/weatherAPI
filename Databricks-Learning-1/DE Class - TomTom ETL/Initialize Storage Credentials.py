# Databricks notebook source
# MAGIC %md
# MAGIC ## Get your access keys from here, but this is not a good practice!
# MAGIC
# MAGIC ![](./AccessKeys.png)
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.key.azwanadlsdev01.dfs.core.windows.net",
                "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

print("ADLS Linked!")