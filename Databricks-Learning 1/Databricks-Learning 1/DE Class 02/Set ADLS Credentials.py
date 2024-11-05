# Databricks notebook source
# https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes
spark.conf.set("fs.azure.account.key.azwanadlsdev01.dfs.core.windows.net",
                dbutils.secrets.get(scope = "scope_name_here", key = "key_name_here")

print("ADLS Linked!")