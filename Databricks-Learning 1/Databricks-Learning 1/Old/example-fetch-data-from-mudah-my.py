# Databricks notebook source
import requests
import pandas as pd
response = requests.get("https://gateway.mudah.my/property-search/public/v1/apartment/filter?category=2020&limit=20&property_type=1&region=9").json()['attributes']
#response = pd.json_normalize(response)
df = pd.DataFrame(response)
df.head()

# COMMAND ----------

import requests
import pandas as pd
response = requests.get("https://gateway.mudah.my/property-search/public/v1/apartment/filter?category=2020&limit=20&property_type=1&region=9").json()['attributes']
response = pd.json_normalize(response)
df = pd.DataFrame(response)
df.head()