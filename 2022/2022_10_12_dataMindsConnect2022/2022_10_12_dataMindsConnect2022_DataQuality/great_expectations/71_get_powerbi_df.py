# Databricks notebook source
query = """
EVALUATE
SUMMARIZECOLUMNS(
    'Date'[Year],
    'Date'[MonthNameShort],
    KEEPFILTERS( TREATAS( {2022}, 'Date'[Year] )),
    "# Orders", [# Orders]
)
ORDER BY 
    'Date'[Year] ASC,
    'Date'[MonthNameShort] ASC
"""
group_id = "<your_group_id>"
dataset_id = "<your_dataset_id>"

# COMMAND ----------

import json, requests
from pyspark.sql import functions as F
try:
    from azure.identity import ClientSecretCredential
except Exception:
     !pip install azure.identity
     from azure.identity import ClientSecretCredential

# COMMAND ----------

# --------------------------------------------------------------------------------------#
# String variables: Replace with your own
tenant = '<your_tenant_id>'
client = '<your_client_id>'
client_secret = '<your_client_secret>' # Better to use key vault!
api = 'https://analysis.windows.net/powerbi/api/.default'
# --------------------------------------------------------------------------------------#

# COMMAND ----------

# Generates the access token for the Service Principal
auth = ClientSecretCredential(authority = 'https://login.microsoftonline.com/',
                                                        tenant_id = tenant,
                                                        client_id = client,
                                                        client_secret = client_secret)
access_token = auth.get_token(api)
access_token = access_token.token

print('\nSuccessfully authenticated.')

# COMMAND ----------

base_url = 'https://api.powerbi.com/v1.0/myorg/'
header = {'Authorization': f'Bearer {access_token}', "Content-Type": "application/json"}
api_function = f"groups/{group_id}/datasets/{dataset_id}/executeQueries"

request_body = {
  "queries": [
    {
      "query": query
    }
  ],
  "serializerSettings": {
    "includeNulls": True
  },
  #"impersonatedUserName": "inbox@daveruijter.nl"
}

response = requests.post(base_url + api_function, headers=header, json=request_body)

# COMMAND ----------

df = spark.read.json(sc.parallelize([json.loads(response.content)])).withColumn("exploded", F.explode("results")).withColumn("exploded", F.explode("exploded.tables")).withColumn("exploded", F.explode("exploded.rows")).select("exploded.*")
