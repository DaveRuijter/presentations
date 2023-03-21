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
group_id = ""
dataset_id = ""

# COMMAND ----------

import json, requests
from pyspark.sql import functions as F
from azure.identity import ClientSecretCredential

# COMMAND ----------

# --------------------------------------------------------------------------------------#
# String variables: Replace with your own
tenant = ''
client = ''
client_secret = '' # Better to use key vault!
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
#   "serializerSettings": {
#     "includeNulls": True
#   },
  #"impersonatedUserName": ""
}

response = requests.post(base_url + api_function, headers=header, json=request_body)
print(f"API call response: {response.status_code}")

# COMMAND ----------

print(json.dumps(json.loads(response.content), indent=4, sort_keys=True))

# COMMAND ----------

df_from_json = spark.read.json(sc.parallelize([json.loads(response.content)]))
df = df_from_json.withColumn("exploded", F.explode("results")).withColumn("exploded", F.explode("exploded.tables")).withColumn("exploded", F.explode("exploded.rows")).select("exploded.*")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run data validation

# COMMAND ----------

dataset_name = "pbi_orders_by_month"

# COMMAND ----------

# DBTITLE 1,Import libraries
import great_expectations as gx

# COMMAND ----------

# DBTITLE 1,Initiate GE data context
context = gx.get_context()

# COMMAND ----------

# DBTITLE 1,Run checkpoint (execute data validation)
checkpoint_name=f"validate_{dataset_name}"
checkpoint_result = context.run_checkpoint(
    checkpoint_name=checkpoint_name,
    batch_request={
        "runtime_parameters": {"batch_data": df},
        "batch_identifiers": {
        "pipeline_stage": "lab",
        "run_id": "d225c4ef-93d2-41cc-b884-128651e356ac",
        },
    },
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Update DataDocs
# MAGIC * Add validation results in DataDocs

# COMMAND ----------

context.build_data_docs()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can open:   
# MAGIC https://yourstorageaccount.z6.web.core.windows.net/index.html
