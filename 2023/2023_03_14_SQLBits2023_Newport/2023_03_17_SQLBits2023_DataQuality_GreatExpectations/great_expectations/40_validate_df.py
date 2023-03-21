# Databricks notebook source
# DBTITLE 1,Import libraries
import great_expectations as gx

# COMMAND ----------

# DBTITLE 1,Initiate GE data context
context = gx.get_context()

# COMMAND ----------

# DBTITLE 1,Read dataframe
dataset_name = "databricks_learning_spark_people"
path = 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta'
df = spark.read.load(path).limit(1000000)

# COMMAND ----------

# DBTITLE 1,Run checkpoint (execute data validation)
checkpoint_name=f"validate_df_{dataset_name}"
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
