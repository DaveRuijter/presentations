# Databricks notebook source
file_path = "abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/orders/2022/10/2022_10_11orders.parquet"
table_name = "dead_orders"

# COMMAND ----------

# DBTITLE 1,Import libraries
import great_expectations as ge
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Initiate GE data context
context = ge.get_context()

# COMMAND ----------

from multicolumn_expression_evaluation import MulticolumnExpressionTrue, ExpectMulticolumnExpressionToEvaluateTrue

# COMMAND ----------

# DBTITLE 1,Read dataframe
df = spark.read.parquet(file_path)
df = df.select([F.col(col).alias(col.replace(' ', '_').replace('-', '_').replace('#', '').replace('&', '').replace('__', '_')) for col in df.columns])

# COMMAND ----------

# DBTITLE 1,Run checkpoint (execute data validation)
checkpoint_name=f"validate_df_dead_orders_with_custom_expecation"
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


