# Databricks notebook source
import datetime
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
import pyspark.sql.functions as F

# COMMAND ----------

context = ge.get_context()

# COMMAND ----------

context.list_datasources()

# COMMAND ----------

df = spark.read.format("csv").option("header",True).load("abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/Products.csv")
df = df.select([F.col(col).alias(col.replace(' ', '_').replace('-', '_').replace('#', '').replace('&', '').replace('__', '_')) for col in df.columns])
display(df)

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="dead_products",
    batch_identifiers={
        "pipeline_stage": "lab",
        "run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},
)

# COMMAND ----------

validator = context.get_validator(
    batch_request=batch_request,
)

# COMMAND ----------

validator.expect_column_values_to_not_be_null(column="Brand_Class")

# COMMAND ----------

validator.expect_column_values_to_be_between(
    column="Product_Key", min_value=0
)

# COMMAND ----------

validator.save_expectation_suite(
    "expectations/dead_products_suite_manually_created.json",
    discard_failed_expectations=False
)

# COMMAND ----------

# DBTITLE 1,Update DataDocs
context.build_data_docs()

# COMMAND ----------

# MAGIC %md
# MAGIC Open:   
# MAGIC https://<your_storage_account>.z6.web.core.windows.net/index.html
