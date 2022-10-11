# Databricks notebook source
table_name = "dead_products"
file_path = "abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/Products.csv"

# COMMAND ----------

df = spark.read.format("csv").option("header",True).load(file_path)

# COMMAND ----------

import datetime
import great_expectations as ge
import pyspark.sql.functions as F
from great_expectations.core.batch import RuntimeBatchRequest

# COMMAND ----------

context = ge.get_context()

# COMMAND ----------

df = df.select([F.col(col).alias(col.replace(' ', '_').replace('-', '_').replace('#', '').replace('&', '').replace('__', '_')) for col in df.columns])

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name=f"da_{table_name}",
    batch_identifiers={
        "pipeline_stage": "lab",
        "run_id": f"lab_run_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},
)

# COMMAND ----------

data_assistant_result = context.assistants.onboarding.run(
    batch_request=batch_request
)

# COMMAND ----------

data_assistant_result

# COMMAND ----------

# MAGIC %md
# MAGIC **OnboardingDataAssistant**  provides dataset exploration and validation to help with Great Expectations "Onboarding".
# MAGIC 
# MAGIC OnboardingDataAssistant.run() Args:
# MAGIC - **batch_request** (BatchRequestBase or dict): The Batch Request to be passed to the Data Assistant.
# MAGIC - **estimation** (str): One of "exact" (default) or "flag_outliers" indicating the type of data you believe the Batch Request to contain. Valid or trusted data should use "exact", while Expectations produced with data that is suspected to have quality issues may benefit from "flag_outliers".
# MAGIC - **include_column_names** (list): A list containing the column names you wish to include.
# MAGIC - **exclude_column_names** (list): A list containing the column names you with to exclude.
# MAGIC - **include_column_name_suffixes** (list): A list containing the column name suffixes you wish to include.
# MAGIC - **exclude_column_name_suffixes** (list): A list containing the column name suffixes you wish to exclude.
# MAGIC - **cardinality_limit_mode** (str): A string defined by the CardinalityLimitMode Enum, which limits the maximum unique value count allowable in column distinct value count Metrics and Expectations. Some examples: "very_few", "few", and "some"; corresponding to 10, 100, and 1,000 respectively.

# COMMAND ----------

context.save_expectation_suite(
    expectation_suite=data_assistant_result.get_expectation_suite(f"_{table_name}_from_data_assistent"),
     discard_failed_expectations=False
)

# COMMAND ----------

context.build_data_docs()
