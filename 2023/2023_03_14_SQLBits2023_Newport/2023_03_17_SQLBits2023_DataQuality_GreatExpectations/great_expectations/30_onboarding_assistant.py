# Databricks notebook source
import datetime
import great_expectations as gx

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

# COMMAND ----------

context = gx.get_context()

# COMMAND ----------

# MAGIC %md
# MAGIC ## databricks-datasets/learning-spark-v2/people/people-10m

# COMMAND ----------

dataset_name = "databricks_learning_spark_people"

# COMMAND ----------

path = 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta'
df = spark.read.load(path).limit(10000)

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name=f"da_{dataset_name}",
    batch_identifiers={
        "pipeline_stage": "lab",
        "run_id": f"lab_run_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Onboarding Data Assistant
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

# data_assistant_result = context.assistants.onboarding.run(
#     batch_request=batch_request
# )

# COMMAND ----------

include_column_names = [
    "id",
#     "firstName",
#     "middleName",
    "lastName",
    "gender",
    "birthDate",
    "ssn",
]
data_assistant_result = context.assistants.onboarding.run(
    batch_request=batch_request,
    estimation="exact",
    include_column_names=include_column_names,
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Look at the results

# COMMAND ----------

data_assistant_result.show_expectations_by_domain_type

# COMMAND ----------

expectation_suite_name = "expectations_for_people_made_by_the_assistant"
expectation_suite = data_assistant_result.get_expectation_suite(expectation_suite_name)

# COMMAND ----------

expectation_suite

# COMMAND ----------

# MAGIC %md
# MAGIC ### Send the suite to the DataContext

# COMMAND ----------

context.add_or_update_expectation_suite(expectation_suite=expectation_suite)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update DataDocs
# MAGIC * Include new expectation suite in the DataDocs

# COMMAND ----------

context.build_data_docs()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can open:   
# MAGIC https://yourstorageaccount.z6.web.core.windows.net/index.html
