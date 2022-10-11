# Databricks notebook source
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

context.save_expectation_suite(
    expectation_suite=data_assistant_result.get_expectation_suite(f"{table_name}_profiled"), 
    discard_failed_expectations=False
)

# COMMAND ----------

context.build_data_docs()
