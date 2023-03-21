# Databricks notebook source
import datetime
import great_expectations as gx

from great_expectations.core.batch import RuntimeBatchRequest

# COMMAND ----------

# DBTITLE 1,Read DataContext
context = gx.get_context()

# COMMAND ----------

# MAGIC %md
# MAGIC Notes on get_context():
# MAGIC * Default is to look locally for a `great-expectations` folder
# MAGIC * YAML file `great_expectations.yml`
# MAGIC 
# MAGIC 
# MAGIC Manually initialize DataContext:
# MAGIC ```
# MAGIC data_context_config = DataContextConfig(
# MAGIC     store_backend_defaults=FilesystemStoreBackendDefaults(
# MAGIC         root_directory=root_directory
# MAGIC     ),
# MAGIC )
# MAGIC context = BaseDataContext(project_config=data_context_config)
# MAGIC ```
# MAGIC 
# MAGIC What is in my DataContext:
# MAGIC 1. Path to store expectations
# MAGIC 1. Path to store validation results
# MAGIC 1. Path to store DataDocs
# MAGIC 1. Path to store checkpoints

# COMMAND ----------

context.list_datasources()

# COMMAND ----------

path = 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta'
df = spark.read.load(path)
display(df)

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="people_data",
    batch_identifiers={
        "pipeline_stage": "lab",
        "run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},
)

# COMMAND ----------

# DBTITLE 1,Initialize validator
validator = context.get_validator(
    batch_request=batch_request,
)

# COMMAND ----------

validator.expect_column_values_to_not_be_null(column="id")

# COMMAND ----------

validator.expect_column_values_to_be_between(column="salary", min_value=0)

# COMMAND ----------

validator.expect_table_row_count_to_be_between(min_value=100000)

# COMMAND ----------

# assert validator.expect_column_values_to_be_between(column="salary", min_value=0).success == True, "Validation failed!"

# COMMAND ----------

# MAGIC %md
# MAGIC ### What shall we add more
# MAGIC _Hint: type `validator.expect_` and then press CTRL + SPACEBAR_

# COMMAND ----------

# validator.expect_

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###Save expectations in a suit

# COMMAND ----------

# DBTITLE 0,Save expectations in a suit
validator.save_expectation_suite(
    "expectations/expectations_for_people_manually_created.json",
    discard_failed_expectations=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update DataDocs
# MAGIC * Include new expectation suite in the DataDocs
# MAGIC * Easily browse (and share) the new expectations

# COMMAND ----------

# DBTITLE 0,Update DataDocs
context.build_data_docs()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can open:   
# MAGIC https://yourstorageaccount.z6.web.core.windows.net/index.html
