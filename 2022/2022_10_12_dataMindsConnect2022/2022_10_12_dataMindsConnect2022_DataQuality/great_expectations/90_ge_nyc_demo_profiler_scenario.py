# Databricks notebook source
# MAGIC   %pip install great-expectations

# COMMAND ----------

import datetime

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler



# COMMAND ----------

#This notebook currently uses a 'monkeypatch' to make it work seamless with a Databricks cluster that has credential-passthrough enabled. In the end this will be solved in the Great Expectations library. Track progress [in this GitHub issue](https://github.com/great-expectations/great_expectations/issues/3690).

from great_expectations.execution_engine.sparkdf_execution_engine import (
    SparkDFExecutionEngine,
)


def patched_init(
    self,
    *args,
    persist=True,
    spark_config=None,
    force_reuse_spark_context=False,
    **kwargs,
):
    self._persist = persist
    self._spark_config = {}
    self.spark = spark
    azure_options = kwargs.pop("azure_options", {})
    self._azure_options = azure_options

    super(SparkDFExecutionEngine, self).__init__(*args, **kwargs)

    self._config.update(
        {
            "persist": self._persist,
            "spark_config": spark_config,
            "azure_options": azure_options,
        }
    )


SparkDFExecutionEngine.__init__ = patched_init

# COMMAND ----------

root_directory = "/dbfs/great_expectations/"

# COMMAND ----------

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/FileStore/yellow_tripdata_2019_01.csv")
    .limit(100)
)

# COMMAND ----------

my_spark_datasource_config = {
    "name": "ds_dbr_nyctaxi_tripdata_yellow",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "dc_connector_dave_1": {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
                "some_key_maybe_pipeline_stage",
                "some_other_key_maybe_run_id",
            ],
        }
    },
}

# COMMAND ----------

#context.test_yaml_config(yaml.dump(my_spark_datasource_config))
context.add_datasource(**my_spark_datasource_config)

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name="ds_dbr_nyctaxi_tripdata_yellow",
    data_connector_name="dc_connector_dave_1",
    data_asset_name="da_dbr_nyctaxi_tripdata_yellow",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "some_key_maybe_pipeline_stage": "prod",
        "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)

# COMMAND ----------

expectation_suite_name = "es_dbr_nyctaxi_tripdata_yellow_profiled"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

# print(validator.head())

# COMMAND ----------

ignored_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    #"passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    #"fare_amount",
    "extra",
    "mta_tax",
    #"tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]
profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=None,
    ignored_columns=ignored_columns,
    not_null_only=False,
    primary_or_compound_key=False,
    semantic_types_dict=None,
    table_expectations_only=False,
    value_set_threshold="MANY",
)
suite = profiler.build_suite()
#https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_with_a_profiler#optional-parameters

# COMMAND ----------

print(validator.get_expectation_suite(discard_failed_expectations=False))

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

#check saved suite
suite = spark.read.options(multiline=True).json('dbfs:/great_expectations/expectations/es_dbr_nyctaxi_tripdata_yellow_profiled.json')
display(suite)

# COMMAND ----------

context.build_data_docs()

# COMMAND ----------

url_for_copy = 'dbfs:/great_expectations/uncommitted/data_docs/local_site/expectations/es_dbr_nyctaxi_tripdata_yellow_profiled.html'
dbutils.fs.cp(url_for_copy, "/FileStore/ge_data_docs_latest_expectations_page.html")
print(f"Open https://<yourws>.0.azuredatabricks.net/files/ge_data_docs_latest_expectations_page.html to download it.")

# COMMAND ----------

html_file_content = open('/dbfs/great_expectations/uncommitted/data_docs/local_site/expectations/es_dbr_nyctaxi_tripdata_yellow_profiled.html', "r").read()
displayHTML(html_file_content)
