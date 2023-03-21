# Databricks notebook source
# MAGIC %md
# MAGIC #### Comments
# MAGIC This notebook can be used to validate a Data Fundament using the [Great Expectation framework](https://greatexpectations.io/).  
# MAGIC It is a generic notebook, via widgets you can provide the correct context (which Data Fundament, target database etc.).  
# MAGIC - A message can be sent to a Teams channel, about the result of a data validation. This is configured in the 'checkpoint' per data fundament. 
# MAGIC - We use Tenacity for retry logic: https://tenacity.readthedocs.io/
# MAGIC 
# MAGIC #### To do
# MAGIC - loading the widget values can be less 'hardcoded'.
# MAGIC - There is bug in the Teams notification integration, that prevents the data asset name from showing. Track progress [in this GitHub issue](https://github.com/great-expectations/great_expectations/issues/3813).

# COMMAND ----------

# DBTITLE 1,Read dataframe
# df = spark.table(f"`delta_lake`.`{widgets_values['table']}`")
#or
df = spark.read.parquet("abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/orders/2022/10/2022_10_11orders.parquet")

# COMMAND ----------

# DBTITLE 1,Enable automatic reload of modules
# You can run the commands below to automatically reload modules.
# https://docs.databricks.com/_static/notebooks/files-in-repos.html
%load_ext autoreload
%autoreload 2

# COMMAND ----------

# DBTITLE 1,Import libraries
import uuid
from typing import Dict
from dateutil.parser import parse
import great_expectations as ge
from tenacity import retry, stop_after_attempt, wait_fixed, stop_after_delay
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Clean up dataframe column names
df = df.select([F.col(col).alias(col.replace(' ', '_').replace('-', '_').replace('#', '').replace('&', '').replace('__', '_')) for col in df.columns])

# COMMAND ----------

# DBTITLE 1,Remove all widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Initialize widgets (with defaults)
run_id = str(uuid.uuid4())
dbutils.widgets.text("table", "dead_orders")

# COMMAND ----------

# DBTITLE 1,Load widget_values in dict
# TODO: see if we can grab these values in a loop, so we don't have to hardcode them.
@retry(stop=(stop_after_attempt(3)), wait=wait_fixed(0.4))
def get_notebook_widgets_values() -> Dict:
    widgets_values = {
        "table": dbutils.widgets.get("table")
    }
    print("get_notebook_widgets_values info:")
    print(f" - widgets_values : {widgets_values}")
    print("")
    return widgets_values

widgets_values = get_notebook_widgets_values()

# COMMAND ----------

# DBTITLE 1,Initiate GE data context
@retry(stop=(stop_after_delay(30) | stop_after_attempt(3)), wait=wait_fixed(0.4))
def load_ge_context():
    context = ge.get_context()
    return context
  
context = load_ge_context()

# COMMAND ----------

# DBTITLE 1,Import custom expectation classes
from multicolumn_expression_evaluation import MulticolumnExpressionTrue
from multicolumn_expression_evaluation import ExpectMulticolumnExpressionToEvaluateTrue

# COMMAND ----------

# DBTITLE 1,Run checkpoint (execute data validation)
checkpoint_name=f"validate_df_{widgets_values['table']}"
print(f"Checkpoint name: {checkpoint_name}")

@retry(stop=(stop_after_attempt(3)), wait=wait_fixed(5))
def run_ge_checkpoint():
  checkpoint_result = context.run_checkpoint(
    checkpoint_name=checkpoint_name,
    batch_request={
      "runtime_parameters": {"batch_data": df},
      "batch_identifiers": {
        "pipeline_stage": "lab",
        "run_id": run_id,
      },
    },
  )
  return checkpoint_result


checkpoint_result = run_ge_checkpoint()

if not checkpoint_result["success"]:
  message = "Validation failed!"
  print(message)
  #dbutils.notebook.exit(message)
else:
  message = "Validation succeeded!"
  print(message)
  #dbutils.notebook.exit(message)
