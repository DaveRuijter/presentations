# Databricks notebook source
display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/learning-spark-v2/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## databricks-datasets/learning-spark-v2/people/people-10m

# COMMAND ----------

path = 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta'
df = spark.read.load(path)

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate the data using Great Expectations (GX)

# COMMAND ----------

import great_expectations as gx

# COMMAND ----------

df_gx = gx.dataset.SparkDFDataset(df)

# COMMAND ----------

df_gx.expect_column_values_to_be_of_type(column="id", type_="StringType")

# COMMAND ----------

df_gx.expect_column_values_to_not_be_null(column="id")

# COMMAND ----------

df_gx.expect_column_values_to_be_unique(column="id")

# COMMAND ----------

df_gx.expect_column_to_exist("dummy_column")

# COMMAND ----------

# MAGIC %md
# MAGIC *mostly*
# MAGIC Hint: it’s common to encounter data issues where most cases match, but you can’t guarantee 100% adherence. In these cases, consider using a mostly parameter. This parameter is an option for all Expectations that are applied on a row-by-row basis, and allows you to control the level of wiggle room you want built into your data validation.

# COMMAND ----------

df_gx.expect_column_values_to_not_be_null(column="salary", mostly=.95)

# COMMAND ----------

# MAGIC %md
# MAGIC ### End of intro into great-expecations (GX)
# MAGIC 
# MAGIC Questions:
# MAGIC * How to run these commands in our production solution?
# MAGIC * How to store the results?
# MAGIC 
# MAGIC 
# MAGIC **Data Context** is the answer!   
# MAGIC _Back to the slides._
