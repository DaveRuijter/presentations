# Databricks notebook source
import datetime
import great_expectations as ge
import pyspark.sql.functions as F

# COMMAND ----------

df = spark.read.format("csv").option("header",True).load("abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/Products.csv")
df = df.select([F.col(col).alias(col.replace(' ', '_').replace('-', '_').replace('#', '').replace('&', '').replace('__', '_')) for col in df.columns])
display(df)

# COMMAND ----------

df_ge = ge.dataset.SparkDFDataset(df)

# COMMAND ----------

df_ge.expect_column_values_to_not_be_null(column="Brand_Class")

# COMMAND ----------

df_ge.expect_column_values_to_be_between(column="Product_Key", min_value=0)

# COMMAND ----------

df_ge.expect_column_to_exist("dummy_column")

# COMMAND ----------

# MAGIC %md
# MAGIC Hint: it’s common to encounter data issues where most cases match, but you can’t guarantee 100% adherence. In these cases, consider using a mostly parameter. This parameter is an option for all Expectations that are applied on a row-by-row basis, and allows you to control the level of wiggle room you want built into your data validation.

# COMMAND ----------

df_ge.expect_column_values_to_not_be_null(column="Product_Group")

# COMMAND ----------

df_ge.expect_column_values_to_not_be_null(column="Product_Group", mostly=.95)
