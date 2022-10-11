# Databricks notebook source
file_path = "abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/Customers.csv"
table_name = "dead_customers"

# COMMAND ----------

df = spark.read.format("csv").option("header",True).load(file_path)

# COMMAND ----------

# MAGIC %run ./59_ge_profiler_using_data_assistant
