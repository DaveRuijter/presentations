# Databricks notebook source
file_path = "abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/clean tables/invoices_Y-M-D.parquet"
table_name = "dead_invoices"

# COMMAND ----------

df = spark.read.parquet(file_path)

# COMMAND ----------

# MAGIC %run ./59_ge_profiler_using_data_assistant
