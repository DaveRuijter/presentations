# Databricks notebook source
display(spark.read.format("csv").option("header",True).load("abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/Exchange Rate.csv"))

# COMMAND ----------

display(spark.read.format("csv").option("header",True).load("abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/Products.csv"))


# COMMAND ----------

display(spark.read.format("csv").option("header",True).load("abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/Customers.csv"))

# COMMAND ----------

display(spark.read.parquet("abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/clean tables/orders_Y-M-D.parquet"))

# COMMAND ----------

display(spark.read.parquet("abfss://deltalake@<your_storage_account>.dfs.core.windows.net/bronze/dg-retail/clean tables/invoices_Y-M-D.parquet"))
