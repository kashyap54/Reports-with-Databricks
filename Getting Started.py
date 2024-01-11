# Databricks notebook source
# MAGIC %fs ls dbfs:/public/retail_db

# COMMAND ----------

dbutils.fs.ls('dbfs:/public/retail_db')

# COMMAND ----------

spark.conf.set('fs.azure.account.key', 'WLg9Vkk4NDqKw+RujlIFoslfLOBfy4dDx7l/yY4YyoDbXl7UMVazTIIKDVQNwbPHbLCIWASTgQcz+ASt+1rWcw==')

# COMMAND ----------

# MAGIC %sql
# MAGIC SET fs.azure.account.key = WLg9Vkk4NDqKw+RujlIFoslfLOBfy4dDx7l/yY4YyoDbXl7UMVazTIIKDVQNwbPHbLCIWASTgQcz+ASt+1rWcw==

# COMMAND ----------

# MAGIC %fs ls abfss://data@kapretail.dfs.core.windows.net/retail_db

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_date
# MAGIC