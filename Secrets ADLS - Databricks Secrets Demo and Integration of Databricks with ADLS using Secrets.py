# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='sa_kapretail')

# COMMAND ----------

dbutils.secrets.get('sa_kapretail', 'sa_kapretail_key')

# COMMAND ----------

dbutils.secrets.get('sa_kapretail', 'sa_kapretail_key') == 'WLg9Vkk4NDqKw+RujlIFoslfLOBfy4dDx7l/yY4YyoDbXl7UMVazTIIKDVQNwbPHbLCIWASTgQcz+ASt+1rWcw=='

# COMMAND ----------

kapretail_key = dbutils.secrets.get('sa_kapretail', 'sa_kapretail_key')

# COMMAND ----------

spark.conf.set('fs.azure.account.key', kapretail_key)

# COMMAND ----------

# MAGIC %fs ls abfss://data@kapretail.dfs.core.windows.net/retail_db