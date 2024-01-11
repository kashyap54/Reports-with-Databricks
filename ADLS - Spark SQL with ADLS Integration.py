# Databricks notebook source
# MAGIC %sql
# MAGIC SET fs.azure.account.key = WLg9Vkk4NDqKw+RujlIFoslfLOBfy4dDx7l/yY4YyoDbXl7UMVazTIIKDVQNwbPHbLCIWASTgQcz+ASt+1rWcw==

# COMMAND ----------

# MAGIC 	%fs ls abfss://data@kapretail.dfs.core.windows.net/retail_db/orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders(
# MAGIC   order_id INT,
# MAGIC   order_date TIMESTAMP,
# MAGIC   order_customer_id INT,
# MAGIC   order_status STRING
# MAGIC ) USING CSV
# MAGIC OPTIONS (
# MAGIC   path = 'abfss://data@kapretail.dfs.core.windows.net/retail_db/orders'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_status,
# MAGIC count(*) as order_count
# MAGIC from orders
# MAGIC group by 1
# MAGIC order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PARQUET.`/public/retail_db/daily_product_revenue`
# MAGIC ORDER BY order_date, revenue DESC