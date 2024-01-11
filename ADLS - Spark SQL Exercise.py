# Databricks notebook source
# MAGIC %sql
# MAGIC SET fs.azure.account.key = WLg9Vkk4NDqKw+RujlIFoslfLOBfy4dDx7l/yY4YyoDbXl7UMVazTIIKDVQNwbPHbLCIWASTgQcz+ASt+1rWcw==

# COMMAND ----------

# MAGIC 	%fs ls abfss://data@kapretail.dfs.core.windows.net/retail_db/order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW order_items(
# MAGIC   order_item_id INT,
# MAGIC   order_item_order_id INT,
# MAGIC   order_item_product_id INT,
# MAGIC   order_item_quantity INT,
# MAGIC   order_item_subtotal FLOAT,
# MAGIC   order_item_product_price FLOAT
# MAGIC ) USING CSV
# MAGIC OPTIONS (
# MAGIC   path = 'abfss://data@kapretail.dfs.core.windows.net/retail_db/order_items'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM order_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_item_product_id,
# MAGIC count(*) as order_count
# MAGIC from order_items
# MAGIC group by 1
# MAGIC order by 2 desc;