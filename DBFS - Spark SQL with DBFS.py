# Databricks notebook source
# MAGIC %fs ls dbfs:/public/retail_db/orders

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
# MAGIC   path = 'dbfs:/public/retail_db/orders/'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select order_status,
# MAGIC count(*) as order_count
# MAGIC from orders
# MAGIC group by 1
# MAGIC order by 2 desc;