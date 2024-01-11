# Databricks notebook source
spark.conf.set('fs.azure.account.key', 'WLg9Vkk4NDqKw+RujlIFoslfLOBfy4dDx7l/yY4YyoDbXl7UMVazTIIKDVQNwbPHbLCIWASTgQcz+ASt+1rWcw==')

# COMMAND ----------

# MAGIC %fs ls abfss://data@kapretail.dfs.core.windows.net/retail_db

# COMMAND ----------

orders_df = spark.read.csv(
    'abfss://data@kapretail.dfs.core.windows.net/retail_db/orders',
    schema = 'order_id INT, order_date TIMESTAMP, order_customer_id INT, order_status STRING'
)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

orders_df. \
    groupBy('order_status'). \
        count(). \
            withColumnRenamed('count', 'order_count'). \
                orderBy(col('order_count').desc()). \
            show()