# Databricks notebook source
from pyspark.sql.functions import col, date_format

# COMMAND ----------

dbutils.widgets.text('order_month', '2013-07', 'Enter Order Month')
order_month = dbutils.widgets.get('order_month')

# COMMAND ----------

orders_df = spark.read.csv(
    'dbfs:/public/retail_db/orders',
    schema = 'order_id INT, order_date TIMESTAMP, order_customer_id INT, order_status STRING'
)

orders_df. \
    filter(date_format('order_date', 'yyyy-MM') == order_month). \
    groupBy('order_status'). \
        count(). \
            withColumnRenamed('count', 'order_count'). \
                orderBy(col('order_count').desc()). \
            show()
