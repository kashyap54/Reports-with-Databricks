# Databricks notebook source
# MAGIC %fs ls dbfs:/public/retail_db/

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db/schemas.json

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM TEXT.`/public/retail_db/schemas.json`

# COMMAND ----------

help(spark.read.text)

# COMMAND ----------

spark.read.text(paths = 'dbfs:/public/retail_db/schemas.json', wholetext = True).show(truncate = False)

# COMMAND ----------

schema_text = spark.read.text(paths = 'dbfs:/public/retail_db/schemas.json', wholetext = True).first().value

# COMMAND ----------

import json
json.loads(schema_text)

# COMMAND ----------

column_details = json.loads(schema_text)['orders']

# COMMAND ----------

columns = [col['column_name'] for col in sorted(column_details, key=lambda col: col['column_position'])]

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM CSV.`dbfs:/public/retail_db/orders`

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

spark.read.csv(path = 'dbfs:/public/retail_db/orders', inferSchema = True).toDF(*columns).show()

# COMMAND ----------

orders_df = spark.read.csv(path = 'dbfs:/public/retail_db/orders', inferSchema = True).toDF(*columns)

# COMMAND ----------

from pyspark.sql.functions import count, col

# COMMAND ----------

orders_df. \
    groupBy('order_status'). \
        agg(count('*').alias('order_ount')). \
            orderBy(col('order_ount').desc()). \
            show()

# COMMAND ----------

import json

def get_columns(schemas_file, ds_name):
    schema_text = spark.read.text(paths = schemas_file, wholetext = True).first().value
    schemas = json.loads(schema_text)
    column_details = schemas[ds_name]
    columns = [col['column_name'] for col in sorted(column_details, key=lambda col: col['column_position'])]
    return columns

# COMMAND ----------

o_cols = get_columns('dbfs:/public/retail_db/schemas.json', 'orders')


# COMMAND ----------

o_df = spark.read.csv(path = 'dbfs:/public/retail_db/orders', inferSchema = True).toDF(*o_cols)

# COMMAND ----------

o_df. \
    write. \
        mode('overwrite'). \
            parquet('dbfs:/public/retail_db_parquet/orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PARQUET.`dbfs:/public/retail_db_parquet/order_items`

# COMMAND ----------

ds_list = [
    'departments', 'categories', 'products', 'customers', 'orders', 'order_items'
]

# COMMAND ----------

base_dir = 'dbfs:/public/retail_db/'

# COMMAND ----------

for ds in ds_list:
    print(f'processing {ds} data')
    column_headers = get_columns(f'{base_dir}/schemas.json', ds)
    ##print(column_headers)
    df = spark.read.csv(path = f'{base_dir}/{ds}', inferSchema = True).toDF(*column_headers)
    df. \
        write. \
        mode('overwrite'). \
            parquet(f'dbfs:/public/retail_db_parquet/{ds}')

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db_parquet/order_items