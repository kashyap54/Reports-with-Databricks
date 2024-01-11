-- Databricks notebook source

CREATE DATABASE IF NOT EXISTS kap_retail_bronze

-- COMMAND ----------

USE kap_retail_bronze

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name}
USING PARQUET
OPTIONS(
  path = '${bronze_base_dir}/${table_name}'
)

-- COMMAND ----------

SHOW tables