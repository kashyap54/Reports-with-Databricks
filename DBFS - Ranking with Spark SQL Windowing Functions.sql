-- Databricks notebook source
SELECT * FROM PARQUET.`/public/retail_db/daily_product_revenue`
ORDER BY order_date, revenue DESC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW daily_product_revenue
USING PARQUET
OPTIONS (
  path = '/public/retail_db/daily_product_revenue'
)

-- COMMAND ----------

SELECT * FROM daily_product_revenue
ORDER BY order_date, revenue DESC

-- COMMAND ----------

SELECT dpr.*
FROM daily_product_revenue AS dpr
WHERE dpr.order_date LIKE '2013-07-26%'
ORDER BY 1, 3 DESC

-- COMMAND ----------

SELECT dpr.*,
  rank() OVER (ORDER BY revenue DESC) AS rnk,
  dense_rank() OVER (ORDER BY revenue DESC) AS den_rnk
FROM daily_product_revenue AS dpr
WHERE dpr.order_date LIKE '2013-07-26%'
ORDER BY 1, 3 DESC

-- COMMAND ----------

SELECT dpr.*,
  rank() OVER (PARTITION BY order_date ORDER BY revenue DESC) AS rnk
FROM daily_product_revenue AS dpr
ORDER BY 1, 3 DESC

-- COMMAND ----------

SELECT * FROM(
  SELECT dpr.*,
    rank() OVER (ORDER BY revenue DESC) AS rnk
  FROM daily_product_revenue AS dpr
  WHERE dpr.order_date LIKE '2013-07-26%'
)WHERE rnk<=5


-- COMMAND ----------

SELECT * FROM(
  SELECT dpr.*,
    rank() OVER (PARTITION BY order_date ORDER BY revenue DESC) AS rnk
  FROM daily_product_revenue AS dpr
)WHERE rnk <= 5


-- COMMAND ----------

WITH dpr_ranked_cte AS (
  SELECT dpr.*,
    rank() OVER (PARTITION BY order_date ORDER BY revenue DESC) AS rnk
  FROM daily_product_revenue AS dpr
)SELECT * FROM dpr_ranked_cte
WHERE rnk <= 5

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW dpr_ranked
AS
SELECT dpr.*,
    rank() OVER (PARTITION BY order_date ORDER BY revenue DESC) AS rnk
FROM daily_product_revenue AS dpr

-- COMMAND ----------

SELECT * FROM dpr_ranked
WHERE rnk <= 5