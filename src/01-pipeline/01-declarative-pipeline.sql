-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/db-icon.png" style="float:left; margin-right:20px" width="60px"/>
-- MAGIC
-- MAGIC # 1 — Spark Declarative Pipeline: Bronze → Silver → Gold
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/delta-live-table/delta-live-table-flow0.png" style="float:right; margin-left:20px" width="500px"/>
-- MAGIC
-- MAGIC **Alex (Data Engineer)** needs to ingest four raw data sources from S3 into a clean, reliable Lakehouse.
-- MAGIC
-- MAGIC Using **Spark Declarative Pipelines (Lakeflow)**, Alex can:
-- MAGIC - Define tables as simple `SELECT` statements — Databricks handles the orchestration
-- MAGIC - Add **data quality constraints** that automatically quarantine bad records
-- MAGIC - Use **Autoloader** for incremental, schema-evolving ingestion
-- MAGIC - Process **CDC** (Change Data Capture) data with `APPLY CHANGES INTO`
-- MAGIC
-- MAGIC <br style="clear:both"/>
-- MAGIC
-- MAGIC > **This notebook is the pipeline definition** — it is deployed via the asset bundle and executed by Lakeflow, not run cell-by-cell.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer — Raw Ingestion
-- MAGIC
-- MAGIC Bronze tables land data exactly as it arrives from the source, with minimal transformation.
-- MAGIC We use **Autoloader** (`cloud_files`) to pick up new files automatically as they land in S3.

-- COMMAND ----------

-- Bronze: Orders (CSV → Streaming Table)
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT "Raw orders landed from S3 CSV files via Autoloader"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.managed" = "true"
)
AS
SELECT
  *,
  _metadata.file_path  AS _source_file,
  _metadata.file_modification_time AS _source_ts,
  current_timestamp()  AS _ingestion_ts
FROM cloud_files(
  "/Volumes/${catalog}/${schema}/${volume}/orders/",
  "csv",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "header", "true"
  )
)

-- COMMAND ----------

-- Bronze: Products (JSON → Streaming Table)
CREATE OR REFRESH STREAMING TABLE bronze_products
COMMENT "Raw product catalog from JSON files"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *,
  current_timestamp() AS _ingestion_ts
FROM cloud_files(
  "/Volumes/${catalog}/${schema}/${volume}/products/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
)

-- COMMAND ----------

-- Bronze: Clickstream (JSON → Streaming Table)
CREATE OR REFRESH STREAMING TABLE bronze_clickstream
COMMENT "Raw clickstream events"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *,
  current_timestamp() AS _ingestion_ts
FROM cloud_files(
  "/Volumes/${catalog}/${schema}/${volume}/clickstream/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Layer — Cleansed & Enriched
-- MAGIC
-- MAGIC Silver tables apply:
-- MAGIC - **Type casting** and **column renaming**
-- MAGIC - **Data quality constraints** — records failing checks are tracked in the pipeline event log
-- MAGIC - **Business logic** (e.g. enriching orders with product details)

-- COMMAND ----------

-- Silver: Orders (with quality constraints)
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_order_id      EXPECT (order_id IS NOT NULL)    ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer      EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount     EXPECT (total_amount > 0)        ON VIOLATION DROP ROW,
  CONSTRAINT valid_status        EXPECT (status IN ('completed','returned','cancelled','pending'))
)
COMMENT "Cleansed orders with quality enforcement"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  order_id,
  customer_id,
  product_id,
  CAST(quantity    AS INT)     AS quantity,
  CAST(unit_price  AS DOUBLE)  AS unit_price,
  CAST(total_amount AS DOUBLE) AS total_amount,
  status,
  CAST(order_date  AS TIMESTAMP) AS order_date,
  ship_country,
  _ingestion_ts
FROM STREAM(LIVE.bronze_orders)

-- COMMAND ----------

-- Silver: Products (deduplicated, cost margin added)
CREATE OR REFRESH MATERIALIZED VIEW silver_products
COMMENT "Cleansed product catalog with margin calculation"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  product_id,
  name         AS product_name,
  category,
  brand,
  CAST(price AS DOUBLE) AS price,
  CAST(cost  AS DOUBLE) AS cost,
  ROUND((CAST(price AS DOUBLE) - CAST(cost AS DOUBLE)) / NULLIF(CAST(price AS DOUBLE), 0) * 100, 2) AS margin_pct,
  CAST(stock_qty AS INT) AS stock_qty,
  CAST(rating AS DOUBLE) AS rating
FROM LIVE.bronze_products
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _ingestion_ts DESC) = 1

-- COMMAND ----------

-- Silver: Clickstream (sessionised, typed)
CREATE OR REFRESH STREAMING TABLE silver_clickstream (
  CONSTRAINT valid_event_id  EXPECT (event_id IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT valid_event_type EXPECT (event_type IN ('view_product','add_to_cart','remove_from_cart',
                                                      'checkout','purchase','search','home_page'))
)
COMMENT "Cleansed clickstream with session context"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  event_id,
  customer_id,
  product_id,
  event_type,
  session_id,
  device,
  CAST(ts AS TIMESTAMP) AS event_ts,
  _ingestion_ts
FROM STREAM(LIVE.bronze_clickstream)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer — Business-Ready Aggregations
-- MAGIC
-- MAGIC Gold tables are **Materialized Views** that Lakeflow keeps fresh automatically.
-- MAGIC These are the tables consumed by dashboards, Genie, and the AI Agent.

-- COMMAND ----------

-- Gold: Daily Revenue
CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_daily
COMMENT "Daily revenue, order count and average order value by country"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  DATE(order_date)            AS order_day,
  ship_country,
  COUNT(DISTINCT order_id)    AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value,
  COUNT(CASE WHEN status = 'returned'  THEN 1 END) AS returns,
  COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancellations
FROM LIVE.silver_orders
WHERE status IN ('completed', 'returned', 'cancelled', 'pending')
GROUP BY DATE(order_date), ship_country

-- COMMAND ----------

-- Gold: Top Products
CREATE OR REFRESH MATERIALIZED VIEW gold_top_products
COMMENT "Product performance: revenue, units sold, return rate, margin"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  o.product_id,
  p.product_name,
  p.category,
  p.brand,
  p.price,
  p.margin_pct,
  COUNT(DISTINCT o.order_id)                                            AS orders_count,
  SUM(o.quantity)                                                       AS units_sold,
  ROUND(SUM(o.total_amount), 2)                                         AS total_revenue,
  ROUND(COUNT(CASE WHEN o.status = 'returned' THEN 1 END) /
        NULLIF(COUNT(*), 0) * 100, 2)                                   AS return_rate_pct
FROM LIVE.silver_orders     o
JOIN LIVE.silver_products   p USING (product_id)
GROUP BY o.product_id, p.product_name, p.category, p.brand, p.price, p.margin_pct

-- COMMAND ----------

-- Gold: Cart Abandonment
CREATE OR REFRESH MATERIALIZED VIEW gold_cart_abandonment
COMMENT "Sessions with add-to-cart but no purchase — abandonment signal"
TBLPROPERTIES ("quality" = "gold")
AS
WITH session_events AS (
  SELECT
    session_id,
    customer_id,
    MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)  AS added_to_cart,
    MAX(CASE WHEN event_type = 'purchase'    THEN 1 ELSE 0 END)  AS purchased,
    COUNT(*)                                                       AS event_count,
    MIN(event_ts)                                                  AS session_start,
    MAX(event_ts)                                                  AS session_end
  FROM LIVE.silver_clickstream
  GROUP BY session_id, customer_id
)
SELECT
  session_id,
  customer_id,
  event_count,
  session_start,
  session_end,
  ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 60.0, 1) AS duration_mins,
  CASE WHEN added_to_cart = 1 AND purchased = 0 THEN TRUE ELSE FALSE END           AS abandoned
FROM session_events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Pipeline Summary
-- MAGIC
-- MAGIC | Layer | Tables | Compute |
-- MAGIC |-------|--------|---------|
-- MAGIC | Bronze | `bronze_orders`, `bronze_products`, `bronze_clickstream` | Serverless Streaming |
-- MAGIC | Silver | `silver_orders`, `silver_products`, `silver_clickstream` | Serverless Streaming + MV |
-- MAGIC | Gold  | `gold_revenue_daily`, `gold_top_products`, `gold_cart_abandonment` | Serverless MV |
-- MAGIC
-- MAGIC **Next →** [02-pipeline-cdc: CDC for Customer Updates]($./02-pipeline-cdc)
