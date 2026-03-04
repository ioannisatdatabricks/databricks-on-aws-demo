-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/db-icon.png" style="float:left; margin-right:20px" width="60px"/>
-- MAGIC
-- MAGIC # 2 — CDC Processing: Customer Profiles
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/delta-live-table/delta-live-table-cdc.png" style="float:right; margin-left:20px" width="480px"/>
-- MAGIC
-- MAGIC Customer profiles arrive as a CDC stream (inserts, updates, deletes) from the operational
-- MAGIC database. **Alex** uses `APPLY CHANGES INTO` — Lakeflow's built-in CDC primitive — to
-- MAGIC automatically merge the stream into a clean Silver table.
-- MAGIC
-- MAGIC **No custom merge logic needed.** Lakeflow handles:
-- MAGIC - Ordering events by `_commit_version`
-- MAGIC - Applying inserts/updates as upserts
-- MAGIC - Processing deletes (soft or hard)
-- MAGIC - Out-of-order event handling
-- MAGIC
-- MAGIC <br style="clear:both"/>

-- COMMAND ----------

-- Bronze: Raw customer CDC feed
CREATE OR REFRESH STREAMING TABLE bronze_customers_cdc
COMMENT "Raw customer CDC events from operational DB (Parquet via Autoloader)"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *,
  current_timestamp() AS _ingestion_ts
FROM cloud_files(
  "/Volumes/${catalog}/${schema}/${volume}/customers/",
  "parquet",
  map("cloudFiles.inferColumnTypes", "true")
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Apply CDC Changes → Silver Customers
-- MAGIC
-- MAGIC `APPLY CHANGES INTO` reads the bronze CDC feed and produces a **materialized, always-current**
-- MAGIC view of the customers table. The `KEYS` clause identifies the primary key; `SEQUENCE BY`
-- MAGIC orders conflicting events deterministically.

-- COMMAND ----------

-- Target Silver table (created by APPLY CHANGES, not manually)
CREATE OR REFRESH STREAMING TABLE silver_customers
COMMENT "Current state of customer profiles after CDC processing"
TBLPROPERTIES ("quality" = "silver")

-- COMMAND ----------

APPLY CHANGES INTO LIVE.silver_customers
FROM STREAM(LIVE.bronze_customers_cdc)
KEYS (customer_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_version
COLUMNS * EXCEPT (_change_type, _commit_version, _ingestion_ts)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold: Customer Lifetime Value
-- MAGIC
-- MAGIC Once customers are clean in Silver, **Maya** can join with orders to derive LTV.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_ltv
COMMENT "Customer lifetime value: revenue, order count, avg order value, segment"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  c.customer_id,
  c.segment,
  c.country,
  c.is_active,
  COUNT(DISTINCT o.order_id)    AS total_orders,
  ROUND(SUM(o.total_amount), 2) AS lifetime_value,
  ROUND(AVG(o.total_amount), 2) AS avg_order_value,
  MAX(o.order_date)             AS last_order_date,
  DATEDIFF(current_date(), MAX(DATE(o.order_date))) AS days_since_last_order
FROM LIVE.silver_customers      c
LEFT JOIN LIVE.silver_orders    o USING (customer_id)
WHERE o.status = 'completed' OR o.status IS NULL
GROUP BY c.customer_id, c.segment, c.country, c.is_active

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Summary
-- MAGIC
-- MAGIC With CDC processing in place, customer profiles stay in sync with the operational DB automatically.
-- MAGIC The `gold_customer_ltv` table is now available for:
-- MAGIC - The AI/BI Dashboard (notebook 4)
-- MAGIC - The Genie Space (notebook 5)
-- MAGIC - The AI Agent as a UC Function tool (notebook 6)
-- MAGIC
-- MAGIC **Next →** [03-unity-catalog: Governance & Security]($../02-governance/03-unity-catalog)
