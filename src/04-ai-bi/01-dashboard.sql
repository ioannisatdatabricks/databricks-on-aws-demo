-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/db-icon.png" style="float:left; margin-right:20px" width="60px"/>
-- MAGIC
-- MAGIC # 4a — AI/BI Dashboard: ShopNow Revenue Intelligence
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/aibi/aibi-dashboard.png" style="float:right; margin-left:20px" width="450px"/>
-- MAGIC
-- MAGIC **Maya (BI Analyst)** uses **Databricks AI/BI Dashboards** to give business stakeholders
-- MAGIC real-time visibility into ShopNow's performance.
-- MAGIC
-- MAGIC The dashboard is built on the gold tables produced by the pipeline — no data movement,
-- MAGIC no separate BI tool, no stale extracts.
-- MAGIC
-- MAGIC **Key metrics on the dashboard:**
-- MAGIC - 📈 Daily & weekly revenue trend
-- MAGIC - 🌍 Revenue by country (choropleth map)
-- MAGIC - 🏆 Top 10 products by revenue and margin
-- MAGIC - 🛒 Cart abandonment rate over time
-- MAGIC - 👥 Customer segment breakdown (LTV heatmap)
-- MAGIC
-- MAGIC <br style="clear:both"/>
-- MAGIC
-- MAGIC > **Note:** The actual dashboard is created in the Databricks UI (Dashboards section).
-- MAGIC > This notebook contains the underlying queries so you can understand / recreate it.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dashboard Query 1 — Revenue Trend (Last 90 Days)

-- COMMAND ----------

SELECT
  order_day,
  SUM(total_revenue)  AS revenue,
  SUM(order_count)    AS orders,
  AVG(avg_order_value) AS avg_order_value
FROM ${catalog}.${schema}.gold_revenue_daily
WHERE order_day >= DATEADD(day, -90, (SELECT MAX(order_day) FROM ${catalog}.${schema}.gold_revenue_daily))
GROUP BY order_day
ORDER BY order_day;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dashboard Query 2 — Revenue by Country

-- COMMAND ----------

SELECT
  ship_country                  AS country,
  SUM(total_revenue)            AS total_revenue,
  SUM(order_count)              AS total_orders,
  ROUND(SUM(returns) / NULLIF(SUM(order_count), 0) * 100, 1) AS return_rate_pct
FROM ${catalog}.${schema}.gold_revenue_daily
GROUP BY ship_country
ORDER BY total_revenue DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dashboard Query 3 — Top 10 Products

-- COMMAND ----------

SELECT
  product_name,
  category,
  brand,
  units_sold,
  total_revenue,
  margin_pct,
  return_rate_pct
FROM ${catalog}.${schema}.gold_top_products
ORDER BY total_revenue DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dashboard Query 4 — Cart Abandonment Trend

-- COMMAND ----------

SELECT
  DATE(session_start)               AS day,
  COUNT(*)                           AS total_sessions,
  SUM(CASE WHEN abandoned THEN 1 ELSE 0 END) AS abandoned_sessions,
  ROUND(SUM(CASE WHEN abandoned THEN 1 ELSE 0 END) /
        NULLIF(COUNT(*), 0) * 100, 1)         AS abandonment_rate_pct
FROM ${catalog}.${schema}.gold_cart_abandonment
GROUP BY DATE(session_start)
ORDER BY day;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dashboard Query 5 — Customer Segment LTV

-- COMMAND ----------

SELECT
  segment,
  country,
  COUNT(customer_id)         AS customer_count,
  ROUND(AVG(lifetime_value), 2) AS avg_ltv,
  ROUND(AVG(total_orders), 1)   AS avg_orders,
  SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_customers
FROM ${catalog}.${schema}.gold_customer_ltv
GROUP BY segment, country
ORDER BY avg_ltv DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🧠 AI-Assisted Authoring
-- MAGIC
-- MAGIC In the AI/BI Dashboard editor, Maya can type natural language to:
-- MAGIC - **Generate new visualisations** — "Show me a bar chart of weekly revenue by category"
-- MAGIC - **Modify existing charts** — "Add a 7-day moving average trend line"
-- MAGIC - **Fix query errors** — just describe the problem, AI rewrites the SQL
-- MAGIC
-- MAGIC All powered by Databricks' understanding of your Unity Catalog metadata.
-- MAGIC
-- MAGIC **Next →** [02-genie-space: Natural Language Q&A]($./02-genie-space)
