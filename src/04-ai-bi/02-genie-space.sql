-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/db-icon.png" style="float:left; margin-right:20px" width="60px"/>
-- MAGIC
-- MAGIC # 4b — Genie Space: Natural Language Analytics
-- MAGIC
-- MAGIC **Maya** wants business users who don't write SQL to be able to ask data questions on their own.
-- MAGIC
-- MAGIC **Databricks Genie** turns the gold tables into a conversational analytics interface.
-- MAGIC Business users type plain-English questions; Genie generates and runs SQL, then explains the answer.
-- MAGIC
-- MAGIC ### Questions the Genie Space can answer out of the box:
-- MAGIC | Question | Gold Table Used |
-- MAGIC |----------|----------------|
-- MAGIC | "What was our revenue last week?" | `gold_revenue_daily` |
-- MAGIC | "Which country has the highest return rate?" | `gold_revenue_daily` |
-- MAGIC | "What are our top 5 products this month?" | `gold_top_products` |
-- MAGIC | "Which product category has the best margin?" | `gold_top_products` |
-- MAGIC | "What % of sessions end in cart abandonment?" | `gold_cart_abandonment` |
-- MAGIC | "Who are our highest-value Premium customers?" | `gold_customer_ltv` |
-- MAGIC
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %run ../_resources/config

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setting Up the Genie Space
-- MAGIC
-- MAGIC The Genie Space is configured in the workspace UI (**Genie** section), but the queries
-- MAGIC below are the "trusted assets" that ground Genie's understanding of the data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Certified Answer 1 — Current Month Revenue

-- COMMAND ----------

-- Genie uses this as a trusted answer for "what is our revenue this month?"
SELECT
  ROUND(SUM(total_revenue), 2) AS current_month_revenue,
  SUM(order_count)             AS current_month_orders
FROM ${catalog}.${schema}.gold_revenue_daily
WHERE DATE_TRUNC('month', order_day) = DATE_TRUNC('month', current_date());

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Certified Answer 2 — Week-over-Week Revenue Change

-- COMMAND ----------

WITH weekly AS (
  SELECT
    DATE_TRUNC('week', order_day) AS week_start,
    SUM(total_revenue)            AS weekly_revenue
  FROM ${catalog}.${schema}.gold_revenue_daily
  GROUP BY DATE_TRUNC('week', order_day)
)
SELECT
  week_start,
  weekly_revenue,
  LAG(weekly_revenue) OVER (ORDER BY week_start)          AS prev_week_revenue,
  ROUND((weekly_revenue - LAG(weekly_revenue) OVER (ORDER BY week_start)) /
        NULLIF(LAG(weekly_revenue) OVER (ORDER BY week_start), 0) * 100, 1) AS wow_change_pct
FROM weekly
ORDER BY week_start DESC
LIMIT 8;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Certified Answer 3 — Category Performance Summary

-- COMMAND ----------

SELECT
  category,
  COUNT(product_id)            AS product_count,
  SUM(units_sold)              AS total_units,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  ROUND(AVG(margin_pct), 1)    AS avg_margin_pct,
  ROUND(AVG(return_rate_pct), 1) AS avg_return_rate
FROM ${catalog}.${schema}.gold_top_products
GROUP BY category
ORDER BY total_revenue DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🤖 Example Genie Conversation
-- MAGIC
-- MAGIC ```
-- MAGIC User:  "Which Electronics products have a return rate above 15%?"
-- MAGIC Genie: Running query...
-- MAGIC        Found 7 products. The highest return rate is "Electronics Item 42" at 22.3%.
-- MAGIC        [Shows table + bar chart]
-- MAGIC
-- MAGIC User:  "What is their average margin?"
-- MAGIC Genie: The average margin for those 7 high-return Electronics products is 34.1%.
-- MAGIC        This is 8 points below the Electronics category average.
-- MAGIC ```
-- MAGIC
-- MAGIC Genie maintains conversation context — follow-up questions build on previous answers.
-- MAGIC
-- MAGIC **Next →** [01-reverse-etl-lakebase: Sync Tables to Lakebase]($../05-lakebase/01-reverse-etl-lakebase)
