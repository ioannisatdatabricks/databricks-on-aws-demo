-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/db-icon.png" style="float:left; margin-right:20px" width="60px"/>
-- MAGIC
-- MAGIC # 2 — Governance & Security with Unity Catalog
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/uc/uc-base-1.png" style="float:right; margin-left:20px" width="450px"/>
-- MAGIC
-- MAGIC **Sam (Data Steward)** needs to ensure that:
-- MAGIC - PII fields (email, name) are masked for non-privileged users
-- MAGIC - Column-level tags identify sensitive data for compliance
-- MAGIC - Row-level security restricts regional data to regional analysts
-- MAGIC - Data lineage is automatically tracked end-to-end
-- MAGIC
-- MAGIC **Unity Catalog** provides all of this from a single control plane — no separate tools required.
-- MAGIC
-- MAGIC <br style="clear:both"/>

-- COMMAND ----------

-- MAGIC %run ../_resources/config

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Re-assert widgets in this notebook's SQL context (values loaded from config)
-- MAGIC dbutils.widgets.text("catalog", catalog, "Catalog")
-- MAGIC dbutils.widgets.text("schema", schema, "Schema")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 — Tagging PII Columns

-- COMMAND ----------

-- Tag PII columns so they appear in the data catalog and trigger automated policies
ALTER TABLE ${catalog}.${schema}.silver_customers
  ALTER COLUMN email   SET TAGS ('pii' = 'true', 'pii_type' = 'email');

ALTER TABLE ${catalog}.${schema}.silver_customers
  ALTER COLUMN first_name SET TAGS ('pii' = 'true', 'pii_type' = 'name');

ALTER TABLE ${catalog}.${schema}.silver_customers
  ALTER COLUMN last_name  SET TAGS ('pii' = 'true', 'pii_type' = 'name');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 — Column Masking (Dynamic Data Masking)
-- MAGIC
-- MAGIC Create a masking function and apply it to the `email` column.
-- MAGIC Users without the `pii_access` group membership see `***@***.***` instead.

-- COMMAND ----------

-- Masking function: reveal email only to members of 'pii_access' group
CREATE OR REPLACE FUNCTION ${catalog}.${schema}.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_access') THEN email
  ELSE REGEXP_REPLACE(email, '(^[^@]{1,3})[^@]*@[^.]+\\.(.+)', '$1***@***.***')
END;

-- Apply the mask to silver_customers
ALTER TABLE ${catalog}.${schema}.silver_customers
  ALTER COLUMN email SET MASK ${catalog}.${schema}.mask_email;

-- COMMAND ----------

-- Verify: run as a non-privileged user to see masking in action
SELECT customer_id, first_name, email, segment
FROM ${catalog}.${schema}.silver_customers
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 — Row-Level Security
-- MAGIC
-- MAGIC Regional analysts should only see orders from their own country.
-- MAGIC A **row filter** function enforces this transparently.

-- COMMAND ----------

-- Row filter: admins see everything; others see only their country
CREATE OR REPLACE FUNCTION ${catalog}.${schema}.filter_by_country(ship_country STRING)
RETURNS BOOLEAN
RETURN is_member('data_admin') OR ship_country = system.builtin.session_context('x_user_country');

ALTER TABLE ${catalog}.${schema}.silver_orders
  SET ROW FILTER ${catalog}.${schema}.filter_by_country ON (ship_country);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4 — Grant Permissions (Least Privilege)
-- MAGIC
-- MAGIC > **Note:** The statements below are illustrative. They will fail if the `analysts` or
-- MAGIC > `data_engineers` groups do not exist in your workspace. Create them in **Settings → Identity and access**
-- MAGIC > or skip this cell — it does not affect the rest of the demo.

-- COMMAND ----------

-- Analysts can query gold tables (no PII, no row filter)
GRANT SELECT ON TABLE ${catalog}.${schema}.gold_revenue_daily  TO `analysts`;
GRANT SELECT ON TABLE ${catalog}.${schema}.gold_top_products   TO `analysts`;
GRANT SELECT ON TABLE ${catalog}.${schema}.gold_cart_abandonment TO `analysts`;
GRANT SELECT ON TABLE ${catalog}.${schema}.gold_customer_ltv   TO `analysts`;

-- Data engineers manage the schema
GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.${schema} TO `data_engineers`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5 — Lineage
-- MAGIC
-- MAGIC Unity Catalog **automatically** tracks column-level lineage. No configuration needed.
-- MAGIC You can view it in the Catalog Explorer or query the system tables:

-- COMMAND ----------

-- Query lineage system table to see upstream sources of gold_revenue_daily
SELECT
  entity_name,
  entity_type,
  source_name,
  source_type
FROM system.access.column_lineage
WHERE entity_name LIKE '%gold_revenue_daily%'
ORDER BY entity_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Summary
-- MAGIC
-- MAGIC | Capability | Implementation |
-- MAGIC |-----------|---------------|
-- MAGIC | PII discovery | Column tags (`pii=true`) |
-- MAGIC | Email masking | Dynamic data mask via UC function |
-- MAGIC | Regional RLS | Row filter via UC function |
-- MAGIC | Least privilege | `GRANT` statements per role |
-- MAGIC | Lineage | Auto-tracked by Unity Catalog |
-- MAGIC
-- MAGIC **Next →** [01-agent-creation: Build an AI Agent]($../03-ai-agent/01-agent-creation)
