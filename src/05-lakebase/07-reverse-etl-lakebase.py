# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/db-icon.png" style="float:left; margin-right:20px" width="60px"/>
# MAGIC
# MAGIC # 7 — Reverse ETL to Lakebase (Synced Tables)
# MAGIC
# MAGIC **The business problem:** ShopNow's operations team uses a custom internal portal
# MAGIC that reads from a Postgres database. They need the gold-layer analytics tables from the
# MAGIC Lakehouse to appear in Postgres — without a separate ETL tool.
# MAGIC
# MAGIC **Databricks Lakebase Synced Tables** automatically replicate Delta tables into a managed
# MAGIC Postgres instance. We use **snapshot sync mode** to push a full copy of each gold table
# MAGIC into Lakebase, where the ShopNow App can query them with low latency.
# MAGIC
# MAGIC ### Flow
# MAGIC ```
# MAGIC gold_revenue_daily  ─┐
# MAGIC gold_top_products   ─┼──► Synced Tables (snapshot) ──► Lakebase (Postgres) ──► ShopNow App
# MAGIC gold_customer_ltv   ─┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog",  "main",             "Catalog")
dbutils.widgets.text("schema",   "aws_webinar_demo", "Schema")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Use Bundle-Created Lakebase Project and Wait for It
# MAGIC
# MAGIC The Lakebase project is created by the **Databricks Asset Bundle** (see
# MAGIC `resources/shopnow_lakebase.yml`). This notebook does **not** create the project;
# MAGIC it waits for the bundle-deployed project to become available, then proceeds to
# MAGIC verify readiness and document synced table setup.

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PROJECT_NAME = "shopnow-lakebase"
BRANCH_NAME  = "production"
MAX_WAIT_SEC = 600   # 10 minutes
POLL_INTERVAL_SEC = 15

project_path = f"projects/{PROJECT_NAME}"
branch_path  = f"{project_path}/branches/{BRANCH_NAME}"

# Wait for the bundle-created project to exist
print(f"Waiting for bundle-created project '{PROJECT_NAME}' to be available...")
start = time.time()
project_ok = False
while (time.time() - start) < MAX_WAIT_SEC:
    try:
        p = w.postgres.get_project(name=project_path)
        if p and p.name:
            project_ok = True
            print(f"Project found: {p.name}")
            break
    except Exception as e:
        if "not found" in str(e).lower() or "404" in str(e):
            print(f"  Project not ready yet ({int(time.time() - start)}s elapsed)...")
        else:
            raise
    time.sleep(POLL_INTERVAL_SEC)

if not project_ok:
    raise RuntimeError(f"Project '{PROJECT_NAME}' did not become available within {MAX_WAIT_SEC}s. Ensure the bundle has been deployed.")

# Wait for the default branch endpoint to exist
print(f"Waiting for branch '{BRANCH_NAME}' endpoint...")
start = time.time()
compute_path = None
while (time.time() - start) < MAX_WAIT_SEC:
    endpoints = list(w.postgres.list_endpoints(parent=branch_path))
    if endpoints:
        compute_path = endpoints[0].name
        print(f"Using endpoint: {compute_path}")
        break
    print(f"  No endpoint yet ({int(time.time() - start)}s elapsed)...")
    time.sleep(POLL_INTERVAL_SEC)

if not compute_path:
    raise RuntimeError(f"No endpoint for branch '{BRANCH_NAME}' within {MAX_WAIT_SEC}s.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create Synced Tables (UI Steps)
# MAGIC
# MAGIC Synced Tables on Lakebase Autoscaling are created via the Databricks UI.
# MAGIC Follow these steps for **each** of the three gold tables:
# MAGIC
# MAGIC ### Tables to sync
# MAGIC
# MAGIC | Source Table (Unity Catalog) | Target Table (Lakebase) | Primary Key |
# MAGIC |-----------------------------|-------------------------|-------------|
# MAGIC | `gold_revenue_daily` | `gold_revenue_daily_synced` | `order_day`, `ship_country` |
# MAGIC | `gold_top_products` | `gold_top_products_synced` | `product_id` |
# MAGIC | `gold_customer_ltv` | `gold_customer_ltv_synced` | `customer_id` |
# MAGIC
# MAGIC ### Instructions
# MAGIC
# MAGIC 1. Open **Catalog Explorer** in the Databricks workspace
# MAGIC 2. Navigate to the source table (e.g. `main.aws_webinar_demo_dev.gold_revenue_daily`)
# MAGIC 3. Click **Create > Synced table**
# MAGIC 4. In the dialog:
# MAGIC    - **Database type:** Lakebase Serverless (Autoscaling)
# MAGIC    - **Project:** `shopnow-lakebase`
# MAGIC    - **Branch:** `production`
# MAGIC    - **Database:** `databricks_postgres`
# MAGIC    - **Sync mode:** Snapshot
# MAGIC    - **Table name:** use the source name with a `_synced` suffix (e.g. `gold_revenue_daily_synced`)
# MAGIC    - **Primary key:** select the columns from the table above
# MAGIC 5. Click **Create**
# MAGIC 6. Repeat for the other two gold tables
# MAGIC
# MAGIC Once created, the synced tables will be available as Postgres tables in Lakebase
# MAGIC (in the `databricks_postgres` database) and can be queried by the ShopNow App.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Verify Lakebase is Ready
# MAGIC
# MAGIC Confirm the project and endpoint are healthy. The synced tables will populate
# MAGIC once they are created via the UI.

# COMMAND ----------

endpoint_info = w.postgres.get_endpoint(name=compute_path)
state = endpoint_info.status.current_state if endpoint_info.status else "unknown"
host = endpoint_info.status.hosts.host if (endpoint_info.status and endpoint_info.status.hosts) else None

print(f"Project       : {PROJECT_NAME}")
print(f"Branch        : {BRANCH_NAME}")
print(f"Compute state : {state}")
print(f"Host          : {host}")
print(f"\nLakebase is ready for synced tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Grant App Service Principal Access to Lakebase
# MAGIC
# MAGIC The ShopNow Ops Hub app runs as a Databricks service principal. For it to query
# MAGIC the synced tables in Lakebase, we need to:
# MAGIC 1. Ensure the SP has `USE CATALOG` and `USE SCHEMA` in Unity Catalog
# MAGIC 2. Create a Postgres role for the SP and grant `SELECT` on the synced tables schema

# COMMAND ----------

import psycopg2

APP_NAME = "shopnow-ops-hub"

# Look up the app's service principal client ID
app_info = w.apps.get(name=APP_NAME)
sp_client_id = app_info.service_principal_client_id
print(f"App SP client ID: {sp_client_id}")

# 1. Grant UC permissions so the SP can also use the SQL warehouse fallback
spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp_client_id}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{sp_client_id}`")
spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `{sp_client_id}`")
print("UC grants applied.")

# 2. Generate Lakebase credential (as current user) and grant PG access to the SP
cred = w.postgres.generate_database_credential(endpoint=compute_path)
email = w.current_user.me().user_name

conn = psycopg2.connect(
    host=host, port=5432, dbname="databricks_postgres",
    user=email, password=cred.token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

# Create role if it doesn't exist
cur.execute(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{sp_client_id}') THEN
            CREATE ROLE "{sp_client_id}" LOGIN;
        END IF;
    END
    $$;
""")
cur.execute(f'GRANT USAGE ON SCHEMA {schema} TO "{sp_client_id}"')
cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{sp_client_id}"')
cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO "{sp_client_id}"')
conn.close()

print(f"Lakebase Postgres grants applied for SP '{sp_client_id}' on schema '{schema}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | What happened |
# MAGIC |------|--------------|
# MAGIC | 1 | Waited for **bundle-created** Lakebase project and its default branch endpoint |
# MAGIC | 2 | **Synced Tables** to be created via UI for 3 gold tables (snapshot mode) |
# MAGIC | 3 | Lakebase endpoint verified and ready for connections |
# MAGIC | 4 | Granted **app service principal** UC and Postgres permissions on synced tables |
# MAGIC
# MAGIC Once the synced tables are created, the ShopNow app can query `gold_revenue_daily_synced`,
# MAGIC `gold_top_products_synced`, and `gold_customer_ltv_synced` in the `databricks_postgres` database.
# MAGIC
# MAGIC **Next** [06-app/app.py — ShopNow Ops Hub Databricks App]($../06-app/app)
