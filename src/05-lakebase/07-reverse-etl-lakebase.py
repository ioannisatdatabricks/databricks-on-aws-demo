# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/db-icon.png" style="float:left; margin-right:20px" width="60px"/>
# MAGIC
# MAGIC # 7 — Reverse ETL to Lakebase Provisioned (Synced Tables)
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
# MAGIC gold_top_products   ─┼──► Synced Tables (snapshot) ──► Lakebase Provisioned ──► ShopNow App
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
# MAGIC ## Step 1 — Create or Get Lakebase Provisioned Instance

# COMMAND ----------

import time
import uuid
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.database import DatabaseInstance

w = WorkspaceClient()

INSTANCE_NAME = "shopnow-lakebase"
CAPACITY      = "CU_1"
MAX_WAIT_SEC  = 600
POLL_SEC      = 15

# Create instance if it doesn't exist, otherwise get existing one
try:
    instance = w.database.get_database_instance(name=INSTANCE_NAME)
    print(f"Instance already exists: {instance.name} (state: {instance.state})")
except NotFound:
    print(f"Creating Lakebase Provisioned instance '{INSTANCE_NAME}' ...")
    instance = w.database.create_database_instance(
        database_instance=DatabaseInstance(
            name=INSTANCE_NAME,
            capacity=CAPACITY,
            stopped=False,
            enable_pg_native_login=True,
        )
    ).result()
    print(f"Instance created: {instance.name}")

# Wait for instance to be RUNNING
print(f"Waiting for instance to be RUNNING ...")
start = time.time()
while (time.time() - start) < MAX_WAIT_SEC:
    instance = w.database.get_database_instance(name=INSTANCE_NAME)
    state = str(instance.state).split(".")[-1]
    if state in ("RUNNING", "AVAILABLE"):
        print(f"Instance is {state}. DNS: {instance.read_write_dns}")
        break
    if state in ("STOPPED", "FAILED"):
        if state == "STOPPED":
            print("Instance is STOPPED — starting it ...")
            w.database.start_database_instance(name=INSTANCE_NAME)
        else:
            raise RuntimeError(f"Instance is in FAILED state")
    print(f"  State: {state} ({int(time.time() - start)}s elapsed) ...")
    time.sleep(POLL_SEC)
else:
    raise RuntimeError(f"Instance '{INSTANCE_NAME}' did not reach RUNNING within {MAX_WAIT_SEC}s")

pg_host = instance.read_write_dns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create Synced Tables Programmatically
# MAGIC
# MAGIC Unlike Lakebase Autoscaling, Lakebase Provisioned supports creating synced tables
# MAGIC via the SDK. We create one synced table per gold table using **snapshot** mode.

# COMMAND ----------

from databricks.sdk.service.database import (
    SyncedDatabaseTable,
    SyncedTableSpec,
    SyncedTableSchedulingPolicy,
)

SYNCED_TABLES = [
    {
        "source": f"{catalog}.{schema}.gold_revenue_daily",
        "target": f"{catalog}.{schema}.gold_revenue_daily_synced",
        "pk": ["order_day", "ship_country"],
    },
    {
        "source": f"{catalog}.{schema}.gold_top_products",
        "target": f"{catalog}.{schema}.gold_top_products_synced",
        "pk": ["product_id"],
    },
    {
        "source": f"{catalog}.{schema}.gold_customer_ltv",
        "target": f"{catalog}.{schema}.gold_customer_ltv_synced",
        "pk": ["customer_id"],
    },
]

for tbl in SYNCED_TABLES:
    try:
        existing = w.database.get_synced_database_table(name=tbl["target"])
        print(f"Synced table already exists: {tbl['target']}")
    except NotFound:
        print(f"Creating synced table: {tbl['source']} → {tbl['target']} ...")
        w.database.create_synced_database_table(
            SyncedDatabaseTable(
                name=tbl["target"],
                database_instance_name=INSTANCE_NAME,
                logical_database_name="databricks_postgres",
                spec=SyncedTableSpec(
                    source_table_full_name=tbl["source"],
                    primary_key_columns=tbl["pk"],
                    scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
                ),
            )
        )
        print(f"  Created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Wait for Synced Tables to Complete

# COMMAND ----------

print("Waiting for synced tables to finish initial sync ...")
for tbl in SYNCED_TABLES:
    start = time.time()
    while (time.time() - start) < MAX_WAIT_SEC:
        status = w.database.get_synced_database_table(name=tbl["target"])
        state = str(status.data_synchronization_status.detailed_state).split(".")[-1] if status.data_synchronization_status else "UNKNOWN"
        if "ONLINE" in state:
            print(f"  {tbl['target']}: {state}")
            break
        if "FAILED" in state:
            msg = status.data_synchronization_status.message if status.data_synchronization_status else ""
            print(f"  WARNING: {tbl['target']}: {state} — {msg}")
            break
        print(f"  {tbl['target']}: {state} ({int(time.time() - start)}s) ...")
        time.sleep(POLL_SEC)
    else:
        print(f"  WARNING: {tbl['target']} did not reach ONLINE within {MAX_WAIT_SEC}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify Lakebase Connectivity

# COMMAND ----------

import psycopg2

cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[INSTANCE_NAME],
)
email = w.current_user.me().user_name

conn = psycopg2.connect(
    host=pg_host, port=5432, dbname="databricks_postgres",
    user=email, password=cred.token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT version()")
print(f"Connected to: {cur.fetchone()[0]}")
cur.execute("SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') LIMIT 20")
tables = cur.fetchall()
print(f"\nTables visible ({len(tables)}):")
for s, t in tables:
    print(f"  {s}.{t}")
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | What happened |
# MAGIC |------|--------------|
# MAGIC | 1 | Created/verified **Lakebase Provisioned** instance (`shopnow-lakebase`, CU_1) |
# MAGIC | 2 | Created **synced tables** programmatically for 3 gold tables (snapshot mode) |
# MAGIC | 3 | Waited for initial sync to complete |
# MAGIC | 4 | Verified Postgres connectivity and listed synced tables |
# MAGIC
# MAGIC App service principal grants are handled by the **start-app** task.
# MAGIC
# MAGIC **Next** [06-app/app.py — ShopNow Ops Hub Databricks App]($../06-app/app)
