# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Setup — Workspace-Only Deployment
# MAGIC
# MAGIC This notebook creates all the resources needed for the ShopNow demo **without** the
# MAGIC Databricks CLI or Asset Bundles. Run it once from within a Databricks workspace after
# MAGIC cloning the repository as a **Git Folder**.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Generates synthetic e-commerce data (customers, orders, products, clickstream)
# MAGIC 2. Creates a serverless SQL warehouse
# MAGIC 3. Creates and triggers the Lakeflow pipeline (Bronze -> Silver -> Gold)
# MAGIC 4. Deploys the AI/BI Lakeview dashboard
# MAGIC 5. Creates a Databricks App shell (configured later by `restart-app.py`)
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Databricks workspace on AWS with **Unity Catalog**, **Serverless Compute**,
# MAGIC   **Lakebase**, and **Databricks Apps** enabled
# MAGIC - This repo cloned as a Git Folder in your workspace
# MAGIC
# MAGIC **After this notebook completes**, walk through the demo notebooks in order.
# MAGIC See the README for the full sequence.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema",  "aws_webinar_demo", "Schema")
dbutils.widgets.text("volume",  "raw_data", "Volume")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
volume  = dbutils.widgets.get("volume")

print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")
print(f"Volume:  {volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Resolve Workspace Paths
# MAGIC
# MAGIC Determine the absolute workspace path of the Git Folder root so we can
# MAGIC reference pipeline notebooks and the app source directory.

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = ctx.notebookPath().get()

# This notebook lives at <git_root>/src/_resources/workshop-setup
# Strip the suffix to get the repo root.
if "/src/_resources/" not in notebook_path:
    raise RuntimeError(
        f"Cannot determine Git Folder root from notebook path: {notebook_path}\n"
        "Expected this notebook to be at <git_root>/src/_resources/workshop-setup"
    )

git_root = notebook_path.rsplit("/src/_resources/", 1)[0]

pipeline_nb1 = f"{git_root}/src/01-pipeline/01-declarative-pipeline"
pipeline_nb2 = f"{git_root}/src/01-pipeline/02-pipeline-cdc"
app_source   = f"{git_root}/src/06-app"
dashboard_json_path = f"{git_root}/src/04-ai-bi/shopnow_revenue.lvdash.json"

print(f"Git Folder root:  {git_root}")
print(f"Pipeline NB 1:    {pipeline_nb1}")
print(f"Pipeline NB 2:    {pipeline_nb2}")
print(f"App source:       {app_source}")
print(f"Dashboard JSON:   {dashboard_json_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Generate Synthetic Data

# COMMAND ----------

dbutils.notebook.run(
    "00-setup",
    timeout_seconds=600,
    arguments={"catalog": catalog, "schema": schema, "volume": volume},
)
print("Data generation complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create SQL Warehouse

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
)

w = WorkspaceClient()

warehouse_name = f"ShopNow Warehouse ({schema})"

# Idempotent: reuse existing warehouse if one already exists with this name
existing_wh = [wh for wh in w.warehouses.list() if wh.name == warehouse_name]

if existing_wh:
    warehouse_id = existing_wh[0].id
    print(f"Reusing existing warehouse '{warehouse_name}' (id={warehouse_id})")
else:
    print(f"Creating warehouse '{warehouse_name}'...")
    wh = w.warehouses.create_and_wait(
        name=warehouse_name,
        cluster_size="2X-Small",
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        enable_serverless_compute=True,
        max_num_clusters=2,
        min_num_clusters=1,
        auto_stop_mins=60,
    )
    warehouse_id = wh.id
    print(f"Created warehouse '{warehouse_name}' (id={warehouse_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Create and Trigger the Lakeflow Pipeline

# COMMAND ----------

import time
from databricks.sdk.service.pipelines import (
    PipelineLibrary,
    NotebookLibrary,
)

pipeline_name = f"ShopNow Pipeline ({schema})"

# Idempotent: reuse existing pipeline
existing_pl = [p for p in w.pipelines.list_pipelines() if p.name == pipeline_name]

if existing_pl:
    pipeline_id = existing_pl[0].pipeline_id
    print(f"Reusing existing pipeline '{pipeline_name}' (id={pipeline_id})")
else:
    print(f"Creating pipeline '{pipeline_name}'...")
    result = w.pipelines.create(
        name=pipeline_name,
        serverless=True,
        catalog=catalog,
        target=schema,
        channel="CURRENT",
        libraries=[
            PipelineLibrary(notebook=NotebookLibrary(path=pipeline_nb1)),
            PipelineLibrary(notebook=NotebookLibrary(path=pipeline_nb2)),
        ],
        configuration={
            "catalog": catalog,
            "schema":  schema,
            "volume":  volume,
        },
        continuous=False,
        development=True,
    )
    pipeline_id = result.pipeline_id
    print(f"Created pipeline '{pipeline_name}' (id={pipeline_id})")

# Trigger a full-refresh update
print("Triggering pipeline full refresh...")
update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True)
update_id = update.update_id
print(f"Pipeline update started (update_id={update_id}). This runs in the background (~5-8 min).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for the pipeline to complete
# MAGIC
# MAGIC The pipeline typically finishes in 5-8 minutes. We poll here so that
# MAGIC subsequent steps (dashboard, etc.) can use the gold tables.

# COMMAND ----------

# Poll for pipeline completion (up to 20 min)
TIMEOUT_S = 1200
POLL_S = 15
start = time.time()

while time.time() - start < TIMEOUT_S:
    detail = w.pipelines.get(pipeline_id=pipeline_id)
    state = str(detail.state).split(".")[-1] if detail.state else "UNKNOWN"
    if state in ("IDLE",):
        # Check if the latest update succeeded
        latest = detail.latest_updates[0] if detail.latest_updates else None
        if latest:
            ustate = str(latest.state).split(".")[-1]
            if ustate == "COMPLETED":
                print(f"Pipeline completed successfully in {int(time.time() - start)}s.")
                break
            elif ustate in ("FAILED", "CANCELED"):
                raise RuntimeError(f"Pipeline update {ustate}. Check the pipeline UI for details.")
        else:
            print(f"Pipeline is IDLE with no updates. It may have completed before polling started.")
            break
    print(f"  Pipeline state: {state} (elapsed: {int(time.time() - start)}s)...")
    time.sleep(POLL_S)
else:
    print(f"WARNING: Pipeline did not complete within {TIMEOUT_S}s. Check the Pipelines UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Deploy the Lakeview Dashboard

# COMMAND ----------

import json
import base64

# Read the dashboard JSON from the workspace (Git Folder)
export_resp = w.workspace.export(path=dashboard_json_path)
dashboard_raw = base64.b64decode(export_resp.content).decode("utf-8")

# Qualify table names in dataset queries so they resolve in any warehouse context.
# The JSON uses unqualified names like "FROM gold_revenue_daily".
# We prepend the catalog.schema prefix.
qualified = dashboard_raw.replace("FROM gold_",  f"FROM {catalog}.{schema}.gold_")
qualified = qualified.replace("from gold_",      f"from {catalog}.{schema}.gold_")

dashboard_display_name = f"ShopNow Revenue Intelligence ({schema})"

# Idempotent: search for existing dashboard via REST API (w.lakeview.list() may
# not be available on all SDK versions installed in workspace runtimes).
existing_dash_id = None
try:
    resp = w.api_client.do("GET", "/api/2.0/lakeview/dashboards", query={"page_size": 100})
    for d in resp.get("dashboards", []):
        if d.get("display_name") == dashboard_display_name:
            existing_dash_id = d["dashboard_id"]
            break
except Exception:
    pass  # First deploy — no dashboards yet

body = {
    "display_name": dashboard_display_name,
    "serialized_dashboard": qualified,
    "warehouse_id": warehouse_id,
}

if existing_dash_id:
    dashboard_id = existing_dash_id
    w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dashboard_id}", body=body)
    print(f"Updated existing dashboard '{dashboard_display_name}' (id={dashboard_id})")
else:
    resp = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body=body)
    dashboard_id = resp["dashboard_id"]
    print(f"Created dashboard '{dashboard_display_name}' (id={dashboard_id})")

# Publish the dashboard so it is viewable
try:
    w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
                    body={"warehouse_id": warehouse_id})
    print("Dashboard published.")
except Exception as e:
    print(f"Dashboard publish note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Create the Databricks App
# MAGIC
# MAGIC This creates the app resource only. The app will be fully configured and deployed
# MAGIC when you run `_resources/restart-app` after the agent endpoint and Lakebase are ready.

# COMMAND ----------

# Derive a unique app name from the schema
if schema in ("aws_webinar_demo", "aws_webinar_demo_dev"):
    app_name = "shopnow-ops-hub"
else:
    suffix = schema.replace("aws_webinar_demo_", "").replace("_", "-")
    app_name = f"shopnow-hub-{suffix}"

try:
    app = w.apps.get(name=app_name)
    print(f"App '{app_name}' already exists.")
except Exception:
    print(f"Creating app '{app_name}'...")
    app = w.apps.create_and_wait(
        name=app_name,
        description="ShopNow Ops Hub — Live KPIs and AI Agent",
    )
    print(f"App '{app_name}' created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC All resources have been created. Walk through the demo notebooks in order:
# MAGIC
# MAGIC | # | Notebook | Action |
# MAGIC |---|---------|--------|
# MAGIC | 0 | `00-introduction` | **Read** — architecture overview |
# MAGIC | 1 | `01-pipeline/01-declarative-pipeline` | **Read only** — executed by the pipeline engine |
# MAGIC | 1 | `01-pipeline/02-pipeline-cdc` | **Read only** — executed by the pipeline engine |
# MAGIC | 2 | `02-governance/01-unity-catalog` | **Run interactively** (set widgets: catalog, schema) |
# MAGIC | 3 | `03-ai-agent/01-agent-creation` | **Run interactively** (~20 min for endpoint deploy) |
# MAGIC | 4 | `04-ai-bi/01-dashboard` | **Read** — open the dashboard in the UI instead |
# MAGIC | 4 | `04-ai-bi/02-genie-space` | **Read** — illustrative queries |
# MAGIC | 5 | `05-lakebase/01-reverse-etl-lakebase` | **Run interactively** |
# MAGIC | 6 | `_resources/restart-app` | **Run** after agent + Lakebase are ready |
# MAGIC
# MAGIC > **Note:** When running `restart-app`, pass the `app_name` widget value: **`{app_name}`**
# MAGIC >
# MAGIC > **Cleanup:** Run `_resources/workshop-cleanup` when you are done to remove all resources.

# COMMAND ----------

print("=" * 70)
print("  WORKSHOP SETUP COMPLETE")
print("=" * 70)
print()
print(f"  Catalog:    {catalog}")
print(f"  Schema:     {schema}")
print(f"  Warehouse:  {warehouse_name} (id={warehouse_id})")
print(f"  Pipeline:   {pipeline_name} (id={pipeline_id})")
print(f"  Dashboard:  {dashboard_display_name} (id={dashboard_id})")
print(f"  App:        {app_name}")
print()
print("  Next: walk through the notebooks listed above in order.")
print("  When running restart-app, set these widgets:")
print(f"    catalog:          {catalog}")
print(f"    schema:           {schema}")
print(f"    source_path:      {app_source}")
print(f"    agent_endpoint:   shopnow-ops-agent")
print(f"    lakebase_instance: shopnow-lakebase")
print(f"    app_name:         {app_name}")
print("=" * 70)
