# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Setup — Workspace-Only Deployment
# MAGIC
# MAGIC This notebook creates all the resources needed for the ShopNow demo **without** the
# MAGIC Databricks CLI or Asset Bundles. Run it once from within a Databricks workspace after
# MAGIC cloning the repository as a **Git Folder**.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Creates a serverless SQL warehouse
# MAGIC 2. Creates the Lakeflow pipeline (Bronze -> Silver -> Gold)
# MAGIC 3. Deploys the AI/BI Lakeview dashboard
# MAGIC 4. Creates a Databricks App shell
# MAGIC 5. Creates and runs the orchestration job (data gen -> pipeline -> agent -> dashboard -> Lakebase -> app)
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Databricks workspace on AWS with **Unity Catalog**, **Serverless Compute**,
# MAGIC   **Lakebase**, and **Databricks Apps** enabled
# MAGIC - This repo cloned as a Git Folder in your workspace
# MAGIC
# MAGIC **After this notebook completes**, the orchestration job runs in the background (~25 min).
# MAGIC Walk through the demo notebooks in order while it runs.

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
# MAGIC ## Step 1 — Resolve Workspace Paths

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = ctx.notebookPath().get()

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

# Notebook paths for job tasks (relative-style paths don't work — use absolute workspace paths)
nb_setup       = f"{git_root}/src/_resources/00-setup"
nb_agent       = f"{git_root}/src/03-ai-agent/01-agent-creation"
nb_lakebase    = f"{git_root}/src/05-lakebase/01-reverse-etl-lakebase"
nb_restart_app = f"{git_root}/src/_resources/restart-app"

print(f"Git Folder root: {git_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create SQL Warehouse

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

warehouse_name = f"ShopNow Warehouse ({schema})"

existing_wh = [wh for wh in w.warehouses.list() if wh.name == warehouse_name]

if existing_wh:
    warehouse_id = existing_wh[0].id
    print(f"Reusing existing warehouse '{warehouse_name}' (id={warehouse_id})")
else:
    print(f"Creating warehouse '{warehouse_name}'...")
    from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType
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
# MAGIC ## Step 3 — Create the Lakeflow Pipeline

# COMMAND ----------

from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

pipeline_name = f"ShopNow Pipeline ({schema})"

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Deploy the Lakeview Dashboard

# COMMAND ----------

import json
import base64

export_resp = w.workspace.export(path=dashboard_json_path)
dashboard_raw = base64.b64decode(export_resp.content).decode("utf-8")

# Qualify table names so they resolve in any warehouse context
qualified = dashboard_raw.replace("FROM gold_",  f"FROM {catalog}.{schema}.gold_")
qualified = qualified.replace("from gold_",      f"from {catalog}.{schema}.gold_")

dashboard_display_name = f"ShopNow Revenue Intelligence ({schema})"

# Idempotent: search for existing dashboard via REST API
existing_dash_id = None
try:
    resp = w.api_client.do("GET", "/api/2.0/lakeview/dashboards", query={"page_size": 100})
    for d in resp.get("dashboards", []):
        if d.get("display_name") == dashboard_display_name:
            existing_dash_id = d["dashboard_id"]
            break
except Exception:
    pass

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

try:
    w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
                    body={"warehouse_id": warehouse_id})
    print("Dashboard published.")
except Exception as e:
    print(f"Dashboard publish note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Create the Databricks App

# COMMAND ----------

if schema in ("aws_webinar_demo", "aws_webinar_demo_dev"):
    app_name = "shopnow-ops-hub"
else:
    suffix = schema.replace("aws_webinar_demo_", "").replace("_", "-")
    app_name = f"shopnow-hub-{suffix}"

try:
    app = w.api_client.do("GET", f"/api/2.0/apps/{app_name}")
    print(f"App '{app_name}' already exists.")
except Exception:
    print(f"Creating app '{app_name}'...")
    app = w.api_client.do("POST", "/api/2.0/apps", body={
        "name": app_name,
        "description": "ShopNow Ops Hub — Live KPIs and AI Agent",
    })
    import time as _time
    for _ in range(60):
        status = w.api_client.do("GET", f"/api/2.0/apps/{app_name}")
        state = status.get("status", {}).get("state", status.get("compute_status", {}).get("state", "UNKNOWN"))
        if state in ("IDLE", "ACTIVE", "RUNNING"):
            break
        print(f"  Waiting for app (state={state})...")
        _time.sleep(10)
    print(f"App '{app_name}' created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Create and Run the Orchestration Job
# MAGIC
# MAGIC This creates the same job that the DAB bundle defines, then triggers it.
# MAGIC The job runs all 6 tasks in the correct dependency order (~25 min total).

# COMMAND ----------

job_name = f"ShopNow — Full Demo Orchestration ({schema})"

# Idempotent: reuse existing job
existing_jobs = [j for j in w.jobs.list(name=job_name) if j.settings.name == job_name]

if existing_jobs:
    job_id = existing_jobs[0].job_id
    print(f"Reusing existing job '{job_name}' (id={job_id})")
else:
    print(f"Creating job '{job_name}'...")
    job = w.jobs.create(
        name=job_name,
        environments=[
            {
                "environment_key": "agent_env",
                "spec": {
                    "environment_version": "4",
                    "dependencies": [
                        "databricks-langchain",
                        "databricks-agents",
                        "mlflow",
                        "langchain",
                        "langgraph",
                    ],
                },
            },
            {
                "environment_key": "lakebase_env",
                "spec": {
                    "environment_version": "4",
                    "dependencies": [
                        "databricks-sdk>=0.89.0",
                        "psycopg2-binary",
                    ],
                },
            },
        ],
        tasks=[
            {
                "task_key": "setup",
                "description": "Generate synthetic data into Unity Catalog Volume",
                "max_retries": 0,
                "notebook_task": {
                    "notebook_path": nb_setup,
                    "base_parameters": {
                        "catalog": catalog,
                        "schema": schema,
                        "volume": volume,
                    },
                },
            },
            {
                "task_key": "run_pipeline",
                "description": "Trigger the Lakeflow pipeline (Bronze → Silver → Gold)",
                "max_retries": 0,
                "depends_on": [{"task_key": "setup"}],
                "pipeline_task": {
                    "pipeline_id": pipeline_id,
                    "full_refresh": True,
                },
            },
            {
                "task_key": "deploy_agent",
                "description": "Build and deploy the ShopNow Ops AI Agent",
                "max_retries": 0,
                "depends_on": [{"task_key": "run_pipeline"}],
                "environment_key": "agent_env",
                "notebook_task": {
                    "notebook_path": nb_agent,
                    "base_parameters": {
                        "catalog": catalog,
                        "schema": schema,
                    },
                },
            },
            {
                "task_key": "reverse_etl",
                "description": "Sync gold table KPIs to Lakebase Postgres",
                "max_retries": 0,
                "depends_on": [{"task_key": "run_pipeline"}],
                "environment_key": "lakebase_env",
                "notebook_task": {
                    "notebook_path": nb_lakebase,
                    "base_parameters": {
                        "catalog": catalog,
                        "schema": schema,
                    },
                },
            },
            {
                "task_key": "refresh_dashboard",
                "description": "Refresh the ShopNow Revenue Intelligence dashboard",
                "max_retries": 0,
                "depends_on": [{"task_key": "run_pipeline"}],
                "dashboard_task": {
                    "dashboard_id": dashboard_id,
                    "warehouse_id": warehouse_id,
                },
            },
            {
                "task_key": "start_app",
                "description": "Grant SP access and deploy the ShopNow Ops Hub app",
                "max_retries": 0,
                "depends_on": [
                    {"task_key": "deploy_agent"},
                    {"task_key": "reverse_etl"},
                ],
                "environment_key": "lakebase_env",
                "notebook_task": {
                    "notebook_path": nb_restart_app,
                    "base_parameters": {
                        "catalog": catalog,
                        "schema": schema,
                        "source_path": app_source,
                        "agent_endpoint": "shopnow-ops-agent",
                        "lakebase_instance": "shopnow-lakebase",
                        "app_name": app_name,
                    },
                },
            },
        ],
    )
    job_id = job.job_id
    print(f"Created job '{job_name}' (id={job_id})")

# Trigger the job
run = w.jobs.run_now(job_id=job_id)
run_id = run.run_id
print(f"\nJob triggered! Run ID: {run_id}")
print(f"Monitor progress at: Workflows > Jobs > '{job_name}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete — Job Running in Background
# MAGIC
# MAGIC The orchestration job is now running (~25 min). It will:
# MAGIC 1. Generate synthetic data (customers, orders, products, clickstream)
# MAGIC 2. Run the Lakeflow pipeline (Bronze → Silver → Gold)
# MAGIC 3. Deploy the AI agent to a Model Serving endpoint
# MAGIC 4. Refresh the AI/BI dashboard
# MAGIC 5. Create Lakebase instance and sync gold tables
# MAGIC 6. Deploy the ShopNow Ops Hub app
# MAGIC
# MAGIC **While it runs**, walk through the demo notebooks in order:
# MAGIC
# MAGIC | # | Notebook | Action |
# MAGIC |---|---------|--------|
# MAGIC | 0 | `00-introduction` | **Read** — architecture overview |
# MAGIC | 1 | `01-pipeline/01-declarative-pipeline` | **Read only** — executed by the pipeline engine |
# MAGIC | 1 | `01-pipeline/02-pipeline-cdc` | **Read only** — executed by the pipeline engine |
# MAGIC | 2 | `02-governance/01-unity-catalog` | **Run interactively** after pipeline completes (~8 min) |
# MAGIC | 3 | `03-ai-agent/01-agent-creation` | **Read** — the job handles deployment |
# MAGIC | 4 | `04-ai-bi/01-dashboard` | **Read** — open the dashboard in the UI |
# MAGIC | 4 | `04-ai-bi/02-genie-space` | **Read** — illustrative queries |
# MAGIC | 5 | `05-lakebase/01-reverse-etl-lakebase` | **Read** — the job handles setup |
# MAGIC | 6 | `06-app/app.py` | **Read** — open the app from the Apps page after job completes |
# MAGIC
# MAGIC **Cleanup:** Run `_resources/workshop-cleanup` when you are done.

# COMMAND ----------

print("=" * 70)
print("  WORKSHOP SETUP COMPLETE — JOB RUNNING")
print("=" * 70)
print()
print(f"  Catalog:    {catalog}")
print(f"  Schema:     {schema}")
print(f"  Warehouse:  {warehouse_name} (id={warehouse_id})")
print(f"  Pipeline:   {pipeline_name} (id={pipeline_id})")
print(f"  Dashboard:  {dashboard_display_name} (id={dashboard_id})")
print(f"  App:        {app_name}")
print(f"  Job:        {job_name} (id={job_id})")
print(f"  Run ID:     {run_id}")
print()
print("  The job runs in the background (~25 min).")
print("  Monitor: Workflows > Jobs")
print("  When complete, open the app from the Apps page.")
print("=" * 70)
