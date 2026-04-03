# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Cleanup
# MAGIC
# MAGIC Removes all resources created by the workshop-setup notebook or by
# MAGIC running the demo notebooks interactively.
# MAGIC
# MAGIC **Deletion order:**
# MAGIC 1. Agent serving endpoint
# MAGIC 2. Lakebase synced tables + instance
# MAGIC 3. Databricks App
# MAGIC 4. Lakeview dashboard
# MAGIC 5. Orchestration job
# MAGIC 6. Lakeflow pipeline
# MAGIC 7. SQL warehouse
# MAGIC 8. Unity Catalog schema (CASCADE — drops all tables, functions, models, volumes)

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

print(f"Cleaning up resources for: {catalog}.{schema}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Delete Agent Serving Endpoint

# COMMAND ----------

AGENT_ENDPOINT = "shopnow-ops-agent"

try:
    w.serving_endpoints.delete(name=AGENT_ENDPOINT)
    print(f"Deleted serving endpoint: {AGENT_ENDPOINT}")
except Exception as e:
    print(f"Serving endpoint '{AGENT_ENDPOINT}' not found or already deleted: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b — Delete MLflow Experiment Traces and Runs

# COMMAND ----------

import mlflow

client = mlflow.MlflowClient()

try:
    experiments = client.search_experiments(
        filter_string="name LIKE '%03-ai-agent/01-agent-creation%'"
    )
    for exp in experiments:
        print(f"Found MLflow experiment: {exp.name} (id={exp.experiment_id})")

        # Delete all traces (autolog traces from agent testing)
        import time as _time
        deleted = client.delete_traces(
            experiment_id=exp.experiment_id,
            max_timestamp_millis=int(_time.time() * 1000) + 60_000,
        )
        print(f"  Deleted {deleted} traces")

        # Delete all runs (model logging runs)
        runs = client.search_runs(experiment_ids=[exp.experiment_id])
        for run in runs:
            client.delete_run(run.info.run_id)
        print(f"  Deleted {len(runs)} runs")

        # Delete the experiment only in DAB mode — Git Folder experiments can't be deleted
        if "/.bundle/" in (exp.name or ""):
            client.delete_experiment(exp.experiment_id)
            print(f"  Deleted experiment: {exp.name}")
        else:
            print(f"  Skipped experiment deletion (Git Folder): {exp.name}")

    if not experiments:
        print("No MLflow experiment found for agent notebook (skipped).")
except Exception as e:
    print(f"Error cleaning up MLflow experiment: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Delete Lakebase Synced Tables and Instance

# COMMAND ----------

LAKEBASE_INSTANCE = "shopnow-lakebase"

synced_tables = [
    f"{catalog}.{schema}.gold_revenue_daily_synced",
    f"{catalog}.{schema}.gold_top_products_synced",
    f"{catalog}.{schema}.gold_customer_ltv_synced",
]

for table_name in synced_tables:
    try:
        w.api_client.do("DELETE", f"/api/2.0/database/synced_tables/{table_name}")
        print(f"Deleted synced table: {table_name}")
    except Exception as e:
        if "NOT_FOUND" in str(e) or "does not exist" in str(e):
            print(f"Synced table not found (skipped): {table_name}")
        else:
            print(f"Error deleting synced table {table_name}: {e}")

# Delete the auto-created Lakeflow pipelines for synced tables
try:
    for p in w.pipelines.list_pipelines():
        if p.name and p.name.startswith("Synced table:") and f"{catalog}.{schema}." in p.name:
            w.pipelines.delete(pipeline_id=p.pipeline_id)
            print(f"Deleted synced table pipeline: {p.name}")
except Exception as e:
    print(f"Error deleting synced table pipelines: {e}")

try:
    w.api_client.do("DELETE", f"/api/2.0/database/instances/{LAKEBASE_INSTANCE}")
    print(f"Deleted Lakebase instance: {LAKEBASE_INSTANCE}")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e):
        print(f"Lakebase instance not found (skipped): {LAKEBASE_INSTANCE}")
    else:
        print(f"Error deleting Lakebase instance: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Delete Databricks App

# COMMAND ----------

import time

# Derive app name using the same logic as workshop-setup
if schema in ("aws_webinar_demo", "aws_webinar_demo_dev"):
    app_name = "shopnow-ops-hub"
else:
    suffix = schema.replace("aws_webinar_demo_", "").replace("_", "-")
    app_name = f"shopnow-hub-{suffix}"

try:
    w.api_client.do("DELETE", f"/api/2.0/apps/{app_name}")
    print(f"Deleting app '{app_name}' (may take several minutes)...")
    for _ in range(30):
        try:
            w.api_client.do("GET", f"/api/2.0/apps/{app_name}")
            time.sleep(10)
        except Exception:
            break
    print(f"App '{app_name}' deleted.")
except Exception as e:
    print(f"App '{app_name}' not found or already deleted: {e}")

# Delete the copied app source directory (created by restart-app in workshop mode)
try:
    my_user = w.current_user.me().user_name
    app_deploy_dir = f"/Workspace/Users/{my_user}/apps/{app_name}"
    w.workspace.delete(app_deploy_dir, recursive=True)
    print(f"Deleted app deploy directory: {app_deploy_dir}")
except Exception:
    pass  # Directory may not exist (DAB mode or already cleaned up)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Delete Lakeview Dashboard

# COMMAND ----------

dashboard_display_name = f"ShopNow Revenue Intelligence ({schema})"

try:
    # Use REST API to find dashboard (w.lakeview.list() may not exist on all SDK versions)
    resp = w.api_client.do("GET", "/api/2.0/lakeview/dashboards", query={"page_size": 100})
    found = False
    for d in resp.get("dashboards", []):
        if d.get("display_name") == dashboard_display_name:
            w.api_client.do("DELETE", f"/api/2.0/lakeview/dashboards/{d['dashboard_id']}")
            print(f"Trashed dashboard: {d['display_name']} (id={d['dashboard_id']})")
            found = True
            break
    if not found:
        print(f"Dashboard '{dashboard_display_name}' not found (skipped).")
except Exception as e:
    print(f"Error deleting dashboard: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Delete Orchestration Job

# COMMAND ----------

job_name = f"ShopNow — Full Demo Orchestration ({schema})"

try:
    found = False
    for j in w.jobs.list(name=job_name):
        if j.settings.name == job_name:
            w.jobs.delete(job_id=j.job_id)
            print(f"Deleted job: {j.settings.name} (id={j.job_id})")
            found = True
            break
    if not found:
        print(f"Job '{job_name}' not found (skipped).")
except Exception as e:
    print(f"Error deleting job: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Delete Lakeflow Pipeline

# COMMAND ----------

pipeline_name = f"ShopNow Pipeline ({schema})"

try:
    for p in w.pipelines.list_pipelines():
        if p.name == pipeline_name:
            w.pipelines.delete(pipeline_id=p.pipeline_id)
            print(f"Deleted pipeline: {p.name} (id={p.pipeline_id})")
            break
    else:
        print(f"Pipeline '{pipeline_name}' not found (skipped).")
except Exception as e:
    print(f"Error deleting pipeline: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Delete SQL Warehouse

# COMMAND ----------

warehouse_name = f"ShopNow Warehouse ({schema})"

try:
    for wh in w.warehouses.list():
        if wh.name == warehouse_name:
            w.warehouses.delete(id=wh.id)
            print(f"Deleted warehouse: {wh.name} (id={wh.id})")
            break
    else:
        print(f"Warehouse '{warehouse_name}' not found (skipped).")
except Exception as e:
    print(f"Error deleting warehouse: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — Drop Unity Catalog Schema
# MAGIC
# MAGIC This removes all tables, functions, registered models, and volumes within the schema.

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
print(f"Dropped schema: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Complete
# MAGIC
# MAGIC All workshop resources have been removed. You can safely delete the Git Folder
# MAGIC from your workspace if you no longer need it.

# COMMAND ----------

print("=" * 70)
print("  WORKSHOP CLEANUP COMPLETE")
print("=" * 70)
print()
print(f"  Catalog:   {catalog}")
print(f"  Schema:    {schema} (dropped)")
print(f"  Endpoint:  {AGENT_ENDPOINT}")
print(f"  Lakebase:  {LAKEBASE_INSTANCE}")
print(f"  App:       {app_name}")
print(f"  Dashboard: {dashboard_display_name}")
print(f"  Pipeline:  {pipeline_name}")
print(f"  Job:       {job_name}")
print(f"  Warehouse: {warehouse_name}")
print("=" * 70)
