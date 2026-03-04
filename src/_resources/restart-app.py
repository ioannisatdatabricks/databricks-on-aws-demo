# Databricks notebook source
# MAGIC %md
# MAGIC # Restart ShopNow Ops Hub App
# MAGIC
# MAGIC This task runs after all dependencies (agent, Lakebase) are ready.
# MAGIC It grants the app's service principal access to UC and Lakebase,
# MAGIC writes the app config, then deploys the app.

# COMMAND ----------

dbutils.widgets.text("catalog",          "", "Catalog")
dbutils.widgets.text("schema",           "", "Schema")
dbutils.widgets.text("source_path",      "",                  "App source code path")
dbutils.widgets.text("agent_endpoint",   "",                  "Agent serving endpoint name")
dbutils.widgets.text("lakebase_project", "",                  "Lakebase project name")
dbutils.widgets.text("lakebase_branch",  "",                  "Lakebase branch name")

catalog          = dbutils.widgets.get("catalog")
schema           = dbutils.widgets.get("schema")
source_path      = dbutils.widgets.get("source_path")
agent_endpoint   = dbutils.widgets.get("agent_endpoint")
lakebase_project = dbutils.widgets.get("lakebase_project")
lakebase_branch  = dbutils.widgets.get("lakebase_branch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Grant App Service Principal Access

# COMMAND ----------

import secrets
import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

APP_NAME = "shopnow-ops-hub"

# Look up the app's service principal client ID
app_info = w.apps.get(name=APP_NAME)
sp_client_id = app_info.service_principal_client_id
print(f"App SP client ID: {sp_client_id}")

# 1. Grant UC permissions
spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp_client_id}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{sp_client_id}`")
spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `{sp_client_id}`")
print("UC grants applied.")

# 2. Grant Lakebase Postgres access to the SP with a native password
branch_path = f"projects/{lakebase_project}/branches/{lakebase_branch}"
endpoints = list(w.postgres.list_endpoints(parent=branch_path))
if not endpoints:
    raise RuntimeError(f"No Lakebase endpoint found for {branch_path}")

endpoint = endpoints[0]
pg_host = endpoint.status.hosts.host if (endpoint.status and endpoint.status.hosts) else None
if not pg_host:
    raise RuntimeError(f"Lakebase endpoint {endpoint.name} has no host")

cred = w.postgres.generate_database_credential(endpoint=endpoint.name)
email = w.current_user.me().user_name

conn = psycopg2.connect(
    host=pg_host, port=5432, dbname="databricks_postgres",
    user=email, password=cred.token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

# Generate a random native password for the SP role
pg_password = secrets.token_urlsafe(32)

# Create role if it doesn't exist, then set native password
cur.execute(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{sp_client_id}') THEN
            CREATE ROLE "{sp_client_id}" LOGIN;
        END IF;
    END
    $$;
""")
cur.execute(f"ALTER ROLE \"{sp_client_id}\" WITH PASSWORD %s", (pg_password,))
cur.execute(f'GRANT USAGE ON SCHEMA {schema} TO "{sp_client_id}"')
cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{sp_client_id}"')
cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO "{sp_client_id}"')
conn.close()

print(f"Lakebase Postgres grants applied for SP '{sp_client_id}' on schema '{schema}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Write app.yaml and Deploy the App

# COMMAND ----------

import time
from databricks.sdk.service.apps import AppDeployment

app = w.apps.get(APP_NAME)
deploy_path = app.default_source_code_path or source_path
if not deploy_path:
    raise RuntimeError("Cannot determine source code path: app has no previous deployment and source_path parameter is not set.")

# Write app.yaml with env vars from job parameters (no hardcoded values)
app_yaml = f"""\
command:
  - uvicorn
  - "app:app"
  - "--host"
  - "0.0.0.0"
  - "--port"
  - "8000"

env:
  - name: AGENT_ENDPOINT
    value: "{agent_endpoint}"
  - name: UC_SCHEMA
    value: "{schema}"
  - name: PGHOST
    value: "{pg_host}"
  - name: PGUSER
    value: "{sp_client_id}"
  - name: PGPASSWORD
    value: "{pg_password}"
"""

# Write app.yaml into the source directory using workspace import API
import io
from databricks.sdk.service.workspace import ImportFormat
w.workspace.upload(
    f"{deploy_path}/app.yaml",
    io.BytesIO(app_yaml.encode()),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print(f"Wrote app.yaml to {deploy_path}/app.yaml")

# Start compute if needed
status = str(app.compute_status.state).split(".")[-1]
if status != "ACTIVE":
    print(f"App compute is {status} — starting first...")
    w.apps.start(APP_NAME).result()
    for _ in range(60):
        app = w.apps.get(APP_NAME)
        s = str(app.compute_status.state).split(".")[-1]
        if s == "ACTIVE":
            break
        print(f"  Waiting for compute to be ACTIVE (currently {s})...")
        time.sleep(10)
    print(f"App compute is ACTIVE.")

# Deploy
print(f"Deploying app '{APP_NAME}' from '{deploy_path}'...")
w.apps.deploy(APP_NAME, app_deployment=AppDeployment(source_code_path=deploy_path)).result()
for _ in range(60):
    app = w.apps.get(APP_NAME)
    s = str(app.active_deployment.status.state).split(".")[-1] if app.active_deployment else "UNKNOWN"
    if s == "SUCCEEDED":
        break
    print(f"  Waiting for deployment (currently {s})...")
    time.sleep(10)
print(f"App '{APP_NAME}' deployed and running.")
