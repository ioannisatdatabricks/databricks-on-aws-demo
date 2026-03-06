# Databricks notebook source
# MAGIC %md
# MAGIC # Restart ShopNow Ops Hub App
# MAGIC
# MAGIC This task runs after all dependencies (agent, Lakebase) are ready.
# MAGIC It grants the app's service principal access to UC,
# MAGIC writes the app config, then deploys the app.

# COMMAND ----------

dbutils.widgets.text("catalog",          "", "Catalog")
dbutils.widgets.text("schema",           "", "Schema")
dbutils.widgets.text("source_path",      "", "App source code path")
dbutils.widgets.text("agent_endpoint",   "", "Agent serving endpoint name")
dbutils.widgets.text("lakebase_instance","", "Lakebase Provisioned instance name")

catalog          = dbutils.widgets.get("catalog")
schema           = dbutils.widgets.get("schema")
source_path      = dbutils.widgets.get("source_path")
agent_endpoint   = dbutils.widgets.get("agent_endpoint")
lakebase_instance = dbutils.widgets.get("lakebase_instance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Grant App Service Principal UC Access

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

APP_NAME = "shopnow-ops-hub"

# Look up the app's service principal client ID
app_info = w.apps.get(name=APP_NAME)
sp_client_id = app_info.service_principal_client_id
print(f"App SP client ID: {sp_client_id}")

# Grant UC permissions
spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp_client_id}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{sp_client_id}`")
spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `{sp_client_id}`")
print("UC grants applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Grant App Service Principal Lakebase Access
# MAGIC
# MAGIC The SP needs a `databricks_auth` security label on its PG role so Lakebase
# MAGIC accepts its OAuth tokens. We also grant SELECT on the synced-tables schema.

# COMMAND ----------

import uuid
import psycopg2

# Generate an OAuth token (as the current user) to connect to Lakebase
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[lakebase_instance],
)
instance = w.database.get_database_instance(name=lakebase_instance)
pg_host = instance.read_write_dns
my_email = w.current_user.me().user_name

conn = psycopg2.connect(
    host=pg_host, port=5432, dbname="databricks_postgres",
    user=my_email, password=cred.token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

# Ensure PG role exists for the SP
cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (sp_client_id,))
if not cur.fetchone():
    cur.execute(f'CREATE ROLE "{sp_client_id}" LOGIN')
    print(f"Created PG role for SP: {sp_client_id}")

# Set the databricks_auth security label so OAuth tokens are accepted
sp_db_id = app_info.service_principal_id
cur.execute(
    f"""SECURITY LABEL FOR databricks_auth ON ROLE "{sp_client_id}" """
    f"""IS 'id={sp_db_id},type=SERVICE_PRINCIPAL'"""
)
print(f"Set databricks_auth security label for SP (id={sp_db_id})")

# Grant schema + table access
cur.execute(f'GRANT USAGE ON SCHEMA {schema} TO "{sp_client_id}"')
cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{sp_client_id}"')
print(f"Granted PG schema access on {schema}")

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Write app.yaml and Deploy the App

# COMMAND ----------

import time
from databricks.sdk.service.apps import AppDeployment

app = w.apps.get(APP_NAME)
deploy_path = app.default_source_code_path or source_path
if not deploy_path:
    raise RuntimeError("Cannot determine source code path: app has no previous deployment and source_path parameter is not set.")

# Write app.yaml — no PG credentials needed (app uses OAuth tokens via SDK)
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
  - name: LAKEBASE_INSTANCE
    value: "{lakebase_instance}"
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
