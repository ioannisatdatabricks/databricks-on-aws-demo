# Databricks notebook source
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
app_name = "shopnow-ops-hub"

app = w.apps.get(app_name)
status = str(app.compute_status.state).split(".")[-1]

if status == "ACTIVE":
    print(f"App '{app_name}' is running — stopping first...")
    w.apps.stop(app_name).result()

print(f"Starting app '{app_name}'...")
w.apps.start(app_name).result()
print(f"App '{app_name}' started successfully.")
