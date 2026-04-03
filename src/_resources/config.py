# Databricks notebook source
# MAGIC %md
# MAGIC # Demo Configuration
# MAGIC
# MAGIC Central configuration for the ShopNow demo. **Update the values below** to match
# MAGIC your environment, then run the demo notebooks in order.
# MAGIC
# MAGIC All demo notebooks load this file automatically via `%run`.

# COMMAND ----------

# ── Update these for your environment ──────────────────────────────────
CATALOG = "workspace"       # Unity Catalog catalog name
SCHEMA  = "aws_webinar_demo"  # Schema (database) for all demo tables
VOLUME  = "raw_data"          # UC Volume for raw landing data

# COMMAND ----------

# Create widgets (overridden by job base_parameters in DAB mode)
dbutils.widgets.text("catalog", CATALOG, "Catalog")
dbutils.widgets.text("schema",  SCHEMA,  "Schema")
dbutils.widgets.text("volume",  VOLUME,  "Volume")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
volume  = dbutils.widgets.get("volume")

print(f"Config: catalog={catalog}  schema={schema}  volume={volume}")
