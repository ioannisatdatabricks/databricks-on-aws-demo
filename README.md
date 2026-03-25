# Data Intelligence with Databricks on AWS

A hands-on demo that takes you from raw data files in S3 to a production AI-powered web application — all on Databricks.

You will follow **ShopNow**, a fictional e-commerce retailer, as it builds a complete data + AI platform. Along the way you will see how Databricks unifies data engineering, governance, business intelligence, AI, and application development on a single platform running on AWS.

## What You Will Learn

| Capability | What You'll See |
|-----------|-----------------|
| **Lakeflow Pipelines** | Ingest raw CSV, JSON, and Parquet files using Autoloader; build a medallion architecture (Bronze/Silver/Gold) with built-in data quality constraints |
| **Unity Catalog** | Tag PII columns, apply dynamic data masking, enforce row-level security, and explore automatic column-level lineage |
| **Mosaic AI Agent** | Build a conversational AI agent backed by live SQL data, using LangGraph + Unity Catalog Functions + Foundation Model API |
| **AI/BI Dashboards & Genie** | Create dashboards and a natural-language analytics interface so business users can ask questions without writing SQL |
| **Lakebase** | Sync gold-layer tables to a managed PostgreSQL instance for operational applications — no external database or reverse ETL tool needed |
| **Databricks Apps** | Deploy a FastAPI web application that combines live KPIs from Lakebase with the AI agent — hosted and managed by Databricks |
| **Asset Bundles** | See how everything above is packaged as code and deployed with a single CLI command |

## Prerequisites

- A Databricks workspace on AWS with **Unity Catalog**, **Serverless Compute**, **Lakebase**, and **Databricks Apps** enabled
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.200+ installed on your laptop

## Option A: Quick Start with Asset Bundles (CLI)

```bash
# 1. Clone this repository
git clone <REPO_URL>
cd dbs-on-aws-webinar-demo

# 2. Authenticate with your Databricks workspace
databricks auth login --host <YOUR_WORKSPACE_URL>

# 3. Deploy the bundle
databricks bundle deploy

# 4. Run the full orchestration job (takes ~25 min)
databricks bundle run shopnow_orchestration
```

That single job will:
1. Generate synthetic e-commerce data (5K customers, 200 products, 50K orders, 200K clickstream events)
2. Run the Lakeflow pipeline to build Bronze, Silver, and Gold tables
3. Deploy an AI agent to a Model Serving endpoint
4. Refresh the AI/BI dashboard
5. Create a Lakebase instance and sync gold tables to PostgreSQL
6. Deploy the ShopNow Ops Hub web application

Once the job completes, open the **Apps** page in your workspace to find the ShopNow Ops Hub URL.

## Option B: Workspace-Only (No CLI Required)

If you prefer to work entirely within the Databricks workspace — no local CLI installation needed.

### 1. Clone this repository as a Git Folder

1. In your Databricks workspace, go to **Workspace** > **Users** > your username
2. Click **Create** > **Git Folder**
3. Enter the repository URL: `<REPO_URL>`
4. Click **Create Git Folder**

### 2. Run the workshop setup notebook

1. Open `src/_resources/workshop-setup`
2. Set the widgets at the top of the notebook:
   - **Catalog**: `main` (or your catalog)
   - **Schema**: `aws_webinar_demo` (use a unique name if sharing a workspace with others)
   - **Volume**: `raw_data`
3. Click **Run All** — this generates data, creates a SQL warehouse, starts the Lakeflow pipeline, deploys the dashboard, and creates the app shell (~10 min)

### 3. Walk through the notebooks in order

After the pipeline completes, follow the notebooks:

| # | Notebook | Action |
|---|---------|--------|
| 0 | `src/00-introduction` | Read — architecture overview |
| 1 | `src/01-pipeline/01-declarative-pipeline` | **Read only** — executed by the pipeline engine |
| 1 | `src/01-pipeline/02-pipeline-cdc` | **Read only** — executed by the pipeline engine |
| 2 | `src/02-governance/01-unity-catalog` | **Run interactively** (set widgets: catalog, schema) |
| 3 | `src/03-ai-agent/01-agent-creation` | **Run interactively** (~20 min for endpoint deploy) |
| 4 | `src/04-ai-bi/01-dashboard` | Read — open the dashboard in the UI instead |
| 4 | `src/04-ai-bi/02-genie-space` | Read — illustrative queries |
| 5 | `src/05-lakebase/01-reverse-etl-lakebase` | **Run interactively** |
| 6 | `src/_resources/restart-app` | **Run** after agent + Lakebase are ready |

> **Tip:** The pipeline notebooks (`01-pipeline/`) are executed by the Lakeflow engine, not run cell-by-cell. Open them to read the code, but do not try to run them interactively.

### 4. Clean up

Open and run `src/_resources/workshop-cleanup` to remove all created resources.

## Exploring the Notebooks

After deployment, open the notebooks in your workspace and walk through them in order:

```
src/
  00-introduction.sql          Start here — overview and architecture
  01-pipeline/
    01-declarative-pipeline    Lakeflow: Bronze -> Silver -> Gold
    02-pipeline-cdc            CDC processing for customer updates
  02-governance/
    01-unity-catalog           PII tags, masking, row filters, lineage
  03-ai-agent/
    01-agent-creation          Build, test, and deploy an AI agent
  04-ai-bi/
    01-dashboard               AI/BI dashboard queries
    02-genie-space             Natural language analytics with Genie
  05-lakebase/
    01-reverse-etl-lakebase    Sync gold KPIs to Lakebase (Postgres)
  06-app/
    app.py                     ShopNow Ops Hub web application
```

> **Tip:** The pipeline notebooks (`01-pipeline/`) are executed by the Lakeflow engine, not run cell-by-cell. Open them to read the code, but do not try to run them interactively.

## Clean Up

### If you used Option A (Asset Bundles)

```bash
# Delete the AI agent endpoint (not managed by the bundle)
databricks serving-endpoints delete shopnow-ops-agent

# Drop the Lakebase instance and synced tables (not managed by the bundle)
# Run in a Databricks notebook or via the Databases UI

# Destroy the bundle (removes pipeline, job, dashboard, warehouse, app, workspace files)
databricks bundle destroy --auto-approve

# Drop the schema
# In the SQL Editor: DROP SCHEMA IF EXISTS main.aws_webinar_demo_dev CASCADE
```

### If you used Option B (Workspace-Only)

Open and run `src/_resources/workshop-cleanup` in your workspace. It deletes all resources
(agent endpoint, Lakebase, app, dashboard, pipeline, warehouse, and the UC schema) in one go.

## Questions or Issues?

Open an issue in this repository or reach out during the workshop Q&A session.
