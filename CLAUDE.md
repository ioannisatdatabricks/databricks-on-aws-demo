# CLAUDE.md — Project Context for AI Assistants

## Project Overview

Declarative Automation Bundle for the **April 7, 2026 AWS Webinar** ("Data Intelligence with Databricks on AWS").

Demo story: **ShopNow** — a fictional e-commerce retailer. The bundle deploys a complete data + AI lifecycle: raw data ingestion → medallion pipeline → governance → AI agent → BI dashboards → reverse ETL to Lakebase → Databricks App. Style follows dbdemos conventions (numbered notebooks, SQL + Python).

The bundle is intended to be hosted in a **public GitHub repo** so workshop participants can clone and deploy it in their own workspaces.

## Repository Structure

```
databricks.yml                          Bundle root (targets: dev, prod)
resources/
  pipeline.yml                          Serverless Lakeflow pipeline
  job.yml                               Orchestration job (6 tasks)
  app.yml                               Databricks App (shopnow-ops-hub)
  shopnow_revenue_dashboard.yml         AI/BI Dashboard resource
  warehouse.yml                         SQL Warehouse resource
src/
  00-introduction.sql                   Entry point, architecture, glossary for newcomers
  01-pipeline/
    01-declarative-pipeline.sql         Lakeflow: Bronze → Silver → Gold (Autoloader, constraints)
    02-pipeline-cdc.sql                 CDC with APPLY CHANGES INTO for customers
  02-governance/
    01-unity-catalog.sql                PII tags, column masking, row-level security, lineage
  03-ai-agent/
    01-agent-creation.py                LangGraph agent, 4 UC Function tools, MLflow, Model Serving
  04-ai-bi/
    01-dashboard.sql                    AI/BI dashboard queries
    02-genie-space.sql                  Genie Space certified answers
    shopnow_revenue.lvdash.json         Dashboard definition
  05-lakebase/
    01-reverse-etl-lakebase.py          Lakebase Provisioned instance + synced tables
  06-app/
    app.py                              FastAPI + HTMX app (KPIs from Lakebase + agent chat)
    requirements.txt                    App dependencies
  _resources/
    00-setup.py                         Synthetic data generation
    restart-app.py                      App SP grants + deploy (UC + PG security label + app.yaml)
    workshop-setup.py                   Workspace-only setup (creates all resources via SDK)
    workshop-cleanup.py                 Workspace-only teardown (deletes all resources)
Instructor.md                           Talk track, timing plan, differentiators
README.md                              Participant-facing quick start
```

Folder prefix numbers match both the **presentation order** and the **job execution order**. Files within folders use local numbering (01, 02, ...).

## Key Configuration

| Setting | Value |
|---------|-------|
| Databricks profile | `psa-devday-ireland` |
| Catalog | `main` |
| Schema (dev) | `aws_webinar_demo_dev` |
| Schema (prod) | `aws_webinar_demo` |
| Volume | `raw_data` |
| Agent LLM | `databricks-claude-sonnet-4-6` (Foundation Model API) |
| Agent endpoint | `shopnow-ops-agent` |
| Lakebase instance | `shopnow-lakebase` (CU_1, Autoscaling — default) |
| App name | `shopnow-ops-hub` |

## Orchestration Job Task Graph

```
setup (data gen)
  └──► run_pipeline (Lakeflow: Bronze → Silver → Gold)
         ├──► deploy_agent          ─┐
         ├──► refresh_dashboard      │  (parallel, all depend on pipeline)
         └──► reverse_etl            ─┘
                   └──► start_app (depends on deploy_agent + reverse_etl)
```

- `setup` runs `_resources/00-setup.py` (generates 5K customers, 200 products, 50K orders, 200K clicks)
- `run_pipeline` triggers the Lakeflow pipeline resource
- `deploy_agent` runs `03-ai-agent/01-agent-creation.py` (env: `agent_env` with langchain/langgraph/mlflow)
- `reverse_etl` runs `05-lakebase/01-reverse-etl-lakebase.py` (env: `lakebase_env` with databricks-sdk/psycopg2)
- `refresh_dashboard` triggers the dashboard resource
- `start_app` runs `_resources/restart-app.py` (env: `lakebase_env`)

## Lakebase Provisioned — API & Patterns

The project uses **Lakebase Provisioned** (not Autoscaling). Key API differences:

| Concept | Provisioned API | Old Autoscaling API |
|---------|----------------|---------------------|
| SDK namespace | `w.database.*` | `w.postgres.*` |
| Instance creation | `w.database.create_database_instance(database_instance=DatabaseInstance(...))` — returns `Wait`, call `.result()` | `w.postgres.create_project(...)` |
| Instance state | `AVAILABLE` (not `RUNNING`) | `RUNNING` |
| Synced tables | `w.database.create_synced_database_table(SyncedDatabaseTable(...))` | UI only |
| Credential generation | `w.database.generate_database_credential(request_id=..., instance_names=[...])` | `w.postgres.generate_database_credential(endpoint=...)` |
| Error handling | `from databricks.sdk.errors import NotFound` — catch `NotFound` directly | String matching |
| Synced table states | `SYNCED_TABLE_ONLINE*` = success, `SYNCED_TABLE_OFFLINE_FAILED*` = failure | N/A |
| Required param | `logical_database_name="databricks_postgres"` for standard catalogs | Not needed |
| DAB resource | No native type yet — instance created in notebook | `postgres_projects` |

## App OAuth Authentication to Lakebase

The app uses **OAuth tokens** (not native PG passwords) to connect to Lakebase:

1. On startup, the app calls `w.database.generate_database_credential()` to get a token
2. A background thread refreshes the token every 50 minutes (tokens expire at 60 min)
3. The app connects to Postgres using `user=<sp_email>, password=<oauth_token>`

For this to work, the app's **service principal** needs:
- UC grants: `USE CATALOG`, `USE SCHEMA`, `SELECT` on the schema
- A PG role with `databricks_auth` security label:
  ```sql
  SECURITY LABEL FOR databricks_auth ON ROLE "<sp_client_id>"
    IS 'id=<sp_databricks_id>,type=SERVICE_PRINCIPAL'
  ```
- PG grants: `USAGE ON SCHEMA`, `SELECT ON ALL TABLES IN SCHEMA`

All of this is handled by `_resources/restart-app.py` (Step 2).

## Serverless Job Environments

Serverless jobs use the `environments` block with `environment_version: "4"` instead of `%pip install` in notebooks. Two environments are defined:

- `agent_env`: databricks-langchain, databricks-agents, mlflow, langchain, langgraph
- `lakebase_env`: databricks-sdk>=0.89.0, psycopg2-binary

## Dual-Mode Deployment

The repo supports two deployment modes:

### Mode A: DAB CLI (Instructor)
- `databricks bundle deploy` + `databricks bundle run shopnow_orchestration`
- Resources defined in `resources/*.yml`
- Orchestration job manages all task dependencies
- Package installs handled by serverless `environments` block in job.yml

### Mode B: Workspace-Only (Participants)
- Clone as Git Folder, run `src/_resources/workshop-setup.py`
- Resources created via Databricks SDK in the setup notebook
- Participants run notebooks interactively in presentation order
- Package installs handled by `%pip install` guards in each notebook (try/except — no-op in Mode A)

### Key differences

| Aspect | DAB Mode (A) | Workspace Mode (B) |
|--------|-------------|-------------------|
| Package installation | `environments` block in job.yml | `%pip install` guards in notebooks |
| Pipeline creation | `resources/pipeline.yml` | SDK `w.pipelines.create()` in workshop-setup |
| Warehouse creation | `resources/warehouse.yml` | SDK `w.warehouses.create()` in workshop-setup |
| Dashboard deployment | `resources/shopnow_revenue_dashboard.yml` | SDK `w.lakeview.create()` in workshop-setup |
| App creation | `resources/app.yml` | SDK `w.apps.create()` in workshop-setup |
| Resource naming | Fixed names (DAB-managed) | Schema-specific names for multi-participant isolation |
| Cleanup | `bundle destroy` + manual steps | `workshop-cleanup.py` notebook |
| APP_NAME | Hardcoded `shopnow-ops-hub` (via DAB) | Parameterized via `app_name` widget in restart-app.py |

## Common Operations

### Deploy and run
```bash
databricks bundle deploy --profile psa-devday-ireland
databricks bundle run shopnow_orchestration --profile psa-devday-ireland
```

### Full cleanup (Mode A — DAB CLI)

> **Mode B alternative:** If you deployed via `workshop-setup.py`, run `workshop-cleanup.py` instead — it handles everything in one notebook.

The bundle does **not** manage all resources it creates. Several are created at runtime by
notebooks and must be deleted manually **before** `bundle destroy` (which removes the SQL
warehouse needed for schema drops).

**Execution order matters** — follow the steps below in sequence:

```bash
# ── Step 1: Delete resources NOT managed by the bundle ────────────────────

# 1a. Agent serving endpoint (created by agents.deploy() in 03-ai-agent)
databricks serving-endpoints delete shopnow-ops-agent --profile psa-devday-ireland

# 1b. Lakebase synced tables + instance (created by 05-lakebase/01-reverse-etl-lakebase.py)
python3 -c "
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
w = WorkspaceClient(profile='psa-devday-ireland')
for t in ['gold_revenue_daily_synced', 'gold_top_products_synced', 'gold_customer_ltv_synced']:
    try: w.database.delete_synced_database_table(name=f'main.aws_webinar_demo_dev.{t}')
    except NotFound: pass
try: w.database.delete_database_instance(name='shopnow-lakebase')
except NotFound: pass
"

# ── Step 2: Drop the UC schema (BEFORE bundle destroy removes the warehouse) ─
# Use SQL Editor, MCP execute_sql, or any available warehouse:
#   DROP SCHEMA IF EXISTS main.aws_webinar_demo_dev CASCADE
# This deletes: all tables (bronze/silver/gold), UC functions, registered models,
# the UC volume (raw_data), and any DatabricksStore agent memory tables.

# ── Step 3: Destroy the bundle ────────────────────────────────────────────
databricks bundle destroy --auto-approve --profile psa-devday-ireland
# Deletes: App (shopnow-ops-hub), Dashboard, Job, Pipeline (+ STs/MVs),
#          SQL Warehouse, and all workspace files.
```

**What each step covers:**

| Resource | Created by | Managed by bundle? | Cleanup step |
|----------|-----------|-------------------|-------------|
| Serving endpoint (`shopnow-ops-agent`) | `agents.deploy()` in notebook | No | Step 1a |
| Lakebase synced tables (3x `*_synced`) | `w.database.create_synced_database_table()` in notebook | No | Step 1b |
| Lakebase instance (`shopnow-lakebase`) | `w.database.create_database_instance()` in notebook | No | Step 1b |
| UC schema + all contents (tables, functions, models, volume) | Notebooks + pipeline | No | Step 2 |
| Agent memory tables (in Lakebase `public` schema) | `DatabricksStore.setup()` in app | No (deleted with Lakebase instance) | Step 1b |
| App (`shopnow-ops-hub`) | `resources/app.yml` | Yes | Step 3 |
| Dashboard (`shopnow_revenue`) | `resources/shopnow_revenue_dashboard.yml` | Yes | Step 3 |
| Job (`shopnow_orchestration`) | `resources/job.yml` | Yes | Step 3 |
| Pipeline (`shopnow_pipeline`) + STs/MVs | `resources/pipeline.yml` | Yes | Step 3 |
| SQL Warehouse (`shopnow_warehouse`) | `resources/warehouse.yml` | Yes | Step 3 |
| Workspace files | Bundle sync | Yes | Step 3 |

### Agent endpoint takes ~20 min to deploy
The serving endpoint provisioning is slow. The Instructor.md timing plan accounts for this by covering Dashboard + Genie + Lakebase during the wait.

## Agent Memory (DatabricksStore backed by Lakebase)

The app uses `DatabricksStore` from `databricks-langchain[memory]` for persistent chat sessions:

- **Package**: `pip install 'databricks-langchain[memory]'` (installs `databricks-ai-bridge[memory]` + `langgraph`)
- **Store init**: `DatabricksStore(instance_name="shopnow-lakebase", workspace_client=w)`
- **Setup**: `store.setup()` creates tables in the Lakebase `public` schema (auto-managed by LangGraph's PostgresStore)
- **Namespace pattern**: `("shopnow", "sessions", session_id)` — hierarchical key-value store
- **CRUD**: `store.get(namespace, key)`, `store.put(namespace, key, value)`, `store.search(namespace, query)`
- **Token handling**: `LakebasePool` from `databricks-ai-bridge` handles OAuth token rotation internally
- **SP permissions**: The app SP needs `CREATE` + `ALL` on the `public` schema (granted in `restart-app.py`)

The store is used to persist conversation history per session so the agent receives full context
on follow-up questions (the Model Serving endpoint is stateless).

## Conventions for New Notebooks

### `%pip install` guard pattern (Python notebooks)

Any Python notebook that depends on packages from the job `environments` block must include a
try/except guard so it can run interactively (Mode B) while being a no-op in the DAB job (Mode A):

```python
# COMMAND ----------
try:
    import <key_package>
except ImportError:
    %pip install <packages> -q
    dbutils.library.restartPython()
```

### SQL widget declarations

Any SQL notebook that uses `${catalog}` or `${schema}` template variables must declare widgets
so it can run interactively:

```sql
-- COMMAND ----------
CREATE WIDGET TEXT catalog DEFAULT 'main';
CREATE WIDGET TEXT schema DEFAULT 'aws_webinar_demo';
```

When run via DABs, the job/pipeline passes parameters that override these defaults.

## Gotchas & Lessons Learned

- `create_database_instance` takes a `DatabaseInstance` object, not keyword args. Returns a `Wait` — must call `.result()`.
- `get_synced_database_table` raises `NotFound` from `databricks.sdk.errors` — do not string-match error messages.
- Synced tables require `logical_database_name="databricks_postgres"` for standard (non-system) catalogs.
- Failed synced table creation can leave orphan PG tables — drop them manually via psycopg2 if `AlreadyExists`.
- `agents.deploy()` is required (not just `serving_endpoints.create`) — it sets up UC Function permissions for the serving SP.
- Model signature must be `ChatCompletionResponse`-compatible for `agents.deploy()` to work.
- Endpoint state enums are objects — use `str(state).split(".")[-1]` for string comparison.
- App deletion can take up to 20 minutes. `bundle destroy` may fail with "state DELETING" — wait and retry.
- The `bundle destroy` command removes the SQL warehouse, so subsequent SQL operations need auto-warehouse selection or a different warehouse.
- Pipeline notebooks are executed by the Lakeflow engine, not cell-by-cell. Do not run them interactively.
- **Bundle deploy stale state**: If the workspace files directory is empty but `bundle deploy` says "Uploaded bundle files" instantly, the local deployment state (`.databricks/bundle/dev/deployment.json`) is stale. Delete it and `sync-snapshots/*.json` to force a full re-upload.
- **Run single job task**: Use `databricks bundle run <job_name> --only <task_key>` to run a single task from a multi-task job.
- **Agent endpoint is stateless**: The Model Serving endpoint does not maintain conversation history. The calling application must send the full message history with each request. Use `DatabricksStore` (Lakebase-backed) to persist sessions server-side.
- **UCFunctionToolkit rejects None for STRING params**: The toolkit validates types strictly. If the LLM passes `null` for a STRING parameter, it raises `ValueError`. Use `COMMENT 'Pass empty string for all'` (not "Pass NULL") so the LLM sends `""`. In the SQL, use `NULLIF(param, '')` to convert empty string to NULL for `COALESCE` logic.
- **`CREATE OR REPLACE FUNCTION` drops ACLs**: Recreating a UC function via SQL drops the EXECUTE grants that `agents.deploy()` set for the serving SP. Always re-run `agents.deploy()` (or the `deploy_agent` job task) after modifying UC functions that the agent uses.
- **LangGraph endpoint rejects multi-turn messages**: Agent endpoints deployed via `agents.deploy()` return `{}` for multi-turn message arrays. Send a single user message with conversation history packed into the prompt text instead.
- **Dashboard JSON table references**: The `shopnow_revenue.lvdash.json` uses unqualified table names (e.g., `FROM gold_revenue_daily`). The DAB injects `dataset_catalog`/`dataset_schema`. When deploying via SDK, `workshop-setup.py` does string replacement to fully qualify them.
- **Workspace path resolution**: In Git Folder notebooks, use `dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()` to derive the repo root for referencing other notebooks/files.
- **Synced table pipelines**: Lakebase auto-creates a Lakeflow pipeline per synced table (named `Synced table: <fqn> <hash>`). These are NOT deleted when the synced table is deleted — must be cleaned up separately via `w.pipelines.delete()`.
- **App deploy from Git Folders**: Git Folder paths are not valid for `w.apps.deploy()`. Copy app files to a regular workspace directory first (`/Workspace/Users/<email>/apps/<app_name>`).
- **Genie breaks on special chars in dashboard name**: Parentheses `()`, em dashes `—`, and likely other non-ASCII or special characters in the dashboard `display_name` cause Genie to report "incompatible SQL expressions" — the error is misleading since the actual dataset SQL is fine. Use only ASCII letters, numbers, spaces, and plain hyphens `-` in dashboard names that will have Genie enabled.
- **Dashboard dataset SQL Genie restrictions (undocumented)**: Beyond the documented incompatibilities (subqueries, `INTERVAL`, `identifier(:param)`, temp views, volume refs), `CASE WHEN`, `ROUND()`, `NULLIF()`, and `DATEADD()` in dataset queries may also trigger Genie incompatibility. Prefer simple aggregations (`SUM`, `AVG`, `COUNT`) and direct column references. Pre-compute complex metrics in gold tables instead.
