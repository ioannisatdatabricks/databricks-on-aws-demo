# Instructor Guide — Data Intelligence with Databricks on AWS

**Event:** April 7, 2026 | AWS Hands-On Virtual Workshop
**Duration:** ~60 minutes live demo (10:45 - 11:45 AM CEST)
**Speaker:** Instructor (you)
**Audience:** Data engineers, analysts, and architects — many new to Databricks

---

## Pre-Demo Preparation (Day Before)

### 1. Deploy and Run Everything Once

```bash
databricks bundle deploy
databricks bundle run shopnow_orchestration
```

Wait for all 6 tasks to complete successfully (~25-30 minutes). This ensures:
- All data is generated and pipeline tables exist
- The AI agent endpoint is deployed and READY
- The Lakebase instance and synced tables are online
- The ShopNow Ops Hub app is running

### 2. Verify Each Component

| Component | How to verify |
|-----------|--------------|
| Pipeline | Open the pipeline in the workspace. All tables should show as "Completed" |
| Gold tables | Run `SELECT COUNT(*) FROM workspace.aws_webinar_demo.gold_revenue_daily` in SQL Editor |
| Agent endpoint | Go to **Serving** > `shopnow-ops-agent` — should show "Ready" |
| Dashboard | Open **Dashboards** > "ShopNow Revenue Intelligence" — all charts should render |
| Lakebase | Go to **Databases** > `shopnow-lakebase` — state should be "Available" |
| App | Open the app URL — KPI cards should load, agent chat should respond |

### 3. Reset for Live Demo

Before going live, destroy and redeploy so participants see a fresh run:

```bash
# Clean up
databricks serving-endpoints delete shopnow-ops-agent
databricks bundle destroy --auto-approve
# In SQL Editor:
# DROP SCHEMA IF EXISTS workspace.aws_webinar_demo CASCADE

# Redeploy
databricks bundle deploy
```

> **Do NOT run the job yet** — you will trigger it live during the demo.

### Alternative: Participant-Led Workshop Mode

If participants will deploy the demo themselves (no pre-deployment by instructor):

1. Share the GitHub repo URL with participants
2. Each participant clones it as a **Git Folder** in their workspace
3. Each participant opens `src/_resources/config` and sets:
   - **CATALOG** to their workspace catalog (default: `workspace`)
   - **SCHEMA** to a **unique name** (e.g., `aws_webinar_demo_<first_name>`) to avoid resource conflicts
4. Each participant runs `src/_resources/workshop-setup` — it picks up the config automatically
5. Walk through notebooks in presentation order — participants follow along in their own workspace

**Timing impact:** The workshop-setup notebook takes ~10 minutes (data generation + pipeline run).
Add 10 minutes to the overall timing or have participants start setup during introductions.

**Cleanup:** Each participant runs `src/_resources/workshop-cleanup` when done.

### 4. Prepare Your Browser

Open these tabs in advance (you will switch between them during the demo):
1. **Databricks workspace** (home page)
2. **This repo** on GitHub (to show the bundle structure)
3. **The webinar landing page** (for reference)

---

## Timing Plan (60 minutes)

| Time | Section | Duration | Notes |
|------|---------|----------|-------|
| 0:00 | Introduction + Kick off Job | 5 min | Trigger the orchestration job live |
| 0:05 | Pipeline (while job runs) | 10 min | Walk through the pipeline notebooks |
| 0:15 | Governance | 7 min | Show UC features (job finishes pipeline ~0:12) |
| 0:22 | AI Agent (trigger deploy) | 8 min | Walk through code, kick off endpoint deploy |
| 0:30 | AI/BI Dashboard + Genie | 10 min | **Fill the agent deploy wait** |
| 0:40 | Lakebase Reverse ETL | 7 min | Show synced tables, Postgres connectivity |
| 0:47 | Databricks App | 8 min | Live demo of the ShopNow Ops Hub |
| 0:52 | Databricks One Chat | 5 min | Cross-asset questions that Genie/Agent can't answer |
| 0:57 | Wrap-up + Q&A | 3 min | Recap differentiators |

> **Key timing trick:** The agent serving endpoint takes ~20 minutes to deploy. You kick it off at ~0:22, then cover Dashboard + Genie + Lakebase (~17 min). By the time you demo the App at ~0:47, the agent endpoint is ready and the app can chat with it live.

---

## Section-by-Section Talk Track

---

### 0:00 — Introduction + Kick Off the Job (5 min)

**What to show:** The `00-introduction.sql` notebook in the workspace, then the Jobs page.

**What to say:**

> "Welcome everyone. Today we are going to build an end-to-end data and AI platform for a fictional e-commerce retailer called ShopNow — entirely on Databricks running on AWS.
>
> We will go from raw data files sitting in S3 all the way to a production AI-powered application, passing through data ingestion, governance, BI, and AI along the way.
>
> Everything you see today is packaged as a Declarative Automation Bundle — infrastructure as code that you can deploy in your own workspace with a single command.
>
> Let me kick off the orchestration job right now so the data starts flowing while we walk through the code."

**What to do:**
1. Open the `00-introduction.sql` notebook — briefly show the architecture diagram
2. Navigate to **Workflows > Jobs** > "ShopNow — Full Demo Orchestration"
3. Click **Run Now**
4. Show the job DAG: "You can see the dependency graph — setup generates data, the pipeline transforms it, then the agent, dashboard, and reverse ETL run in parallel, and finally the app deploys."

**Differentiator to highlight:**

> "Notice that this entire workflow — data generation, pipeline, AI model deployment, database sync, app deployment — is defined in a single job. No external orchestrator needed. Databricks is the unified platform."

---

### 0:05 — Data Pipeline: Lakeflow (10 min)

**What to show:** The pipeline UI and `01-pipeline/01-declarative-pipeline.sql` notebook.

**What to say:**

> "Let's look at how ShopNow's data engineer, Alex, builds the data pipeline.
>
> Databricks uses what we call Spark Declarative Pipelines — you define WHAT you want (the tables and their transformations), and the platform handles the HOW (orchestration, retries, scaling, monitoring).
>
> Look at how simple this is — a Bronze table is just a `CREATE STREAMING TABLE ... FROM cloud_files(...)`. That `cloud_files` call is **Autoloader** — it automatically detects new files as they land in S3. No manual file listing, no glob patterns, no scheduling file-by-file. It handles schema evolution too."

**Walk through the code:**
1. **Bronze layer** — Point out Autoloader (`cloud_files`), metadata tracking (`_metadata.file_path`), and the different source formats (CSV, JSON, Parquet)
2. **Silver layer** — Highlight data quality constraints: `EXPECT (total_amount > 0) ON VIOLATION DROP ROW`. "Bad records are automatically quarantined — you get a data quality dashboard for free."
3. **Gold layer** — Show the materialized views. "These are automatically kept fresh by the pipeline engine. No manual refresh, no scheduling."

**Switch to the Pipeline UI:**
1. Click into the running pipeline (should be processing by now)
2. Show the lineage graph: "Every table, its dependencies, data quality metrics, and processing status — all visible in one place."
3. Point out the data quality tab: "You can see exactly how many records passed or failed each constraint."

**Differentiators to highlight:**

> "Three key differentiators here:
> 1. **Declarative** — you write SQL, not orchestration code. The engine figures out the execution order.
> 2. **Serverless** — no clusters to configure, no autoscaling to tune. It just works.
> 3. **Built-in data quality** — constraints are first-class citizens, not afterthoughts."

**Then briefly show `02-pipeline-cdc.sql`:**

> "For customer profiles, we get a CDC feed — inserts, updates, and deletes. Instead of writing complex merge logic, Alex uses `APPLY CHANGES INTO`. One statement handles everything: ordering events, applying upserts, processing deletes. The silver customers table is always current."

---

### 0:15 — Governance with Unity Catalog (7 min)

**What to show:** The `02-governance/01-unity-catalog.sql` notebook, then Catalog Explorer in the UI.

**What to say:**

> "Now let's switch to Sam, the Data Steward. Sam needs to ensure that PII is protected, access is controlled, and the data team can trace where every number comes from.
>
> In many organisations, governance is a separate tool, a separate team, a separate workflow. On Databricks, governance is built into the platform from day one through Unity Catalog."

**Walk through:**
1. **Column tags:** "Sam tags columns like `email` and `first_name` with `pii=true`. These tags are visible in the Catalog Explorer and can drive automated policies."
   - Switch to **Catalog Explorer** > navigate to `silver_customers` > show the tags on columns
2. **Dynamic data masking:** "This masking function checks group membership at query time. If you are not in the `pii_access` group, you see `***@***.***` instead of the real email. No data duplication, no separate views — it is the same table."
3. **Row-level security:** "Regional analysts only see orders from their country. Again, one function applied to the table — transparent to every query tool."
4. **Lineage:** Switch to Catalog Explorer > click on `gold_revenue_daily` > show the Lineage tab. "Unity Catalog automatically tracks column-level lineage. You can trace every gold metric back to its raw source files. No manual documentation, no lineage tools to configure."

**Differentiators to highlight:**

> "Unity Catalog gives you a single place for:
> - **Access control** (who can see what)
> - **Data protection** (masking, row filters)
> - **Discovery** (tags, comments, search)
> - **Lineage** (column-level, automatic)
>
> All without any additional tools or infrastructure. It works across SQL, Python, Spark, ML — everything."

---

### 0:22 — AI Agent (8 min)

**What to show:** `03-ai-agent/01-agent-creation.py` notebook.

> "Now we hand off to Leo, the AI/ML engineer. Leo needs to build an AI assistant that the operations team can talk to and get answers backed by real data.
>
> This is not a chatbot that hallucinates — it is an agent that calls real SQL functions against the gold tables we just built."

**Walk through:**
1. **UC Functions as tools (Steps 1):** "These are regular SQL functions registered in Unity Catalog. The agent can call them like any developer would call an API. The function `get_revenue_summary` takes a date range and queries the gold table. Because it is a UC Function, it is governed, discoverable, and reusable."
2. **LangGraph agent (Step 2):** "We use the Foundation Model API — Llama 4 Maverick hosted on Databricks. No API keys to manage, no external model provider, no data leaving your environment. The agent framework is LangGraph, and MLflow traces every call automatically."
3. **Test the agent (Step 3):** Show the 4 test questions — "Look at how the agent chooses the right tool for each question, calls the UC Function, and formats the answer with business insight."
4. **Deploy (Steps 4-5):** "The agent is logged to MLflow, registered in Unity Catalog, and deployed to a Model Serving endpoint. This endpoint is serverless, auto-scales, and can scale to zero when idle."

**What to do now (critical timing):**

> "The endpoint deployment takes about 20 minutes. Let me kick it off now, and we will come back to see it working live in the app. In the meantime, let me show you the BI capabilities."

If running this live (not from pre-deployed state), trigger the deploy_agent task or run the endpoint deploy cell. If everything is pre-deployed, you can show the serving endpoint page and say:

> "Here you can see the endpoint is ready. In a live deployment, this takes about 20 minutes to provision. During a real workshop you would kick this off and come back to it."

**Differentiators to highlight:**

> "Key differentiators:
> - **UC Functions as tools** — the agent is grounded in governed, production data
> - **Foundation Model API** — no external API keys, data stays in your environment
> - **MLflow tracing** — every agent step is automatically traced for debugging and evaluation
> - **One-click deploy** — `agents.deploy()` handles everything: packaging, serving, scaling"

---

### 0:30 — AI/BI Dashboard + Genie Space (10 min)

**What to show:** The live dashboard in the workspace, then the Genie space.

> "While our agent endpoint deploys, let's look at how Maya, the BI Analyst, creates dashboards and self-service analytics."

**Dashboard (5 min):**
1. Navigate to **Dashboards** > "ShopNow Revenue Intelligence"
2. Walk through each visualization:
   - Revenue trend: "This queries the gold_revenue_daily table directly — no data extraction, no separate BI warehouse."
   - Revenue by country: "A geographic view of our top markets."
   - Top products: "Revenue, margin, and return rate in one view."
   - Cart abandonment: "Using the clickstream gold table to identify drop-off trends."
   - Customer LTV by segment: "Which segments drive the most value?"
3. Show the **AI assistant** in the dashboard editor: "Maya can type 'add a 7-day moving average' and the dashboard generates the visualization. This is AI-assisted authoring — it understands the data because it reads the Unity Catalog metadata."

**Differentiator:**

> "The AI/BI dashboard is not just a visualization tool. It understands your data model, suggests charts, writes SQL, and even lets business users ask follow-up questions. There is no ETL between the gold tables and the dashboard — it queries the Lakehouse directly."

**Genie Space (5 min):**
1. Navigate to **Genie** (or show the notebook `04-ai-bi/02-genie-space.sql`)
2. If you have a Genie Space set up, type: "What was our revenue last week?"
   - Show how Genie writes SQL, executes it, and explains the result
3. Type a follow-up: "Which country performed best?"
   - "Genie maintains context — it knows 'last week' from the previous question."
4. Ask something more complex: "Show me Electronics products with a return rate above 15%"

**Differentiator:**

> "Genie is self-service analytics without the 'self-service' learning curve. Business users do not need to know SQL, table names, or join paths. They just ask questions in plain English, and Genie uses the gold tables and certified answers we configured to give accurate responses."

---

### 0:40 — Lakebase: Reverse ETL (7 min)

**What to show:** `05-lakebase/01-reverse-etl-lakebase.py` notebook, then the Databases page in the workspace.

> "Now let's talk about one of the newest capabilities on Databricks: Lakebase.
>
> ShopNow's operations team uses an internal portal that reads from a Postgres database. They need the gold-layer KPIs to be available in Postgres — not just in the Lakehouse.
>
> Traditionally, this requires a separate Postgres instance on RDS, a reverse ETL tool like Fivetran or Airbyte, and a lot of plumbing. With Lakebase, it is all built into the platform."

**Walk through:**
1. **Instance creation:** "One API call to create a fully managed PostgreSQL instance. No RDS, no VPC peering, no security group configuration."
2. **Synced tables:** "We create synced tables that automatically mirror the gold tables from the Lakehouse into Postgres. Any time the gold tables update, the synced tables follow."
3. Show the **Databases page** in the workspace: "Here is our Lakebase instance — `shopnow-lakebase`. You can see the synced tables, their sync status, and the connection details."
4. **OAuth authentication:** "The app authenticates to Lakebase using OAuth tokens — the same identity system as the rest of Databricks. No separate database passwords to manage."

**Differentiator:**

> "Lakebase eliminates the need for a separate operational database and a reverse ETL tool. Your gold tables in the Lakehouse are automatically synced to a managed Postgres instance that any application can query. Same governance, same identity, same platform."

---

### 0:47 — The Databricks App: ShopNow Ops Hub (5 min)

**What to show:** The live app in a browser, then the app code briefly.

> "Finally, let's see everything come together in a production application.
>
> Priya, the app developer, has built the ShopNow Ops Hub — a single-page web app that combines:
> - Live KPI cards reading from Lakebase
> - The AI Agent chat interface calling the Model Serving endpoint
> - All deployed as a Databricks App — no external hosting, no Kubernetes, no infrastructure."

**Live demo:**
1. Open the app URL
2. **KPI cards:** "These numbers come from Lakebase Postgres — the synced gold tables. Total revenue, order count, average order value, customer LTV."
3. **Agent chat:** Type: "What was our revenue last week and which country performed best?"
   - Show the response: "The agent calls the UC Function, queries the gold table, and formats the answer. This is the same agent we deployed earlier."
4. Type a follow-up: "Show me our top 5 Electronics products"
5. Type: "Do we have Premium customers at risk of churning?"

**Show the code briefly:**
1. Open `06-app/app.py`: "It is a standard FastAPI application. The KPIs come from a Postgres query to Lakebase. The agent chat is an HTTP call to the Model Serving endpoint. About 300 lines of code total."
2. Show `resources/app.yml`: "The app is deployed as a Databricks App resource in the bundle. Three environment variables is all it needs."

**Differentiator:**

> "Databricks Apps let you deploy full-stack web applications directly on the platform. They get:
> - **Managed compute** — no servers to provision
> - **Built-in authentication** — SSO with your workspace identity
> - **Native access to platform services** — Serving endpoints, Lakebase, Unity Catalog
>
> The entire application — from raw S3 files to this web UI — runs on a single platform. No data movement, no integration glue, no infrastructure management."

---

### 0:52 — Databricks One Chat: The Unified AI Interface (5 min)

**What to show:** The Databricks One Chat panel (accessible from any workspace page via the chat icon).

> **Prerequisite:** One Chat must be enabled in workspace settings (Admin Console > Previews > Databricks Unified Chat). Verify this before the demo.

**What to say:**

> "We have seen how different personas interact with data through purpose-built tools: Maya uses dashboards and Genie for analytics, Leo's agent answers operational questions through UC Functions, and the app combines both for the ops team.
>
> But what if someone asks a question that spans across multiple data assets — something that neither the Genie Space nor the agent was specifically designed to handle?
>
> For example, our agent's `get_top_products` function returns an all-time ranking. It cannot filter by a specific date range. So if I ask 'which was the most selling product last week?', the agent cannot answer that.
>
> This is where **Databricks One Chat** comes in. Let me show you."

**Live demo:**

1. Open the One Chat panel (chat icon in the top bar or sidebar)
2. Type: **"Which was the most selling product last week?"**
   - Show how One Chat understands the question, discovers the relevant tables in Unity Catalog (`silver_orders`, `silver_products`, or the gold tables), and generates the correct SQL with date filtering
   - "Notice what just happened: One Chat didn't rely on a pre-built function or a pre-configured Genie Space. It looked at all the data assets available to me in Unity Catalog and figured out how to answer the question on its own."
3. Type a follow-up: **"Break it down by country"**
   - "It maintains context, just like a conversation — and it can join across tables to add the country dimension."
4. Optionally, ask something that combines multiple domains: **"Compare last week's top product revenue to the cart abandonment rate for that product's category"**
   - "This is a question that requires joining orders, products, and clickstream data. No single dashboard, Genie Space, or agent tool covers this. One Chat traverses Unity Catalog to find the right tables and build the query."

**What to say (wrap the demo):**

> "The key insight here is the **layered approach to AI-powered analytics** on Databricks:
>
> 1. **Dashboards** give you curated, pre-built visualizations for known KPIs
> 2. **Genie Spaces** let business users ask natural language questions within a scoped set of tables
> 3. **AI Agents** answer operational questions through governed, purpose-built tools
> 4. **One Chat** is the safety net — it can answer anything by leveraging the full breadth of Unity Catalog
>
> Each layer is appropriate for a different use case, and they all share the same governed data, the same identity, and the same platform."

**Differentiator:**

> "One Chat is unique because it has access to the **entire Unity Catalog**. It understands table schemas, column descriptions, tags, and relationships — all the metadata you invest in governance. That metadata is not just for compliance — it makes AI smarter. The more you govern your data, the better One Chat becomes at answering questions."

---

### 0:57 — Wrap-Up + Q&A (3 min)

**What to say:**

> "Let me recap what we built today in under an hour:
>
> 1. **Ingested** raw data from S3 into the Lakehouse using Autoloader and Lakeflow Pipelines — fully serverless, with built-in data quality
> 2. **Governed** that data with Unity Catalog — PII masking, row-level security, automatic lineage, all without extra tools
> 3. **Built an AI Agent** that answers business questions using live data from governed UC Functions — deployed as a REST endpoint with one command
> 4. **Created dashboards** and a **Genie Space** for self-service analytics — SQL and natural language, powered by the same gold tables
> 5. **Synced KPIs to Lakebase** — managed Postgres built into the platform, no external database needed
> 6. **Deployed a web application** — combining everything into a single operational tool
> 7. Used **One Chat** to answer ad-hoc questions that span across all data assets — the unified AI interface for the entire Lakehouse
>
> All of this is defined in code as a Declarative Automation Bundle. You can deploy it in your own workspace in minutes.
>
> The key takeaway: **Databricks is the unified platform for data, analytics, and AI.** You don't need separate tools for ingestion, governance, BI, ML, operational databases, or application hosting. It all works together, on AWS, from a single control plane."

Open the floor for questions.

---

## Key Differentiators Summary

Use these throughout the demo. Each one addresses a common pain point:

| Pain Point | Databricks Differentiator | Where You Show It |
|-----------|---------------------------|-------------------|
| "We need separate tools for ingestion, transformation, and orchestration" | **Lakeflow Pipelines** — declarative SQL, serverless, built-in orchestration | Pipeline section |
| "Data quality is an afterthought" | **Expectations (constraints)** — first-class data quality in the pipeline definition | Pipeline section |
| "Governance is a separate tool and workflow" | **Unity Catalog** — tags, masking, RLS, lineage, all built in | Governance section |
| "Our AI models hallucinate because they lack real data" | **UC Functions as agent tools** — agent is grounded in governed, live data | Agent section |
| "We need external model providers and API keys" | **Foundation Model API** — hosted LLMs (Llama 4 Maverick), no data leaves the environment | Agent section |
| "Business users can not self-serve analytics" | **Genie Spaces** — natural language over governed data | AI/BI section |
| "Reverse ETL to our operational DB is complex" | **Lakebase** — managed Postgres with automatic table sync | Lakebase section |
| "Deploying apps requires separate infrastructure" | **Databricks Apps** — managed hosting with native platform access | App section |
| "Ad-hoc questions fall through the cracks" | **One Chat** — unified AI interface over all Unity Catalog assets | One Chat section |
| "Our deployments are manual and error-prone" | **Declarative Automation Bundles** — infrastructure as code, one command to deploy | Throughout |

---

## Troubleshooting During the Demo

| Issue | Quick Fix |
|-------|-----------|
| Pipeline stuck or slow | It runs on serverless — usually completes in 5-8 min. If stuck, show the pre-deployed tables |
| Agent endpoint not ready | Expected. Explain it takes 20 min and show the serving page. Use pre-deployed endpoint if available |
| App KPIs show "Database unavailable" | Lakebase instance may be stopped. Check **Databases** page and start it |
| Dashboard shows no data | Pipeline may not have completed. Wait or use the pre-deployed target |
| Genie Space not available | Show the notebook queries instead. Explain Genie requires workspace setup |
| One Chat not available | Enable in Admin Console > Previews > Databricks Unified Chat. Needs workspace admin access |
| One Chat gives wrong answer | Rephrase with explicit table hints: "using the silver_orders and silver_products tables, which product had the most revenue last week?" |
