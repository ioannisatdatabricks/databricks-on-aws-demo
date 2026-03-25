# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/db-icon.png" style="float:left; margin-right:20px" width="60px"/>
# MAGIC
# MAGIC # 3 — AI Agent: ShopNow Operations Assistant
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/mosaic-ai/databricks-model-serving-mosaic-ai-agent.png" style="float:right; margin-left:20px" width="450px"/>
# MAGIC
# MAGIC **Leo (AI/ML Engineer)** builds an agent that the ops team can talk to in plain English
# MAGIC to get answers backed by the live gold tables.
# MAGIC
# MAGIC The agent uses:
# MAGIC - **Databricks Foundation Model API** (Claude/Llama hosted on Databricks) as the LLM
# MAGIC - **Unity Catalog Functions** as tools — the agent can call SQL functions to query gold tables
# MAGIC - **MLflow** to trace, evaluate, and log the agent
# MAGIC - **Model Serving** to deploy it as a REST endpoint
# MAGIC
# MAGIC <br style="clear:both"/>

# COMMAND ----------

# Install dependencies (needed when running outside the DAB job environment)
try:
    import databricks_langchain
except ImportError:
    %pip install databricks-langchain databricks-agents mlflow langchain langgraph -q
    dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "main",             "Catalog")
dbutils.widgets.text("schema",  "aws_webinar_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"Using {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Register UC Functions as Agent Tools
# MAGIC
# MAGIC We expose the gold tables as SQL functions in Unity Catalog.
# MAGIC The agent can call these tools to answer user questions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tool 1: Get revenue summary for a given date range
# MAGIC CREATE OR REPLACE FUNCTION ${catalog}.${schema}.get_revenue_summary(
# MAGIC   start_date DATE COMMENT 'Start date (inclusive), e.g. 2024-01-01',
# MAGIC   end_date   DATE COMMENT 'End date (inclusive), e.g. 2024-01-31'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   order_day       DATE,
# MAGIC   ship_country    STRING,
# MAGIC   order_count     BIGINT,
# MAGIC   total_revenue   DOUBLE,
# MAGIC   avg_order_value DOUBLE
# MAGIC )
# MAGIC COMMENT 'Returns daily revenue broken down by country for the given date range'
# MAGIC RETURN
# MAGIC   SELECT order_day, ship_country, order_count, total_revenue, avg_order_value
# MAGIC   FROM   ${catalog}.${schema}.gold_revenue_daily
# MAGIC   WHERE  order_day BETWEEN start_date AND end_date
# MAGIC   ORDER  BY order_day, total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tool 2: Get top N products by revenue
# MAGIC CREATE OR REPLACE FUNCTION ${catalog}.${schema}.get_top_products(
# MAGIC   n        INT     COMMENT 'Number of top products to return (e.g. 10)',
# MAGIC   cat_filter STRING  COMMENT 'Category filter, e.g. Electronics. Pass empty string for all categories.'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   product_name    STRING,
# MAGIC   category        STRING,
# MAGIC   brand           STRING,
# MAGIC   units_sold      BIGINT,
# MAGIC   total_revenue   DOUBLE,
# MAGIC   margin_pct      DOUBLE,
# MAGIC   return_rate_pct DOUBLE
# MAGIC )
# MAGIC COMMENT 'Returns top N products ranked by total revenue, optionally filtered by category'
# MAGIC RETURN
# MAGIC   SELECT product_name, category, brand, units_sold, total_revenue, margin_pct, return_rate_pct
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS rn
# MAGIC     FROM   ${catalog}.${schema}.gold_top_products
# MAGIC     WHERE  category = COALESCE(NULLIF(cat_filter, ''), category)
# MAGIC   )
# MAGIC   WHERE rn <= n

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tool 3: Get cart abandonment rate
# MAGIC CREATE OR REPLACE FUNCTION ${catalog}.${schema}.get_abandonment_rate(
# MAGIC   days_back INT COMMENT 'How many days back to look, e.g. 7 for last week'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   day                  DATE,
# MAGIC   total_sessions       BIGINT,
# MAGIC   abandoned_sessions   BIGINT,
# MAGIC   abandonment_rate_pct DOUBLE
# MAGIC )
# MAGIC COMMENT 'Returns daily cart abandonment rate for the last N days'
# MAGIC RETURN
# MAGIC   SELECT
# MAGIC     DATE(session_start)                                                AS day,
# MAGIC     COUNT(*)                                                           AS total_sessions,
# MAGIC     SUM(CASE WHEN abandoned THEN 1 ELSE 0 END)                        AS abandoned_sessions,
# MAGIC     ROUND(SUM(CASE WHEN abandoned THEN 1 ELSE 0 END) /
# MAGIC           NULLIF(COUNT(*), 0) * 100, 1)                               AS abandonment_rate_pct
# MAGIC   FROM ${catalog}.${schema}.gold_cart_abandonment
# MAGIC   WHERE session_start >= DATEADD(day, -days_back, current_date())
# MAGIC   GROUP BY DATE(session_start)
# MAGIC   ORDER BY day

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tool 4: Get high-value customers at risk (no order in N days)
# MAGIC CREATE OR REPLACE FUNCTION ${catalog}.${schema}.get_at_risk_customers(
# MAGIC   min_ltv       DOUBLE COMMENT 'Minimum lifetime value threshold, e.g. 500.0',
# MAGIC   inactive_days INT    COMMENT 'Inactive for at least this many days, e.g. 60'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   customer_id          STRING,
# MAGIC   segment              STRING,
# MAGIC   country              STRING,
# MAGIC   lifetime_value       DOUBLE,
# MAGIC   days_since_last_order INT
# MAGIC )
# MAGIC COMMENT 'Returns high-value customers who have not ordered recently — at-risk cohort for retention'
# MAGIC RETURN
# MAGIC   SELECT customer_id, segment, country, lifetime_value, days_since_last_order
# MAGIC   FROM   ${catalog}.${schema}.gold_customer_ltv
# MAGIC   WHERE  lifetime_value >= min_ltv
# MAGIC     AND  days_since_last_order >= inactive_days
# MAGIC     AND  is_active = TRUE
# MAGIC   ORDER  BY lifetime_value DESC
# MAGIC   LIMIT  100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Build the Agent with LangGraph + UC Tool Calling

# COMMAND ----------

import mlflow
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langgraph.prebuilt import create_react_agent

mlflow.langchain.autolog()

# LLM: use Databricks-hosted Meta Llama (Foundation Model API)
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    temperature=0.1,
    max_tokens=2048,
)

# Tools: expose the 4 UC functions we just created
toolkit = UCFunctionToolkit(
    function_names=[
        f"{catalog}.{schema}.get_revenue_summary",
        f"{catalog}.{schema}.get_top_products",
        f"{catalog}.{schema}.get_abandonment_rate",
        f"{catalog}.{schema}.get_at_risk_customers",
    ]
)

tools = toolkit.tools

from datetime import date as _date
_today = _date.today().isoformat()

system_prompt = f"""You are ShopNow's Operations Assistant — a helpful, data-driven AI
built on Databricks. You have access to live analytics tools backed by Unity Catalog gold tables.

IMPORTANT: Today's date is {_today}. Use this to calculate correct date ranges for queries
like "last week", "last month", etc.

When answering questions:
1. Use the appropriate tool to fetch fresh data.
2. Present numbers clearly (format large numbers with commas, percentages with 1 decimal).
3. Add a short business insight after the data — what does the number mean?
4. If a question cannot be answered with the available tools, say so clearly.

Available tools:
- get_revenue_summary: Revenue by day and country for a date range
- get_top_products: Top N products by revenue, optionally filtered by category
- get_abandonment_rate: Cart abandonment rate over the last N days
- get_at_risk_customers: High-value customers at risk of churning

Data is from catalog: {catalog}, schema: {schema}
"""

agent = create_react_agent(
    model=llm,
    tools=tools,
    prompt=system_prompt,
)

print("Agent created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Test the Agent

# COMMAND ----------

from datetime import date

def ask(question: str):
    """Helper to run a question through the agent and print the response."""
    print(f"\n{'='*60}")
    print(f"Q: {question}")
    print('='*60)
    response = agent.invoke({
        "messages": [{"role": "user", "content": question}],
    })
    answer = response["messages"][-1].content
    print(f"A: {answer}")
    return answer

# COMMAND ----------

# Test 1: Revenue question
ask("What was our total revenue last week, and which country performed best?")

# COMMAND ----------

# Test 2: Product question
ask("Show me the top 5 Electronics products by revenue. Do any have a high return rate?")

# COMMAND ----------

# Test 3: Cart abandonment
ask("What is our cart abandonment rate over the last 30 days? Is it trending up or down?")

# COMMAND ----------

# Test 4: Customer retention
ask("Find our Premium customers with over $1,000 in lifetime value who haven't ordered in 90 days.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Log & Deploy the Agent with MLflow (skip if model already exists)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
model_fqn = f"{catalog}.{schema}.shopnow_ops_agent"

# Check if the registered model exists by listing models in the schema (avoids calling get/list_versions on a non-existent model)
models_in_schema = list(w.registered_models.list(catalog_name=catalog, schema_name=schema))
model_exists = any((m.full_name or "") == model_fqn for m in models_in_schema)

if False:  # Always re-register to pick up system prompt changes
    pass
else:
    # No existing version — log and register the agent
    import mlflow
    import os, tempfile

    mlflow.set_registry_uri("databricks-uc")

    agent_code = f'''
import mlflow
from datetime import date
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langgraph.prebuilt import create_react_agent

mlflow.langchain.autolog()

CATALOG = "{catalog}"
SCHEMA  = "{schema}"

llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    temperature=0.1,
    max_tokens=2048,
)

toolkit = UCFunctionToolkit(
    function_names=[
        f"{{CATALOG}}.{{SCHEMA}}.get_revenue_summary",
        f"{{CATALOG}}.{{SCHEMA}}.get_top_products",
        f"{{CATALOG}}.{{SCHEMA}}.get_abandonment_rate",
        f"{{CATALOG}}.{{SCHEMA}}.get_at_risk_customers",
    ]
)

today = date.today().isoformat()

system_prompt = f"""You are ShopNow\\'s Operations Assistant — a helpful, data-driven AI
built on Databricks. You have access to live analytics tools backed by Unity Catalog gold tables.

IMPORTANT: Today\\'s date is {{today}}. Use this to calculate correct date ranges for queries
like "last week", "last month", etc.

When answering questions:
1. Use the appropriate tool to fetch fresh data.
2. Present numbers clearly (format large numbers with commas, percentages with 1 decimal).
3. Add a short business insight after the data — what does the number mean?
4. If a question cannot be answered with the available tools, say so clearly.

Available tools:
- get_revenue_summary: Revenue by day and country for a date range
- get_top_products: Top N products by revenue, optionally filtered by category
- get_abandonment_rate: Cart abandonment rate over the last N days
- get_at_risk_customers: High-value customers at risk of churning

Data is from catalog: {{CATALOG}}, schema: {{SCHEMA}}
"""

agent = create_react_agent(
    model=llm,
    tools=toolkit.tools,
    prompt=system_prompt,
)

mlflow.models.set_model(agent)
'''

    agent_code_path = os.path.join(tempfile.mkdtemp(), "agent.py")
    with open(agent_code_path, "w") as f:
        f.write(agent_code)

    input_example = {"messages": [{"role": "user", "content": "What was revenue last week?"}]}
    output_example = {"id": "1", "object": "chat.completion", "created": 0, "model": "agent",
                      "choices": [{"index": 0, "message": {"role": "assistant", "content": "test"}, "finish_reason": "stop"}],
                      "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}}
    signature = mlflow.models.infer_signature(input_example, output_example)

    with mlflow.start_run(run_name="shopnow-ops-agent"):
        model_info = mlflow.langchain.log_model(
            lc_model=agent_code_path,
            artifact_path="agent",
            pip_requirements=["databricks-langchain", "langchain", "langgraph", "mlflow"],
            input_example=input_example,
            signature=signature,
            registered_model_name=model_fqn,
        )
        print(f"Model logged: {model_info.model_uri}")

    # Refresh latest version after logging
    model_versions = list(w.model_versions.list(full_name=model_fqn))
    latest_version = max(int(mv.version) for mv in model_versions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Deploy to Model Serving (skip if endpoint already running same model)

# COMMAND ----------

import databricks.agents as agents  # type: ignore[import-untyped]

endpoint_name = "shopnow-ops-agent"

def _endpoint_serves_model(ep, model_fqn: str, version: int) -> bool:
    """Return True if endpoint is READY and already serves the given model version."""
    if not ep.state or str(ep.state.ready).split(".")[-1] != "READY":
        return False
    config = getattr(ep, "config", None)
    if not config:
        return False
    # Served entities (real-time) or served_models (foundation) — check both
    entities = getattr(config, "served_entities", None) or getattr(config, "served_models", None) or []
    for ent in entities:
        name = getattr(ent, "entity_name", None) or getattr(ent, "name", None) or ""
        # Entity name often is model name or contains it; version may be in entity_version or model_version
        if model_fqn in name or getattr(ent, "model_name", "") == model_fqn:
            v = getattr(ent, "entity_version", None) or getattr(ent, "model_version", None)
            if v is not None and str(v) == str(version):
                return True
    return False

# If endpoint exists, is READY, and already serves this model version — skip deploy
deployment = None
try:
    ep = w.serving_endpoints.get(name=endpoint_name)
    if _endpoint_serves_model(ep, model_fqn, latest_version):
        print(f"Endpoint '{endpoint_name}' is already READY and serving {model_fqn} version {latest_version}. Skipping deploy.")
        deployment = type("Deployment", (), {"endpoint_name": endpoint_name})()
except Exception:
    ep = None

if deployment is None:
    deployment = agents.deploy(
        model_name=model_fqn,
        model_version=latest_version,
        endpoint_name=endpoint_name,
        scale_to_zero=True,
    )
    print(f"Agent deployed: endpoint={deployment.endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Wait for Endpoint to be Ready

# COMMAND ----------

import time

endpoint_name = "shopnow-ops-agent"
print(f"Waiting for endpoint '{endpoint_name}' to become READY...")

for i in range(180):  # 180 * 15s = 45 min
    ep = w.serving_endpoints.get(name=endpoint_name)
    ready = str(ep.state.ready).split(".")[-1] if ep.state and ep.state.ready else "UNKNOWN"
    config = str(ep.state.config_update).split(".")[-1] if ep.state and ep.state.config_update else "UNKNOWN"

    if ready == "READY" and config in ("NOT_UPDATING", "UNKNOWN"):
        print(f"Endpoint is READY (config={config})!")
        break

    if "FAILED" in config:
        raise RuntimeError(f"Endpoint deployment FAILED: config_update={config}")

    if i % 4 == 0:
        print(f"  [{i*20}s] ready={ready}, config_update={config}")
    time.sleep(15)
else:
    raise TimeoutError("Endpoint did not become ready within 45 minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Grant Endpoint Permissions
# MAGIC
# MAGIC Allow all workspace users (including app service principals) to query the agent endpoint.

# COMMAND ----------

from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel

ep = w.serving_endpoints.get(name=endpoint_name)
w.serving_endpoints.set_permissions(
    serving_endpoint_id=ep.id,
    access_control_list=[
        ServingEndpointAccessControlRequest(
            group_name="users",
            permission_level=ServingEndpointPermissionLevel.CAN_QUERY,
        )
    ],
)
print(f"Granted CAN_QUERY on '{endpoint_name}' to all users.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC We built an agent that:
# MAGIC - Uses **live Unity Catalog gold tables** as data sources via UC Functions
# MAGIC - Is grounded in **real business data** — no hallucination risk on factual queries
# MAGIC - Is deployed as a **REST endpoint** via Model Serving, callable from any application
# MAGIC - Is **tested end-to-end** and confirmed working
# MAGIC
# MAGIC **Next** [01-dashboard: AI/BI Dashboard]($../04-ai-bi/01-dashboard)
