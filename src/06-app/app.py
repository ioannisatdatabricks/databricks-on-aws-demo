"""
ShopNow Ops Hub — Databricks App
=================================
A FastAPI + HTMX single-page application that combines:
  1. Live KPI cards (from Lakebase Postgres)
  2. AI Agent chat interface (calls the Model Serving endpoint)

Run locally:  uvicorn app:app --reload
Deploy:       databricks bundle deploy && databricks bundle run shopnow_app
"""

import asyncio
import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Configuration (injected by Databricks Apps as environment variables)
# ---------------------------------------------------------------------------
AGENT_ENDPOINT      = os.environ.get("AGENT_ENDPOINT", "shopnow-ops-agent")
LAKEBASE_PROJECT    = os.environ.get("LAKEBASE_PROJECT", "shopnow-lakebase")
LAKEBASE_DATABASE   = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")
LAKEBASE_BRANCH     = os.environ.get("LAKEBASE_BRANCH", "production")
UC_CATALOG          = os.environ.get("UC_CATALOG", "main")
UC_SCHEMA           = os.environ.get("UC_SCHEMA", "aws_webinar_demo_dev")

# Resolved Lakebase endpoint (from bundle-created project); cached after first lookup
_lakebase_host: str | None = None
_lakebase_endpoint_path: str | None = None

def _resolve_lakebase_endpoint() -> tuple[str | None, str | None]:
    """Resolve the Lakebase endpoint host and path for the bundle-created project via SDK. Returns (host, endpoint_path)."""
    global _lakebase_host, _lakebase_endpoint_path
    if _lakebase_host is not None and _lakebase_endpoint_path is not None:
        return _lakebase_host, _lakebase_endpoint_path
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        branch_path = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}"
        endpoints = list(w.postgres.list_endpoints(parent=branch_path))
        if not endpoints:
            return None, None
        endpoint_path = endpoints[0].name
        ep = w.postgres.get_endpoint(name=endpoint_path)
        if ep and ep.status and ep.status.hosts:
            _lakebase_host = ep.status.hosts.host
            _lakebase_endpoint_path = endpoint_path
            return _lakebase_host, _lakebase_endpoint_path
    except Exception:
        pass
    return None, None


def _get_lakebase_conn():
    """Get a psycopg2 connection to Lakebase using OAuth credentials. Uses the bundle-created project."""
    host, endpoint_path = _resolve_lakebase_endpoint()
    if not host or not endpoint_path:
        return None
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
    email = w.current_user.me().user_name
    conn = psycopg2.connect(
        host=host, port=5432, dbname=LAKEBASE_DATABASE,
        user=email, password=cred.token, sslmode="require",
        options=f"-c search_path={UC_SCHEMA},public",
    )
    return conn

app = FastAPI(title="ShopNow Ops Hub")

# ---------------------------------------------------------------------------
# HTML Template (inline for simplicity — no separate templates dir needed)
# ---------------------------------------------------------------------------

MAIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>ShopNow Ops Hub</title>
<script src="https://unpkg.com/htmx.org@1.9.10"></script>
<style>
  :root {
    --db-red: #FF3621;
    --db-dark: #1B1B1B;
    --db-gray: #F5F5F5;
    --db-border: #E0E0E0;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: var(--db-gray); color: var(--db-dark); }
  header {
    background: var(--db-dark); color: white; padding: 16px 32px;
    display: flex; align-items: center; gap: 16px;
  }
  header .logo { color: var(--db-red); font-size: 24px; font-weight: 700; }
  header h1   { font-size: 18px; font-weight: 500; }
  .main       { display: grid; grid-template-columns: 1fr 1fr; grid-template-rows: auto 1fr;
                gap: 16px; padding: 24px; height: calc(100vh - 64px); }
  .card       { background: white; border-radius: 8px; border: 1px solid var(--db-border);
                padding: 20px; overflow: hidden; }
  .card h2    { font-size: 14px; text-transform: uppercase; letter-spacing: 0.05em;
                color: #666; margin-bottom: 16px; }
  /* KPI cards */
  .kpis       { grid-column: 1 / -1; display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
  .kpi-card   { background: white; border-radius: 8px; border: 1px solid var(--db-border);
                padding: 16px; text-align: center; }
  .kpi-card .value { font-size: 28px; font-weight: 700; color: var(--db-dark); }
  .kpi-card .label { font-size: 12px; color: #888; margin-top: 4px; }
  .kpi-card .delta { font-size: 12px; margin-top: 6px; }
  .delta.up   { color: #22c55e; }
  .delta.down { color: var(--db-red); }
  /* Agent chat */
  .chat-card  { grid-column: 1 / -1; display: flex; flex-direction: column; height: 500px; }
  .messages   { flex: 1; overflow-y: auto; padding: 8px; display: flex; flex-direction: column; gap: 8px; }
  .msg        { padding: 10px 14px; border-radius: 12px; max-width: 85%; font-size: 14px; line-height: 1.5; }
  .msg.user   { background: var(--db-red); color: white; align-self: flex-end; }
  .msg.agent  { background: #F0F0F0; color: var(--db-dark); align-self: flex-start; }
  .chat-input { display: flex; gap: 8px; padding-top: 12px; border-top: 1px solid var(--db-border); }
  .chat-input input { flex: 1; padding: 10px 14px; border: 1px solid var(--db-border);
                      border-radius: 8px; font-size: 14px; outline: none; }
  .chat-input input:focus { border-color: var(--db-red); }
  .chat-input button { padding: 10px 20px; background: var(--db-red); color: white;
                       border: none; border-radius: 8px; cursor: pointer; font-size: 14px; }
  .chat-input button:hover { background: #cc2b1a; }
  .spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid #ccc;
             border-top-color: var(--db-red); border-radius: 50%; animation: spin 0.6s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>

<header>
  <div class="logo">▲</div>
  <h1>ShopNow Ops Hub &nbsp;|&nbsp; Powered by Databricks on AWS</h1>
</header>

<div class="main">

  <!-- KPI Bar -->
  <div class="kpis" id="kpis"
       hx-get="/api/kpis" hx-trigger="load, every 60s" hx-swap="innerHTML">
    <div class="kpi-card"><div class="value">…</div><div class="label">Loading KPIs</div></div>
  </div>

  <!-- Agent Chat -->
  <div class="card chat-card">
    <h2>🤖 ShopNow Assistant</h2>
    <div class="messages" id="messages">
      <div class="msg agent">
        Hi! I'm the ShopNow Operations Assistant. Ask me about revenue, top products,
        cart abandonment, or at-risk customers.
      </div>
    </div>
    <div class="chat-input">
      <input id="user-input" type="text" placeholder="e.g. What was our revenue last week?"
             onkeydown="if(event.key==='Enter') sendMessage()"/>
      <button onclick="sendMessage()">Send</button>
    </div>
  </div>

</div>

<script>
async function sendMessage() {
  const input   = document.getElementById('user-input');
  const messages = document.getElementById('messages');
  const question = input.value.trim();
  if (!question) return;

  // Show user message
  messages.innerHTML += `<div class="msg user">${escapeHtml(question)}</div>`;
  input.value = '';

  // Show spinner
  const spinnerId = 'spinner-' + Date.now();
  messages.innerHTML += `<div class="msg agent" id="${spinnerId}"><span class="spinner"></span></div>`;
  messages.scrollTop = messages.scrollHeight;

  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 180000);
    const res  = await fetch('/api/agent', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({question}),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    const data = await res.json();
    document.getElementById(spinnerId).textContent = data.answer;
  } catch (e) {
    document.getElementById(spinnerId).textContent = 'Error contacting agent. Please retry.';
  }

  messages.scrollTop = messages.scrollHeight;
}

function escapeHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
</script>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    from jinja2 import Environment
    env = Environment()
    tpl = env.from_string(MAIN_HTML)
    html = tpl.render()
    return HTMLResponse(html)


def _fetch_kpis_from_lakebase() -> dict | None:
    """Try to fetch KPIs from Lakebase synced tables. Returns dict or None on failure."""
    try:
        conn = _get_lakebase_conn()
        if conn is None:
            return None
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(total_revenue), 0) AS revenue,
                           COALESCE(SUM(order_count), 0)   AS orders
                    FROM   gold_revenue_daily_synced
                """)
                rev_row = cur.fetchone()
                cur.execute("""
                    SELECT ROUND(AVG(lifetime_value)::numeric, 2) AS avg_ltv
                    FROM   gold_customer_ltv_synced
                    WHERE  lifetime_value > 0
                """)
                ltv_row = cur.fetchone()
        return {
            "revenue": float(rev_row["revenue"]),
            "orders": int(rev_row["orders"]),
            "ltv": float(ltv_row["avg_ltv"] or 0),
        }
    except Exception:
        return None


def _fetch_kpis_from_sql_warehouse() -> dict | None:
    """Fallback: fetch KPIs directly from Unity Catalog gold tables via SDK."""
    import logging
    logger = logging.getLogger("shopnow")
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import Disposition, Format
        w = WorkspaceClient()
        wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

        rev = w.statement_execution.execute_statement(
            statement=f"""
                SELECT COALESCE(SUM(total_revenue), 0) AS revenue,
                       COALESCE(SUM(order_count), 0)   AS orders
                FROM   {UC_CATALOG}.{UC_SCHEMA}.gold_revenue_daily
            """,
            warehouse_id=wh_id,
            wait_timeout="50s",
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if rev.status.state.value != "SUCCEEDED":
            logger.error(f"Revenue query status: {rev.status}")
            return None
        rev_row = rev.result.data_array[0]
        rev_cols = [c.name for c in rev.manifest.schema.columns]
        rev_dict = dict(zip(rev_cols, rev_row))

        ltv = w.statement_execution.execute_statement(
            statement=f"""
                SELECT ROUND(AVG(lifetime_value), 2) AS avg_ltv
                FROM   {UC_CATALOG}.{UC_SCHEMA}.gold_customer_ltv
                WHERE  lifetime_value > 0
            """,
            warehouse_id=wh_id,
            wait_timeout="50s",
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if ltv.status.state.value != "SUCCEEDED":
            logger.error(f"LTV query status: {ltv.status}")
            return None
        ltv_row = ltv.result.data_array[0]
        ltv_cols = [c.name for c in ltv.manifest.schema.columns]
        ltv_dict = dict(zip(ltv_cols, ltv_row))

        return {
            "revenue": float(rev_dict["revenue"]),
            "orders": int(float(rev_dict["orders"])),
            "ltv": float(ltv_dict["avg_ltv"] or 0),
        }
    except Exception as e:
        logger.error(f"SQL warehouse KPI fallback failed: {e}")
        return None


@app.get("/api/kpis")
async def get_kpis():
    """Read KPI summary — tries Lakebase first, falls back to SQL warehouse, then mock data."""
    data = await asyncio.to_thread(_fetch_kpis_from_lakebase)
    source = "Lakebase"
    if data is None:
        data = await asyncio.to_thread(_fetch_kpis_from_sql_warehouse)
        source = "SQL Warehouse"
    if data is None:
        return HTMLResponse(_mock_kpi_cards())

    revenue = data["revenue"]
    orders  = data["orders"]
    ltv     = data["ltv"]
    aov     = revenue / orders if orders else 0

    cards = [
        ("Total Revenue",     f"${revenue:,.0f}",  "All time",             ""),
        ("Total Orders",      f"{orders:,.0f}",    "All time",             ""),
        ("Avg Order Value",   f"${aov:,.2f}",      "Across all countries", ""),
        ("Avg Customer LTV",  f"${ltv:,.2f}",      "Active customers",     ""),
    ]
    return HTMLResponse(_render_kpi_cards(cards))


def _query_agent(question: str) -> str:
    """Synchronous call to the agent endpoint (runs in thread pool)."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    resp = w.api_client.do(
        "POST",
        f"/serving-endpoints/{AGENT_ENDPOINT}/invocations",
        body={"messages": [{"role": "user", "content": question}]},
    )
    # LangGraph agent returns {"messages": [...]} — extract last AI message
    messages = resp.get("messages", [])
    return next(
        (m["content"] for m in reversed(messages) if m.get("type") == "ai"),
        resp.get("choices", [{}])[0].get("message", {}).get("content", str(resp)),
    )


@app.post("/api/agent")
async def call_agent(request: Request):
    """Forward question to the Model Serving agent endpoint."""
    body     = await request.json()
    question = body.get("question", "")

    try:
        answer = await asyncio.to_thread(_query_agent, question)
    except Exception as e:
        answer = f"Agent error: {str(e)[:300]}"

    return JSONResponse({"answer": answer})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _render_kpi_cards(cards: list) -> str:
    html = ""
    for label, value, delta_text, delta_cls in cards:
        delta_html = f'<div class="delta {delta_cls}">{delta_text}</div>' if delta_text else ""
        html += f"""
        <div class="kpi-card">
          <div class="value">{value}</div>
          <div class="label">{label}</div>
          {delta_html}
        </div>"""
    return html


def _mock_kpi_cards() -> str:
    """Fallback mock cards when Lakebase is not configured."""
    return _render_kpi_cards([
        ("Monthly Revenue",  "$2,847,391",  "↑ 12% vs last month", "up"),
        ("Monthly Orders",   "18,234",       "↑ 8% vs last month",  "up"),
        ("Avg Order Value",  "$156.15",     "Across all countries", ""),
        ("Avg Customer LTV", "$423.80",     "Active customers",      ""),
    ])
