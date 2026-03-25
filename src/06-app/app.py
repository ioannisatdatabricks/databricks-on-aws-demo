"""
ShopNow Ops Hub — Databricks App
=================================
A FastAPI + HTMX single-page application that combines:
  1. Live KPI cards (from Lakebase Postgres)
  2. AI Agent chat interface (calls the Model Serving endpoint)
  3. Persistent chat sessions (DatabricksStore backed by Lakebase)

Run locally:  uvicorn app:app --reload
Deploy:       databricks bundle deploy && databricks bundle run shopnow_app
"""

import asyncio
import json as _json
import logging
import os
import threading
import uuid
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
import psycopg2
import psycopg2.extras
from databricks.sdk import WorkspaceClient
from databricks_langchain import DatabricksStore

logger = logging.getLogger("shopnow")

# ---------------------------------------------------------------------------
# Configuration (injected by Databricks Apps as environment variables)
# ---------------------------------------------------------------------------
AGENT_ENDPOINT    = os.environ["AGENT_ENDPOINT"]
UC_SCHEMA         = os.environ["UC_SCHEMA"]
LAKEBASE_INSTANCE = os.environ["LAKEBASE_INSTANCE"]

# ---------------------------------------------------------------------------
# Lakebase connection (OAuth token auth — auto-refreshed)
# ---------------------------------------------------------------------------

_w = WorkspaceClient()
_pg_token: str = ""
_pg_user: str = ""
_token_lock = threading.Lock()
TOKEN_REFRESH_SEC = 50 * 60  # refresh every 50 min (tokens expire at 60 min)


def _refresh_token():
    """Generate a fresh OAuth token for Lakebase."""
    global _pg_token, _pg_user
    cred = _w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[LAKEBASE_INSTANCE],
    )
    with _token_lock:
        _pg_token = cred.token
        if not _pg_user:
            _pg_user = _w.current_user.me().user_name
    logger.info("Lakebase OAuth token refreshed")


def _token_refresh_loop():
    """Background thread that refreshes the token periodically."""
    import time
    while True:
        try:
            _refresh_token()
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
        time.sleep(TOKEN_REFRESH_SEC)


def _get_lakebase_conn():
    """Get a psycopg2 connection to Lakebase using OAuth token."""
    instance = _w.database.get_database_instance(name=LAKEBASE_INSTANCE)
    pg_host = instance.read_write_dns
    if not pg_host:
        raise RuntimeError("Lakebase instance has no DNS endpoint")
    with _token_lock:
        token = _pg_token
        user = _pg_user
    if not token:
        raise RuntimeError("Lakebase token not yet available")
    return psycopg2.connect(
        host=pg_host,
        port=5432,
        dbname="databricks_postgres",
        user=user,
        password=token,
        sslmode="require",
        options=f"-c search_path={UC_SCHEMA},public",
    )


# ---------------------------------------------------------------------------
# Agent Memory Store (DatabricksStore backed by Lakebase)
# ---------------------------------------------------------------------------

_store = DatabricksStore(instance_name=LAKEBASE_INSTANCE, workspace_client=_w)

app = FastAPI(title="ShopNow Ops Hub")


@app.on_event("startup")
async def startup():
    """Generate initial token, init memory store, and start background refresh."""
    await asyncio.to_thread(_refresh_token)
    await asyncio.to_thread(_store.setup)
    logger.info("DatabricksStore initialized in Lakebase")
    t = threading.Thread(target=_token_refresh_loop, daemon=True)
    t.start()

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
  .chat-btns  { display: flex; gap: 8px; padding-top: 8px; }
  .chat-btns button { padding: 6px 14px; background: white; color: #666;
                      border: 1px solid var(--db-border); border-radius: 6px;
                      cursor: pointer; font-size: 12px; }
  .chat-btns button:hover { background: var(--db-gray); }
  .spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid #ccc;
             border-top-color: var(--db-red); border-radius: 50%; animation: spin 0.6s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>

<header>
  <div class="logo">&#9650;</div>
  <h1>ShopNow Ops Hub &nbsp;|&nbsp; Powered by Databricks on AWS</h1>
</header>

<div class="main">

  <!-- KPI Bar -->
  <div class="kpis" id="kpis"
       hx-get="/api/kpis" hx-trigger="load, every 60s" hx-swap="innerHTML">
    <div class="kpi-card"><div class="value">&hellip;</div><div class="label">Loading KPIs</div></div>
  </div>

  <!-- Agent Chat -->
  <div class="card chat-card">
    <h2>&#129302; ShopNow Assistant</h2>
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
    <div class="chat-btns">
      <button onclick="newSession()">New conversation</button>
    </div>
  </div>

</div>

<script>
// Session ID persisted in sessionStorage — survives page refresh within same tab
let sessionId = sessionStorage.getItem('shopnow_session');
if (!sessionId) {
  sessionId = crypto.randomUUID();
  sessionStorage.setItem('shopnow_session', sessionId);
}

// On page load, restore chat history from Lakebase
window.addEventListener('DOMContentLoaded', async () => {
  try {
    const res = await fetch(`/api/session/${sessionId}`);
    const data = await res.json();
    const messagesEl = document.getElementById('messages');
    for (const msg of (data.messages || [])) {
      const cls = msg.role === 'user' ? 'user' : 'agent';
      messagesEl.innerHTML += `<div class="msg ${cls}">${escapeHtml(msg.content)}</div>`;
    }
    messagesEl.scrollTop = messagesEl.scrollHeight;
  } catch (e) { /* first visit — no history */ }
});

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
      body: JSON.stringify({session_id: sessionId, question: question}),
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

function newSession() {
  sessionId = crypto.randomUUID();
  sessionStorage.setItem('shopnow_session', sessionId);
  const messagesEl = document.getElementById('messages');
  messagesEl.innerHTML = `<div class="msg agent">
    Hi! I'm the ShopNow Operations Assistant. Ask me about revenue, top products,
    cart abandonment, or at-risk customers.</div>`;
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
async def index():
    return HTMLResponse(MAIN_HTML)


def _fetch_kpis_from_lakebase() -> dict | None:
    """Try to fetch KPIs from Lakebase synced tables. Returns dict or None on failure."""
    try:
        conn = _get_lakebase_conn()
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT COALESCE(SUM(total_revenue), 0) AS revenue,
                           COALESCE(SUM(order_count), 0)   AS orders
                    FROM   {UC_SCHEMA}.gold_revenue_daily_synced
                """)
                rev_row = cur.fetchone()
                cur.execute(f"""
                    SELECT ROUND(AVG(lifetime_value)::numeric, 2) AS avg_ltv
                    FROM   {UC_SCHEMA}.gold_customer_ltv_synced
                    WHERE  lifetime_value > 0
                """)
                ltv_row = cur.fetchone()
        return {
            "revenue": float(rev_row["revenue"]),
            "orders": int(rev_row["orders"]),
            "ltv": float(ltv_row["avg_ltv"] or 0),
        }
    except psycopg2.OperationalError as e:
        logger.error(f"Lakebase KPI fetch failed: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Lakebase KPI fetch failed: {e}")
        return {"error": str(e)}


@app.get("/api/kpis")
async def get_kpis():
    """Read KPI summary from Lakebase."""
    try:
        data = await asyncio.to_thread(_fetch_kpis_from_lakebase)
    except Exception as e:
        return HTMLResponse(f'<div class="kpi-card" style="grid-column:1/-1;"><div class="value" style="font-size:14px;color:#888;">Error: {str(e)[:200]}</div></div>')
    if data is None or "error" in data:
        err = data.get("error", "Unknown") if data else "Connection failed"
        return HTMLResponse(f'<div class="kpi-card" style="grid-column:1/-1;"><div class="value" style="font-size:14px;color:#888;">Database unavailable: {err[:200]}</div></div>')

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


# ---------------------------------------------------------------------------
# Agent Chat — session-aware with Lakebase-backed memory
# ---------------------------------------------------------------------------

def _get_session_messages(session_id: str) -> list[dict]:
    """Retrieve conversation history for a session from DatabricksStore."""
    namespace = ("shopnow", "sessions", session_id)
    item = _store.get(namespace, "messages")
    if item and item.value:
        return item.value.get("messages", [])
    return []


def _save_session_messages(session_id: str, messages: list[dict]):
    """Persist conversation history for a session to DatabricksStore."""
    namespace = ("shopnow", "sessions", session_id)
    _store.put(namespace, "messages", {"messages": messages})


def _query_agent(messages: list[dict]) -> str:
    """Call the agent endpoint with conversation context.

    The LangGraph agent endpoint returns empty responses for multi-turn message
    arrays. Instead, we consolidate the conversation history into a single user
    message so the agent can resolve follow-up references like 'that period'.
    """
    if len(messages) > 1:
        # Pack history into a single user message
        lines = ["Here is our conversation so far:"]
        for msg in messages[:-1]:
            role_label = "User" if msg["role"] == "user" else "Assistant"
            lines.append(f"{role_label}: {msg['content']}")
        lines.append("")
        lines.append(f"Now answer this follow-up question: {messages[-1]['content']}")
        payload = [{"role": "user", "content": "\n".join(lines)}]
    else:
        payload = messages

    resp = _w.api_client.do(
        "POST",
        f"/serving-endpoints/{AGENT_ENDPOINT}/invocations",
        body={"messages": payload},
    )
    # LangGraph format: {"messages": [...]}
    for m in reversed(resp.get("messages", [])):
        if m.get("type") == "ai" and m.get("content"):
            c = m["content"]
            return c if isinstance(c, str) else str(c)
    # OpenAI ChatCompletion format: {"choices": [...]}
    for c in resp.get("choices", []):
        content = c.get("message", {}).get("content", "")
        if content:
            return content
    logger.error(f"Unexpected agent response: {_json.dumps(resp)[:500]}")
    return f"I couldn't process that request. Please try rephrasing your question."


@app.get("/api/session/{session_id}")
async def get_session(session_id: str):
    """Retrieve stored conversation history for a session."""
    try:
        messages = await asyncio.to_thread(_get_session_messages, session_id)
    except Exception as e:
        logger.error(f"Failed to load session {session_id}: {e}")
        messages = []
    return JSONResponse({"session_id": session_id, "messages": messages})


@app.post("/api/agent")
async def call_agent(request: Request):
    """Forward question to the agent with full session context from Lakebase."""
    body = await request.json()
    session_id = body.get("session_id", str(uuid.uuid4()))
    question = body.get("question", "")

    try:
        # 1. Load conversation history from Lakebase (non-blocking on failure)
        history = []
        try:
            history = await asyncio.to_thread(_get_session_messages, session_id)
        except Exception as e:
            logger.warning(f"Store read failed (continuing without history): {e}")

        # 2. Append new user message
        history.append({"role": "user", "content": question})

        # 3. Send full conversation history to the agent endpoint
        answer = await asyncio.to_thread(_query_agent, history)

        # 5. Persist to Lakebase (fire-and-forget on failure)
        history.append({"role": "assistant", "content": answer})
        try:
            await asyncio.to_thread(_save_session_messages, session_id, history)
        except Exception as e:
            logger.warning(f"Store write failed: {e}")

    except Exception as e:
        logger.error(f"Agent call failed: {e}", exc_info=True)
        answer = f"Agent error: {str(e)[:300]}"

    return JSONResponse({"answer": answer, "session_id": session_id})


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
