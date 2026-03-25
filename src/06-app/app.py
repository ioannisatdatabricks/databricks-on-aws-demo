"""
ShopNow Ops Hub — Databricks App
=================================
A FastAPI + HTMX single-page application that combines:
  1. Dashboard with live KPI cards and charts (from Lakebase Postgres)
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
# HTML Template — Sidebar layout with Dashboard + AI Assistant pages
# ---------------------------------------------------------------------------

MAIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>ShopNow Ops Hub</title>
<script src="https://unpkg.com/htmx.org@1.9.10"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<style>
  :root {
    --db-red: #FF3621;
    --db-red-hover: #cc2b1a;
    --db-dark: #1B1B1B;
    --db-gray: #F5F5F5;
    --db-border: #E0E0E0;
    --sidebar-w: 240px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: var(--db-gray); color: var(--db-dark); }

  /* Header */
  header {
    background: var(--db-dark); color: white; padding: 0 24px;
    display: flex; align-items: center; gap: 14px; height: 56px;
    position: fixed; top: 0; left: 0; right: 0; z-index: 100;
  }
  header .logo { color: var(--db-red); font-size: 22px; font-weight: 700; }
  header h1 { font-size: 16px; font-weight: 500; opacity: 0.95; }

  /* Layout */
  .app-layout { display: flex; margin-top: 56px; height: calc(100vh - 56px); }

  /* Sidebar */
  .sidebar {
    width: var(--sidebar-w); min-width: var(--sidebar-w);
    background: white; border-right: 1px solid var(--db-border);
    display: flex; flex-direction: column; padding: 16px 0;
  }
  .sidebar .nav-section { padding: 0 16px; margin-bottom: 8px; }
  .sidebar .nav-section-label {
    font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em;
    color: #999; margin-bottom: 8px; padding: 0 12px;
  }
  .nav-item {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 12px; margin: 2px 8px; border-radius: 6px;
    cursor: pointer; font-size: 14px; color: #555;
    transition: background 0.15s, color 0.15s;
    border: none; background: none; width: calc(100% - 16px); text-align: left;
  }
  .nav-item:hover { background: var(--db-gray); color: var(--db-dark); }
  .nav-item.active { background: #FFF0EE; color: var(--db-red); font-weight: 600; }
  .nav-item svg { width: 18px; height: 18px; flex-shrink: 0; }
  .sidebar-footer {
    margin-top: auto; padding: 16px 20px; border-top: 1px solid var(--db-border);
    font-size: 11px; color: #aaa;
  }

  /* Content area */
  .content { flex: 1; overflow-y: auto; padding: 24px; }
  .page { display: none; }
  .page.active { display: block; }

  /* Cards */
  .card {
    background: white; border-radius: 8px; border: 1px solid var(--db-border);
    padding: 20px; margin-bottom: 16px;
  }
  .card h2 {
    font-size: 13px; text-transform: uppercase; letter-spacing: 0.06em;
    color: #888; margin-bottom: 16px; font-weight: 600;
  }

  /* KPI row */
  .kpi-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 20px; }
  .kpi-card {
    background: white; border-radius: 8px; border: 1px solid var(--db-border);
    padding: 20px; text-align: center;
  }
  .kpi-card .value { font-size: 28px; font-weight: 700; color: var(--db-dark); }
  .kpi-card .label { font-size: 12px; color: #888; margin-top: 4px; }
  .kpi-card .sublabel { font-size: 11px; color: #bbb; margin-top: 2px; }

  /* Chart grid */
  .chart-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  .chart-card { background: white; border-radius: 8px; border: 1px solid var(--db-border); padding: 20px; }
  .chart-card h3 { font-size: 13px; color: #666; margin-bottom: 12px; font-weight: 600; }
  .chart-card canvas { width: 100% !important; max-height: 280px; }

  /* Chat page */
  .chat-container {
    display: flex; flex-direction: column;
    height: calc(100vh - 56px - 48px); /* header + padding */
    max-width: 900px;
  }
  .chat-header { margin-bottom: 16px; }
  .chat-header h2 { font-size: 18px; font-weight: 600; color: var(--db-dark); margin-bottom: 4px; }
  .chat-header p { font-size: 13px; color: #888; }
  .chat-box {
    flex: 1; background: white; border-radius: 8px; border: 1px solid var(--db-border);
    display: flex; flex-direction: column; overflow: hidden;
  }
  .messages {
    flex: 1; overflow-y: auto; padding: 20px;
    display: flex; flex-direction: column; gap: 12px;
  }
  .msg {
    padding: 12px 16px; border-radius: 16px; max-width: 80%;
    font-size: 14px; line-height: 1.6; white-space: pre-wrap;
  }
  .msg.user {
    background: var(--db-red); color: white; align-self: flex-end;
    border-bottom-right-radius: 4px;
  }
  .msg.agent {
    background: var(--db-gray); color: var(--db-dark); align-self: flex-start;
    border-bottom-left-radius: 4px;
  }
  .chat-input-bar {
    display: flex; gap: 8px; padding: 16px;
    border-top: 1px solid var(--db-border); background: white;
  }
  .chat-input-bar input {
    flex: 1; padding: 12px 16px; border: 1px solid var(--db-border);
    border-radius: 8px; font-size: 14px; outline: none;
  }
  .chat-input-bar input:focus { border-color: var(--db-red); }
  .chat-input-bar button {
    padding: 12px 24px; background: var(--db-red); color: white;
    border: none; border-radius: 8px; cursor: pointer; font-size: 14px; font-weight: 500;
  }
  .chat-input-bar button:hover { background: var(--db-red-hover); }
  .chat-actions {
    display: flex; gap: 8px; padding: 0 16px 12px;
  }
  .chat-actions button {
    padding: 6px 14px; background: white; color: #666;
    border: 1px solid var(--db-border); border-radius: 6px;
    cursor: pointer; font-size: 12px;
  }
  .chat-actions button:hover { background: var(--db-gray); }

  .spinner {
    display: inline-block; width: 16px; height: 16px;
    border: 2px solid #ddd; border-top-color: var(--db-red);
    border-radius: 50%; animation: spin 0.6s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* Responsive */
  @media (max-width: 1024px) {
    .kpi-row { grid-template-columns: repeat(2, 1fr); }
    .chart-grid { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>

<header>
  <div class="logo">&#9650;</div>
  <h1>ShopNow Ops Hub &nbsp;|&nbsp; Powered by Databricks on AWS</h1>
</header>

<div class="app-layout">

  <!-- Sidebar Navigation -->
  <nav class="sidebar">
    <div class="nav-section">
      <div class="nav-section-label">Analytics</div>
      <button class="nav-item active" onclick="showPage('dashboard', this)">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="4" rx="1"/>
          <rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="11" width="7" height="10" rx="1"/>
        </svg>
        Dashboard
      </button>
    </div>
    <div class="nav-section">
      <div class="nav-section-label">Intelligence</div>
      <button class="nav-item" onclick="showPage('assistant', this)">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/>
        </svg>
        AI Assistant
      </button>
    </div>
    <div class="sidebar-footer">
      Powered by Databricks<br/>Lakebase + Mosaic AI
    </div>
  </nav>

  <!-- Main Content -->
  <div class="content">

    <!-- ==================== Dashboard Page ==================== -->
    <div id="page-dashboard" class="page active">

      <!-- KPI Row -->
      <div class="kpi-row" id="kpis"
           hx-get="/api/kpis" hx-trigger="load, every 60s" hx-swap="innerHTML">
        <div class="kpi-card"><div class="value">&hellip;</div><div class="label">Loading</div></div>
        <div class="kpi-card"><div class="value">&hellip;</div><div class="label">Loading</div></div>
        <div class="kpi-card"><div class="value">&hellip;</div><div class="label">Loading</div></div>
        <div class="kpi-card"><div class="value">&hellip;</div><div class="label">Loading</div></div>
      </div>

      <!-- Charts -->
      <div class="chart-grid">
        <div class="chart-card">
          <h3>Revenue Trend (Last 30 Days)</h3>
          <canvas id="chart-revenue-trend"></canvas>
        </div>
        <div class="chart-card">
          <h3>Revenue by Country</h3>
          <canvas id="chart-revenue-country"></canvas>
        </div>
        <div class="chart-card">
          <h3>Top 10 Products by Revenue</h3>
          <canvas id="chart-top-products"></canvas>
        </div>
        <div class="chart-card">
          <h3>Customer Segments</h3>
          <canvas id="chart-customer-segments"></canvas>
        </div>
      </div>
    </div>

    <!-- ==================== AI Assistant Page ==================== -->
    <div id="page-assistant" class="page">
      <div class="chat-container">
        <div class="chat-header">
          <h2>ShopNow AI Assistant</h2>
          <p>Ask questions about revenue, products, cart abandonment, or customer insights. Backed by live data from Unity Catalog.</p>
        </div>
        <div class="chat-box">
          <div class="messages" id="messages">
            <div class="msg agent">Hi! I'm the ShopNow Operations Assistant. Ask me about revenue, top products, cart abandonment, or at-risk customers.</div>
          </div>
          <div class="chat-input-bar">
            <input id="user-input" type="text"
                   placeholder="e.g. What was our revenue last week?"
                   onkeydown="if(event.key==='Enter') sendMessage()"/>
            <button onclick="sendMessage()">Send</button>
          </div>
          <div class="chat-actions">
            <button onclick="newSession()">New conversation</button>
          </div>
        </div>
      </div>
    </div>

  </div>
</div>

<script>
// ---------------------------------------------------------------------------
// Page Navigation
// ---------------------------------------------------------------------------
function showPage(pageId, btn) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('page-' + pageId).classList.add('active');
  btn.classList.add('active');
  if (pageId === 'dashboard' && !window._chartsLoaded) loadCharts();
}

// ---------------------------------------------------------------------------
// Charts (loaded once from Lakebase via API)
// ---------------------------------------------------------------------------
const DB_RED = '#FF3621';
const CHART_COLORS = ['#FF3621','#FF6B4A','#1B1B1B','#4A90D9','#50C878','#FFB347','#9B59B6','#E74C3C','#3498DB','#2ECC71'];

window._chartsLoaded = false;

async function loadCharts() {
  window._chartsLoaded = true;
  try {
    const res = await fetch('/api/charts');
    const data = await res.json();

    // Revenue Trend
    if (data.revenue_trend && data.revenue_trend.length) {
      new Chart(document.getElementById('chart-revenue-trend'), {
        type: 'line',
        data: {
          labels: data.revenue_trend.map(r => r.day),
          datasets: [{
            label: 'Daily Revenue',
            data: data.revenue_trend.map(r => r.revenue),
            borderColor: DB_RED, backgroundColor: 'rgba(255,54,33,0.08)',
            fill: true, tension: 0.3, pointRadius: 2,
          }]
        },
        options: { responsive: true, plugins: { legend: { display: false } },
          scales: { y: { beginAtZero: true, ticks: { callback: v => '$' + (v/1000).toFixed(0) + 'k' } },
                    x: { ticks: { maxTicksLimit: 8 } } } }
      });
    }

    // Revenue by Country
    if (data.revenue_by_country && data.revenue_by_country.length) {
      new Chart(document.getElementById('chart-revenue-country'), {
        type: 'doughnut',
        data: {
          labels: data.revenue_by_country.map(r => r.country),
          datasets: [{ data: data.revenue_by_country.map(r => r.revenue), backgroundColor: CHART_COLORS }]
        },
        options: { responsive: true, plugins: { legend: { position: 'right' } } }
      });
    }

    // Top Products
    if (data.top_products && data.top_products.length) {
      new Chart(document.getElementById('chart-top-products'), {
        type: 'bar',
        data: {
          labels: data.top_products.map(r => r.name.length > 18 ? r.name.slice(0,18)+'...' : r.name),
          datasets: [{
            label: 'Revenue',
            data: data.top_products.map(r => r.revenue),
            backgroundColor: CHART_COLORS,
          }]
        },
        options: { responsive: true, indexAxis: 'y', plugins: { legend: { display: false } },
          scales: { x: { ticks: { callback: v => '$' + (v/1000).toFixed(0) + 'k' } } } }
      });
    }

    // Customer Segments
    if (data.customer_segments && data.customer_segments.length) {
      new Chart(document.getElementById('chart-customer-segments'), {
        type: 'bar',
        data: {
          labels: data.customer_segments.map(r => r.segment),
          datasets: [
            { label: 'Avg LTV', data: data.customer_segments.map(r => r.avg_ltv),
              backgroundColor: DB_RED },
            { label: 'Customers', data: data.customer_segments.map(r => r.count),
              backgroundColor: '#1B1B1B' },
          ]
        },
        options: { responsive: true,
          scales: { y: { beginAtZero: true, ticks: { callback: v => '$' + v.toLocaleString() } } } }
      });
    }
  } catch (e) {
    console.error('Failed to load charts:', e);
  }
}

// Load charts on initial page load
window.addEventListener('DOMContentLoaded', () => loadCharts());

// ---------------------------------------------------------------------------
// Agent Chat
// ---------------------------------------------------------------------------
let sessionId = sessionStorage.getItem('shopnow_session');
if (!sessionId) {
  sessionId = crypto.randomUUID();
  sessionStorage.setItem('shopnow_session', sessionId);
}

// Restore chat history on load
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
  } catch (e) { /* first visit */ }
});

async function sendMessage() {
  const input = document.getElementById('user-input');
  const messages = document.getElementById('messages');
  const question = input.value.trim();
  if (!question) return;

  messages.innerHTML += `<div class="msg user">${escapeHtml(question)}</div>`;
  input.value = '';

  const spinnerId = 'spinner-' + Date.now();
  messages.innerHTML += `<div class="msg agent" id="${spinnerId}"><span class="spinner"></span></div>`;
  messages.scrollTop = messages.scrollHeight;

  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 180000);
    const res = await fetch('/api/agent', {
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
  document.getElementById('messages').innerHTML =
    `<div class="msg agent">Hi! I'm the ShopNow Operations Assistant. Ask me about revenue, top products, cart abandonment, or at-risk customers.</div>`;
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
    """Fetch KPI summary from Lakebase synced tables."""
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
    except Exception as e:
        logger.error(f"Lakebase KPI fetch failed: {e}")
        return {"error": str(e)}


def _fetch_chart_data() -> dict:
    """Fetch all chart datasets from Lakebase in a single connection."""
    result = {}
    try:
        conn = _get_lakebase_conn()
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Revenue trend (last 30 days)
                cur.execute(f"""
                    SELECT order_day AS day,
                           SUM(total_revenue) AS revenue
                    FROM   {UC_SCHEMA}.gold_revenue_daily_synced
                    WHERE  order_day >= (SELECT MAX(order_day) - INTERVAL '30 days'
                                        FROM {UC_SCHEMA}.gold_revenue_daily_synced)
                    GROUP BY order_day ORDER BY order_day
                """)
                result["revenue_trend"] = [
                    {"day": str(r["day"]), "revenue": float(r["revenue"])}
                    for r in cur.fetchall()
                ]

                # Revenue by country
                cur.execute(f"""
                    SELECT ship_country AS country,
                           SUM(total_revenue) AS revenue
                    FROM   {UC_SCHEMA}.gold_revenue_daily_synced
                    GROUP BY ship_country ORDER BY revenue DESC
                """)
                result["revenue_by_country"] = [
                    {"country": r["country"], "revenue": float(r["revenue"])}
                    for r in cur.fetchall()
                ]

                # Top 10 products
                cur.execute(f"""
                    SELECT product_name AS name,
                           total_revenue AS revenue
                    FROM   {UC_SCHEMA}.gold_top_products_synced
                    ORDER BY total_revenue DESC LIMIT 10
                """)
                result["top_products"] = [
                    {"name": r["name"], "revenue": float(r["revenue"])}
                    for r in cur.fetchall()
                ]

                # Customer segments
                cur.execute(f"""
                    SELECT segment,
                           COUNT(*) AS count,
                           ROUND(AVG(lifetime_value)::numeric, 2) AS avg_ltv
                    FROM   {UC_SCHEMA}.gold_customer_ltv_synced
                    GROUP BY segment ORDER BY avg_ltv DESC
                """)
                result["customer_segments"] = [
                    {"segment": r["segment"], "count": int(r["count"]),
                     "avg_ltv": float(r["avg_ltv"] or 0)}
                    for r in cur.fetchall()
                ]
    except Exception as e:
        logger.error(f"Chart data fetch failed: {e}")
        result["error"] = str(e)
    return result


@app.get("/api/kpis")
async def get_kpis():
    """KPI cards HTML fragment (loaded by HTMX)."""
    try:
        data = await asyncio.to_thread(_fetch_kpis_from_lakebase)
    except Exception as e:
        return HTMLResponse(
            '<div class="kpi-card" style="grid-column:1/-1;">'
            f'<div class="value" style="font-size:14px;color:#888;">Error: {str(e)[:200]}</div></div>'
        )
    if data is None or "error" in data:
        err = data.get("error", "Unknown") if data else "Connection failed"
        return HTMLResponse(
            '<div class="kpi-card" style="grid-column:1/-1;">'
            f'<div class="value" style="font-size:14px;color:#888;">Database unavailable: {err[:200]}</div></div>'
        )

    revenue = data["revenue"]
    orders  = data["orders"]
    ltv     = data["ltv"]
    aov     = revenue / orders if orders else 0

    cards = [
        ("Total Revenue",    f"${revenue:,.0f}",  "All time"),
        ("Total Orders",     f"{orders:,.0f}",     "All time"),
        ("Avg Order Value",  f"${aov:,.2f}",       "Across all countries"),
        ("Avg Customer LTV", f"${ltv:,.2f}",       "Active customers"),
    ]
    html = ""
    for label, value, sublabel in cards:
        html += f"""
        <div class="kpi-card">
          <div class="value">{value}</div>
          <div class="label">{label}</div>
          <div class="sublabel">{sublabel}</div>
        </div>"""
    return HTMLResponse(html)


@app.get("/api/charts")
async def get_charts():
    """JSON chart data for the dashboard."""
    data = await asyncio.to_thread(_fetch_chart_data)
    return JSONResponse(data)


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
    return "I couldn't process that request. Please try rephrasing your question."


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
        # 1. Load conversation history from Lakebase
        history = []
        try:
            history = await asyncio.to_thread(_get_session_messages, session_id)
        except Exception as e:
            logger.warning(f"Store read failed (continuing without history): {e}")

        # 2. Append new user message
        history.append({"role": "user", "content": question})

        # 3. Send to agent with context
        answer = await asyncio.to_thread(_query_agent, history)

        # 4. Persist updated history to Lakebase
        history.append({"role": "assistant", "content": answer})
        try:
            await asyncio.to_thread(_save_session_messages, session_id, history)
        except Exception as e:
            logger.warning(f"Store write failed: {e}")

    except Exception as e:
        logger.error(f"Agent call failed: {e}", exc_info=True)
        answer = f"Agent error: {str(e)[:300]}"

    return JSONResponse({"answer": answer, "session_id": session_id})
