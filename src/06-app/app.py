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

  /* Assistant layout */
  .assistant-layout {
    display: flex; height: calc(100vh - 56px - 48px); gap: 16px;
  }
  .conv-panel {
    width: 280px; min-width: 280px; background: white;
    border-radius: 8px; border: 1px solid var(--db-border);
    display: flex; flex-direction: column; overflow: hidden;
  }
  .conv-panel-header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 16px; border-bottom: 1px solid var(--db-border);
  }
  .conv-panel-header h3 { font-size: 14px; font-weight: 600; color: var(--db-dark); }
  .conv-new-btn {
    display: flex; align-items: center; gap: 4px;
    padding: 6px 12px; background: var(--db-red); color: white;
    border: none; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 500;
  }
  .conv-new-btn:hover { background: var(--db-red-hover); }
  .conv-list { flex: 1; overflow-y: auto; padding: 8px; }
  .conv-list-empty { padding: 24px 16px; text-align: center; color: #bbb; font-size: 13px; }
  .conv-item {
    display: flex; align-items: flex-start; gap: 8px;
    padding: 10px 12px; border-radius: 6px; cursor: pointer;
    border: none; background: none; width: 100%; text-align: left;
    transition: background 0.15s;
  }
  .conv-item:hover { background: var(--db-gray); }
  .conv-item.active { background: #FFF0EE; }
  .conv-item-content { flex: 1; min-width: 0; }
  .conv-item-title {
    font-size: 13px; font-weight: 500; color: var(--db-dark);
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .conv-item-time { font-size: 11px; color: #aaa; margin-top: 2px; }
  .conv-item-delete {
    opacity: 0; border: none; background: none; cursor: pointer;
    color: #bbb; padding: 2px; border-radius: 4px; flex-shrink: 0;
  }
  .conv-item:hover .conv-item-delete { opacity: 1; }
  .conv-item-delete:hover { color: var(--db-red); background: rgba(255,54,33,0.08); }

  .chat-area { flex: 1; display: flex; flex-direction: column; min-width: 0; }
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
      <div class="assistant-layout">

        <!-- Conversation list panel -->
        <div class="conv-panel">
          <div class="conv-panel-header">
            <h3>Conversations</h3>
            <button class="conv-new-btn" onclick="newSession()">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16">
                <line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>
              </svg>
              New
            </button>
          </div>
          <div class="conv-list" id="conv-list">
            <div class="conv-list-empty">No conversations yet</div>
          </div>
        </div>

        <!-- Chat area -->
        <div class="chat-area">
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
// Agent Chat — with persistent conversation list
// ---------------------------------------------------------------------------
let sessionId = sessionStorage.getItem('shopnow_session');
if (!sessionId) {
  sessionId = crypto.randomUUID();
  sessionStorage.setItem('shopnow_session', sessionId);
}

const WELCOME_MSG = "Hi! I'm the ShopNow Operations Assistant. Ask me about revenue, top products, cart abandonment, or at-risk customers.";

// Load conversation list and current session on page load
window.addEventListener('DOMContentLoaded', async () => {
  await loadConversationList();
  await loadSession(sessionId);
});

async function loadConversationList() {
  try {
    const res = await fetch('/api/sessions');
    const data = await res.json();
    const listEl = document.getElementById('conv-list');
    if (!data.sessions || data.sessions.length === 0) {
      listEl.innerHTML = '<div class="conv-list-empty">No conversations yet</div>';
      return;
    }
    listEl.innerHTML = '';
    for (const s of data.sessions) {
      const active = s.id === sessionId ? ' active' : '';
      const time = s.updated_at ? new Date(s.updated_at).toLocaleDateString('en-US', {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '';
      listEl.innerHTML += `
        <button class="conv-item${active}" data-sid="${s.id}" onclick="switchSession('${s.id}')">
          <div class="conv-item-content">
            <div class="conv-item-title">${escapeHtml(s.title)}</div>
            <div class="conv-item-time">${time}</div>
          </div>
          <span class="conv-item-delete" onclick="event.stopPropagation(); deleteSession('${s.id}')" title="Delete">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
              <polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/>
            </svg>
          </span>
        </button>`;
    }
  } catch (e) { console.error('Failed to load sessions:', e); }
}

async function loadSession(sid) {
  sessionId = sid;
  sessionStorage.setItem('shopnow_session', sid);
  const messagesEl = document.getElementById('messages');
  messagesEl.innerHTML = `<div class="msg agent">${WELCOME_MSG}</div>`;
  // Highlight in list
  document.querySelectorAll('.conv-item').forEach(el => {
    el.classList.toggle('active', el.dataset.sid === sid);
  });
  try {
    const res = await fetch(`/api/session/${sid}`);
    const data = await res.json();
    for (const msg of (data.messages || [])) {
      const cls = msg.role === 'user' ? 'user' : 'agent';
      messagesEl.innerHTML += `<div class="msg ${cls}">${escapeHtml(msg.content)}</div>`;
    }
    messagesEl.scrollTop = messagesEl.scrollHeight;
  } catch (e) { /* new session */ }
}

function switchSession(sid) { loadSession(sid); }

async function newSession() {
  sessionId = crypto.randomUUID();
  sessionStorage.setItem('shopnow_session', sessionId);
  document.getElementById('messages').innerHTML =
    `<div class="msg agent">${WELCOME_MSG}</div>`;
  // Deselect all in list
  document.querySelectorAll('.conv-item').forEach(el => el.classList.remove('active'));
}

async function deleteSession(sid) {
  try {
    await fetch(`/api/session/${sid}`, { method: 'DELETE' });
    if (sid === sessionId) { await newSession(); }
    await loadConversationList();
  } catch (e) { console.error('Delete failed:', e); }
}

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
    // Refresh conversation list (new session may have appeared)
    await loadConversationList();
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
    """Persist conversation history and session metadata to DatabricksStore."""
    from datetime import datetime, timezone
    namespace = ("shopnow", "sessions", session_id)
    _store.put(namespace, "messages", {"messages": messages})

    # Update session index (title from first user message, timestamp)
    first_user_msg = next((m["content"] for m in messages if m["role"] == "user"), "New conversation")
    title = first_user_msg[:80] + ("..." if len(first_user_msg) > 80 else "")
    now = datetime.now(timezone.utc).isoformat()
    index_ns = ("shopnow", "session_index")
    existing = _store.get(index_ns, session_id)
    created = existing.value.get("created_at", now) if existing and existing.value else now
    _store.put(index_ns, session_id, {
        "title": title,
        "created_at": created,
        "updated_at": now,
    })


def _list_sessions() -> list[dict]:
    """List all sessions from the session index, newest first."""
    index_ns = ("shopnow", "session_index")
    items = _store.search(index_ns)
    sessions = []
    for item in items:
        if item.value:
            sessions.append({
                "id": item.key,
                "title": item.value.get("title", "Untitled"),
                "created_at": item.value.get("created_at"),
                "updated_at": item.value.get("updated_at"),
            })
    sessions.sort(key=lambda s: s.get("updated_at") or "", reverse=True)
    return sessions


def _delete_session(session_id: str):
    """Delete a session's messages and index entry."""
    _store.delete(("shopnow", "sessions", session_id), "messages")
    _store.delete(("shopnow", "session_index"), session_id)


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


@app.get("/api/sessions")
async def list_sessions():
    """List all conversation sessions."""
    try:
        sessions = await asyncio.to_thread(_list_sessions)
    except Exception as e:
        logger.error(f"Failed to list sessions: {e}")
        sessions = []
    return JSONResponse({"sessions": sessions})


@app.get("/api/session/{session_id}")
async def get_session(session_id: str):
    """Retrieve stored conversation history for a session."""
    try:
        messages = await asyncio.to_thread(_get_session_messages, session_id)
    except Exception as e:
        logger.error(f"Failed to load session {session_id}: {e}")
        messages = []
    return JSONResponse({"session_id": session_id, "messages": messages})


@app.delete("/api/session/{session_id}")
async def delete_session(session_id: str):
    """Delete a conversation session."""
    try:
        await asyncio.to_thread(_delete_session, session_id)
    except Exception as e:
        logger.error(f"Failed to delete session {session_id}: {e}")
    return JSONResponse({"deleted": session_id})


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
