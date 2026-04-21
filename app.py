""" app.py """

from __future__ import annotations

import copy
import traceback
from typing import Optional

import streamlit as st

from engine.catalog import Catalog
from engine.cbo import CostBasedOptimizer
from engine.database import DatabaseManager
from engine.executor import QueryExecutor
from engine.nodes import PlanNode
from engine.parser import QueryParser
from engine.rbo import RuleBasedOptimizer
from engine.visualizer import PlanVisualizer

# ─────────────────────────────────────────────────────────────────────────────
# Page configuration (must be first Streamlit call)
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Query Optimizer",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS  —  dark-mode-first + explicit light-mode overrides
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    /* ── Dark mode tokens (default) ──────────────────────────────── */
    :root {
        --bg-primary:     #0f1115;
        --bg-secondary:   #181b22;
        --bg-card:        #1e2130;
        --accent:         #4a7cf7;
        --accent-dark:    #3a6ce0;
        --accent-amber:   #c9913d;
        --accent-green:   #3ecf8e;
        --accent-red:     #f87171;
        --text-primary:   #e8eaf0;
        --text-secondary: #9ca3af;
        --text-muted:     #6b7280;
        --border:         #2a2f3a;
        --border-light:   #353b48;
        --success-bg:     rgba(45, 106, 79, 0.12);
        --success-border: rgba(82, 183, 136, 0.25);
        --success-text:   #52b788;
        --error-bg:       rgba(107, 39, 55, 0.12);
        --error-border:   rgba(248, 113, 113, 0.25);
        --error-text:     #f87171;
        --db-badge-bg:    rgba(62,207,142,0.12);
        --db-badge-border:rgba(62,207,142,0.30);
        --db-badge-text:  #3ecf8e;
    }

    /* ── Light mode overrides ─────────────────────────────────────── */
    @media (prefers-color-scheme: light) {
        :root {
            --bg-primary:     #f5f7fa;
            --bg-secondary:   #ffffff;
            --bg-card:        #f0f2f5;
            --text-primary:   #111827;
            --text-secondary: #374151;
            --text-muted:     #6b7280;
            --border:         #d1d5db;
            --border-light:   #e5e7eb;
            --success-bg:     rgba(16, 185, 129, 0.08);
            --success-border: rgba(16, 185, 129, 0.30);
            --success-text:   #059669;
            --error-bg:       rgba(239, 68, 68, 0.08);
            --error-border:   rgba(239, 68, 68, 0.30);
            --error-text:     #dc2626;
        }
    }

    /* ── Also handle Streamlit's explicit theme class ─────────────── */
    [data-theme="light"] {
        --bg-primary:     #f5f7fa !important;
        --bg-secondary:   #ffffff !important;
        --bg-card:        #f0f2f5 !important;
        --text-primary:   #111827 !important;
        --text-secondary: #374151 !important;
        --text-muted:     #4b5563 !important;
        --border:         #d1d5db !important;
        --border-light:   #e5e7eb !important;
        --success-text:   #059669 !important;
        --error-text:     #dc2626 !important;
        --accent-amber:   #b45309 !important;
    }

    /* ── Global ───────────────────────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        background-color: var(--bg-primary) !important;
        color: var(--text-primary) !important;
        -webkit-font-smoothing: antialiased;
    }

    /* ── Main container ───────────────────────────────────────────── */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 4rem;
        max-width: 1320px;
    }

    /* ── Sidebar ──────────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: var(--bg-secondary) !important;
        border-right: 1px solid var(--border) !important;
    }
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] span,
    section[data-testid="stSidebar"] div {
        color: var(--text-primary) !important;
    }
    section[data-testid="stSidebar"] hr {
        border: none !important;
        border-top: 1px solid var(--border) !important;
        margin: 0.75rem 0 !important;
    }

    /* ── Text inputs (sidebar credential fields) ─────────────────── */
    input[type="text"], input[type="password"], input[type="number"] {
        background: var(--bg-card) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border) !important;
        border-radius: 4px !important;
    }
    input[type="text"]:focus,
    input[type="password"]:focus,
    input[type="number"]:focus {
        border-color: var(--accent) !important;
        outline: none !important;
        box-shadow: none !important;
    }

    /* ── HR dividers ──────────────────────────────────────────────── */
    hr {
        border: none !important;
        border-top: 1px solid var(--border) !important;
        margin: 1.5rem 0 !important;
    }

    /* ── Buttons ──────────────────────────────────────────────────── */
    .stButton > button {
        background: var(--accent) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 6px !important;
        padding: 0.55rem 1.75rem !important;
        font-weight: 600 !important;
        font-size: 0.88rem !important;
        transition: background 150ms ease, transform 150ms ease !important;
        box-shadow: none !important;
    }
    .stButton > button:hover {
        background: var(--accent-dark) !important;
        transform: scale(1.01) !important;
    }
    .stButton > button:active { transform: scale(0.99) !important; }

    /* ── Textarea ─────────────────────────────────────────────────── */
    textarea {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.875rem !important;
        background: var(--bg-secondary) !important;
        border: 1px solid var(--border) !important;
        border-radius: 6px !important;
        color: var(--text-primary) !important;
    }
    textarea:focus { border-color: var(--accent) !important; outline: none !important; }

    /* ── Code blocks ──────────────────────────────────────────────── */
    .stCodeBlock pre, .stCodeBlock code {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.82rem !important;
        background: var(--bg-secondary) !important;
        border: 1px solid var(--border) !important;
        border-radius: 6px !important;
        color: var(--text-primary) !important;
    }

    /* ── Tabs ─────────────────────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        background: transparent !important;
        border-bottom: 1px solid var(--border) !important;
        gap: 0 !important; padding: 0 !important;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent !important;
        border: none !important;
        border-bottom: 2px solid transparent !important;
        border-radius: 0 !important;
        padding: 0.6rem 1.2rem !important;
        font-size: 0.875rem !important;
        font-weight: 500 !important;
        color: var(--text-muted) !important;
        transition: color 150ms ease, border-color 150ms ease !important;
        margin-bottom: -1px !important;
    }
    .stTabs [data-baseweb="tab"]:hover { color: var(--text-primary) !important; }
    .stTabs [aria-selected="true"] {
        color: var(--text-primary) !important;
        border-bottom: 2px solid var(--accent) !important;
    }
    .stTabs [data-baseweb="tab-highlight"],
    .stTabs [data-baseweb="tab-border"] { display: none !important; }

    /* ── st.metric — fix light mode value colour ──────────────────── */
    [data-testid="stMetric"] label {
        color: var(--text-muted) !important;
        font-size: 0.72rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.07em !important;
        text-transform: uppercase !important;
    }
    [data-testid="stMetricValue"] {
        color: var(--text-primary) !important;
        font-weight: 700 !important;
    }
    [data-testid="stMetricDelta"] svg { display: inline !important; }

    /* ── Spinner ──────────────────────────────────────────────────── */
    .stSpinner > div { color: var(--text-muted) !important; font-size: 0.875rem !important; }

    /* ── Expander ─────────────────────────────────────────────────── */
    .streamlit-expanderHeader {
        background: var(--bg-card) !important;
        border: 1px solid var(--border) !important;
        border-radius: 6px !important;
        font-size: 0.875rem !important;
        font-weight: 500 !important;
        color: var(--text-secondary) !important;
    }

    /* ── Data editor ──────────────────────────────────────────────── */
    [data-testid="stDataEditor"] {
        border: 1px solid var(--border) !important;
        border-radius: 6px !important;
        overflow: hidden;
    }

    /* ══════════════════════ Custom components ═════════════════════ */

    /* Page header */
    .page-header {
        padding: 2rem 0 1.5rem;
        border-bottom: 1px solid var(--border);
        margin-bottom: 2rem;
    }
    .page-header-title {
        font-size: 1.75rem; font-weight: 700;
        color: var(--text-primary);
        letter-spacing: -0.02em; margin: 0 0 0.4rem; line-height: 1.2;
    }
    .page-header-desc {
        font-size: 0.9rem; color: var(--text-muted);
        font-weight: 400; margin: 0; line-height: 1.6; max-width: 720px;
    }
    .page-header-tags { margin-top: 1rem; display: flex; gap: 0.45rem; flex-wrap: wrap; }

    /* Tags */
    .tag {
        display: inline-block; padding: 0.2rem 0.65rem; border-radius: 4px;
        font-size: 0.7rem; font-weight: 600; letter-spacing: 0.06em;
        text-transform: uppercase;
        background: var(--bg-card); color: var(--text-muted); border: 1px solid var(--border);
    }
    .tag-accent {
        background: rgba(74,124,247,0.1); color: var(--accent);
        border-color: rgba(74,124,247,0.25);
    }
    .tag-green {
        background: rgba(62,207,142,0.1); color: var(--accent-green);
        border-color: rgba(62,207,142,0.25);
    }
    .tag-red {
        background: rgba(248,113,113,0.1); color: var(--accent-red);
        border-color: rgba(248,113,113,0.25);
    }

    /* Section labels */
    .section-label {
        font-size: 0.68rem; font-weight: 700; letter-spacing: 0.1em;
        text-transform: uppercase; color: var(--text-muted); margin-bottom: 0.5rem;
    }

    /* Input hints */
    .input-hint {
        font-size: 0.8rem; color: var(--text-muted);
        font-weight: 400; padding-top: 0.5rem; line-height: 1.5;
    }

    /* Metric cards */
    .metric-card {
        padding: 1.1rem 1rem;
        background: var(--bg-secondary);
        border: 1px solid var(--border);
        border-radius: 6px;
    }
    .metric-card-value {
        font-size: 1.75rem; font-weight: 700;
        color: var(--text-primary); line-height: 1; margin-bottom: 0.4rem;
    }
    .metric-card-value.accent  { color: var(--accent); }
    .metric-card-value.amber   { color: var(--accent-amber); }
    .metric-card-value.success { color: var(--success-text); }
    .metric-card-value.green   { color: var(--accent-green); }
    .metric-card-value.sm      { font-size: 1rem; }
    .metric-card-label {
        font-size: 0.68rem; font-weight: 700; letter-spacing: 0.09em;
        text-transform: uppercase; color: var(--text-muted);
    }

    /* Tree output */
    .tree-container {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.82rem !important; line-height: 1.65 !important;
        background: #0a0c10 !important; color: #c9d1d9 !important;
        padding: 1.25rem 1.5rem !important;
        border: 1px solid var(--border) !important; border-radius: 6px !important;
        white-space: pre !important; overflow-x: auto !important;
        margin-bottom: 1rem !important;
    }

    /* SQL Unparser badge */
    .sql-unparser-header { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem; margin-top: 1.5rem; }
    .sql-unparser-badge {
        display: inline-block; padding: 0.15rem 0.55rem; border-radius: 4px;
        font-size: 0.65rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase;
        background: rgba(62,207,142,0.1); color: var(--accent-green);
        border: 1px solid rgba(62,207,142,0.25);
    }

    /* DB connection section */
    .db-section-title {
        font-size: 0.78rem; font-weight: 600; color: var(--text-primary);
        margin: 0.75rem 0 0.4rem;
    }
    .db-connected-badge {
        display: inline-flex; align-items: center; gap: 0.35rem;
        padding: 0.2rem 0.6rem; border-radius: 4px;
        background: var(--db-badge-bg, rgba(62,207,142,0.12));
        border: 1px solid var(--db-badge-border, rgba(62,207,142,0.3));
        color: var(--db-badge-text, #3ecf8e);
        font-size: 0.7rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase;
    }

    /* Live metrics tab */
    .metrics-compare-header {
        display: flex; align-items: center; gap: 0.6rem; margin-bottom: 1rem;
    }
    .metrics-badge {
        display: inline-block; padding: 0.18rem 0.6rem; border-radius: 4px;
        font-size: 0.65rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase;
        background: rgba(74,124,247,0.1); color: var(--accent);
        border: 1px solid rgba(74,124,247,0.25);
    }
    .metrics-col-header {
        font-size: 0.72rem; font-weight: 700; letter-spacing: 0.09em; text-transform: uppercase;
        padding: 0.5rem 0.75rem; border-radius: 4px; text-align: center; margin-bottom: 0.75rem;
    }
    .metrics-col-header.unopt { background: rgba(248,113,113,0.08); color: var(--accent-red); border: 1px solid rgba(248,113,113,0.2); }
    .metrics-col-header.opt   { background: rgba(62,207,142,0.08);  color: var(--accent-green); border: 1px solid rgba(62,207,142,0.2); }

    /* Catalog entries */
    .catalog-entry {
        background: var(--bg-primary); border: 1px solid var(--border);
        border-radius: 6px; padding: 0.6rem 0.85rem; margin-bottom: 0.45rem;
    }
    .catalog-entry .ce-name { font-size: 0.82rem; font-weight: 600; color: var(--text-primary); margin-bottom: 0.1rem; }
    .catalog-entry .ce-rows { font-size: 0.74rem; font-weight: 500; color: var(--accent); margin-bottom: 0.08rem; }
    .catalog-entry .ce-cols { font-size: 0.68rem; color: var(--text-muted); }

    /* Schema info box */
    .schema-info {
        background: rgba(74,124,247,0.06); border: 1px solid rgba(74,124,247,0.18);
        border-radius: 6px; padding: 0.85rem 1rem; margin-bottom: 1rem;
        font-size: 0.82rem; color: var(--text-secondary); line-height: 1.6;
    }

    /* Pipeline steps */
    .pipeline-step {
        display: flex; align-items: flex-start; gap: 0.6rem;
        padding: 0.4rem 0; border-bottom: 1px solid var(--border); font-size: 0.8rem;
    }
    .pipeline-step:last-child { border-bottom: none; }
    .pipeline-step .step-num {
        font-size: 0.64rem; font-weight: 700; color: var(--text-muted);
        min-width: 1.4rem; padding-top: 0.1rem; letter-spacing: 0.04em;
        font-family: 'JetBrains Mono', monospace;
    }
    .pipeline-step .step-body .step-title { font-weight: 600; color: var(--text-primary); line-height: 1.3; }
    .pipeline-step .step-body .step-desc  { color: var(--text-muted); font-size: 0.74rem; line-height: 1.4; margin-top: 0.06rem; }

    /* App name */
    .sidebar-app-name { font-size: 1rem; font-weight: 700; color: var(--text-primary); }
    .sidebar-app-version { font-size: 0.73rem; color: var(--text-muted); margin-top: 0.1rem; }

    /* Tab section typography */
    .tab-section-title {
        font-size: 0.95rem; font-weight: 600; color: var(--text-primary);
        margin-bottom: 0.3rem; margin-top: 1.25rem;
    }
    .tab-section-desc {
        font-size: 0.83rem; color: var(--text-muted); font-weight: 400;
        margin-bottom: 0.75rem; line-height: 1.6; max-width: 680px;
    }
    .tab-section-desc code {
        font-family: 'JetBrains Mono', monospace; font-size: 0.8rem;
        background: var(--bg-card); padding: 0.1em 0.4em; border-radius: 3px; color: var(--accent);
    }

    /* Compare label */
    .compare-label {
        font-size: 0.68rem; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.09em; color: var(--text-muted);
        margin-bottom: 0.5rem; padding-bottom: 0.35rem; border-bottom: 1px solid var(--border);
    }

    /* Error state for cost query card */
    .cost-error { color: var(--text-muted); font-style: italic; font-size: 0.8rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# Session-state singletons
# ─────────────────────────────────────────────────────────────────────────────

if "catalog" not in st.session_state:
    st.session_state["catalog"] = Catalog()

if "db_manager" not in st.session_state:
    st.session_state["db_manager"] = None   # DatabaseManager | None

catalog: Catalog                        = st.session_state["catalog"]
db_manager: Optional[DatabaseManager]  = st.session_state["db_manager"]

@st.cache_resource
def get_parser() -> QueryParser:
    return QueryParser()

@st.cache_resource
def get_visualizer() -> PlanVisualizer:
    return PlanVisualizer()

parser = get_parser()
vis    = get_visualizer()

# ─────────────────────────────────────────────────────────────────────────────
# Default SQL — Olist e-commerce database
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_SQL = """\
SELECT
    olist_orders_dataset.order_id,
    olist_customers_dataset.customer_city,
    olist_order_payments_dataset.payment_value
FROM olist_orders_dataset
JOIN olist_customers_dataset
    ON olist_orders_dataset.customer_id = olist_customers_dataset.customer_id
JOIN olist_order_payments_dataset
    ON olist_orders_dataset.order_id = olist_order_payments_dataset.order_id
WHERE olist_orders_dataset.order_status = 'delivered'"""

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        """
        <div class="sidebar-app-name">Query Optimizer</div>
        <div class="sidebar-app-version">SQL Optimizer UI</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # ── Live DB Connection ─────────────────────────────────────────────
    st.markdown("<div class='section-label'>Live DB Connection</div>", unsafe_allow_html=True)

    if db_manager and db_manager.is_connected:
        st.markdown(
            f"<div class='db-connected-badge'>Connected — {db_manager.database}</div>",
            unsafe_allow_html=True,
        )
        if st.button("Disconnect", key="btn_disconnect", use_container_width=True):
            db_manager.disconnect()
            st.session_state["db_manager"] = None
            st.rerun()
    else:
        # Checking if the app is running locally
        host_header = st.context.headers.get("host", "")
        is_local = host_header.startswith("localhost") or host_header.startswith("127.0.0.1")

        # Credential inputs — pre-filled from .env defaults
        import os
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

        if is_local:
            def_host = os.getenv("DB_HOST", "localhost")
            def_port = int(os.getenv("DB_PORT", "3306"))
            def_user = os.getenv("DB_USER", "root")
            def_pass = os.getenv("DB_PASSWORD", "")
            def_name = os.getenv("DB_NAME", "")
        else:
            # For network users, keep it blank/generic
            def_host = "localhost"
            def_port = 3306
            def_user = ""
            def_pass = ""
            def_name = ""

        # 3. Use the determined default values in the UI
        db_host = st.text_input("Host",     value=def_host, key="db_host")
        db_port = st.number_input("Port",   value=def_port, min_value=1, max_value=65535, key="db_port")
        db_user = st.text_input("User",     value=def_user, key="db_user")
        db_pass = st.text_input("Password", value=def_pass, key="db_pass", type="password")
        db_name = st.text_input("Database", value=def_name, key="db_name")

        if st.button("Connect & Sync Catalog", key="btn_connect", use_container_width=True):
            with st.spinner("Connecting to MySQL…"):
                try:
                    mgr = DatabaseManager(
                        host=db_host, port=int(db_port),
                        user=db_user, password=db_pass,
                        database=db_name,
                    )
                    mgr.connect()
                    updated_catalog, n_tables = mgr.sync_schema_to_catalog(catalog)
                    st.session_state["catalog"]    = updated_catalog
                    st.session_state["db_manager"] = mgr
                    st.success(f"Connected! {n_tables} table(s) synced into catalog.")
                    st.rerun()
                except RuntimeError as e:
                    st.error(str(e))
                except Exception as e:
                    st.error(f"Unexpected error: {e}")

    st.markdown("---")

    # ── Catalog viewer ───────────────────────────────────────────────────
    st.markdown("<div class='section-label'>Database Catalog</div>", unsafe_allow_html=True)
    st.markdown(
        "<p style='color:var(--text-muted);font-size:0.78rem;margin-bottom:0.6rem;line-height:1.5'>"
        "Live statistics used by the CBO. Edit in the Schema tab.</p>",
        unsafe_allow_html=True,
    )

    all_stats = catalog.get_all_stats()
    # Show at most 8 entries to keep sidebar compact; full list in Schema tab
    shown = list(all_stats.items())[:8]
    for table_name, info in shown:
        cols_str = ", ".join(info["columns"][:5])
        if len(info["columns"]) > 5:
            cols_str += f" +{len(info['columns'])-5} more"
        st.markdown(
            f"""
            <div class="catalog-entry">
                <div class="ce-name">{table_name}</div>
                <div class="ce-rows">{info['row_count']:,} rows</div>
                <div class="ce-cols">{cols_str}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    if len(all_stats) > 8:
        st.markdown(
            f"<p style='font-size:0.72rem;color:var(--text-muted);margin:0.3rem 0 0.6rem'>"
            f"+ {len(all_stats)-8} more tables — see Schema tab</p>",
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ── Pipeline stages ──────────────────────────────────────────────────
    st.markdown("<div class='section-label'>Pipeline Stages</div>", unsafe_allow_html=True)
    pipeline_steps = [
        ("01", "SQL Parsing",               "sqlglot AST-based parser"),
        ("02", "Logical Plan",              "Relational algebra tree"),
        ("03", "RBO — Predicate Pushdown",  "AND-split + OR-block safety"),
        ("04", "RBO — Projection Pushdown", "Narrow columns above scans"),
        ("05", "CBO — Join Reordering",     "Cheapest inner-join ordering"),
        ("06", "Physical Plan",             "All RBO nodes preserved"),
        ("07", "SQL Unparser",              "Tree → valid SQL string"),
        ("08", "Live Benchmarking",         "Unopt vs Opt on real MySQL"),
    ]
    steps_html = ""
    for num, title, desc in pipeline_steps:
        steps_html += f"""
        <div class="pipeline-step">
            <div class="step-num">{num}</div>
            <div class="step-body">
                <div class="step-title">{title}</div>
                <div class="step-desc">{desc}</div>
            </div>
        </div>"""
    st.markdown(steps_html, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AREA — Header
# ─────────────────────────────────────────────────────────────────────────────

db_manager = st.session_state.get("db_manager")   # refresh after sidebar rerun
catalog    = st.session_state["catalog"]

live_db = db_manager is not None and db_manager.is_connected

st.markdown(
    f"""
    <div class="page-header">
        <div class="page-header-title">Query Optimizer</div>
        <div class="page-header-desc">
            Parses SQL, builds relational-algebra plans, applies RBO + CBO optimization,
            unparsed back to SQL, {"and benchmarks both against a live MySQL instance." if live_db else
            "and generates equivalent SQL. Connect a live DB for real execution benchmarks."}
        </div>
        <div class="page-header-tags">
            <span class="tag tag-accent">SQL Parser</span>
            <span class="tag">Predicate Pushdown</span>
            <span class="tag">OR-block Safety</span>
            <span class="tag">Outer Join Support</span>
            <span class="tag">Join Reordering</span>
            <span class="tag">Cost Model</span>
            <span class="tag tag-green">SQL Unparser</span>
            <span class="tag tag-green">Dynamic Schema</span>
            {"<span class='tag tag-green'>Live MySQL</span>" if live_db else
             "<span class='tag tag-red'>Offline Mode</span>"}
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ── SQL input ────────────────────────────────────────────────────────────────

st.markdown("<div class='section-label'>SQL Query</div>", unsafe_allow_html=True)
st.markdown(
    "<p style='color:var(--text-muted);font-size:0.82rem;margin-bottom:0.5rem;line-height:1.5'>"
    "Enter a SELECT with JOINs and WHERE clauses. Supports INNER/LEFT/RIGHT JOINs, "
    "AND/OR predicates, CTEs, and GROUP BY.</p>",
    unsafe_allow_html=True,
)

sql_input = st.text_area(
    label="SQL Query",
    value=DEFAULT_SQL,
    height=145,
    label_visibility="collapsed",
    key="sql_textarea",
)

col_btn, col_hint = st.columns([1, 5])
with col_btn:
    run_clicked = st.button("Optimize Query", use_container_width=True, key="btn_optimize")
with col_hint:
    hint = "Connected to MySQL — will benchmark both plans live." if live_db else \
           "Connect a MySQL database in the sidebar to enable live benchmarking."
    st.markdown(f"<p class='input-hint'>{hint}</p>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE EXECUTION
# ─────────────────────────────────────────────────────────────────────────────

if run_clicked or sql_input:
    st.markdown("---")

    try:
        # ── Step 1: Parse ──────────────────────────────────────────────────
        with st.spinner("Parsing SQL…"):
            logical_tree = parser.parse(sql_input)

        # ── Step 2: RBO — Predicate Pushdown ──────────────────────────────
        with st.spinner("Predicate Pushdown (RBO pass 1)…"):
            rbo = RuleBasedOptimizer(catalog=catalog)
            tree_after_predpush = rbo._apply_predicate_pushdown(copy.deepcopy(logical_tree))
            predicate_rules = list(rbo._predicate_rules)

        # ── Step 3: RBO — Projection Pushdown ─────────────────────────────
        with st.spinner("Projection Pushdown (RBO pass 2)…"):
            rbo._projection_rules = []
            rbo_tree = rbo._apply_projection_pushdown(copy.deepcopy(tree_after_predpush))
            projection_rules = list(rbo._projection_rules)

        # ── Step 4: CBO — Join Reordering ─────────────────────────────────
        with st.spinner("Cost-Based Join Reordering…"):
            cbo        = CostBasedOptimizer(catalog=catalog)
            cbo_result = cbo.optimize(copy.deepcopy(rbo_tree))

        # ── Step 5: SQL Unparser ───────────────────────────────────────────
        with st.spinner("Unparsing optimized tree to SQL…"):
            try:
                optimized_sql = QueryExecutor.sanitize_for_mysql(
                    cbo_result.plan.to_sql()
                )
            except Exception as unparse_err:
                optimized_sql = f"-- SQL Unparser error: {unparse_err}"

        # ── Step 6: Live Benchmarking (only when DB connected) ─────────────
        bench_unopt: Optional[dict] = None
        bench_opt:   Optional[dict] = None

        if live_db:
            with st.spinner("Benchmarking unoptimized SQL on MySQL…"):
                exe = QueryExecutor(db_manager, row_limit=10_000)
                bench_unopt = exe.benchmark_query(sql_input)
            if not optimized_sql.startswith("--"):
                with st.spinner("Benchmarking optimized SQL on MySQL…"):
                    bench_opt = exe.benchmark_query(optimized_sql)

        # ── Render ─────────────────────────────────────────────────────────
        logical_str   = vis.render(logical_tree)
        pred_push_str = vis.render(tree_after_predpush)
        proj_push_str = vis.render(rbo_tree)
        physical_str  = vis.render(cbo_result.plan)

        st.success("Pipeline complete. Explore the tabs below.")

        # ── Top metrics row ────────────────────────────────────────────────
        m1, m2, m3, m4, m5 = st.columns(5)
        with m1:
            tables = logical_tree.source_tables
            st.markdown(
                f'<div class="metric-card"><div class="metric-card-value accent">{len(tables)}</div>'
                f'<div class="metric-card-label">Tables Joined</div></div>',
                unsafe_allow_html=True,
            )
        with m2:
            st.markdown(
                f'<div class="metric-card"><div class="metric-card-value">{len(predicate_rules)}</div>'
                f'<div class="metric-card-label">Predicate Rules</div></div>',
                unsafe_allow_html=True,
            )
        with m3:
            st.markdown(
                f'<div class="metric-card"><div class="metric-card-value">{len(projection_rules)}</div>'
                f'<div class="metric-card-label">Projection Rules</div></div>',
                unsafe_allow_html=True,
            )
        with m4:
            cost_fmt = f"{cbo_result.cost:,}" if cbo_result.cost > 0 else "N/A"
            st.markdown(
                f'<div class="metric-card"><div class="metric-card-value success">{cost_fmt}</div>'
                f'<div class="metric-card-label">CBO Est. Cost</div></div>',
                unsafe_allow_html=True,
            )
        with m5:
            if getattr(cbo_result, "reorder_disabled", False):
                ord_str = "Preserved"
                ord_cls = "metric-card-value amber sm"
            elif cbo_result.ordering:
                ord_str = " → ".join(cbo_result.ordering)
                ord_cls = "metric-card-value amber sm"
            else:
                ord_str = "—"
                ord_cls = "metric-card-value sm"
            st.markdown(
                f'<div class="metric-card"><div class="{ord_cls}">{ord_str}</div>'
                f'<div class="metric-card-label">Join Order</div></div>',
                unsafe_allow_html=True,
            )

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Tabs ───────────────────────────────────────────────────────────
        tab_labels = [
            "Logical Plan",
            "After RBO",
            "Physical Plan",
            "Schema Editor",
            "Live Metrics",
            "Debug",
        ]
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_labels)

        # ──────────────────────────────────────────────────────────────────
        # Tab 1: Logical Plan
        # ──────────────────────────────────────────────────────────────────
        with tab1:
            st.markdown(
                "<div class='tab-section-title'>Unoptimized Logical Plan</div>"
                "<div class='tab-section-desc'></div>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{logical_str}</div>', unsafe_allow_html=True)

        # ──────────────────────────────────────────────────────────────────
        # Tab 2: After RBO
        # ──────────────────────────────────────────────────────────────────
        with tab2:
            st.markdown(
                "<div class='tab-section-title'>After Predicate Pushdown</div>"
                "<div class='tab-section-desc'></div>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{pred_push_str}</div>', unsafe_allow_html=True)

            if predicate_rules:
                with st.expander(f"Predicate Rules Fired ({len(predicate_rules)})"):
                    for i, r in enumerate(predicate_rules, 1):
                        st.markdown(
                            f"<div style='font-size:0.82rem;padding:0.3rem 0;"
                            f"border-bottom:1px solid var(--border);color:var(--text-secondary)'>"
                            f"<strong style='color:var(--accent);margin-right:0.5rem'>{i}.</strong>{r}</div>",
                            unsafe_allow_html=True,
                        )

            st.markdown("---")
            st.markdown(
                "<div class='tab-section-title'>After Projection Pushdown</div>"
                "<div class='tab-section-desc'></div>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{proj_push_str}</div>', unsafe_allow_html=True)

            if projection_rules:
                with st.expander(f"Projection Rules Fired ({len(projection_rules)})"):
                    for i, r in enumerate(projection_rules, 1):
                        st.markdown(
                            f"<div style='font-size:0.82rem;padding:0.3rem 0;"
                            f"border-bottom:1px solid var(--border);color:var(--text-secondary)'>"
                            f"<strong style='color:var(--accent);margin-right:0.5rem'>{i}.</strong>{r}</div>",
                            unsafe_allow_html=True,
                        )

        # ──────────────────────────────────────────────────────────────────
        # Tab 3: Physical Plan + SQL Unparser
        # ──────────────────────────────────────────────────────────────────
        with tab3:
            st.markdown(
                "<div class='tab-section-title'>Final Optimized Physical Plan</div>"
                "<div class='tab-section-desc'></div>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{physical_str}</div>', unsafe_allow_html=True)

            with st.expander("CBO Cost Analysis"):
                st.code(cbo_result.cost_report, language=None)

            st.markdown("---")

            # SQL Unparser
            st.markdown(
                "<div class='sql-unparser-header'>"
                "<div class='tab-section-title' style='margin:0'>Equivalent SQL</div>"
                "<span class='sql-unparser-badge'>SQL Unparser</span></div>"
                "<div class='tab-section-desc'></div>",
                unsafe_allow_html=True,
            )
            st.code(optimized_sql, language="sql")

            st.markdown("---")
            st.markdown(
                "<div class='tab-section-title'>Side-by-Side Comparison</div>"
                "<div class='tab-section-desc'></div>",
                unsafe_allow_html=True,
            )
            col_l, col_r = st.columns(2)
            with col_l:
                st.markdown("<div class='compare-label'>Logical Plan — original</div>", unsafe_allow_html=True)
                st.markdown(f'<div class="tree-container">{logical_str}</div>', unsafe_allow_html=True)
            with col_r:
                st.markdown("<div class='compare-label'>Physical Plan — optimized</div>", unsafe_allow_html=True)
                st.markdown(f'<div class="tree-container">{physical_str}</div>', unsafe_allow_html=True)

        # ──────────────────────────────────────────────────────────────────
        # Tab 4: Schema Editor
        # ──────────────────────────────────────────────────────────────────
        with tab4:
            st.markdown(
                "<div class='tab-section-title'>Dynamic Schema Editor</div>"
                "<div class='tab-section-desc'></div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                "<div class='schema-info'>"
                "<strong>How to use:</strong> Edit row counts or column lists in-place. "
                "Add rows via the <strong>+</strong> button; delete via row menu. "
                "Click <strong>Apply Schema Changes</strong> when done.<br><br>"
                "The <strong>columns</strong> field is comma-separated "
                "(e.g. <code>id, name, city_id</code>). Row counts must be ≥ 1."
                "</div>",
                unsafe_allow_html=True,
            )

            catalog_df = catalog.to_dataframe()
            edited_df = st.data_editor(
                catalog_df,
                num_rows="dynamic",
                use_container_width=True,
                column_config={
                    "table":     st.column_config.TextColumn("Table Name", required=True),
                    "row_count": st.column_config.NumberColumn("Row Count", min_value=1, step=1, format="%d", required=True),
                    "columns":   st.column_config.TextColumn("Columns (comma-separated)"),
                },
                key="schema_editor",
            )

            apply_col, reset_col, sync_col, _ = st.columns([1.2, 1.2, 1.4, 2.2])
            with apply_col:
                if st.button("Apply Changes", use_container_width=True, key="apply_schema"):
                    catalog.sync_from_dataframe(edited_df)
                    st.session_state["catalog"] = catalog
                    st.success(f"Schema updated — {len(catalog.get_all_stats())} table(s).")
                    st.rerun()
            with reset_col:
                if st.button("Reset Defaults", use_container_width=True, key="reset_schema"):
                    st.session_state["catalog"] = Catalog()
                    st.success("Catalog reset to Olist defaults.")
                    st.rerun()
            with sync_col:
                if live_db:
                    if st.button("Re-Sync from DB", use_container_width=True, key="resync_schema"):
                        with st.spinner("Syncing schema…"):
                            try:
                                updated, n = db_manager.sync_schema_to_catalog(catalog)
                                st.session_state["catalog"] = updated
                                st.success(f"Synced {n} tables from {db_manager.database}.")
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
                else:
                    st.markdown(
                        "<p style='font-size:0.78rem;color:var(--text-muted);padding-top:0.5rem'>"
                        "Connect a DB to enable sync.</p>",
                        unsafe_allow_html=True,
                    )

            st.markdown("---")
            st.markdown("<div class='section-label' style='margin-bottom:0.4rem'>Current Live Catalog</div>", unsafe_allow_html=True)
            st.json(catalog.get_all_stats())

        # ──────────────────────────────────────────────────────────────────
        # Tab 5: Live Execution Metrics
        # ──────────────────────────────────────────────────────────────────
        with tab5:
            if not live_db:
                st.markdown(
                    "<div style='margin-top:1.5rem;padding:1.5rem;background:var(--bg-secondary);"
                    "border:1px solid var(--border);border-radius:8px;text-align:center'>"
                    "<div style='font-size:1.5rem;margin-bottom:0.5rem'></div>"
                    "<div style='font-weight:600;color:var(--text-primary);margin-bottom:0.3rem'>"
                    "No Live Database Connected</div>"
                    "<div style='font-size:0.85rem;color:var(--text-muted)'>"
                    "Enter MySQL credentials in the sidebar and click "
                    "<strong>Connect & Sync Catalog</strong> to enable query benchmarking.</div>"
                    "</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    "<div class='metrics-compare-header'>"
                    "<div class='tab-section-title' style='margin:0'>Live Execution Metrics</div>"
                    "<span class='metrics-badge'>MySQL Benchmarks</span></div>"
                    "<div class='tab-section-desc'></div>"
                    "<strong>Rows Returned</strong> should match — proving semantic correctness. "
                    "<strong>Time</strong> and <strong>MySQL Cost</strong> should be lower for "
                    "the optimized plan (delta shown in green = improvement)."
                    "</div>",
                    unsafe_allow_html=True,
                )

                # Error display
                if bench_unopt and bench_unopt.get("error"):
                    st.error(f"Unoptimized query error: {bench_unopt['error']}")
                if bench_opt and bench_opt.get("error"):
                    st.error(f"Optimized query error: {bench_opt['error']}")

                # Column headers
                _, hc1, hc2 = st.columns([0.8, 2, 2])
                with hc1:
                    st.markdown("<div class='metrics-col-header unopt'>Unoptimized SQL</div>", unsafe_allow_html=True)
                with hc2:
                    st.markdown("<div class='metrics-col-header opt'>Optimized SQL</div>", unsafe_allow_html=True)

                def safe_get(d, key, default=0):
                    return d.get(key, default) if d else default

                t_unopt  = safe_get(bench_unopt, "execution_time_ms")
                t_opt    = safe_get(bench_opt,   "execution_time_ms")
                r_unopt  = safe_get(bench_unopt, "rows_returned")
                r_opt    = safe_get(bench_opt,   "rows_returned")
                c_unopt  = safe_get(bench_unopt, "mysql_cost")
                c_opt    = safe_get(bench_opt,   "mysql_cost")

                # ── Execution Time ────────────────────────────────────────
                row_label, mc1, mc2 = st.columns([0.8, 2, 2])
                with row_label:
                    st.markdown(
                        "<div style='font-size:0.75rem;font-weight:600;color:var(--text-muted);"
                        "text-transform:uppercase;letter-spacing:0.07em;padding-top:1.2rem'>"
                        "⏱ Exec Time</div>",
                        unsafe_allow_html=True,
                    )
                with mc1:
                    st.metric(
                        label="Unoptimized — Execution Time",
                        value=f"{t_unopt:.1f} ms",
                        label_visibility="collapsed",
                    )
                with mc2:
                    delta_t = t_opt - t_unopt
                    delta_t_str = f"{delta_t:+.1f} ms"
                    st.metric(
                        label="Optimized — Execution Time",
                        value=f"{t_opt:.1f} ms",
                        delta=delta_t_str,
                        delta_color="inverse",   # negative delta = improvement = green
                        label_visibility="collapsed",
                    )

                # ── Rows Returned ─────────────────────────────────────────
                row_label2, mc3, mc4 = st.columns([0.8, 2, 2])
                with row_label2:
                    st.markdown(
                        "<div style='font-size:0.75rem;font-weight:600;color:var(--text-muted);"
                        "text-transform:uppercase;letter-spacing:0.07em;padding-top:1.2rem'>"
                        "Rows</div>",
                        unsafe_allow_html=True,
                    )
                with mc3:
                    st.metric(
                        label="Unoptimized — Rows",
                        value=f"{r_unopt:,}",
                        label_visibility="collapsed",
                    )
                with mc4:
                    delta_r = r_opt - r_unopt
                    st.metric(
                        label="Optimized — Rows",
                        value=f"{r_opt:,}",
                        delta=f"{delta_r:+,}" if delta_r != 0 else "Match",
                        delta_color="off" if delta_r == 0 else "normal",
                        label_visibility="collapsed",
                    )

                # ── MySQL Query Cost ──────────────────────────────────────
                row_label3, mc5, mc6 = st.columns([0.8, 2, 2])
                with row_label3:
                    st.markdown(
                        "<div style='font-size:0.75rem;font-weight:600;color:var(--text-muted);"
                        "text-transform:uppercase;letter-spacing:0.07em;padding-top:1.2rem'>"
                        "MySQL Cost</div>",
                        unsafe_allow_html=True,
                    )
                with mc5:
                    st.metric(
                        label="Unoptimized — MySQL Cost",
                        value=f"{c_unopt:,.2f}" if c_unopt else "N/A",
                        label_visibility="collapsed",
                    )
                with mc6:
                    delta_c = c_opt - c_unopt if c_opt and c_unopt else 0
                    st.metric(
                        label="Optimized — MySQL Cost",
                        value=f"{c_opt:,.2f}" if c_opt else "N/A",
                        delta=f"{delta_c:+,.2f}" if delta_c else None,
                        delta_color="inverse",
                        label_visibility="collapsed",
                    )

                st.markdown("---")

                # Raw SQL side-by-side
                st.markdown(
                    "<div class='tab-section-title'>Query SQL Comparison</div>"
                    "<div class='tab-section-desc'></div>",
                    unsafe_allow_html=True,
                )
                csql1, csql2 = st.columns(2)
                with csql1:
                    st.markdown("<div class='compare-label'>Unoptimized (original input)</div>", unsafe_allow_html=True)
                    st.code(sql_input.strip(), language="sql")
                with csql2:
                    st.markdown("<div class='compare-label'>Optimized (SQL Unparser output)</div>", unsafe_allow_html=True)
                    st.code(optimized_sql, language="sql")

        # ──────────────────────────────────────────────────────────────────
        # Tab 6: Debug
        # ──────────────────────────────────────────────────────────────────
        with tab6:
            st.markdown(
                "<div class='tab-section-title'>Internal Debug Information</div>"
                "<div class='tab-section-desc'></div>",
                unsafe_allow_html=True,
            )
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                st.markdown("<div class='section-label' style='margin-bottom:0.4rem'>Logical Tree (repr)</div>", unsafe_allow_html=True)
                st.code(repr(logical_tree), language="python")
                st.markdown("<div class='section-label' style='margin-top:1rem;margin-bottom:0.4rem'>Physical Plan (repr)</div>", unsafe_allow_html=True)
                st.code(repr(cbo_result.plan), language="python")
            with col_d2:
                st.markdown("<div class='section-label' style='margin-bottom:0.4rem'>CBO Ordering</div>", unsafe_allow_html=True)
                st.code(str(cbo_result.ordering), language="python")
                if cbo_result.residual_filters:
                    st.markdown("<div class='section-label' style='margin-top:1rem;margin-bottom:0.4rem'>Residual Filters</div>", unsafe_allow_html=True)
                    st.code("\n".join(repr(f) for f in cbo_result.residual_filters), language="python")
                if bench_unopt or bench_opt:
                    st.markdown("<div class='section-label' style='margin-top:1rem;margin-bottom:0.4rem'>Raw Benchmark Results</div>", unsafe_allow_html=True)
                    st.json({"unoptimized": bench_unopt, "optimized": bench_opt})
                st.markdown("<div class='section-label' style='margin-top:1rem;margin-bottom:0.4rem'>Catalog Stats</div>", unsafe_allow_html=True)
                st.json(catalog.get_all_stats())

    except Exception as exc:
        st.error(f"Optimizer Error: {exc}")
        with st.expander("Full Traceback"):
            st.code(traceback.format_exc(), language="python")

else:
    st.markdown(
        "<div style='margin-top:2rem;padding:1.25rem 1.5rem;"
        "background:var(--bg-secondary);border:1px solid var(--border);"
        "border-radius:6px;color:var(--text-muted);font-size:0.875rem;line-height:1.7'>"
        "Enter an SQL query above and click <strong style='color:var(--text-primary)'>"
        "Optimize Query</strong> to start. "
        "Connect a MySQL database in the sidebar for live execution benchmarks."
        "</div>",
        unsafe_allow_html=True,
    )