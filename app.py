"""
app.py
------
Mini Query Optimizer — Interactive Streamlit Frontend

Provides a polished, dark-themed web interface that walks the user through
every stage of the query optimization pipeline:

    SQL Input → Parse → Logical Plan → RBO → CBO → Physical Plan

Run with:
    streamlit run app.py
"""

from __future__ import annotations

import copy
import traceback
from typing import Optional

import streamlit as st

from engine.catalog import Catalog
from engine.cbo import CostBasedOptimizer
from engine.nodes import PlanNode
from engine.parser import QueryParser
from engine.rbo import RuleBasedOptimizer
from engine.visualizer import PlanVisualizer

# ─────────────────────────────────────────────────────────────────────────────
# Page configuration (must be the first Streamlit call)
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Mini Query Optimizer",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Custom CSS — dark, premium look
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
    /* ── Google Fonts ────────────────────────────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    /* ── Root variables ──────────────────────────────────────────── */
    :root {
        --bg-primary:   #0d1117;
        --bg-secondary: #161b22;
        --bg-card:      #1c2130;
        --accent-blue:  #58a6ff;
        --accent-green: #3fb950;
        --accent-orange:#f0883e;
        --accent-purple:#bc8cff;
        --accent-red:   #f85149;
        --text-primary: #ffffff;
        --text-muted:   #a1a1aa; /* Brightened for better contrast */
        --border:       #30363d;
        --glow-blue:    0 0 20px rgba(88,166,255,0.15);
        --glow-green:   0 0 20px rgba(63,185,80,0.15);
    }

    /* ── Global Override ──────────────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif !important;
        background-color: var(--bg-primary) !important;
        color: var(--text-primary) !important;
    }

    /* ── Main container ──────────────────────────────────────────── */
    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 3rem;
        max-width: 1300px;
    }

    /* ── Sidebar ─────────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: var(--bg-secondary) !important;
        border-right: 1px solid var(--border) !important;
    }

    /* ── Hero header ─────────────────────────────────────────────── */
    .hero-header {
        background: linear-gradient(135deg, #1a237e 0%, #0d2137 40%, #0d1117 100%);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 2rem 2.5rem;
        margin-bottom: 1.5rem;
        position: relative;
        overflow: hidden;
    }
    .hero-header::before {
        content: '';
        position: absolute;
        top: -50%;
        left: -10%;
        width: 50%;
        height: 200%;
        background: radial-gradient(ellipse, rgba(88,166,255,0.08) 0%, transparent 70%);
        pointer-events: none;
    }
    .hero-title {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(90deg, #58a6ff, #ffffff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin: 0 0 0.4rem 0;
    }
    .hero-sub {
        font-size: 1rem;
        color: var(--text-muted);
        margin: 0;
        font-weight: 400;
    }
    .hero-badges {
        margin-top: 1rem;
        display: flex;
        gap: 0.6rem;
        flex-wrap: wrap;
    }
    .badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }
    .badge-blue   { background: rgba(88,166,255,0.15); color: #58a6ff; border: 1px solid rgba(88,166,255,0.3); }
    .badge-green  { background: rgba(63,185,80,0.15);  color: #3fb950; border: 1px solid rgba(63,185,80,0.3); }
    .badge-purple { background: rgba(188,140,255,0.15);color: #bc8cff; border: 1px solid rgba(188,140,255,0.3); }
    .badge-orange { background: rgba(240,136,62,0.15); color: #f0883e; border: 1px solid rgba(240,136,62,0.3); }

    /* ── Stage cards ─────────────────────────────────────────────── */
    .stage-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 1.25rem 1.5rem;
        margin-bottom: 1rem;
        transition: border-color 0.2s;
    }
    .stage-card:hover { border-color: #58a6ff55; }

    .stage-title {
        font-size: 0.85rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #ffffff;
        margin-bottom: 0.5rem;
    }

    /* ── Metric boxes ────────────────────────────────────────────── */
    .metric-box {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }
    .metric-value {
        font-size: 1.9rem;
        font-weight: 700;
        color: var(--accent-blue);
    }
    .metric-label {
        font-size: 0.75rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-top: 0.2rem;
    }

    /* ── Tree Output Formatting ──────────────────────────────────── */
    .tree-container {
        font-family: 'JetBrains Mono', 'Fira Code', monospace !important;
        font-size: 14px !important;
        line-height: 1.5 !important;
        background-color: #000000 !important;
        color: #FFFFFF !important;
        padding: 1rem !important;
        border: 1px solid #30363d !important;
        border-radius: 8px !important;
        white-space: pre !important;    /* This strictly preserves the ASCII formatting */
        overflow-x: auto !important;
        margin-bottom: 1rem !important;
    }

    /* ── Standard Code blocks override ───────────────────────────── */
    .stCodeBlock pre {
        font-family: 'JetBrains Mono', 'Fira Code', monospace !important;
        font-size: 0.85rem !important;
        background: #0d1117 !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
    }

    /* ── Buttons ─────────────────────────────────────────────────── */
    .stButton > button {
        background: linear-gradient(135deg, #1f6feb, #388bfd) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.6rem 2rem !important;
        font-weight: 600 !important;
        font-size: 0.95rem !important;
        letter-spacing: 0.02em !important;
        transition: opacity 0.2s, box-shadow 0.2s !important;
        box-shadow: var(--glow-blue) !important;
    }
    .stButton > button:hover {
        opacity: 0.9 !important;
        box-shadow: 0 0 30px rgba(88,166,255,0.35) !important;
    }

    /* ── Text area ──────────────────────────────────────────────── */
    textarea {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.95rem !important;
        background: #000000 !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
        color: #ffffff !important;
    }

    /* ── Divider ─────────────────────────────────────────────────── */
    hr { border-color: var(--border) !important; }

    /* ── Sidebar table entry ─────────────────────────────────────── */
    .catalog-table {
        background: rgba(88,166,255,0.05);
        border: 1px solid rgba(88,166,255,0.15);
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin-bottom: 0.75rem;
        font-size: 0.85rem;
    }
    .catalog-table .tname {
        font-weight: 600;
        color: var(--accent-blue);
        font-size: 0.9rem;
    }
    .catalog-table .trows {
        color: var(--accent-green);
        font-weight: 600;
    }
    .catalog-table .tcols {
        color: var(--text-muted);
        font-size: 0.78rem;
        margin-top: 0.2rem;
    }

    /* ── Rule tag ────────────────────────────────────────────────── */
    .rule-tag {
        background: rgba(63,185,80,0.12);
        border: 1px solid rgba(63,185,80,0.25);
        border-radius: 6px;
        padding: 0.5rem 0.9rem;
        margin-bottom: 0.5rem;
        font-size: 0.83rem;
        color: #3fb950;
    }
    .no-rule-tag {
        background: rgba(125,133,144,0.1);
        border: 1px solid rgba(125,133,144,0.2);
        border-radius: 6px;
        padding: 0.5rem 0.9rem;
        color: var(--text-muted);
        font-size: 0.83rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# Singleton helpers (cached across re-renders)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_catalog() -> Catalog:
    return Catalog()

@st.cache_resource
def get_parser() -> QueryParser:
    return QueryParser()

@st.cache_resource
def get_visualizer() -> PlanVisualizer:
    return PlanVisualizer()


catalog   = get_catalog()
parser    = get_parser()
vis       = get_visualizer()

# ─────────────────────────────────────────────────────────────────────────────
# Default SQL query
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_SQL = """\
SELECT users.name, countries.country_name
FROM users
JOIN cities ON users.city_id = cities.id
JOIN countries ON cities.country_id = countries.id
WHERE users.id > 500"""

# ─────────────────────────────────────────────────────────────────────────────
# ── SIDEBAR ──────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚡ Query Optimizer")
    st.markdown(
        "<p style='color:#a1a1aa;font-size:0.85rem;'>SQL Engine Simulator · v1.0</p>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    st.markdown("### 📊 Database Catalog")
    st.markdown(
        "<p style='color:#a1a1aa;font-size:0.85rem;'>Mock statistics used by the Cost‑Based Optimizer to estimate join costs.</p>",
        unsafe_allow_html=True,
    )

    all_stats = catalog.get_all_stats()
    row_icon  = {"users": "👤", "cities": "🏙️", "countries": "🌍"}

    for table_name, info in all_stats.items():
        icon = row_icon.get(table_name, "📋")
        cols_str = ", ".join(info["columns"])
        st.markdown(
            f"""
            <div class="catalog-table">
                <div class="tname">{icon} {table_name}</div>
                <div class="trows">{info['row_count']:,} rows</div>
                <div class="tcols">Columns: {cols_str}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("---")
    st.markdown("### 🔧 Pipeline Stages")
    stages_html = """
    <div style='font-size:0.85rem;color:#a1a1aa;line-height:1.9'>
    1️⃣ &nbsp;<b style='color:#ffffff'>SQL Parsing</b><br>
    &nbsp;&nbsp;&nbsp;&nbsp;↓  sqlparse tokenizer<br>
    2️⃣ &nbsp;<b style='color:#ffffff'>Logical Plan</b><br>
    &nbsp;&nbsp;&nbsp;&nbsp;↓  Relational Algebra<br>
    3️⃣A &nbsp;<b style='color:#ffffff'>RBO – Predicate Pushdown</b><br>
    &nbsp;&nbsp;&nbsp;&nbsp;↓  Filter moved to scan level<br>
    3️⃣B &nbsp;<b style='color:#ffffff'>RBO – Projection Pushdown</b><br>
    &nbsp;&nbsp;&nbsp;&nbsp;↓  Narrow columns added above scans<br>
    4️⃣ &nbsp;<b style='color:#ffffff'>CBO</b><br>
    &nbsp;&nbsp;&nbsp;&nbsp;↓  Join Reordering<br>
    5️⃣ &nbsp;<b style='color:#58a6ff'>Physical Plan ✔</b>
    </div>
    """
    st.markdown(stages_html, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# ── MAIN AREA ─────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

# Hero header
st.markdown(
    """
    <div class="hero-header">
        <p class="hero-title">⚡ Mini Query Optimizer</p>
        <p class="hero-sub">
            A visual SQL engine simulator that parses SQL, builds relational algebra trees,
            and applies Rule-Based &amp; Cost-Based optimization strategies.
        </p>
        <div class="hero-badges">
            <span class="badge badge-blue">SQL Parser</span>
            <span class="badge badge-green">Predicate Pushdown</span>
            <span class="badge badge-purple">Projection Pushdown</span>
            <span class="badge badge-orange">Cost Model</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ── SQL Input ────────────────────────────────────────────────────────────────
st.markdown("### 📝 SQL Query Input")
st.markdown(
    "<p style='color:#a1a1aa;font-size:0.9rem;margin-top:-0.5rem'>Enter a SELECT statement with JOINs and WHERE clauses. The optimizer supports up to 3 tables.</p>",
    unsafe_allow_html=True,
)

sql_input = st.text_area(
    label="SQL Query",
    value=DEFAULT_SQL,
    height=130,
    label_visibility="collapsed",
)

col_btn, col_hint = st.columns([1, 4])
with col_btn:
    run_clicked = st.button("▶  Optimize Query", use_container_width=True)

with col_hint:
    st.markdown(
        "<p style='color:#a1a1aa;font-size:0.85rem;padding-top:0.5rem'>"
        "Tip: Try changing the WHERE condition or swapping table order.</p>",
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────────────────────────────────────
# ── PIPELINE EXECUTION ────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

if run_clicked or sql_input:
    st.markdown("---")

    try:
        with st.spinner("🔍 Parsing SQL..."):
            logical_tree = parser.parse(sql_input)
            parse_report = parser.explain_parse(sql_input)

        with st.spinner("⚙ Applying Predicate Pushdown (RBO pass 1)..."):
            rbo_pp = RuleBasedOptimizer(catalog=catalog)
            tree_after_predpush = copy.deepcopy(logical_tree)
            rbo_pp._predicate_rules = []
            rbo_pp._projection_rules = []
            tree_after_predpush = rbo_pp._apply_predicate_pushdown(tree_after_predpush)
            predicate_rules = list(rbo_pp._predicate_rules)

        with st.spinner("🔬 Applying Projection Pushdown (RBO pass 2)..."):
            rbo_pp._projection_rules = []
            rbo_tree = rbo_pp._apply_projection_pushdown(copy.deepcopy(tree_after_predpush))
            projection_rules = list(rbo_pp._projection_rules)

        with st.spinner("📊 Running Cost-Based Optimization..."):
            cbo        = CostBasedOptimizer(catalog=catalog)
            cbo_result = cbo.optimize(copy.deepcopy(rbo_tree))

        # Render all trees
        logical_str       = vis.render(logical_tree)
        pred_push_str     = vis.render(tree_after_predpush)
        proj_push_str     = vis.render(rbo_tree)
        physical_str      = vis.render(cbo_result.plan)

        st.success("✅ Query optimized successfully! Select the tabs below to view the pipeline.")

        # Metrics
        m1, m2, m3, m4, m5 = st.columns(5)
        with m1:
            tables = logical_tree.source_tables
            st.markdown(
                f"""<div class="metric-box">
                    <div class="metric-value">{len(tables)}</div>
                    <div class="metric-label">Tables Joined</div>
                </div>""", unsafe_allow_html=True)
        with m2:
            st.markdown(
                f"""<div class="metric-box">
                    <div class="metric-value">{len(predicate_rules)}</div>
                    <div class="metric-label">Predicate Rules</div>
                </div>""", unsafe_allow_html=True)
        with m3:
            st.markdown(
                f"""<div class="metric-box">
                    <div class="metric-value" style='color:#bc8cff'>{len(projection_rules)}</div>
                    <div class="metric-label">Projection Rules</div>
                </div>""", unsafe_allow_html=True)
        with m4:
            cost_fmt = f"{cbo_result.cost:,}" if cbo_result.cost > 0 else "N/A"
            st.markdown(
                f"""<div class="metric-box">
                    <div class="metric-value" style='color:#3fb950'>{cost_fmt}</div>
                    <div class="metric-label">CBO Min Cost</div>
                </div>""", unsafe_allow_html=True)
        with m5:
            ordering_str = " → ".join(cbo_result.ordering) if cbo_result.ordering else "—"
            st.markdown(
                f"""<div class="metric-box">
                    <div class="metric-value" style='font-size:0.85rem;color:#f0883e'>{ordering_str}</div>
                    <div class="metric-label">Optimal Join Order</div>
                </div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "1️⃣ Parsed Logical Plan",
            "2️⃣ After RBO",
            "3️⃣ After CBO · Physical Plan",
            "🔬 Debug Info",
        ])

        with tab1:
            st.markdown("#### Unoptimized Logical Plan")
            st.markdown(
                "<p style='color:#a1a1aa;font-size:0.9rem'>"
                "This is the raw relational-algebra tree produced directly by the parser. "
                "Notice how the <code>WHERE</code> filter sits high in the tree.</p>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{logical_str}</div>', unsafe_allow_html=True)

        with tab2:
            st.subheader("2️⃣A  After RBO — Predicate Pushdown")
            st.markdown(
                "<p style='color:#a1a1aa;font-size:0.9rem'>"
                "The <code>WHERE</code> filter is pushed down to the <b>scan</b> level.</p>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{pred_push_str}</div>', unsafe_allow_html=True)

            st.markdown("---")

            st.subheader("2️⃣B  After RBO — Projection Pushdown")
            st.markdown(
                "<p style='color:#a1a1aa;font-size:0.9rem'>"
                "Narrow <code>ProjectNode</code>s are inserted above scans to drop unused columns.</p>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{proj_push_str}</div>', unsafe_allow_html=True)

        with tab3:
            st.markdown("#### Physical Execution Plan (after CBO)")
            st.markdown(
                "<p style='color:#a1a1aa;font-size:0.9rem'>"
                "The tree is reordered to join the smallest tables first.</p>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{physical_str}</div>', unsafe_allow_html=True)

            st.markdown("#### 🔄 Side-by-Side Comparison")
            col_l, col_r = st.columns(2)
            with col_l:
                st.markdown("<div class='stage-title'>📌 Logical Plan (original)</div>", unsafe_allow_html=True)
                st.markdown(f'<div class="tree-container">{logical_str}</div>', unsafe_allow_html=True)
            with col_r:
                st.markdown("<div class='stage-title'>🚀 Physical Plan (optimized)</div>", unsafe_allow_html=True)
                st.markdown(f'<div class="tree-container">{physical_str}</div>', unsafe_allow_html=True)

        with tab4:
            st.markdown("#### 🔬 Internal Debug Information")
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                st.markdown("**Logical Tree (repr):**")
                st.code(repr(logical_tree), language="python")
            with col_d2:
                st.markdown("**CBO Ordering:**")
                st.code(str(cbo_result.ordering), language="python")
                st.markdown("**Catalog Stats:**")
                st.json(catalog.get_all_stats())

    except Exception as exc:
        st.error(f"❌ Optimizer Error: {exc}")
        with st.expander("📋 Full Traceback"):
            st.code(traceback.format_exc(), language="python")