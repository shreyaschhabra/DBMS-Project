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
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Custom CSS — clean, intentional, typography-driven SaaS aesthetic
# No gradients, no glow, no neon. Solid colors, layered surfaces.
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
    /* ── Google Fonts ────────────────────────────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    /* ── Design tokens ───────────────────────────────────────────── */
    :root {
        --bg-primary:     #0f1115;
        --bg-secondary:   #181b22;
        --bg-card:        #1e2130;
        --accent:         #4a7cf7;
        --accent-dark:    #3a6ce0;
        --accent-amber:   #c9913d;
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
    }

    /* ── Global font + background ─────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Helvetica Neue', sans-serif !important;
        background-color: var(--bg-primary) !important;
        color: var(--text-primary) !important;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }

    /* ── Main block container ─────────────────────────────────────── */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 4rem;
        max-width: 1280px;
    }

    /* ── Sidebar ──────────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: var(--bg-secondary) !important;
        border-right: 1px solid var(--border) !important;
    }
    section[data-testid="stSidebar"] * {
        color: var(--text-primary) !important;
    }
    section[data-testid="stSidebar"] hr {
        border: none !important;
        border-top: 1px solid var(--border) !important;
        margin: 1rem 0 !important;
    }
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        font-weight: 600 !important;
        letter-spacing: -0.01em !important;
        color: var(--text-primary) !important;
    }

    /* ── Global headings ──────────────────────────────────────────── */
    h1, h2, h3, h4 {
        font-weight: 600 !important;
        letter-spacing: -0.01em !important;
        color: var(--text-primary) !important;
    }

    /* ── HR dividers ──────────────────────────────────────────────── */
    hr {
        border: none !important;
        border-top: 1px solid var(--border) !important;
        margin: 1.5rem 0 !important;
    }

    /* ── Buttons: solid, no glow ──────────────────────────────────── */
    .stButton > button {
        background: var(--accent) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 6px !important;
        padding: 0.55rem 1.75rem !important;
        font-weight: 600 !important;
        font-size: 0.9rem !important;
        letter-spacing: 0.01em !important;
        transition: background 150ms ease, transform 150ms ease !important;
        box-shadow: none !important;
    }
    .stButton > button:hover {
        background: var(--accent-dark) !important;
        transform: scale(1.01) !important;
        box-shadow: none !important;
    }
    .stButton > button:active {
        transform: scale(0.99) !important;
    }

    /* ── SQL text area — monospace only ───────────────────────────── */
    textarea {
        font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace !important;
        font-size: 0.875rem !important;
        line-height: 1.6 !important;
        background: var(--bg-secondary) !important;
        border: 1px solid var(--border) !important;
        border-radius: 6px !important;
        color: var(--text-primary) !important;
        transition: border-color 150ms ease !important;
    }
    textarea:focus {
        border-color: var(--accent) !important;
        outline: none !important;
        box-shadow: none !important;
    }

    /* ── Code blocks — monospace ──────────────────────────────────── */
    .stCodeBlock pre,
    .stCodeBlock code {
        font-family: 'JetBrains Mono', 'Fira Code', monospace !important;
        font-size: 0.82rem !important;
        background: var(--bg-secondary) !important;
        border: 1px solid var(--border) !important;
        border-radius: 6px !important;
        color: var(--text-primary) !important;
    }

    /* ── Tabs — underline active state only, no pill ──────────────── */
    .stTabs [data-baseweb="tab-list"] {
        background: transparent !important;
        border-bottom: 1px solid var(--border) !important;
        gap: 0 !important;
        padding: 0 !important;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent !important;
        border: none !important;
        border-bottom: 2px solid transparent !important;
        border-radius: 0 !important;
        padding: 0.6rem 1.25rem !important;
        font-size: 0.875rem !important;
        font-weight: 500 !important;
        color: var(--text-muted) !important;
        transition: color 150ms ease, border-color 150ms ease !important;
        margin-bottom: -1px !important;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: var(--text-primary) !important;
        background: transparent !important;
    }
    .stTabs [aria-selected="true"] {
        color: var(--text-primary) !important;
        border-bottom: 2px solid var(--accent) !important;
        background: transparent !important;
    }
    .stTabs [data-baseweb="tab-highlight"],
    .stTabs [data-baseweb="tab-border"] {
        display: none !important;
    }

    /* ── Notifications ────────────────────────────────────────────── */
    div[data-testid="stNotificationContentSuccess"] {
        background: var(--success-bg) !important;
        border: 1px solid var(--success-border) !important;
        border-radius: 6px !important;
        color: var(--success-text) !important;
    }
    div[data-testid="stNotificationContentError"] {
        background: var(--error-bg) !important;
        border: 1px solid var(--error-border) !important;
        border-radius: 6px !important;
        color: var(--error-text) !important;
    }

    /* ── Spinner ──────────────────────────────────────────────────── */
    .stSpinner > div {
        color: var(--text-muted) !important;
        font-size: 0.875rem !important;
    }

    /* ── Expander ─────────────────────────────────────────────────── */
    .streamlit-expanderHeader {
        background: var(--bg-card) !important;
        border: 1px solid var(--border) !important;
        border-radius: 6px !important;
        font-size: 0.875rem !important;
        font-weight: 500 !important;
        color: var(--text-secondary) !important;
    }

    /* ── Native metric cleanup ────────────────────────────────────── */
    [data-testid="metric-container"] {
        background: transparent !important;
        border: none !important;
        padding: 0 !important;
        box-shadow: none !important;
    }

    /* ══════════════════════════════════════════════════════════════ */
    /* Custom component classes                                        */
    /* ══════════════════════════════════════════════════════════════ */

    /* ── Page header ──────────────────────────────────────────────── */
    .page-header {
        padding: 2rem 0 1.5rem 0;
        border-bottom: 1px solid var(--border);
        margin-bottom: 2rem;
    }
    .page-header-title {
        font-size: 1.75rem;
        font-weight: 600;
        color: var(--text-primary);
        letter-spacing: -0.02em;
        margin: 0 0 0.4rem 0;
        line-height: 1.2;
    }
    .page-header-desc {
        font-size: 0.9rem;
        color: var(--text-muted);
        font-weight: 400;
        margin: 0;
        line-height: 1.6;
        max-width: 680px;
    }
    .page-header-tags {
        margin-top: 1rem;
        display: flex;
        gap: 0.45rem;
        flex-wrap: wrap;
    }
    .tag {
        display: inline-block;
        padding: 0.2rem 0.65rem;
        border-radius: 4px;
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        background: var(--bg-card);
        color: var(--text-muted);
        border: 1px solid var(--border);
    }
    .tag-accent {
        background: rgba(74, 124, 247, 0.08);
        color: var(--accent);
        border-color: rgba(74, 124, 247, 0.22);
    }

    /* ── Section label ────────────────────────────────────────────── */
    .section-label {
        font-size: 0.68rem;
        font-weight: 600;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: var(--text-muted);
        margin-bottom: 0.5rem;
    }

    /* ── Input hint text ──────────────────────────────────────────── */
    .input-hint {
        font-size: 0.8rem;
        color: var(--text-muted);
        font-weight: 400;
        padding-top: 0.5rem;
        line-height: 1.5;
    }

    /* ── Metric card — no border highlight, no shadow ─────────────── */
    .metric-card {
        padding: 1.1rem 1rem;
        background: var(--bg-secondary);
        border: 1px solid var(--border);
        border-radius: 6px;
    }
    .metric-card-value {
        font-size: 1.75rem;
        font-weight: 700;
        color: var(--text-primary);
        line-height: 1;
        margin-bottom: 0.4rem;
    }
    .metric-card-value.accent  { color: var(--accent); }
    .metric-card-value.amber   { color: var(--accent-amber); }
    .metric-card-value.success { color: var(--success-text); }
    .metric-card-value.sm      { font-size: 1rem; }
    .metric-card-label {
        font-size: 0.68rem;
        font-weight: 600;
        letter-spacing: 0.09em;
        text-transform: uppercase;
        color: var(--text-muted);
    }

    /* ── Tree output — strictly monospace, whitespace preserved ───── */
    .tree-container {
        font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace !important;
        font-size: 0.825rem !important;
        line-height: 1.65 !important;
        background: #0a0c10 !important;
        color: #c9d1d9 !important;
        padding: 1.25rem 1.5rem !important;
        border: 1px solid var(--border) !important;
        border-radius: 6px !important;
        white-space: pre !important;
        overflow-x: auto !important;
        margin-bottom: 1rem !important;
        tab-size: 4 !important;
    }

    /* ── Catalog entry (sidebar) ──────────────────────────────────── */
    .catalog-entry {
        background: var(--bg-primary);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin-bottom: 0.55rem;
    }
    .catalog-entry .ce-name {
        font-size: 0.875rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 0.2rem;
    }
    .catalog-entry .ce-rows {
        font-size: 0.78rem;
        font-weight: 500;
        color: var(--accent);
        margin-bottom: 0.12rem;
    }
    .catalog-entry .ce-cols {
        font-size: 0.72rem;
        color: var(--text-muted);
        font-weight: 300;
    }

    /* ── Pipeline step list (sidebar) ────────────────────────────── */
    .pipeline-step {
        display: flex;
        align-items: flex-start;
        gap: 0.75rem;
        padding: 0.5rem 0;
        border-bottom: 1px solid var(--border);
        font-size: 0.82rem;
    }
    .pipeline-step:last-child { border-bottom: none; }
    .pipeline-step .step-num {
        font-size: 0.67rem;
        font-weight: 700;
        color: var(--text-muted);
        min-width: 1.5rem;
        padding-top: 0.12rem;
        letter-spacing: 0.04em;
        font-family: 'JetBrains Mono', monospace;
    }
    .pipeline-step .step-body .step-title {
        font-weight: 600;
        color: var(--text-primary);
        line-height: 1.4;
    }
    .pipeline-step .step-body .step-desc {
        color: var(--text-muted);
        font-weight: 300;
        font-size: 0.77rem;
        line-height: 1.4;
        margin-top: 0.08rem;
    }

    /* ── Sidebar app name ─────────────────────────────────────────── */
    .sidebar-app-name {
        font-size: 1rem;
        font-weight: 600;
        color: var(--text-primary);
        letter-spacing: -0.01em;
    }
    .sidebar-app-version {
        font-size: 0.75rem;
        color: var(--text-muted);
        font-weight: 300;
        margin-top: 0.15rem;
    }

    /* ── Tab section typography ───────────────────────────────────── */
    .tab-section-title {
        font-size: 0.95rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 0.3rem;
        margin-top: 1.25rem;
    }
    .tab-section-desc {
        font-size: 0.83rem;
        color: var(--text-muted);
        font-weight: 400;
        margin-bottom: 0.75rem;
        line-height: 1.6;
        max-width: 680px;
    }
    .tab-section-desc code {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.8rem;
        background: var(--bg-card);
        padding: 0.1em 0.4em;
        border-radius: 3px;
        color: var(--accent);
    }

    /* ── Comparison label ─────────────────────────────────────────── */
    .compare-label {
        font-size: 0.68rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.09em;
        color: var(--text-muted);
        margin-bottom: 0.5rem;
        padding-bottom: 0.35rem;
        border-bottom: 1px solid var(--border);
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
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        """
        <div class="sidebar-app-name">Query Optimizer</div>
        <div class="sidebar-app-version">SQL Engine Simulator &nbsp;&middot;&nbsp; v1.0</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("---")

    st.markdown("<div class='section-label'>Database Catalog</div>", unsafe_allow_html=True)
    st.markdown(
        "<p style='color:var(--text-muted);font-size:0.8rem;margin-bottom:0.75rem;"
        "line-height:1.5;font-weight:300'>"
        "Mock statistics used by the Cost-Based Optimizer to estimate join costs.</p>",
        unsafe_allow_html=True,
    )

    all_stats = catalog.get_all_stats()

    for table_name, info in all_stats.items():
        cols_str = ", ".join(info["columns"])
        st.markdown(
            f"""
            <div class="catalog-entry">
                <div class="ce-name">{table_name}</div>
                <div class="ce-rows">{info['row_count']:,} rows</div>
                <div class="ce-cols">Columns: {cols_str}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("---")
    st.markdown("<div class='section-label'>Pipeline Stages</div>", unsafe_allow_html=True)

    pipeline_steps = [
        ("01", "SQL Parsing",              "sqlglot AST-based parser"),
        ("02", "Logical Plan",             "Relational algebra tree"),
        ("03", "RBO — Predicate Pushdown", "AND-split + OR-block safety"),
        ("04", "RBO — Projection Pushdown","Narrow columns above scans"),
        ("05", "CBO — Join Reordering",    "Inner joins only (outer joins preserved)"),
        ("06", "Physical Plan",            "Final executable plan"),
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
        </div>
        """
    st.markdown(steps_html, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────────────────────────────

# Page header — left-aligned, minimal
st.markdown(
    """
    <div class="page-header">
        <div class="page-header-title">Mini Query Optimizer</div>
        <div class="page-header-desc">
            A visual SQL engine simulator. Parses SQL queries, builds relational algebra trees,
            and applies Rule-Based and Cost-Based optimization strategies.
        </div>
        <div class="page-header-tags">
            <span class="tag tag-accent">SQL Parser</span>
            <span class="tag">Predicate Pushdown</span>
            <span class="tag">OR-block Safety</span>
            <span class="tag">Outer Join Support</span>
            <span class="tag">Join Reordering</span>
            <span class="tag">Cost Model</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# SQL Input
st.markdown("<div class='section-label'>SQL Query</div>", unsafe_allow_html=True)
st.markdown(
    "<p style='color:var(--text-muted);font-size:0.82rem;margin-bottom:0.5rem;"
    "line-height:1.5;font-weight:300'>"
    "Enter a SELECT statement with JOINs and WHERE clauses. "
    "Supports INNER, LEFT, and RIGHT JOINs; AND-split and OR-block predicates; "
    "CTEs; and GROUP BY.</p>",
    unsafe_allow_html=True,
)

sql_input = st.text_area(
    label="SQL Query",
    value=DEFAULT_SQL,
    height=130,
    label_visibility="collapsed",
)

col_btn, col_hint = st.columns([1, 5])
with col_btn:
    run_clicked = st.button("Optimize Query", use_container_width=True)
with col_hint:
    st.markdown(
        "<p class='input-hint'>Try a LEFT JOIN, an OR condition, or a WITH (CTE) clause "
        "to see how the optimizer handles each case.</p>",
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE EXECUTION
# ─────────────────────────────────────────────────────────────────────────────

if run_clicked or sql_input:
    st.markdown("---")

    try:
        with st.spinner("Parsing SQL..."):
            logical_tree = parser.parse(sql_input)
            parse_report = parser.explain_parse(sql_input)

        with st.spinner("Applying Predicate Pushdown (RBO pass 1)..."):
            rbo_pp = RuleBasedOptimizer(catalog=catalog)
            tree_after_predpush = copy.deepcopy(logical_tree)
            rbo_pp._predicate_rules = []
            rbo_pp._projection_rules = []
            tree_after_predpush = rbo_pp._apply_predicate_pushdown(tree_after_predpush)
            predicate_rules = list(rbo_pp._predicate_rules)

        with st.spinner("Applying Projection Pushdown (RBO pass 2)..."):
            rbo_pp._projection_rules = []
            rbo_tree = rbo_pp._apply_projection_pushdown(copy.deepcopy(tree_after_predpush))
            projection_rules = list(rbo_pp._projection_rules)

        with st.spinner("Running Cost-Based Optimization..."):
            cbo        = CostBasedOptimizer(catalog=catalog)
            cbo_result = cbo.optimize(copy.deepcopy(rbo_tree))

        # Render plan trees
        logical_str   = vis.render(logical_tree)
        pred_push_str = vis.render(tree_after_predpush)
        proj_push_str = vis.render(rbo_tree)
        physical_str  = vis.render(cbo_result.plan)

        st.success("Query optimized successfully. Use the tabs below to explore the pipeline.")

        # Metrics row
        m1, m2, m3, m4, m5 = st.columns(5)
        with m1:
            tables = logical_tree.source_tables
            st.markdown(
                f"""<div class="metric-card">
                    <div class="metric-card-value accent">{len(tables)}</div>
                    <div class="metric-card-label">Tables Joined</div>
                </div>""",
                unsafe_allow_html=True,
            )
        with m2:
            st.markdown(
                f"""<div class="metric-card">
                    <div class="metric-card-value">{len(predicate_rules)}</div>
                    <div class="metric-card-label">Predicate Rules</div>
                </div>""",
                unsafe_allow_html=True,
            )
        with m3:
            st.markdown(
                f"""<div class="metric-card">
                    <div class="metric-card-value">{len(projection_rules)}</div>
                    <div class="metric-card-label">Projection Rules</div>
                </div>""",
                unsafe_allow_html=True,
            )
        with m4:
            cost_fmt = f"{cbo_result.cost:,}" if cbo_result.cost > 0 else "N/A"
            st.markdown(
                f"""<div class="metric-card">
                    <div class="metric-card-value success">{cost_fmt}</div>
                    <div class="metric-card-label">CBO Min Cost</div>
                </div>""",
                unsafe_allow_html=True,
            )
        with m5:
            if getattr(cbo_result, "reorder_disabled", False):
                ordering_str = "Preserved (outer join)"
                order_class  = "metric-card-value amber sm"
            elif cbo_result.ordering:
                ordering_str = " \u2192 ".join(cbo_result.ordering)
                order_class  = "metric-card-value amber sm"
            else:
                ordering_str = "\u2014"
                order_class  = "metric-card-value sm"
            st.markdown(
                f"""<div class="metric-card">
                    <div class="{order_class}">{ordering_str}</div>
                    <div class="metric-card-label">Join Order / Reorder Status</div>
                </div>""",
                unsafe_allow_html=True,
            )

        st.markdown("<br>", unsafe_allow_html=True)

        # Tabs — labels stripped of emojis, clean minimal style
        tab1, tab2, tab3, tab4 = st.tabs([
            "Parsed Logical Plan",
            "After RBO",
            "After CBO \u2014 Physical Plan",
            "Debug Info",
        ])

        with tab1:
            st.markdown(
                "<div class='tab-section-title'>Unoptimized Logical Plan</div>"
                "<div class='tab-section-desc'>"
                "Raw relational-algebra tree produced directly by the parser. "
                "Notice how the <code>WHERE</code> filter sits high in the tree, "
                "before any optimization has been applied."
                "</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="tree-container">{logical_str}</div>',
                unsafe_allow_html=True,
            )

        with tab2:
            st.markdown(
                "<div class='tab-section-title'>After Predicate Pushdown</div>"
                "<div class='tab-section-desc'>"
                "The <code>WHERE</code> filter is pushed down to the <code>scan</code> level, "
                "reducing rows processed before any JOIN operation."
                "</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="tree-container">{pred_push_str}</div>',
                unsafe_allow_html=True,
            )

            st.markdown("---")

            st.markdown(
                "<div class='tab-section-title'>After Projection Pushdown</div>"
                "<div class='tab-section-desc'>"
                "Narrow <code>ProjectNode</code>s are inserted above scans to drop unused columns "
                "as early as possible in the pipeline."
                "</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="tree-container">{proj_push_str}</div>',
                unsafe_allow_html=True,
            )

        with tab3:
            st.markdown(
                "<div class='tab-section-title'>Physical Execution Plan</div>"
                "<div class='tab-section-desc'>"
                "The CBO reorders joins so the smallest intermediate result is produced first, "
                "minimizing total I/O across the pipeline."
                "</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="tree-container">{physical_str}</div>',
                unsafe_allow_html=True,
            )

            st.markdown("---")

            st.markdown(
                "<div class='tab-section-title'>Side-by-Side Comparison</div>"
                "<div class='tab-section-desc'>"
                "Original logical plan versus final optimized physical plan."
                "</div>",
                unsafe_allow_html=True,
            )
            col_l, col_r = st.columns(2)
            with col_l:
                st.markdown(
                    "<div class='compare-label'>Logical Plan \u2014 original</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div class="tree-container">{logical_str}</div>',
                    unsafe_allow_html=True,
                )
            with col_r:
                st.markdown(
                    "<div class='compare-label'>Physical Plan \u2014 optimized</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div class="tree-container">{physical_str}</div>',
                    unsafe_allow_html=True,
                )

        with tab4:
            st.markdown(
                "<div class='tab-section-title'>Internal Debug Information</div>"
                "<div class='tab-section-desc'>"
                "Raw internal representations for inspection and development."
                "</div>",
                unsafe_allow_html=True,
            )
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                st.markdown(
                    "<div class='section-label' style='margin-bottom:0.4rem'>Logical Tree (repr)</div>",
                    unsafe_allow_html=True,
                )
                st.code(repr(logical_tree), language="python")
            with col_d2:
                st.markdown(
                    "<div class='section-label' style='margin-bottom:0.4rem'>CBO Ordering</div>",
                    unsafe_allow_html=True,
                )
                st.code(str(cbo_result.ordering), language="python")
                st.markdown(
                    "<div class='section-label' style='margin-top:1rem;margin-bottom:0.4rem'>"
                    "Catalog Stats</div>",
                    unsafe_allow_html=True,
                )
                st.json(catalog.get_all_stats())

    except Exception as exc:
        st.error(f"Optimizer Error: {exc}")
        with st.expander("Full Traceback"):
            st.code(traceback.format_exc(), language="python")