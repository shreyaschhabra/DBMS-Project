"""
app.py
------
Mini Query Optimizer — Interactive Streamlit Frontend  v3.0

Pipeline:
    SQL Input → Parse → Logical Plan → RBO → CBO → Physical Plan → SQL Unparser
    (optional) Live MySQL → Schema Sync + Benchmark Unoptimized vs Optimized SQL

Run with:
    uv run streamlit run app.py
"""

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
    page_title="Mini Query Optimizer",
    page_icon="Q",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS  —  dark-mode-first + explicit light-mode overrides
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&family=JetBrains+Mono:wght@400;500&display=swap');

    /* ── Material You tokens ─────────────────────────────────────── */
    :root {
        --md-background: #FFFBFE;
        --md-on-background: #1C1B1F;
        --md-primary: #6750A4;
        --md-on-primary: #FFFFFF;
        --md-secondary-container: #E8DEF8;
        --md-on-secondary-container: #1D192B;
        --md-tertiary: #7D5260;
        --md-surface-container: #F3EDF7;
        --md-surface-container-low: #E7E0EC;
        --md-outline: #79747E;
        --md-on-surface-variant: #49454F;
        --md-surface: #FFFBFE;
        --md-shadow: rgba(28, 27, 31, 0.10);

        --bg-primary: var(--md-background);
        --bg-secondary: var(--md-surface-container);
        --bg-card: var(--md-surface-container);
        --accent: var(--md-primary);
        --accent-dark: #4F378B;
        --accent-amber: var(--md-tertiary);
        --accent-green: #4F6355;
        --accent-red: #B3261E;
        --text-primary: var(--md-on-background);
        --text-secondary: var(--md-on-surface-variant);
        --text-muted: var(--md-on-surface-variant);
        --border: var(--md-outline);
        --border-light: rgba(121, 116, 126, 0.22);
        --success-bg: rgba(79, 99, 85, 0.12);
        --success-border: rgba(79, 99, 85, 0.24);
        --success-text: #3F6B4D;
        --error-bg: rgba(179, 38, 30, 0.10);
        --error-border: rgba(179, 38, 30, 0.22);
        --error-text: #B3261E;
        --db-badge-bg: rgba(103, 80, 164, 0.12);
        --db-badge-border: rgba(103, 80, 164, 0.24);
        --db-badge-text: #5B3F96;
    }

    /* ── Global ───────────────────────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'Roboto', -apple-system, BlinkMacSystemFont, sans-serif !important;
        background-color: var(--bg-primary) !important;
        color: var(--text-primary) !important;
        -webkit-font-smoothing: antialiased;
    }

    body {
        background:
            radial-gradient(circle at top left, rgba(103, 80, 164, 0.10), transparent 28%),
            radial-gradient(circle at top right, rgba(125, 82, 96, 0.08), transparent 24%),
            linear-gradient(180deg, var(--md-background), #f6effb 100%);
    }

    .stApp {
        background: transparent;
    }

    /* ── Main container ───────────────────────────────────────────── */
    .main .block-container {
        padding-top: 1.6rem;
        padding-bottom: 4rem;
        max-width: 1320px;
    }

    .main .block-container::before {
        content: "";
        position: fixed;
        inset: 0;
        pointer-events: none;
        background:
            radial-gradient(circle at 12% 12%, rgba(103, 80, 164, 0.08), transparent 22%),
            radial-gradient(circle at 88% 18%, rgba(125, 82, 96, 0.08), transparent 18%),
            radial-gradient(circle at 50% 100%, rgba(232, 222, 248, 0.26), transparent 24%);
        z-index: -1;
    }

    /* ── Sidebar ──────────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(243, 237, 247, 0.94), rgba(231, 224, 236, 0.96)) !important;
        border-right: 1px solid rgba(121, 116, 126, 0.20) !important;
        box-shadow: 8px 0 24px rgba(28, 27, 31, 0.06);
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
        background: var(--md-surface-container-low) !important;
        color: var(--text-primary) !important;
        border: 1px solid rgba(121, 116, 126, 0.22) !important;
        border-top-left-radius: 12px !important;
        border-top-right-radius: 12px !important;
        border-bottom-left-radius: 0 !important;
        border-bottom-right-radius: 0 !important;
        min-height: 3.5rem !important;
    }
    input[type="text"]:focus,
    input[type="password"]:focus,
    input[type="number"]:focus {
        border-color: var(--accent) !important;
        outline: none !important;
        box-shadow: 0 0 0 3px rgba(103, 80, 164, 0.14) !important;
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
        color: var(--md-on-primary) !important;
        border: none !important;
        border-radius: 9999px !important;
        padding: 0.6rem 1.8rem !important;
        font-weight: 600 !important;
        font-size: 0.88rem !important;
        transition: transform 220ms cubic-bezier(0.2, 0, 0, 1), box-shadow 220ms cubic-bezier(0.2, 0, 0, 1), background 220ms cubic-bezier(0.2, 0, 0, 1) !important;
        box-shadow: 0 4px 14px rgba(28, 27, 31, 0.08) !important;
    }
    .stButton > button:hover {
        background: #5b3f96 !important;
        transform: translateY(-1px) scale(1.01) !important;
        box-shadow: 0 10px 24px rgba(28, 27, 31, 0.12) !important;
    }
    .stButton > button:active { transform: scale(0.95) !important; }

    /* ── Textarea ─────────────────────────────────────────────────── */
    textarea {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.875rem !important;
        background: var(--md-surface-container-low) !important;
        border: 1px solid rgba(121, 116, 126, 0.22) !important;
        border-radius: 20px !important;
        color: var(--text-primary) !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.55);
    }
    textarea:focus { border-color: var(--accent) !important; outline: none !important; box-shadow: 0 0 0 3px rgba(103, 80, 164, 0.12) !important; }

    /* ── Code blocks ──────────────────────────────────────────────── */
    .stCodeBlock pre, .stCodeBlock code {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.82rem !important;
        background: var(--md-surface-container-low) !important;
        border: 1px solid rgba(121, 116, 126, 0.18) !important;
        border-radius: 20px !important;
        color: var(--text-primary) !important;
    }

    /* ── Tabs ─────────────────────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        background: transparent !important;
        border-bottom: 1px solid rgba(121, 116, 126, 0.22) !important;
        gap: 0 !important; padding: 0 !important;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent !important;
        border: none !important;
        border-bottom: 2px solid transparent !important;
        border-radius: 0 !important;
        padding: 0.7rem 1.25rem !important;
        font-size: 0.875rem !important;
        font-weight: 500 !important;
        color: var(--text-muted) !important;
        transition: color 220ms cubic-bezier(0.2, 0, 0, 1), border-color 220ms cubic-bezier(0.2, 0, 0, 1), background 220ms cubic-bezier(0.2, 0, 0, 1) !important;
        margin-bottom: -1px !important;
        border-top-left-radius: 9999px !important;
        border-top-right-radius: 9999px !important;
    }
    .stTabs [data-baseweb="tab"]:hover { color: var(--text-primary) !important; background: rgba(103, 80, 164, 0.08) !important; }
    .stTabs [aria-selected="true"] {
        color: var(--text-primary) !important;
        border-bottom: 2px solid var(--accent) !important;
        background: rgba(103, 80, 164, 0.08) !important;
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
        background: rgba(243, 237, 247, 0.9) !important;
        border: 1px solid rgba(121, 116, 126, 0.20) !important;
        border-radius: 24px !important;
        font-size: 0.875rem !important;
        font-weight: 500 !important;
        color: var(--text-secondary) !important;
    }

    /* ── Data editor ──────────────────────────────────────────────── */
    [data-testid="stDataEditor"] {
        border: 1px solid rgba(121, 116, 126, 0.20) !important;
        border-radius: 24px !important;
        overflow: hidden;
    }

    /* ══════════════════════ Custom components ═════════════════════ */

    /* Page header */
    .page-header {
        position: relative;
        overflow: hidden;
        padding: 2rem 1.5rem 1.5rem;
        border-bottom: none;
        margin-bottom: 2rem;
        background: linear-gradient(180deg, rgba(243, 237, 247, 0.98), rgba(255, 251, 254, 0.78));
        border: 1px solid rgba(121, 116, 126, 0.18);
        border-radius: 32px;
        box-shadow: 0 12px 30px rgba(28, 27, 31, 0.08);
    }
    .page-header::before,
    .page-header::after {
        content: "";
        position: absolute;
        border-radius: 9999px;
        filter: blur(12px);
        pointer-events: none;
        opacity: 0.7;
    }
    .page-header::before {
        width: 260px;
        height: 260px;
        right: -80px;
        top: -120px;
        background: radial-gradient(circle, rgba(103, 80, 164, 0.18), transparent 65%);
    }
    .page-header::after {
        width: 220px;
        height: 220px;
        left: -70px;
        bottom: -120px;
        background: radial-gradient(circle, rgba(125, 82, 96, 0.14), transparent 65%);
    }
    .page-header-title {
        font-size: 2rem; font-weight: 700;
        color: var(--text-primary);
        letter-spacing: -0.02em; margin: 0 0 0.45rem; line-height: 1.15;
    }
    .page-header-desc {
        font-size: 0.98rem; color: var(--text-secondary);
        font-weight: 400; margin: 0; line-height: 1.6; max-width: 760px;
    }
    .page-header-tags { margin-top: 1rem; display: flex; gap: 0.45rem; flex-wrap: wrap; }

    /* Tags */
    .tag {
        display: inline-block; padding: 0.28rem 0.75rem; border-radius: 9999px;
        font-size: 0.7rem; font-weight: 600; letter-spacing: 0.06em;
        text-transform: uppercase;
        background: rgba(232, 222, 248, 0.88); color: var(--md-on-secondary-container); border: 1px solid rgba(121, 116, 126, 0.14);
    }
    .tag-accent {
        background: rgba(103, 80, 164, 0.12); color: var(--accent);
        border-color: rgba(103, 80, 164, 0.20);
    }
    .tag-green {
        background: rgba(125, 82, 96, 0.10); color: var(--accent-amber);
        border-color: rgba(125, 82, 96, 0.18);
    }
    .tag-red {
        background: rgba(179, 38, 30, 0.10); color: var(--error-text);
        border-color: rgba(179, 38, 30, 0.20);
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
        padding: 1.15rem 1rem;
        background: linear-gradient(180deg, rgba(243, 237, 247, 0.94), rgba(231, 224, 236, 0.92));
        border: 1px solid rgba(121, 116, 126, 0.18);
        border-radius: 24px;
        box-shadow: 0 8px 22px rgba(28, 27, 31, 0.06);
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
        position: relative;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.84rem !important;
        line-height: 1.72 !important;
        background:
            radial-gradient(circle at top right, rgba(103, 80, 164, 0.10), transparent 36%),
            radial-gradient(circle at bottom left, rgba(125, 82, 96, 0.08), transparent 30%),
            linear-gradient(180deg, rgba(243, 237, 247, 0.96), rgba(231, 224, 236, 0.92)) !important;
        color: var(--text-primary) !important;
        padding: 1.4rem 1.55rem !important;
        border: 1px solid rgba(121, 116, 126, 0.22) !important;
        border-radius: 28px !important;
        box-shadow: 0 14px 40px rgba(28, 27, 31, 0.10) !important;
        white-space: pre !important; overflow-x: auto !important;
        margin-bottom: 1rem !important;
    }
    .tree-container::before {
        content: "";
        position: absolute;
        inset: 0;
        border-radius: 28px;
        pointer-events: none;
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.28), transparent 40%);
    }
    .tree-container .tree-connector { color: var(--accent-amber); font-weight: 700; }
    .tree-container .tree-node {
        display: inline-block;
        margin-right: 0.55rem;
        padding: 0.14rem 0.6rem;
        border-radius: 9999px;
        letter-spacing: 0.02em;
        box-shadow: inset 0 0 0 1px rgba(121, 116, 126, 0.14);
    }
    .tree-container .tree-node-project { background: rgba(103, 80, 164, 0.12); color: #5b3f96; font-weight: 700; }
    .tree-container .tree-node-filter { background: rgba(125, 82, 96, 0.10); color: #7d5260; }
    .tree-container .tree-node-or-filter { background: rgba(156, 79, 88, 0.12); color: #9c4f58; }
    .tree-container .tree-node-join { background: rgba(74, 106, 211, 0.12); color: #355ab8; font-weight: 600; }
    .tree-container .tree-node-scan { background: rgba(47, 93, 80, 0.10); color: #2f5d50; font-weight: 600; }
    .tree-container .tree-node-aggregate { background: rgba(106, 27, 154, 0.12); color: #6a1b9a; font-weight: 700; }
    .tree-container .tree-node-subquery { background: rgba(85, 80, 122, 0.10); color: #55507a; }
    .tree-container .tree-meta { color: var(--text-secondary); }

    .tree-container .tree-node,
    .tree-container .tree-meta {
        line-height: 1.7;
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

    /* Small atmospheric accents */
    .surface-chip {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        padding: 0.35rem 0.8rem;
        border-radius: 9999px;
        background: rgba(243, 237, 247, 0.92);
        color: var(--text-secondary);
        border: 1px solid rgba(121, 116, 126, 0.16);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700;800&family=Source+Serif+4:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');

    :root {
        --mono-bg: #FFFFFF;
        --mono-fg: #000000;
        --mono-muted: #F5F5F5;
        --mono-muted-fg: #525252;
        --mono-border: #000000;
        --mono-border-light: #E5E5E5;
        --mono-card: #FFFFFF;
        --mono-invert-bg: #000000;
        --mono-invert-fg: #FFFFFF;
        --mono-sidebar: #000000;
        --mono-sidebar-fg: #FFFFFF;
    }

    html, body, [class*="css"] {
        font-family: 'Source Serif 4', Georgia, serif !important;
        background: var(--mono-bg) !important;
        color: var(--mono-fg) !important;
        -webkit-font-smoothing: antialiased;
    }

    body {
        background-image:
            repeating-linear-gradient(0deg, transparent, transparent 23px, rgba(0, 0, 0, 0.03) 24px),
            repeating-linear-gradient(90deg, transparent, transparent 143px, rgba(0, 0, 0, 0.018) 144px);
        background-attachment: fixed;
    }

    .stApp {
        background: transparent;
    }

    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 4rem;
        max-width: 1220px;
    }

    .main .block-container::before {
        content: "";
        position: fixed;
        inset: 0;
        pointer-events: none;
        background:
            radial-gradient(circle at top right, rgba(0, 0, 0, 0.03), transparent 24%),
            radial-gradient(circle at bottom left, rgba(0, 0, 0, 0.025), transparent 20%);
        z-index: -1;
    }

    section[data-testid="stSidebar"] {
        background: var(--mono-sidebar) !important;
        border-right: 2px solid var(--mono-border) !important;
        box-shadow: none !important;
    }
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] span,
    section[data-testid="stSidebar"] div,
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3,
    section[data-testid="stSidebar"] h4,
    section[data-testid="stSidebar"] h5,
    section[data-testid="stSidebar"] h6 {
        color: var(--mono-sidebar-fg) !important;
    }
    section[data-testid="stSidebar"] hr {
        border: none !important;
        border-top: 1px solid rgba(255, 255, 255, 0.28) !important;
        margin: 1rem 0 !important;
    }
    /* Sidebar buttons: invert default (white bg, black text) */
    section[data-testid="stSidebar"] .stButton > button {
        background: #FFFFFF !important;
        color: #000000 !important;
        border: 2px solid #FFFFFF !important;
    }
    section[data-testid="stSidebar"] .stButton > button p,
    section[data-testid="stSidebar"] .stButton > button span,
    section[data-testid="stSidebar"] .stButton > button div {
        color: #000000 !important;
    }
    section[data-testid="stSidebar"] .stButton > button:hover {
        background: #000000 !important;
        color: #FFFFFF !important;
        border-color: #FFFFFF !important;
    }
    section[data-testid="stSidebar"] .stButton > button:hover p,
    section[data-testid="stSidebar"] .stButton > button:hover span,
    section[data-testid="stSidebar"] .stButton > button:hover div {
        color: #FFFFFF !important;
    }
    /* Dark sidebar inputs — must come AFTER the global input rule to win */
    section[data-testid="stSidebar"] [data-baseweb="input"] input,
    section[data-testid="stSidebar"] [data-baseweb="base-input"] input,
    section[data-testid="stSidebar"] input[type="text"],
    section[data-testid="stSidebar"] input[type="password"],
    section[data-testid="stSidebar"] input[type="number"] {
        background: rgba(255,255,255,0.10) !important;
        color: #FFFFFF !important;
        border: 2px solid rgba(255,255,255,0.4) !important;
        border-radius: 0 !important;
        box-shadow: none !important;
        -webkit-text-fill-color: #FFFFFF !important;
    }
    section[data-testid="stSidebar"] [data-baseweb="input"],
    section[data-testid="stSidebar"] [data-baseweb="base-input"] {
        background: rgba(255,255,255,0.10) !important;
        border: 2px solid rgba(255,255,255,0.4) !important;
        border-radius: 0 !important;
    }
    section[data-testid="stSidebar"] input::placeholder,
    section[data-testid="stSidebar"] input::-webkit-input-placeholder {
        color: rgba(255,255,255,0.45) !important;
        -webkit-text-fill-color: rgba(255,255,255,0.45) !important;
        opacity: 1 !important;
    }
    section[data-testid="stSidebar"] input[type="text"]:focus,
    section[data-testid="stSidebar"] input[type="password"]:focus,
    section[data-testid="stSidebar"] input[type="number"]:focus {
        border-color: #FFFFFF !important;
        outline: 2px solid rgba(255,255,255,0.6) !important;
        outline-offset: 2px !important;
        box-shadow: none !important;
    }

    input[type="text"], input[type="password"], input[type="number"], textarea {
        font-family: 'JetBrains Mono', monospace !important;
        background: #FFFFFF !important;
        color: #000000 !important;
        border: 2px solid #000000 !important;
        border-radius: 0 !important;
        box-shadow: none !important;
    }
    input[type="text"], input[type="password"], input[type="number"] {
        min-height: 3.25rem !important;
    }
    input[type="text"]:focus,
    input[type="password"]:focus,
    input[type="number"]:focus,
    textarea:focus {
        border-color: #000000 !important;
        outline: 3px solid #000000 !important;
        outline-offset: 2px !important;
        box-shadow: none !important;
    }

    .stButton > button {
        background: #000000 !important;
        color: #FFFFFF !important;
        border: 2px solid #000000 !important;
        border-radius: 0 !important;
        padding: 0.75rem 1.35rem !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.78rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.1em !important;
        text-transform: uppercase !important;
        transition: background 100ms linear, color 100ms linear, border-color 100ms linear !important;
        box-shadow: none !important;
        white-space: nowrap !important;
    }
    /* Target all text nodes Streamlit injects inside buttons */
    .stButton > button p,
    .stButton > button span,
    .stButton > button div {
        color: #FFFFFF !important;
        transition: color 100ms linear !important;
    }
    .stButton > button:hover {
        background: #FFFFFF !important;
        color: #000000 !important;
        border-color: #000000 !important;
        transform: none !important;
        box-shadow: none !important;
    }
    .stButton > button:hover p,
    .stButton > button:hover span,
    .stButton > button:hover div {
        color: #000000 !important;
    }
    .stButton > button:active { transform: none !important; }

    .stCodeBlock pre, .stCodeBlock code, pre, code {
        font-family: 'JetBrains Mono', monospace !important;
        background: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #000000 !important;
        border-radius: 0 !important;
        box-shadow: none !important;
    }

    .stTabs [data-baseweb="tab-list"] {
        background: #FFFFFF !important;
        border-bottom: 2px solid #000000 !important;
        gap: 0 !important;
        padding: 0 !important;
    }
    .stTabs [data-baseweb="tab"] {
        background: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #000000 !important;
        border-bottom: none !important;
        border-radius: 0 !important;
        padding: 0.8rem 1.05rem !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.75rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.08em !important;
        text-transform: uppercase !important;
        transition: background 100ms linear, color 100ms linear !important;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background: #000000 !important;
        color: #FFFFFF !important;
    }
    .stTabs [aria-selected="true"] {
        background: #000000 !important;
        color: #FFFFFF !important;
    }

    [data-testid="stMetric"] {
        border: 1px solid #000000 !important;
        border-radius: 0 !important;
        background: #FFFFFF !important;
        padding: 1rem !important;
    }
    [data-testid="stMetric"] label {
        color: #000000 !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.68rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.12em !important;
        text-transform: uppercase !important;
    }
    [data-testid="stMetricValue"] {
        color: #000000 !important;
        font-family: 'Playfair Display', Georgia, serif !important;
        font-weight: 700 !important;
    }

    .streamlit-expanderHeader {
        background: #FFFFFF !important;
        border: 1px solid #000000 !important;
        border-radius: 0 !important;
        color: #000000 !important;
        box-shadow: none !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.78rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.08em !important;
        text-transform: uppercase !important;
    }

    [data-testid="stDataEditor"] {
        border: 1px solid #000000 !important;
        border-radius: 0 !important;
        overflow: hidden;
        box-shadow: none !important;
    }

    .page-header {
        position: relative;
        overflow: hidden;
        padding: 2.5rem 1.5rem 1.6rem;
        margin-bottom: 2rem;
        background: #000000;
        color: #FFFFFF;
        border: 2px solid #000000;
        border-radius: 0;
        box-shadow: none;
    }
    .page-header::before {
        content: "";
        position: absolute;
        inset: 0;
        background-image: repeating-linear-gradient(90deg, transparent, transparent 1px, rgba(255, 255, 255, 0.06) 1px, rgba(255, 255, 255, 0.06) 2px);
        opacity: 0.12;
        pointer-events: none;
    }
    .page-header-title {
        position: relative;
        z-index: 1;
        font-family: 'Playfair Display', Georgia, serif;
        font-size: clamp(3.25rem, 7vw, 7rem);
        line-height: 1.2;
        letter-spacing: -0.05em;
        font-weight: 800;
        margin: 0 0 0.85rem;
        color: #FFFFFF;
    }
    .page-header-desc {
        position: relative;
        z-index: 1;
        font-family: 'Source Serif 4', Georgia, serif;
        font-size: 1.05rem;
        line-height: 1.8;
        color: rgba(255, 255, 255, 0.9);
        max-width: 780px;
        margin: 0;
    }
    .page-header-tags { position: relative; z-index: 1; margin-top: 1.25rem; display: flex; gap: 0.5rem; flex-wrap: wrap; }

    .tag,
    .tag-accent,
    .tag-green,
    .tag-red {
        display: inline-block;
        padding: 0.3rem 0.8rem;
        border-radius: 0 !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.65rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        border: 1px solid #000000;
        background: #FFFFFF;
        color: #000000;
    }
    .tag-accent,
    .tag-green,
    .tag-red {
        background: #000000;
        color: #FFFFFF;
    }

    .section-label,
    .tab-section-title,
    .compare-label,
    .metrics-col-header,
    .db-section-title,
    .sidebar-app-name {
        font-family: 'JetBrains Mono', monospace !important;
        text-transform: uppercase;
        letter-spacing: 0.12em;
    }

    .section-label {
        font-size: 0.72rem;
        font-weight: 700;
        color: #000000;
        margin-bottom: 0.55rem;
        padding-bottom: 0.35rem;
        border-bottom: 2px solid #000000;
    }

    .input-hint,
    .page-header-desc,
    .tab-section-desc,
    .metrics-compare-header,
    .db-connected-badge,
    .pipeline-step .step-body .step-desc,
    .compare-label,
    .catalog-entry .ce-cols,
    .catalog-entry .ce-rows,
    .schema-info {
        color: #525252 !important;
    }

    .metric-card,
    .catalog-entry,
    .schema-info,
    .tree-container,
    .streamlit-expanderHeader,
    .surface-chip {
        background: #FFFFFF !important;
        border: 1px solid #000000 !important;
        border-radius: 0 !important;
        box-shadow: none !important;
    }
    .metric-card {
        padding: 1rem;
    }
    .metric-card-value,
    .metric-card-value.accent,
    .metric-card-value.amber,
    .metric-card-value.success,
    .metric-card-value.green,
    .metric-card-value.sm {
        color: #000000 !important;
        font-family: 'Playfair Display', Georgia, serif !important;
        font-weight: 700 !important;
    }
    .metric-card-label {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.65rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #525252;
    }

    .metric-card {
        transition: background 100ms linear, color 100ms linear;
        cursor: default;
    }
    .metric-card:hover {
        background: #000000 !important;
    }
    .metric-card:hover .metric-card-value,
    .metric-card:hover .metric-card-value.accent,
    .metric-card:hover .metric-card-value.amber,
    .metric-card:hover .metric-card-value.success,
    .metric-card:hover .metric-card-value.green,
    .metric-card:hover .metric-card-value.sm,
    .metric-card:hover .metric-card-label {
        color: #FFFFFF !important;
    }

    .catalog-entry {
        transition: background 100ms linear, color 100ms linear;
        cursor: default;
    }
    .catalog-entry:hover {
        background: #000000 !important;
    }
    .catalog-entry:hover .ce-name,
    .catalog-entry:hover .ce-rows,
    .catalog-entry:hover .ce-cols {
        color: #FFFFFF !important;
    }

    .tree-container {
        position: relative;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.84rem !important;
        line-height: 1.9 !important;
        background: #FFFFFF !important;
        color: #000000 !important;
        padding: 1.4rem 1.6rem !important;
        border: 2px solid #000000 !important;
        border-radius: 0 !important;
        box-shadow: none !important;
        white-space: pre !important;
        overflow-x: auto !important;
        margin-bottom: 1rem !important;
    }

    .sql-unparser-header {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 0.5rem;
        margin-top: 1.5rem;
    }
    .sql-unparser-badge,
    .db-connected-badge,
    .metrics-badge {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border: 1px solid #000000;
        border-radius: 0 !important;
        background: #FFFFFF;
        color: #000000;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.65rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
    }

    .metrics-compare-header {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 1rem;
    }
    .metrics-col-header.unopt,
    .metrics-col-header.opt {
        background: #000000 !important;
        color: #FFFFFF !important;
        border: 1px solid #000000 !important;
    }

    .catalog-entry {
        padding: 0.8rem 0.9rem;
        margin-bottom: 0.45rem;
    }
    .catalog-entry .ce-name {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.76rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 0.25rem;
    }
    .catalog-entry .ce-rows,
    .catalog-entry .ce-cols {
        font-family: 'Source Serif 4', Georgia, serif;
        font-size: 0.9rem;
    }

    .pipeline-step {
        display: flex;
        align-items: flex-start;
        gap: 0.75rem;
        padding: 0.6rem 0;
        border-bottom: 1px solid #000000;
        font-size: 0.85rem;
    }
    .pipeline-step:last-child { border-bottom: none; }
    .pipeline-step .step-num {
        min-width: 2.5rem;
        font-family: 'JetBrains Mono', monospace;
        font-weight: 700;
        letter-spacing: 0.12em;
        color: #000000;
    }
    .pipeline-step .step-body .step-title {
        font-family: 'Playfair Display', Georgia, serif;
        font-size: 1rem;
        font-weight: 700;
        line-height: 1.25;
        color: #000000;
    }

    .tab-section-title {
        font-size: 0.78rem;
        font-weight: 700;
        color: #000000;
        margin-bottom: 0.5rem;
        margin-top: 1.5rem;
    }
    .tab-section-desc {
        font-family: 'Source Serif 4', Georgia, serif;
        font-size: 1rem;
        color: #525252;
        line-height: 1.85;
        margin-bottom: 1rem;
        max-width: 760px;
    }
    .tab-section-desc code {
        font-family: 'JetBrains Mono', monospace;
        background: #FFFFFF;
        color: #000000;
        padding: 0 0.2rem;
        border: 1px solid #000000;
        border-radius: 0;
    }

    .compare-label {
        font-size: 0.68rem;
        font-weight: 700;
        color: #000000;
        margin-bottom: 0.5rem;
        padding-bottom: 0.35rem;
        border-bottom: 1px solid #000000;
    }

    .surface-chip {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        padding: 0.25rem 0.6rem;
    }

    .stTabs [data-baseweb="tab-highlight"],
    .stTabs [data-baseweb="tab-border"] { display: none !important; }

    /* ── Pipeline step desc line height ──────────────────────────── */
    .pipeline-step .step-body .step-desc {
        line-height: 1.6;
        margin-top: 0.15rem;
    }
    .pipeline-step .step-body .step-title {
        line-height: 1.35;
    }

    /* ── Page header desc line height ─────────────────────────────── */
    .page-header-desc {
        line-height: 1.85;
    }

    /* ── Expander hover ─────────────────────────────────────────── */
    .streamlit-expanderHeader {
        transition: background 100ms linear, color 100ms linear !important;
    }
    .streamlit-expanderHeader:hover {
        background: #000000 !important;
        color: #FFFFFF !important;
    }

    /* ── Tag hover ───────────────────────────────────────────────── */
    .tag, .tag-accent, .tag-green, .tag-red {
        transition: background 100ms linear, color 100ms linear;
        cursor: default;
    }
    .tag:hover {
        background: #000000 !important;
        color: #FFFFFF !important;
    }

    /* ── Responsive ──────────────────────────────────────────────── */
    @media (max-width: 900px) {
        .page-header-title {
            font-size: clamp(2.5rem, 13vw, 4.5rem);
        }
        .page-header {
            padding: 2rem 1rem 1.25rem;
        }
        /* Stack metric cards */
        [data-testid="stHorizontalBlock"] {
            flex-wrap: wrap !important;
        }
        /* Reduce tab font for narrow screens */
        .stTabs [data-baseweb="tab"] {
            padding: 0.65rem 0.65rem !important;
            font-size: 0.68rem !important;
            letter-spacing: 0.05em !important;
        }
        /* Tree container: allow horizontal scroll */
        .tree-container {
            font-size: 0.76rem !important;
            padding: 1rem 1rem !important;
        }
        /* Full-width metric cards on mobile */
        .metric-card {
            min-width: 120px;
        }
        /* Tab section descriptions: reduce size slightly */
        .tab-section-desc {
            font-size: 0.93rem !important;
            line-height: 1.75 !important;
        }
        /* Pipeline steps: tighten */
        .pipeline-step {
            font-size: 0.8rem !important;
        }
    }

    @media (max-width: 640px) {
        .page-header-title {
            font-size: clamp(2rem, 16vw, 3.5rem);
        }
        .page-header {
            padding: 1.5rem 0.85rem 1rem;
        }
        .page-header-desc {
            font-size: 0.93rem;
        }
        .page-header-tags {
            gap: 0.35rem;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 0.55rem 0.45rem !important;
            font-size: 0.62rem !important;
            letter-spacing: 0.03em !important;
        }
        .tab-section-desc {
            font-size: 0.88rem !important;
        }
        .tree-container {
            font-size: 0.7rem !important;
            padding: 0.85rem 0.85rem !important;
        }
        .metric-card-value {
            font-size: 1.35rem !important;
        }
        .main .block-container {
            padding-left: 0.85rem !important;
            padding-right: 0.85rem !important;
        }
    }
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
        <div class="sidebar-app-version">SQL Engine Simulator v3.0</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # ── Live DB Connection ───────────────────────────────────────────────
    st.markdown("<div class='section-label'>Live DB Connection</div>", unsafe_allow_html=True)

    if db_manager and db_manager.is_connected:
        st.markdown(
            f"<div class='db-connected-badge'>Connected {db_manager.database}</div>",
            unsafe_allow_html=True,
        )
        if st.button("Disconnect", key="btn_disconnect", use_container_width=True):
            db_manager.disconnect()
            st.session_state["db_manager"] = None
            st.rerun()
    else:
        # Credential inputs — pre-filled from .env defaults
        import os
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

        db_host = st.text_input("Host",     value=os.getenv("DB_HOST", "localhost"), key="db_host")
        db_port = st.number_input("Port",   value=int(os.getenv("DB_PORT", "3306")), min_value=1, max_value=65535, key="db_port")
        db_user = st.text_input("User",     value=os.getenv("DB_USER", "root"),      key="db_user")
        db_pass = st.text_input("Password", value=os.getenv("DB_PASSWORD", ""),      key="db_pass", type="password")
        db_name = st.text_input("Database", value=os.getenv("DB_NAME", ""),          key="db_name")

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
        "<p style='color:#525252;font-size:0.78rem;margin-bottom:0.7rem;line-height:1.65'>"
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
            f"<p style='font-size:0.72rem;color:#525252;margin:0.3rem 0 0.6rem'>"
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
        <div class="page-header-title">Mini Query Optimizer</div>
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
    "<p style='color:#525252;font-size:0.9rem;margin-bottom:0.65rem;line-height:1.75'>"
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

col_btn, col_hint = st.columns([2, 4])
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
                "<div class='tab-section-desc'>Raw relational-algebra tree from the parser. "
                "The <code>WHERE</code> filter sits high — no optimization applied yet.</div>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{logical_str}</div>', unsafe_allow_html=True)

        # ──────────────────────────────────────────────────────────────────
        # Tab 2: After RBO
        # ──────────────────────────────────────────────────────────────────
        with tab2:
            st.markdown(
                "<div class='tab-section-title'>After Predicate Pushdown</div>"
                "<div class='tab-section-desc'>Filters pushed to scan level — "
                "fewer rows flow into expensive JOINs.</div>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{pred_push_str}</div>', unsafe_allow_html=True)

            if predicate_rules:
                with st.expander(f"Predicate Rules Fired ({len(predicate_rules)})"):
                    for i, r in enumerate(predicate_rules, 1):
                        st.markdown(
                            f"<div style='font-size:0.85rem;padding:0.4rem 0;"
                            f"border-bottom:1px solid #000000;color:#525252;line-height:1.65'>"
                            f"<strong style='color:#000000;margin-right:0.5rem'>{i}.</strong>{r}</div>",
                            unsafe_allow_html=True,
                        )

            st.markdown("---")
            st.markdown(
                "<div class='tab-section-title'>After Projection Pushdown</div>"
                "<div class='tab-section-desc'>Narrow <code>ProjectNode</code>s inserted above "
                "scans — unused columns dropped as early as possible.</div>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{proj_push_str}</div>', unsafe_allow_html=True)

            if projection_rules:
                with st.expander(f"Projection Rules Fired ({len(projection_rules)})"):
                    for i, r in enumerate(projection_rules, 1):
                        st.markdown(
                            f"<div style='font-size:0.85rem;padding:0.4rem 0;"
                            f"border-bottom:1px solid #000000;color:#525252;line-height:1.65'>"
                            f"<strong style='color:#000000;margin-right:0.5rem'>{i}.</strong>{r}</div>",
                            unsafe_allow_html=True,
                        )

        # ──────────────────────────────────────────────────────────────────
        # Tab 3: Physical Plan + SQL Unparser
        # ──────────────────────────────────────────────────────────────────
        with tab3:
            st.markdown(
                "<div class='tab-section-title'>Final Optimized Physical Plan</div>"
                "<div class='tab-section-desc'>CBO reorders joins for minimum intermediate size. "
                "All RBO <code>Filter</code> and <code>Project</code> nodes are preserved.</div>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{physical_str}</div>', unsafe_allow_html=True)

            st.markdown("---")

            # SQL Unparser
            st.markdown(
                "<div class='sql-unparser-header'>"
                "<div class='tab-section-title' style='margin:0'>Equivalent SQL</div>"
                "<span class='sql-unparser-badge'>SQL Unparser</span></div>"
                "<div class='tab-section-desc'>Optimized plan tree recursively traversed to "
                "regenerate valid MySQL SQL. Each nested operator → subquery with unique "
                "<code>subq_N</code> alias.</div>",
                unsafe_allow_html=True,
            )
            st.code(optimized_sql, language="sql")

            st.markdown("---")
            st.markdown(
                "<div class='tab-section-title'>Side-by-Side Comparison</div>"
                "<div class='tab-section-desc'>Original logical plan vs final optimized physical plan.</div>",
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
                "<div class='tab-section-desc'>Edit the catalog the CBO uses for cost estimation. "
                "Changes take effect on the next optimization run. "
                "Use 'Connect & Sync Catalog' in the sidebar to auto-populate from a live DB.</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                "<div class='schema-info'>"
                "📝 <strong>How to use:</strong> Edit row counts or column lists in-place. "
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
                    "<div class='tab-section-desc'>"
                    "Both the original SQL and the optimizer-generated SQL were executed on the "
                    "live MySQL instance. Metrics are compared side-by-side.<br>"
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
                        "Exec Time</div>",
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
                        delta=f"{delta_r:+,}" if delta_r != 0 else "✓ Match",
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
                    "<div class='tab-section-desc'>The exact SQL strings sent to MySQL.</div>",
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
                "<div class='tab-section-desc'>Raw representations for inspection.</div>",
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
        "<div style='margin-top:2rem;padding:1.5rem 1.75rem;"
        "background:#F5F5F5;border:2px solid #000000;"
        "font-family:&quot;Source Serif 4&quot;,Georgia,serif;"
        "color:#525252;font-size:0.95rem;line-height:1.8'>"
        "Enter an SQL query above and click <strong style='color:#000000'>"
        "Optimize Query</strong> to start. "
        "Connect a MySQL database in the sidebar for live execution benchmarks."
        "</div>",
        unsafe_allow_html=True,
    )