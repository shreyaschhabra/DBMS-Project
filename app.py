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
import os
import traceback
from typing import Optional

import streamlit as st

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

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
# Session-state singletons
# ─────────────────────────────────────────────────────────────────────────────

if "catalog" not in st.session_state:
    st.session_state["catalog"] = Catalog()

if "db_manager" not in st.session_state:
    st.session_state["db_manager"] = None

catalog: Catalog                       = st.session_state["catalog"]
db_manager: Optional[DatabaseManager] = st.session_state["db_manager"]

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
                    result = mgr.connect()
                    if result["status"] != "success":
                        st.error(f"Connection failed: {result['message']}")
                    else:
                        updated_catalog, n_tables = mgr.sync_schema_to_catalog(catalog)
                        st.session_state["catalog"]    = updated_catalog
                        st.session_state["db_manager"] = mgr
                        st.success(f"Connected! {n_tables} table(s) synced into catalog.")
                        st.rerun()
                except Exception as e:
                    st.error(f"Unexpected error: {e}")

    st.markdown("---")

    # ── Catalog viewer ───────────────────────────────────────────────────
    st.markdown("<div class='section-label'>Database Catalog</div>", unsafe_allow_html=True)
    st.markdown(
        "<p style='color:rgba(255,255,255,0.55);font-size:0.75rem;margin-bottom:0.7rem;line-height:1.65;"
        "font-family:JetBrains Mono,monospace;font-weight:500;letter-spacing:0.04em'>"
        "Live statistics used by the CBO. Edit in the Schema tab.</p>",
        unsafe_allow_html=True,
    )

    all_stats = catalog.get_all_stats()
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
            f"<p style='font-size:0.68rem;color:rgba(255,255,255,0.45);margin:0.3rem 0 0.6rem;"
            f"font-family:JetBrains Mono,monospace;font-weight:700;letter-spacing:0.08em;text-transform:uppercase'>"
            f"+ {len(all_stats)-8} more — see Schema tab</p>",
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

db_manager = st.session_state.get("db_manager")
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
    "<p style='color:rgba(255,255,255,0.75);font-size:0.88rem;margin-bottom:0.65rem;line-height:1.65;"
    "font-family:DM Sans,sans-serif;font-weight:500'>"
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
        with st.spinner("Parsing SQL…"):
            logical_tree = parser.parse(sql_input)

        with st.spinner("Predicate Pushdown (RBO pass 1)…"):
            rbo = RuleBasedOptimizer(catalog=catalog)
            tree_after_predpush = rbo._apply_predicate_pushdown(copy.deepcopy(logical_tree))
            predicate_rules = list(rbo._predicate_rules)

        with st.spinner("Projection Pushdown (RBO pass 2)…"):
            rbo._projection_rules = []
            rbo_tree = rbo._apply_projection_pushdown(copy.deepcopy(tree_after_predpush))
            projection_rules = list(rbo._projection_rules)

        with st.spinner("Cost-Based Join Reordering…"):
            cbo        = CostBasedOptimizer(catalog=catalog)
            cbo_result = cbo.optimize(copy.deepcopy(rbo_tree))

        with st.spinner("Unparsing optimized tree to SQL…"):
            try:
                optimized_sql = QueryExecutor.sanitize_for_mysql(
                    cbo_result.plan.to_sql()
                )
            except Exception as unparse_err:
                optimized_sql = f"-- SQL Unparser error: {unparse_err}"

        bench_unopt: Optional[dict] = None
        bench_opt:   Optional[dict] = None

        if live_db:
            with st.spinner("Benchmarking unoptimized SQL on MySQL…"):
                exe = QueryExecutor(db_manager, row_limit=10_000)
                bench_unopt = exe.benchmark_query(sql_input)
            if not optimized_sql.startswith("--"):
                with st.spinner("Benchmarking optimized SQL on MySQL…"):
                    bench_opt = exe.benchmark_query(optimized_sql)

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

        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "Logical Plan", "After RBO", "Physical Plan",
            "Schema Editor", "Live Metrics", "Debug",
        ])

        with tab1:
            st.markdown(
                "<div class='tab-section-title'>Unoptimized Logical Plan</div>"
                "<div class='tab-section-desc'>Raw relational-algebra tree from the parser. "
                "The <code>WHERE</code> filter sits high — no optimization applied yet.</div>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{logical_str}</div>', unsafe_allow_html=True)

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
                            f"<div style='font-size:0.83rem;padding:0.4rem 0;"
                            f"border-bottom:3px solid #000000;color:#000000;line-height:1.65;"
                            f"font-family:Outfit,sans-serif;font-weight:500'>"
                            f"<strong style='color:var(--max-tertiary);margin-right:0.5rem;font-weight:900'>{i}.</strong>{r}</div>",
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
                            f"<div style='font-size:0.83rem;padding:0.4rem 0;"
                            f"border-bottom:3px solid #000000;color:#000000;line-height:1.65;"
                            f"font-family:Outfit,sans-serif;font-weight:500'>"
                            f"<strong style='color:var(--max-tertiary);margin-right:0.5rem;font-weight:900'>{i}.</strong>{r}</div>",
                            unsafe_allow_html=True,
                        )

        with tab3:
            st.markdown(
                "<div class='tab-section-title'>Final Optimized Physical Plan</div>"
                "<div class='tab-section-desc'>CBO reorders joins for minimum intermediate size. "
                "All RBO <code>Filter</code> and <code>Project</code> nodes are preserved.</div>",
                unsafe_allow_html=True,
            )
            st.markdown(f'<div class="tree-container">{physical_str}</div>', unsafe_allow_html=True)

            st.markdown("---")
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

        with tab4:
            st.markdown(
                "<div class='tab-section-title'>Dynamic Schema Editor</div>"
                "<div class='tab-section-desc'>Edit the catalog the CBO uses for cost estimation. "
                "Changes take effect on the next optimization run. "
                "Use 'Connect &amp; Sync Catalog' in the sidebar to auto-populate from a live DB.</div>",
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
                                reconnect = db_manager.ensure_connected()
                                if reconnect["status"] != "success":
                                    st.error(f"Reconnection failed: {reconnect['message']}")
                                else:
                                    updated, n = db_manager.sync_schema_to_catalog(catalog)
                                    st.session_state["catalog"] = updated
                                    st.success(f"Synced {n} tables from {db_manager.database}.")
                                    st.rerun()
                            except Exception as e:
                                st.error(str(e))
                else:
                    st.markdown(
                        "<p style='font-size:0.72rem;color:rgba(255,255,255,0.45);padding-top:0.5rem;"
                        "font-family:JetBrains Mono,monospace;font-weight:500;letter-spacing:0.06em'>"
                        "Connect a DB to enable sync.</p>",
                        unsafe_allow_html=True,
                    )

            st.markdown("---")
            st.markdown("<div class='section-label' style='margin-bottom:0.4rem'>Current Live Catalog</div>", unsafe_allow_html=True)
            st.json(catalog.get_all_stats())

        with tab5:
            if not live_db:
                st.markdown(
                    "<div style='margin-top:1.5rem;padding:1.75rem;background:rgba(45,27,78,0.6);"
                    "border:4px solid #000000;text-align:center;"
                    "box-shadow:8px 8px 0px 0px #000000'>"
                    "<div style='font-weight:900;color:#FFFFFF;margin-bottom:0.4rem;"
                    "font-family:Outfit,sans-serif;font-size:1rem;text-transform:uppercase;letter-spacing:0.1em'>"
                    "No Live Database Connected</div>"
                    "<div style='font-size:0.85rem;color:rgba(255,255,255,0.75);font-family:DM Sans,sans-serif;font-weight:500'>"
                    "Enter MySQL credentials in the sidebar and click "
                    "<strong>Connect &amp; Sync Catalog</strong> to enable query benchmarking.</div>"
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

                if bench_unopt and bench_unopt.get("error"):
                    st.error(f"Unoptimized query error: {bench_unopt['error']}")
                if bench_opt and bench_opt.get("error"):
                    st.error(f"Optimized query error: {bench_opt['error']}")

                _, hc1, hc2 = st.columns([0.8, 2, 2])
                with hc1:
                    st.markdown("<div class='metrics-col-header unopt'>Unoptimized SQL</div>", unsafe_allow_html=True)
                with hc2:
                    st.markdown("<div class='metrics-col-header opt'>Optimized SQL</div>", unsafe_allow_html=True)

                def safe_get(d, key, default=0):
                    return d.get(key, default) if d else default

                t_unopt = safe_get(bench_unopt, "execution_time_ms")
                t_opt   = safe_get(bench_opt,   "execution_time_ms")
                r_unopt = safe_get(bench_unopt, "rows_returned")
                r_opt   = safe_get(bench_opt,   "rows_returned")
                c_unopt = safe_get(bench_unopt, "mysql_cost")
                c_opt   = safe_get(bench_opt,   "mysql_cost")

                row_label, mc1, mc2 = st.columns([0.8, 2, 2])
                with row_label:
                    st.markdown(
                        "<div style='font-size:0.68rem;font-weight:800;color:var(--max-secondary);"
                        "text-transform:uppercase;letter-spacing:0.18em;padding-top:1.2rem;"
                        "font-family:Outfit,sans-serif;text-shadow:0 0 8px rgba(0,245,212,0.4)'>"
                        "Exec Time</div>",
                        unsafe_allow_html=True,
                    )
                with mc1:
                    st.metric(label="Unoptimized — Execution Time", value=f"{t_unopt:.1f} ms", label_visibility="collapsed")
                with mc2:
                    delta_t = t_opt - t_unopt
                    st.metric(label="Optimized — Execution Time", value=f"{t_opt:.1f} ms", delta=f"{delta_t:+.1f} ms", delta_color="inverse", label_visibility="collapsed")

                row_label2, mc3, mc4 = st.columns([0.8, 2, 2])
                with row_label2:
                    st.markdown(
                        "<div style='font-size:0.68rem;font-weight:800;color:var(--max-secondary);"
                        "text-transform:uppercase;letter-spacing:0.18em;padding-top:1.2rem;"
                        "font-family:Outfit,sans-serif;text-shadow:0 0 8px rgba(0,245,212,0.4)'>"
                        "Rows</div>",
                        unsafe_allow_html=True,
                    )
                with mc3:
                    st.metric(label="Unoptimized — Rows", value=f"{r_unopt:,}", label_visibility="collapsed")
                with mc4:
                    delta_r = r_opt - r_unopt
                    st.metric(label="Optimized — Rows", value=f"{r_opt:,}", delta=f"{delta_r:+,}" if delta_r != 0 else "Match", delta_color="off" if delta_r == 0 else "normal", label_visibility="collapsed")

                row_label3, mc5, mc6 = st.columns([0.8, 2, 2])
                with row_label3:
                    st.markdown(
                        "<div style='font-size:0.68rem;font-weight:800;color:var(--max-secondary);"
                        "text-transform:uppercase;letter-spacing:0.18em;padding-top:1.2rem;"
                        "font-family:Outfit,sans-serif;text-shadow:0 0 8px rgba(0,245,212,0.4)'>"
                        "MySQL Cost</div>",
                        unsafe_allow_html=True,
                    )
                with mc5:
                    st.metric(label="Unoptimized — MySQL Cost", value=f"{c_unopt:,.2f}" if c_unopt else "N/A", label_visibility="collapsed")
                with mc6:
                    delta_c = c_opt - c_unopt if c_opt and c_unopt else 0
                    st.metric(label="Optimized — MySQL Cost", value=f"{c_opt:,.2f}" if c_opt else "N/A", delta=f"{delta_c:+,.2f}" if delta_c else None, delta_color="inverse", label_visibility="collapsed")

                st.markdown("---")
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
        "<div style='margin-top:2rem;padding:1.75rem 2rem;"
        "background:#FFFDF5;border:4px solid #000000;"
        "box-shadow:8px 8px 0px 0px #000000;"
        "font-family:Outfit,sans-serif;"
        "color:rgba(255,255,255,0.75);font-size:0.95rem;line-height:1.75;font-weight:400'>"
        "Enter an SQL query above and click <strong style='color:var(--max-tertiary);font-weight:900'>"
        "Optimize Query</strong> to start. "
        "Connect a MySQL database in the sidebar for live execution benchmarks."
        "</div>",
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────────────────────────────────────
# CSS — single block, injected last so it wins by load order, zero !important
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;700;800;900&family=DM+Sans:wght@400;500;700&family=Bungee&family=JetBrains+Mono:wght@400;500;700&display=swap');

    /* ── Design tokens ──────────────────────────────────────────────── */
    :root {
        --max-bg:        #0D0D1A;
        --max-fg:        #FFFFFF;
        --max-muted:     #2D1B4E;
        --max-border:    #FF3AF2;
        --max-accent:    #FF3AF2;
        --max-secondary: #00F5D4;
        --max-tertiary:  #FFE600;
        --max-quaternary:#FF6B35;
        --max-quinary:   #7B2FFF;
        --sidebar-bg:    #0D0D1A;
        --sidebar-fg:    #FFFFFF;
        --glow-sm:       0 0 20px rgba(255,58,242,0.5), 0 0 40px rgba(0,245,212,0.3);
        --glow-lg:       0 0 40px rgba(255,58,242,0.6), 0 0 80px rgba(255,230,0,0.4), 0 0 120px rgba(123,47,255,0.3);
        --shadow-hard:   8px 8px 0 #FFE600, 16px 16px 0 #FF3AF2;
        --shadow-hard-lg:12px 12px 0 #00F5D4, 24px 24px 0 #FF3AF2, 36px 36px 0 #FFE600;
    }

    /* ── Keyframe Animations ────────────────────────────────────────── */
    @keyframes float {
        0%, 100% { transform: translateY(0) rotate(0deg); }
        50%       { transform: translateY(-18px) rotate(4deg); }
    }
    @keyframes float-reverse {
        0%, 100% { transform: translateY(0) rotate(0deg); }
        50%       { transform: translateY(18px) rotate(-4deg); }
    }
    @keyframes pulse-glow {
        0%, 100% { box-shadow: 0 0 20px rgba(255,58,242,0.5), 0 0 40px rgba(0,245,212,0.3); }
        50%       { box-shadow: 0 0 40px rgba(255,58,242,0.8), 0 0 80px rgba(0,245,212,0.5), 0 0 100px rgba(123,47,255,0.4); }
    }
    @keyframes gradient-shift {
        0%   { background-position: 0% 50%; }
        50%  { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    @keyframes spin-slow {
        from { transform: rotate(0deg); }
        to   { transform: rotate(360deg); }
    }
    @keyframes wiggle {
        0%, 100% { transform: rotate(-3deg); }
        50%       { transform: rotate(3deg); }
    }
    @keyframes bounce-subtle {
        0%, 100% { transform: translateY(0); }
        50%       { transform: translateY(-10px); }
    }

    /* ── Global reset ───────────────────────────────────────────────── */
    html, body, .stApp {
        font-family: 'DM Sans', sans-serif;
        background: var(--max-bg);
        color: var(--max-fg);
        -webkit-font-smoothing: antialiased;
    }

    body {
        background-color: var(--max-bg);
        background-image:
            radial-gradient(circle, rgba(255,58,242,0.8) 1px, transparent 1px),
            repeating-linear-gradient(
                45deg,
                transparent,
                transparent 10px,
                rgba(255,230,0,0.04) 10px,
                rgba(255,230,0,0.04) 20px
            );
        background-size: 28px 28px, 100% 100%;
        background-attachment: fixed;
    }

    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 4rem;
        max-width: 1220px;
    }

    /* ── HR ──────────────────────────────────────────────────────────── */
    hr {
        border: none;
        border-top: 4px dashed var(--max-accent);
        margin: 1.5rem 0;
        opacity: 0.6;
    }

    /* ── Sidebar ────────────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: var(--sidebar-bg);
        border-right: 4px solid var(--max-accent);
        box-shadow: 0 0 40px rgba(255,58,242,0.4), 0 0 80px rgba(123,47,255,0.2);
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
        color: var(--sidebar-fg);
    }

    section[data-testid="stSidebar"] div.db-connected-badge {
        color: #0D0D1A;
        background: var(--max-tertiary);
        border-color: var(--max-secondary);
    }

    section[data-testid="stSidebar"] hr {
        border: none;
        border-top: 2px dashed rgba(255,58,242,0.4);
        margin: 1rem 0;
    }

    /* Sidebar buttons */
    section[data-testid="stSidebar"] .stButton > button {
        background: linear-gradient(135deg, var(--max-accent), var(--max-quinary), var(--max-secondary));
        background-size: 200% 200%;
        color: #FFFFFF;
        border: 3px solid var(--max-tertiary);
        border-radius: 999px;
        font-family: 'Outfit', sans-serif;
        font-size: 0.75rem;
        font-weight: 800;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        box-shadow: 0 0 20px rgba(255,58,242,0.5), 4px 4px 0 var(--max-tertiary);
        transition: transform 200ms cubic-bezier(0.68,-0.55,0.265,1.55), box-shadow 200ms ease-out;
        animation: gradient-shift 4s ease infinite;
    }
    section[data-testid="stSidebar"] .stButton > button p,
    section[data-testid="stSidebar"] .stButton > button span,
    section[data-testid="stSidebar"] .stButton > button div {
        color: #FFFFFF;
    }
    section[data-testid="stSidebar"] .stButton > button:hover {
        transform: scale(1.05);
        box-shadow: 0 0 30px rgba(255,58,242,0.7), 0 0 60px rgba(0,245,212,0.4), 6px 6px 0 var(--max-tertiary);
    }
    section[data-testid="stSidebar"] .stButton > button:active {
        transform: scale(0.96);
        box-shadow: 0 0 10px rgba(255,58,242,0.3);
    }
    section[data-testid="stSidebar"] .stButton > button:hover p,
    section[data-testid="stSidebar"] .stButton > button:hover span,
    section[data-testid="stSidebar"] .stButton > button:hover div {
        color: #FFFFFF;
    }

    /* Sidebar inputs */
    section[data-testid="stSidebar"] [data-baseweb="input"],
    section[data-testid="stSidebar"] [data-baseweb="base-input"] {
        background: rgba(45,27,78,0.6);
        border: 3px solid var(--max-secondary);
        border-radius: 12px;
    }
    section[data-testid="stSidebar"] [data-baseweb="input"] input,
    section[data-testid="stSidebar"] [data-baseweb="base-input"] input,
    section[data-testid="stSidebar"] input[type="text"],
    section[data-testid="stSidebar"] input[type="password"],
    section[data-testid="stSidebar"] input[type="number"] {
        background: rgba(45,27,78,0.6);
        color: #FFFFFF;
        -webkit-text-fill-color: #FFFFFF;
        border: 3px solid var(--max-secondary);
        border-radius: 12px;
        box-shadow: 0 0 12px rgba(0,245,212,0.3);
        min-height: 3.25rem;
        font-family: 'JetBrains Mono', monospace;
        font-weight: 700;
    }
    section[data-testid="stSidebar"] input::placeholder,
    section[data-testid="stSidebar"] input::-webkit-input-placeholder {
        color: rgba(255,255,255,0.35);
        -webkit-text-fill-color: rgba(255,255,255,0.35);
        opacity: 1;
    }
    section[data-testid="stSidebar"] input[type="text"]:focus,
    section[data-testid="stSidebar"] input[type="password"]:focus,
    section[data-testid="stSidebar"] input[type="number"]:focus {
        border-color: var(--max-accent);
        outline: none;
        box-shadow: 0 0 20px rgba(255,58,242,0.5), 0 0 40px rgba(0,245,212,0.3);
    }

    /* ── Main inputs + textarea ──────────────────────────────────────── */
    input[type="text"],
    input[type="password"],
    input[type="number"],
    textarea {
        font-family: 'JetBrains Mono', monospace;
        font-weight: 700;
        background: rgba(45,27,78,0.7);
        color: #FFFFFF;
        border: 4px solid var(--max-accent);
        border-radius: 16px;
        box-shadow: 0 0 20px rgba(255,58,242,0.4), 8px 8px 0 var(--max-tertiary);
    }
    input[type="text"],
    input[type="password"],
    input[type="number"] { min-height: 3.25rem; }
    input[type="text"]:focus,
    input[type="password"]:focus,
    input[type="number"]:focus,
    textarea:focus {
        border-color: var(--max-secondary);
        outline: none;
        box-shadow: 0 0 30px rgba(0,245,212,0.6), 0 0 60px rgba(255,58,242,0.3), 8px 8px 0 var(--max-tertiary);
        background: rgba(45,27,78,0.9);
    }

    /* ── Main buttons ────────────────────────────────────────────────── */
    .stButton > button {
        background: linear-gradient(90deg, var(--max-accent), var(--max-quinary), var(--max-secondary));
        background-size: 200% 200%;
        color: #FFFFFF;
        border: 4px solid var(--max-tertiary);
        border-radius: 999px;
        padding: 0.75rem 1.75rem;
        font-family: 'Outfit', sans-serif;
        font-size: 0.82rem;
        font-weight: 800;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        box-shadow: 0 0 20px rgba(255,58,242,0.5), 8px 8px 0 var(--max-tertiary), 16px 16px 0 var(--max-accent);
        transition: transform 200ms cubic-bezier(0.68,-0.55,0.265,1.55), box-shadow 200ms ease-out;
        white-space: nowrap;
        animation: gradient-shift 4s ease infinite;
        will-change: transform;
    }
    .stButton > button p,
    .stButton > button span,
    .stButton > button div { color: #FFFFFF; }
    .stButton > button:hover {
        transform: scale(1.08) translateY(-2px);
        box-shadow: 0 0 40px rgba(255,58,242,0.7), 0 0 80px rgba(0,245,212,0.4), 10px 10px 0 var(--max-tertiary), 20px 20px 0 var(--max-accent);
        color: #FFFFFF;
        border-color: var(--max-tertiary);
    }
    .stButton > button:hover p,
    .stButton > button:hover span,
    .stButton > button:hover div { color: #FFFFFF; }
    .stButton > button:active {
        transform: scale(0.96);
        box-shadow: 0 0 12px rgba(255,58,242,0.4);
    }

    /* ── Code blocks ─────────────────────────────────────────────────── */
    .stCodeBlock pre, .stCodeBlock code, pre, code {
        font-family: 'JetBrains Mono', monospace;
        background: rgba(45,27,78,0.8);
        color: var(--max-secondary);
        border: 4px solid var(--max-quinary);
        border-radius: 16px;
        box-shadow: 0 0 20px rgba(123,47,255,0.4), 8px 8px 0 var(--max-accent);
    }

    /* ── Tabs ────────────────────────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        background: rgba(45,27,78,0.5);
        border-bottom: 4px solid var(--max-accent);
        gap: 4px;
        padding: 4px 4px 0;
        border-radius: 16px 16px 0 0;
    }
    .stTabs [data-baseweb="tab"] {
        background: rgba(45,27,78,0.6);
        color: rgba(255,255,255,0.7);
        border: 3px solid var(--max-quinary);
        border-bottom: none;
        border-radius: 12px 12px 0 0;
        padding: 0.8rem 1.1rem;
        font-family: 'Outfit', sans-serif;
        font-size: 0.75rem;
        font-weight: 800;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        transition: background 200ms ease-out, color 200ms ease-out, box-shadow 200ms ease-out, transform 200ms ease-out;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background: rgba(123,47,255,0.4);
        color: #FFFFFF;
        box-shadow: 0 0 16px rgba(123,47,255,0.5);
        transform: translateY(-2px);
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, var(--max-accent), var(--max-quinary));
        color: #FFFFFF;
        box-shadow: 0 0 20px rgba(255,58,242,0.5);
        border-color: var(--max-tertiary);
    }
    .stTabs [data-baseweb="tab-highlight"],
    .stTabs [data-baseweb="tab-border"] { display: none; }

    /* ── st.metric ───────────────────────────────────────────────────── */
    [data-testid="stMetric"] {
        border: 4px solid var(--max-secondary);
        border-radius: 24px;
        background: rgba(45,27,78,0.7);
        padding: 1rem;
        box-shadow: 0 0 20px rgba(0,245,212,0.3), 8px 8px 0 var(--max-accent);
        transition: transform 300ms cubic-bezier(0.68,-0.55,0.265,1.55), box-shadow 300ms ease-out;
    }
    [data-testid="stMetric"]:hover {
        transform: scale(1.04) rotate(1deg);
        box-shadow: 0 0 40px rgba(0,245,212,0.5), 0 0 80px rgba(255,58,242,0.3), 10px 10px 0 var(--max-accent);
    }
    [data-testid="stMetric"] label {
        color: var(--max-secondary);
        font-family: 'Outfit', sans-serif;
        font-size: 0.68rem;
        font-weight: 800;
        letter-spacing: 0.18em;
        text-transform: uppercase;
    }
    [data-testid="stMetricValue"] {
        color: #FFFFFF;
        font-family: 'Outfit', sans-serif;
        font-weight: 900;
        text-shadow: 2px 2px 0px var(--max-quinary), 4px 4px 0px var(--max-accent);
    }
    [data-testid="stMetricDelta"] svg { display: inline; }

    /* ── Expander ────────────────────────────────────────────────────── */
    .streamlit-expanderHeader {
        background: rgba(45,27,78,0.6);
        border: 4px solid var(--max-quaternary);
        border-radius: 12px;
        color: #FFFFFF;
        box-shadow: 0 0 16px rgba(255,107,53,0.4), 6px 6px 0 var(--max-accent);
        font-family: 'Outfit', sans-serif;
        font-size: 0.78rem;
        font-weight: 800;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        transition: background 200ms ease-out, box-shadow 200ms ease-out, transform 200ms ease-out;
    }
    .streamlit-expanderHeader:hover {
        background: rgba(123,47,255,0.4);
        color: #FFFFFF;
        transform: scale(1.01) translateY(-1px);
        box-shadow: 0 0 30px rgba(255,107,53,0.5), 8px 8px 0 var(--max-accent);
    }

    /* ── Data editor ─────────────────────────────────────────────────── */
    [data-testid="stDataEditor"] {
        border: 4px solid var(--max-quinary);
        border-radius: 16px;
        overflow: hidden;
        box-shadow: 0 0 30px rgba(123,47,255,0.4), 8px 8px 0 var(--max-secondary);
    }

    /* ══════════════════ Custom components ════════════════════════════ */

    /* Page header */
    .page-header {
        position: relative;
        overflow: hidden;
        padding: 3rem 2rem 2.25rem;
        margin-bottom: 2rem;
        background: linear-gradient(135deg, #0D0D1A 0%, #1A0A2E 50%, #0D1A1A 100%);
        color: #FFFFFF;
        border: 4px solid var(--max-accent);
        border-radius: 24px;
        box-shadow: 0 0 40px rgba(255,58,242,0.5), 0 0 80px rgba(123,47,255,0.3), 12px 12px 0 var(--max-tertiary), 24px 24px 0 var(--max-accent);
    }
    .page-header::before {
        content: "";
        position: absolute;
        inset: 0;
        background-image:
            radial-gradient(ellipse at 15% 20%, rgba(255,58,242,0.2) 0%, transparent 45%),
            radial-gradient(ellipse at 85% 80%, rgba(0,245,212,0.15) 0%, transparent 45%),
            radial-gradient(ellipse at 50% 50%, rgba(123,47,255,0.1) 0%, transparent 60%),
            radial-gradient(circle, rgba(255,230,0,0.6) 1px, transparent 1px);
        background-size: 100% 100%, 100% 100%, 100% 100%, 22px 22px;
        opacity: 0.85;
        pointer-events: none;
        border-radius: 20px;
    }
    .page-header::after {
        content: "SQL";
        position: absolute;
        right: 1rem;
        bottom: -2rem;
        font-family: 'Bungee', cursive;
        font-size: clamp(5rem, 14vw, 11rem);
        font-weight: 900;
        color: transparent;
        -webkit-text-stroke: 2px rgba(255,58,242,0.18);
        pointer-events: none;
        line-height: 1;
        z-index: 0;
        text-shadow: 4px 4px 0px rgba(0,245,212,0.1);
    }
    .page-header-title {
        position: relative;
        z-index: 1;
        font-family: 'Outfit', sans-serif;
        font-size: clamp(2.2rem, 6vw, 5rem);
        line-height: 1;
        letter-spacing: -0.03em;
        font-weight: 900;
        margin: 0 0 0.85rem;
        color: #FFFFFF;
        text-transform: uppercase;
        text-shadow: 2px 2px 0px var(--max-quinary), 4px 4px 0px var(--max-accent), 6px 6px 0px var(--max-secondary);
    }
    .page-header-desc {
        position: relative;
        z-index: 1;
        font-family: 'DM Sans', sans-serif;
        font-size: 1rem;
        line-height: 1.75;
        color: rgba(255,255,255,0.85);
        max-width: 780px;
        margin: 0;
        font-weight: 500;
    }
    .page-header-tags {
        position: relative;
        z-index: 1;
        margin-top: 1.25rem;
        display: flex;
        gap: 0.5rem;
        flex-wrap: wrap;
    }

    /* Tags */
    .tag, .tag-accent, .tag-green, .tag-red {
        display: inline-block;
        padding: 0.3rem 0.8rem;
        border-radius: 999px;
        font-family: 'Outfit', sans-serif;
        font-size: 0.62rem;
        font-weight: 800;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        border: 3px solid var(--max-secondary);
        background: rgba(0,245,212,0.12);
        color: var(--max-secondary);
        transition: background 200ms ease-out, transform 200ms ease-out, box-shadow 200ms ease-out;
        cursor: default;
    }
    .tag-accent  { background: rgba(255,58,242,0.2); color: var(--max-accent); border-color: var(--max-accent); }
    .tag-green   { background: rgba(255,230,0,0.15); color: var(--max-tertiary); border-color: var(--max-tertiary); }
    .tag-red     { background: rgba(255,107,53,0.2); color: var(--max-quaternary); border-color: var(--max-quaternary); }
    .tag:hover   { background: rgba(0,245,212,0.3); transform: scale(1.06) rotate(1deg); box-shadow: 0 0 12px rgba(0,245,212,0.4); }

    /* Section label */
    .section-label {
        font-family: 'Outfit', sans-serif;
        font-size: 0.7rem;
        font-weight: 800;
        letter-spacing: 0.2em;
        text-transform: uppercase;
        color: var(--max-accent);
        margin-bottom: 0.55rem;
        padding-bottom: 0.4rem;
        border-bottom: 3px dashed var(--max-accent);
        text-shadow: 0 0 10px rgba(255,58,242,0.5);
    }
    section[data-testid="stSidebar"] .section-label {
        color: var(--max-secondary);
        border-bottom-color: var(--max-secondary);
        text-shadow: 0 0 10px rgba(0,245,212,0.5);
    }

    /* Input hint */
    .input-hint {
        font-family: 'DM Sans', sans-serif;
        font-size: 0.82rem;
        font-weight: 500;
        color: rgba(255,255,255,0.65);
        padding-top: 0.6rem;
        line-height: 1.5;
    }

    /* Metric cards */
    .metric-card {
        padding: 1.25rem 1.1rem;
        background: rgba(45,27,78,0.7);
        border: 4px solid var(--max-accent);
        border-radius: 20px;
        box-shadow: 0 0 20px rgba(255,58,242,0.35), 8px 8px 0 var(--max-tertiary);
        transition: transform 300ms cubic-bezier(0.68,-0.55,0.265,1.55), box-shadow 300ms ease-out;
        cursor: default;
    }
    .metric-card:hover {
        transform: scale(1.04) rotate(-1deg);
        box-shadow: 0 0 40px rgba(255,58,242,0.55), 0 0 80px rgba(255,230,0,0.3), 10px 10px 0 var(--max-tertiary);
    }
    .metric-card-value {
        font-family: 'Outfit', sans-serif;
        font-size: 2.2rem;
        font-weight: 900;
        color: #FFFFFF;
        line-height: 1;
        margin-bottom: 0.4rem;
        letter-spacing: -0.03em;
        text-shadow: 2px 2px 0px var(--max-quinary), 4px 4px 0px var(--max-accent);
    }
    .metric-card-value.accent  { color: var(--max-accent); }
    .metric-card-value.amber   { color: var(--max-tertiary); }
    .metric-card-value.success { color: var(--max-secondary); }
    .metric-card-value.green   { color: var(--max-secondary); }
    .metric-card-value.sm      { font-size: 1rem; }
    .metric-card:hover .metric-card-value,
    .metric-card:hover .metric-card-label { color: #FFFFFF; }
    .metric-card-label {
        font-family: 'Outfit', sans-serif;
        font-size: 0.6rem;
        font-weight: 800;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        color: var(--max-secondary);
        text-shadow: 0 0 8px rgba(0,245,212,0.4);
    }

    /* Tree container */
    .tree-container {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.84rem;
        line-height: 1.9;
        background: rgba(13,13,26,0.85);
        color: var(--max-fg);
        padding: 1.5rem 1.75rem;
        border: 4px solid var(--max-quinary);
        border-radius: 16px;
        box-shadow: 0 0 30px rgba(123,47,255,0.4), 8px 8px 0 var(--max-accent);
        white-space: pre;
        overflow-x: auto;
        margin-bottom: 1rem;
    }
    .tree-container .tree-connector { color: var(--max-secondary); font-weight: 700; }
    .tree-container .tree-node {
        display: inline-block;
        margin-right: 0.55rem;
        padding: 0.14rem 0.6rem;
        border-radius: 999px;
        border: 3px solid var(--max-accent);
    }
    .tree-container .tree-node-project   { background: rgba(123,47,255,0.35); color: #FFFFFF; font-weight: 700; border-color: var(--max-quinary); }
    .tree-container .tree-node-filter    { background: rgba(255,230,0,0.2); color: var(--max-tertiary); border-color: var(--max-tertiary); }
    .tree-container .tree-node-or-filter { background: rgba(255,107,53,0.2); color: var(--max-quaternary); border-color: var(--max-quaternary); }
    .tree-container .tree-node-join      { background: rgba(255,58,242,0.3); color: #FFFFFF; font-weight: 700; border-color: var(--max-accent); }
    .tree-container .tree-node-scan      { background: rgba(0,245,212,0.2); color: var(--max-secondary); font-weight: 700; border-color: var(--max-secondary); }
    .tree-container .tree-node-aggregate { background: rgba(255,58,242,0.25); color: var(--max-accent); font-weight: 700; border-color: var(--max-accent); }
    .tree-container .tree-node-subquery  { background: rgba(45,27,78,0.8); color: rgba(255,255,255,0.8); border-color: var(--max-quinary); }
    .tree-container .tree-meta           { color: rgba(255,255,255,0.45); font-style: italic; }
    .tree-container .tree-node,
    .tree-container .tree-meta           { line-height: 1.7; }

    /* SQL Unparser */
    .sql-unparser-header {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 0.5rem;
        margin-top: 1.5rem;
    }

    /* Badges */
    .sql-unparser-badge,
    .db-connected-badge,
    .metrics-badge {
        display: inline-block;
        padding: 0.3rem 0.85rem;
        border: 3px solid var(--max-tertiary);
        border-radius: 999px;
        background: linear-gradient(90deg, rgba(255,230,0,0.2), rgba(255,58,242,0.2));
        color: var(--max-tertiary);
        font-family: 'Outfit', sans-serif;
        font-size: 0.62rem;
        font-weight: 800;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        box-shadow: 0 0 12px rgba(255,230,0,0.4);
        animation: wiggle 2s ease-in-out infinite;
    }
    .db-connected-badge {
        background: linear-gradient(90deg, rgba(0,245,212,0.2), rgba(255,230,0,0.2));
        color: var(--max-secondary);
        border-color: var(--max-secondary);
        box-shadow: 0 0 12px rgba(0,245,212,0.4);
    }

    /* Live metrics */
    .metrics-compare-header {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 1rem;
    }
    .metrics-col-header {
        font-family: 'Outfit', sans-serif;
        font-size: 0.72rem;
        font-weight: 800;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        padding: 0.65rem 1rem;
        border-radius: 12px;
        text-align: center;
        margin-bottom: 0.75rem;
        background: rgba(45,27,78,0.8);
        color: #FFFFFF;
        border: 4px solid var(--max-quinary);
        box-shadow: 0 0 20px rgba(123,47,255,0.4), 6px 6px 0 var(--max-accent);
    }
    .metrics-col-header.opt {
        background: rgba(255,58,242,0.2);
        color: var(--max-accent);
        border-color: var(--max-accent);
        box-shadow: 0 0 20px rgba(255,58,242,0.5), 6px 6px 0 var(--max-tertiary);
    }

    /* Catalog entries */
    .catalog-entry {
        padding: 0.8rem 0.9rem;
        margin-bottom: 0.5rem;
        background: rgba(45,27,78,0.5);
        border: 3px solid rgba(0,245,212,0.25);
        border-radius: 12px;
        transition: background 200ms ease-out, border-color 200ms ease-out, transform 200ms ease-out, box-shadow 200ms ease-out;
        cursor: default;
    }
    .catalog-entry:hover {
        background: rgba(45,27,78,0.8);
        border-color: var(--max-secondary);
        transform: translateX(4px);
        box-shadow: 0 0 16px rgba(0,245,212,0.3);
    }
    .catalog-entry .ce-name {
        font-family: 'Outfit', sans-serif;
        font-size: 0.73rem;
        font-weight: 800;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        margin-bottom: 0.2rem;
        color: var(--max-secondary);
    }
    .catalog-entry .ce-rows,
    .catalog-entry .ce-cols {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.78rem;
        font-weight: 500;
        color: rgba(255,255,255,0.55);
    }
    .catalog-entry:hover .ce-rows,
    .catalog-entry:hover .ce-cols { color: rgba(255,255,255,0.85); }

    /* Schema info */
    .schema-info {
        background: rgba(123,47,255,0.15);
        border: 4px solid var(--max-quinary);
        border-left: 8px solid var(--max-accent);
        border-radius: 16px;
        padding: 1rem 1.25rem;
        margin-bottom: 1.25rem;
        font-family: 'DM Sans', sans-serif;
        font-size: 0.92rem;
        font-weight: 500;
        color: rgba(255,255,255,0.9);
        line-height: 1.65;
        box-shadow: 0 0 20px rgba(123,47,255,0.3);
    }

    /* Pipeline steps */
    .pipeline-step {
        display: flex;
        align-items: flex-start;
        gap: 0.75rem;
        padding: 0.7rem 0;
        border-bottom: 2px dashed rgba(255,58,242,0.2);
        font-size: 0.85rem;
    }
    .pipeline-step:last-child { border-bottom: none; }
    .pipeline-step .step-num {
        min-width: 2.5rem;
        font-family: 'Bungee', cursive;
        font-weight: 400;
        letter-spacing: 0.05em;
        color: var(--max-tertiary);
        text-shadow: 0 0 8px rgba(255,230,0,0.6);
    }
    .pipeline-step .step-body .step-title {
        font-family: 'Outfit', sans-serif;
        font-size: 0.95rem;
        font-weight: 800;
        line-height: 1.35;
        color: #FFFFFF;
        letter-spacing: 0.02em;
    }
    .pipeline-step .step-body .step-desc {
        color: rgba(255,255,255,0.45);
        font-size: 0.73rem;
        line-height: 1.6;
        margin-top: 0.1rem;
        font-family: 'JetBrains Mono', monospace;
    }

    /* Sidebar app name */
    .sidebar-app-name {
        font-family: 'Bungee', cursive;
        font-size: 1.1rem;
        font-weight: 400;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        display: inline-block;
        background: linear-gradient(90deg, var(--max-accent), var(--max-quinary));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        text-shadow: none;
        filter: drop-shadow(0 0 8px rgba(255,58,242,0.6));
    }
    .sidebar-app-version {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.68rem;
        font-weight: 500;
        color: rgba(255,255,255,0.4);
        margin-top: 0.3rem;
        letter-spacing: 0.1em;
        text-transform: uppercase;
    }

    /* Tab section typography */
    .tab-section-title {
        font-family: 'Outfit', sans-serif;
        font-size: 0.7rem;
        font-weight: 800;
        letter-spacing: 0.2em;
        text-transform: uppercase;
        color: var(--max-tertiary);
        margin-bottom: 0.5rem;
        margin-top: 1.5rem;
        padding: 0.35rem 0.75rem;
        background: rgba(255,230,0,0.12);
        border: 2px solid var(--max-tertiary);
        border-radius: 999px;
        display: inline-block;
        text-shadow: 0 0 8px rgba(255,230,0,0.5);
        box-shadow: 0 0 12px rgba(255,230,0,0.25);
    }
    .tab-section-desc {
        font-family: 'DM Sans', sans-serif;
        font-size: 0.98rem;
        font-weight: 400;
        color: rgba(255,255,255,0.75);
        line-height: 1.75;
        margin-bottom: 1rem;
        margin-top: 0.5rem;
        max-width: 760px;
    }
    .tab-section-desc code {
        font-family: 'JetBrains Mono', monospace;
        background: rgba(0,245,212,0.15);
        color: var(--max-secondary);
        padding: 0.1rem 0.35rem;
        border: 2px solid var(--max-secondary);
        border-radius: 6px;
        font-weight: 700;
    }

    /* Compare label */
    .compare-label {
        font-family: 'Outfit', sans-serif;
        font-size: 0.62rem;
        font-weight: 800;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        color: var(--max-accent);
        background: rgba(255,58,242,0.12);
        margin-bottom: 0.5rem;
        padding: 0.3rem 0.75rem;
        display: inline-block;
        border: 2px solid var(--max-accent);
        border-radius: 999px;
        box-shadow: 0 0 8px rgba(255,58,242,0.3);
    }

    /* Surface chip */
    .surface-chip {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        padding: 0.3rem 0.8rem;
        background: rgba(45,27,78,0.6);
        border: 3px solid var(--max-secondary);
        border-radius: 999px;
        box-shadow: 0 0 10px rgba(0,245,212,0.3);
    }

    /* ── Responsive ──────────────────────────────────────────────────── */
    @media (max-width: 900px) {
        .page-header-title  { font-size: clamp(2rem, 10vw, 3.5rem); }
        .page-header        { padding: 2rem 1rem 1.5rem; }
        [data-testid="stHorizontalBlock"] { flex-wrap: wrap; }
        .stTabs [data-baseweb="tab"] { padding: 0.6rem 0.5rem; font-size: 0.64rem; letter-spacing: 0.06em; }
        .tree-container     { font-size: 0.76rem; padding: 1rem; }
        .metric-card        { min-width: 120px; }
        .tab-section-desc   { font-size: 0.9rem; line-height: 1.65; }
        .pipeline-step      { font-size: 0.8rem; }
    }

    @media (max-width: 640px) {
        .page-header-title  { font-size: clamp(1.75rem, 14vw, 2.75rem); }
        .page-header        { padding: 1.5rem 0.85rem 1rem; }
        .page-header-desc   { font-size: 0.9rem; }
        .page-header-tags   { gap: 0.35rem; }
        .stTabs [data-baseweb="tab"] { padding: 0.5rem 0.4rem; font-size: 0.58rem; letter-spacing: 0.03em; }
        .tab-section-desc   { font-size: 0.88rem; }
        .tree-container     { font-size: 0.7rem; padding: 0.85rem; }
        .metric-card-value  { font-size: 1.5rem; }
        .main .block-container { padding-left: 0.85rem; padding-right: 0.85rem; }
    }

    @media (prefers-reduced-motion: reduce) {
        * {
            animation-duration: 0.01ms;
            animation-iteration-count: 1;
            transition-duration: 0.15s;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)