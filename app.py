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
        "<p style='color:rgba(255,255,255,0.6);font-size:0.78rem;margin-bottom:0.7rem;line-height:1.65'>"
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
            f"<p style='font-size:0.72rem;color:rgba(255,255,255,0.5);margin:0.3rem 0 0.6rem'>"
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
                        "<p style='font-size:0.78rem;color:#525252;padding-top:0.5rem'>"
                        "Connect a DB to enable sync.</p>",
                        unsafe_allow_html=True,
                    )

            st.markdown("---")
            st.markdown("<div class='section-label' style='margin-bottom:0.4rem'>Current Live Catalog</div>", unsafe_allow_html=True)
            st.json(catalog.get_all_stats())

        with tab5:
            if not live_db:
                st.markdown(
                    "<div style='margin-top:1.5rem;padding:1.5rem;background:#F5F5F5;"
                    "border:1px solid #000000;text-align:center'>"
                    "<div style='font-weight:600;color:#000000;margin-bottom:0.3rem'>"
                    "No Live Database Connected</div>"
                    "<div style='font-size:0.85rem;color:#525252'>"
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
                        "<div style='font-size:0.75rem;font-weight:600;color:#525252;"
                        "text-transform:uppercase;letter-spacing:0.07em;padding-top:1.2rem'>"
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
                        "<div style='font-size:0.75rem;font-weight:600;color:#525252;"
                        "text-transform:uppercase;letter-spacing:0.07em;padding-top:1.2rem'>"
                        "Rows</div>",
                        unsafe_allow_html=True,
                    )
                with mc3:
                    st.metric(label="Unoptimized — Rows", value=f"{r_unopt:,}", label_visibility="collapsed")
                with mc4:
                    delta_r = r_opt - r_unopt
                    st.metric(label="Optimized — Rows", value=f"{r_opt:,}", delta=f"{delta_r:+,}" if delta_r != 0 else "✓ Match", delta_color="off" if delta_r == 0 else "normal", label_visibility="collapsed")

                row_label3, mc5, mc6 = st.columns([0.8, 2, 2])
                with row_label3:
                    st.markdown(
                        "<div style='font-size:0.75rem;font-weight:600;color:#525252;"
                        "text-transform:uppercase;letter-spacing:0.07em;padding-top:1.2rem'>"
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

# ─────────────────────────────────────────────────────────────────────────────
# CSS — single block, injected last so it wins by load order, zero !important
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700;800&family=Source+Serif+4:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');

    /* ── Design tokens ──────────────────────────────────────────────── */
    :root {
        --bg:            #FFFFFF;
        --fg:            #000000;
        --muted:         #525252;
        --border:        #000000;
        --border-light:  #E5E5E5;
        --sidebar-bg:    #000000;
        --sidebar-fg:    #FFFFFF;
        --sidebar-muted: rgba(255,255,255,0.55);
    }

    /* ── Global reset ───────────────────────────────────────────────── */
    html, body, .stApp {
        font-family: 'Source Serif 4', Georgia, serif;
        background: var(--bg);
        color: var(--fg);
        -webkit-font-smoothing: antialiased;
    }

    body {
        background-image:
            repeating-linear-gradient(0deg, transparent, transparent 23px, rgba(0,0,0,0.03) 24px),
            repeating-linear-gradient(90deg, transparent, transparent 143px, rgba(0,0,0,0.018) 144px);
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
        border-top: 1px solid #000000;
        margin: 1.5rem 0;
    }

    /* ── Sidebar ────────────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: var(--sidebar-bg);
        border-right: 2px solid var(--border);
        box-shadow: none;
    }

    /* Broad rule: all text inside sidebar is white */
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

    /* db-connected-badge is inside the sidebar but has a white background
       and needs black text. Being more specific than the rule above wins. */
    section[data-testid="stSidebar"] div.db-connected-badge {
        color: #000000;
        background: #FFFFFF;
        border-color: #000000;
    }

    section[data-testid="stSidebar"] hr {
        border: none;
        border-top: 1px solid rgba(255,255,255,0.28);
        margin: 1rem 0;
    }

    /* Sidebar buttons */
    section[data-testid="stSidebar"] .stButton > button {
        background: #FFFFFF;
        color: #000000;
        border: 2px solid #FFFFFF;
        border-radius: 0;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        transition: background 100ms linear, color 100ms linear, border-color 100ms linear;
    }
    section[data-testid="stSidebar"] .stButton > button p,
    section[data-testid="stSidebar"] .stButton > button span,
    section[data-testid="stSidebar"] .stButton > button div {
        color: #000000;
    }
    section[data-testid="stSidebar"] .stButton > button:hover {
        background: #000000;
        color: #FFFFFF;
        border-color: #FFFFFF;
    }
    section[data-testid="stSidebar"] .stButton > button:hover p,
    section[data-testid="stSidebar"] .stButton > button:hover span,
    section[data-testid="stSidebar"] .stButton > button:hover div {
        color: #FFFFFF;
    }

    /* Sidebar inputs */
    section[data-testid="stSidebar"] [data-baseweb="input"],
    section[data-testid="stSidebar"] [data-baseweb="base-input"] {
        background: rgba(255,255,255,0.10);
        border: 2px solid rgba(255,255,255,0.4);
        border-radius: 0;
    }
    section[data-testid="stSidebar"] [data-baseweb="input"] input,
    section[data-testid="stSidebar"] [data-baseweb="base-input"] input,
    section[data-testid="stSidebar"] input[type="text"],
    section[data-testid="stSidebar"] input[type="password"],
    section[data-testid="stSidebar"] input[type="number"] {
        background: rgba(255,255,255,0.10);
        color: #FFFFFF;
        -webkit-text-fill-color: #FFFFFF;
        border: 2px solid rgba(255,255,255,0.4);
        border-radius: 0;
        box-shadow: none;
        min-height: 3.25rem;
    }
    section[data-testid="stSidebar"] input::placeholder,
    section[data-testid="stSidebar"] input::-webkit-input-placeholder {
        color: rgba(255,255,255,0.45);
        -webkit-text-fill-color: rgba(255,255,255,0.45);
        opacity: 1;
    }
    section[data-testid="stSidebar"] input[type="text"]:focus,
    section[data-testid="stSidebar"] input[type="password"]:focus,
    section[data-testid="stSidebar"] input[type="number"]:focus {
        border-color: #FFFFFF;
        outline: 2px solid rgba(255,255,255,0.6);
        outline-offset: 2px;
        box-shadow: none;
    }

    /* ── Main inputs + textarea ──────────────────────────────────────── */
    input[type="text"],
    input[type="password"],
    input[type="number"],
    textarea {
        font-family: 'JetBrains Mono', monospace;
        background: #FFFFFF;
        color: #000000;
        border: 2px solid #000000;
        border-radius: 0;
        box-shadow: none;
    }
    input[type="text"],
    input[type="password"],
    input[type="number"] { min-height: 3.25rem; }
    input[type="text"]:focus,
    input[type="password"]:focus,
    input[type="number"]:focus,
    textarea:focus {
        border-color: #000000;
        outline: 3px solid #000000;
        outline-offset: 2px;
        box-shadow: none;
    }

    /* ── Main buttons ────────────────────────────────────────────────── */
    .stButton > button {
        background: #000000;
        color: #FFFFFF;
        border: 2px solid #000000;
        border-radius: 0;
        padding: 0.75rem 1.35rem;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        transition: background 100ms linear, color 100ms linear, border-color 100ms linear;
        box-shadow: none;
        white-space: nowrap;
    }
    .stButton > button p,
    .stButton > button span,
    .stButton > button div { color: #FFFFFF; }
    .stButton > button:hover {
        background: #FFFFFF;
        color: #000000;
        border-color: #000000;
        transform: none;
        box-shadow: none;
    }
    .stButton > button:hover p,
    .stButton > button:hover span,
    .stButton > button:hover div { color: #000000; }
    .stButton > button:active { transform: none; }

    /* ── Code blocks ─────────────────────────────────────────────────── */
    .stCodeBlock pre, .stCodeBlock code, pre, code {
        font-family: 'JetBrains Mono', monospace;
        background: #FFFFFF;
        color: #000000;
        border: 1px solid #000000;
        border-radius: 0;
        box-shadow: none;
    }

    /* ── Tabs ────────────────────────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        background: #FFFFFF;
        border-bottom: 2px solid #000000;
        gap: 0;
        padding: 0;
    }
    .stTabs [data-baseweb="tab"] {
        background: #FFFFFF;
        color: #000000;
        border: 1px solid #000000;
        border-bottom: none;
        border-radius: 0;
        padding: 0.8rem 1.05rem;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        transition: background 100ms linear, color 100ms linear;
    }
    .stTabs [data-baseweb="tab"]:hover { background: #000000; color: #FFFFFF; }
    .stTabs [aria-selected="true"]     { background: #000000; color: #FFFFFF; }
    .stTabs [data-baseweb="tab-highlight"],
    .stTabs [data-baseweb="tab-border"] { display: none; }

    /* ── st.metric ───────────────────────────────────────────────────── */
    [data-testid="stMetric"] {
        border: 1px solid #000000;
        border-radius: 0;
        background: #FFFFFF;
        padding: 1rem;
    }
    [data-testid="stMetric"] label {
        color: #000000;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
    }
    [data-testid="stMetricValue"] {
        color: #000000;
        font-family: 'Playfair Display', Georgia, serif;
        font-weight: 700;
    }
    [data-testid="stMetricDelta"] svg { display: inline; }

    /* ── Expander ────────────────────────────────────────────────────── */
    .streamlit-expanderHeader {
        background: #FFFFFF;
        border: 1px solid #000000;
        border-radius: 0;
        color: #000000;
        box-shadow: none;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        transition: background 100ms linear, color 100ms linear;
    }
    .streamlit-expanderHeader:hover { background: #000000; color: #FFFFFF; }

    /* ── Data editor ─────────────────────────────────────────────────── */
    [data-testid="stDataEditor"] {
        border: 1px solid #000000;
        border-radius: 0;
        overflow: hidden;
        box-shadow: none;
    }

    /* ══════════════════ Custom components ════════════════════════════ */

    /* Page header */
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
        background-image: repeating-linear-gradient(
            90deg, transparent, transparent 1px,
            rgba(255,255,255,0.06) 1px, rgba(255,255,255,0.06) 2px
        );
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
        line-height: 1.85;
        color: rgba(255,255,255,0.9);
        max-width: 780px;
        margin: 0;
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
        border-radius: 0;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.65rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        border: 1px solid #000000;
        background: #FFFFFF;
        color: #000000;
        transition: background 100ms linear, color 100ms linear;
        cursor: default;
    }
    .tag-accent, .tag-green, .tag-red { background: #000000; color: #FFFFFF; }
    .tag:hover                         { background: #000000; color: #FFFFFF; }

    /* Section label */
    .section-label {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #000000;
        margin-bottom: 0.55rem;
        padding-bottom: 0.35rem;
        border-bottom: 2px solid #000000;
    }
    section[data-testid="stSidebar"] .section-label {
        color: #FFFFFF;
        border-bottom-color: rgba(255,255,255,0.4);
    }

    /* Input hint */
    .input-hint {
        font-size: 0.8rem;
        color: var(--muted);
        padding-top: 0.5rem;
        line-height: 1.5;
    }

    /* Metric cards */
    .metric-card {
        padding: 1rem;
        background: #FFFFFF;
        border: 1px solid #000000;
        border-radius: 0;
        box-shadow: none;
        transition: background 100ms linear;
        cursor: default;
    }
    .metric-card:hover { background: #000000; }
    .metric-card-value {
        font-family: 'Playfair Display', Georgia, serif;
        font-size: 1.75rem;
        font-weight: 700;
        color: #000000;
        line-height: 1;
        margin-bottom: 0.4rem;
    }
    .metric-card-value.accent,
    .metric-card-value.amber,
    .metric-card-value.success,
    .metric-card-value.green { color: #000000; }
    .metric-card-value.sm    { font-size: 1rem; }
    .metric-card:hover .metric-card-value,
    .metric-card:hover .metric-card-value.accent,
    .metric-card:hover .metric-card-value.amber,
    .metric-card:hover .metric-card-value.success,
    .metric-card:hover .metric-card-value.green,
    .metric-card:hover .metric-card-label { color: #FFFFFF; }
    .metric-card-label {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.65rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--muted);
    }

    /* Tree container */
    .tree-container {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.84rem;
        line-height: 1.9;
        background: #FFFFFF;
        color: #000000;
        padding: 1.4rem 1.6rem;
        border: 2px solid #000000;
        border-radius: 0;
        box-shadow: none;
        white-space: pre;
        overflow-x: auto;
        margin-bottom: 1rem;
    }
    .tree-container .tree-connector { color: #000000; font-weight: 700; }
    .tree-container .tree-node {
        display: inline-block;
        margin-right: 0.55rem;
        padding: 0.14rem 0.6rem;
        border-radius: 0;
        border: 1px solid #000000;
    }
    .tree-container .tree-node-project   { background: #F5F5F5; color: #000000; font-weight: 700; }
    .tree-container .tree-node-filter    { background: #F5F5F5; color: #000000; }
    .tree-container .tree-node-or-filter { background: #F5F5F5; color: #000000; }
    .tree-container .tree-node-join      { background: #000000; color: #FFFFFF; font-weight: 600; }
    .tree-container .tree-node-scan      { background: #F5F5F5; color: #000000; font-weight: 600; }
    .tree-container .tree-node-aggregate { background: #000000; color: #FFFFFF; font-weight: 700; }
    .tree-container .tree-node-subquery  { background: #F5F5F5; color: #000000; }
    .tree-container .tree-meta           { color: var(--muted); }
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
        padding: 0.2rem 0.6rem;
        border: 1px solid #000000;
        border-radius: 0;
        background: #FFFFFF;
        color: #000000;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.65rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
    }

    /* Live metrics */
    .metrics-compare-header {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 1rem;
    }
    .metrics-col-header {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.09em;
        text-transform: uppercase;
        padding: 0.5rem 0.75rem;
        border-radius: 0;
        text-align: center;
        margin-bottom: 0.75rem;
        background: #000000;
        color: #FFFFFF;
        border: 1px solid #000000;
    }

    /* Catalog entries */
    .catalog-entry {
        padding: 0.8rem 0.9rem;
        margin-bottom: 0.45rem;
        background: #FFFFFF;
        border: 1px solid #000000;
        border-radius: 0;
        transition: background 100ms linear;
        cursor: default;
    }
    .catalog-entry:hover { background: #000000; }
    .catalog-entry .ce-name {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.76rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 0.25rem;
        color: #000000;
    }
    .catalog-entry .ce-rows,
    .catalog-entry .ce-cols {
        font-family: 'Source Serif 4', Georgia, serif;
        font-size: 0.9rem;
        color: var(--muted);
    }
    .catalog-entry:hover .ce-name,
    .catalog-entry:hover .ce-rows,
    .catalog-entry:hover .ce-cols { color: #FFFFFF; }

    /* Catalog entries in the black sidebar need inverted base colors */
    section[data-testid="stSidebar"] .catalog-entry {
        background: rgba(255,255,255,0.08);
        border-color: rgba(255,255,255,0.25);
    }
    section[data-testid="stSidebar"] .catalog-entry .ce-name { color: #FFFFFF; }
    section[data-testid="stSidebar"] .catalog-entry .ce-rows,
    section[data-testid="stSidebar"] .catalog-entry .ce-cols { color: rgba(255,255,255,0.6); }
    section[data-testid="stSidebar"] .catalog-entry:hover { background: rgba(255,255,255,0.18); }
    section[data-testid="stSidebar"] .catalog-entry:hover .ce-name,
    section[data-testid="stSidebar"] .catalog-entry:hover .ce-rows,
    section[data-testid="stSidebar"] .catalog-entry:hover .ce-cols { color: #FFFFFF; }

    /* Schema info */
    .schema-info {
        background: #FFFFFF;
        border: 1px solid #000000;
        border-radius: 0;
        padding: 0.85rem 1rem;
        margin-bottom: 1rem;
        font-family: 'Source Serif 4', Georgia, serif;
        font-size: 0.9rem;
        color: var(--muted);
        line-height: 1.6;
    }

    /* Pipeline steps (inside black sidebar) */
    .pipeline-step {
        display: flex;
        align-items: flex-start;
        gap: 0.75rem;
        padding: 0.6rem 0;
        border-bottom: 1px solid rgba(255,255,255,0.2);
        font-size: 0.85rem;
    }
    .pipeline-step:last-child { border-bottom: none; }
    .pipeline-step .step-num {
        min-width: 2.5rem;
        font-family: 'JetBrains Mono', monospace;
        font-weight: 700;
        letter-spacing: 0.12em;
        color: rgba(255,255,255,0.55);
    }
    .pipeline-step .step-body .step-title {
        font-family: 'Playfair Display', Georgia, serif;
        font-size: 1rem;
        font-weight: 700;
        line-height: 1.35;
        color: #FFFFFF;
    }
    .pipeline-step .step-body .step-desc {
        color: rgba(255,255,255,0.55);
        font-size: 0.78rem;
        line-height: 1.6;
        margin-top: 0.15rem;
    }

    /* Sidebar app name */
    .sidebar-app-name {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #FFFFFF;
    }
    .sidebar-app-version {
        font-size: 0.73rem;
        color: rgba(255,255,255,0.5);
        margin-top: 0.1rem;
    }

    /* Tab section typography */
    .tab-section-title {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #000000;
        margin-bottom: 0.5rem;
        margin-top: 1.5rem;
    }
    .tab-section-desc {
        font-family: 'Source Serif 4', Georgia, serif;
        font-size: 1rem;
        color: var(--muted);
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

    /* Compare label */
    .compare-label {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #000000;
        margin-bottom: 0.5rem;
        padding-bottom: 0.35rem;
        border-bottom: 1px solid #000000;
    }

    /* Surface chip */
    .surface-chip {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        padding: 0.25rem 0.6rem;
        background: #FFFFFF;
        border: 1px solid #000000;
        border-radius: 0;
    }

    /* ── Responsive ──────────────────────────────────────────────────── */
    @media (max-width: 900px) {
        .page-header-title  { font-size: clamp(2.5rem, 13vw, 4.5rem); }
        .page-header        { padding: 2rem 1rem 1.25rem; }
        [data-testid="stHorizontalBlock"] { flex-wrap: wrap; }
        .stTabs [data-baseweb="tab"] { padding: 0.65rem; font-size: 0.68rem; letter-spacing: 0.05em; }
        .tree-container     { font-size: 0.76rem; padding: 1rem; }
        .metric-card        { min-width: 120px; }
        .tab-section-desc   { font-size: 0.93rem; line-height: 1.75; }
        .pipeline-step      { font-size: 0.8rem; }
    }

    @media (max-width: 640px) {
        .page-header-title  { font-size: clamp(2rem, 16vw, 3.5rem); }
        .page-header        { padding: 1.5rem 0.85rem 1rem; }
        .page-header-desc   { font-size: 0.93rem; }
        .page-header-tags   { gap: 0.35rem; }
        .stTabs [data-baseweb="tab"] { padding: 0.55rem 0.45rem; font-size: 0.62rem; letter-spacing: 0.03em; }
        .tab-section-desc   { font-size: 0.88rem; }
        .tree-container     { font-size: 0.7rem; padding: 0.85rem; }
        .metric-card-value  { font-size: 1.35rem; }
        .main .block-container { padding-left: 0.85rem; padding-right: 0.85rem; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)