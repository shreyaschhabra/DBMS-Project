"""
engine/executor.py
------------------
Query Execution & Benchmarking Engine

Runs SQL queries against a live MySQL connection and collects performance metrics:
    - Wall-clock execution time (Python time.time() before/after cursor.execute)
    - Number of rows returned (proves semantic correctness between plans)
    - MySQL's internal query_cost (from EXPLAIN FORMAT=JSON)

The SQL Unparser generates nested subquery SQL.  Before benchmarking we apply a
light post-processing step to make it strictly valid MySQL:
    - Remove any leftover `AS alias` on the outermost SELECT (MySQL doesn't need it).
    - The subquery aliases (subq_1, subq_2 …) are already valid MySQL identifiers.

Usage::

    from engine.executor import QueryExecutor
    from engine.database import DatabaseManager

    mgr = DatabaseManager(...)
    mgr.connect()
    exe = QueryExecutor(mgr)

    metrics = exe.benchmark_query("SELECT * FROM olist_orders WHERE ...")
    print(metrics)
    # {"execution_time_ms": 42.3, "rows_returned": 99441, "mysql_cost": 10231.5}
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

from engine.database import DatabaseManager


class QueryExecutor:
    """
    Executes SQL against a live MySQL connection and returns benchmarking metrics.

    Parameters
    ----------
    db_manager : DatabaseManager
        An already-connected DatabaseManager instance.
    row_limit : int
        Safety cap on the number of rows fetched (prevents OOM on huge tables).
        Defaults to 10,000.  Set to None to fetch all rows (use with caution).
    """

    def __init__(self, db_manager: DatabaseManager, row_limit: int = 10_000) -> None:
        self._db  = db_manager
        self._row_limit = row_limit

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def benchmark_query(self, sql: str) -> dict:
        """
        Execute *sql* and return performance metrics.

        Returns dict with keys:
            execution_time_ms : float
            rows_returned     : int
            mysql_cost        : float
            error             : str  (empty string if no error)
        """
        result: dict = {
            "execution_time_ms": 0.0,
            "rows_returned":     0,
            "mysql_cost":        0.0,
            "error":             "",
        }

        sql = sql.strip().rstrip(";").strip()
        if not sql:
            result["error"] = "Empty query."
            return result

        # ── EXPLAIN cost first (uses its own cursor, fully closes it) ────
        result["mysql_cost"] = self._explain_cost(sql)

        # ── Timed execution ───────────────────────────────────────────────
        try:
            cursor = self._db.get_cursor()
            try:
                t_start = time.time()
                cursor.execute(sql)

                # Fetch up to row_limit rows; then drain any remaining
                # rows so mysql-connector doesn't raise "Unread result found"
                if self._row_limit:
                    rows = cursor.fetchmany(self._row_limit)
                    # Drain the rest silently
                    try:
                        while cursor.fetchone() is not None:
                            pass
                    except Exception:
                        pass
                else:
                    rows = cursor.fetchall()

                t_end = time.time()
                result["execution_time_ms"] = round((t_end - t_start) * 1000, 2)
                result["rows_returned"]     = len(rows)
            finally:
                # Always close — swallow any "Unread result" cleanup errors
                try:
                    cursor.close()
                except Exception:
                    pass
        except Exception as exc:
            result["error"] = str(exc)

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _explain_cost(self, sql: str) -> float:
        """
        Run ``EXPLAIN FORMAT=JSON <sql>`` and extract ``query_cost``.

        Returns 0.0 on any error (e.g. MySQL version doesn't support JSON EXPLAIN).
        """
        try:
            cursor = self._db.get_cursor()
            try:
                cursor.execute(f"EXPLAIN FORMAT=JSON {sql}")
                row = cursor.fetchone()
                if row is None:
                    return 0.0
                # The EXPLAIN JSON is returned as a string in the first column
                explain_json = row[0]
                if isinstance(explain_json, (bytes, bytearray)):
                    explain_json = explain_json.decode("utf-8")
                parsed = json.loads(explain_json)
                # query_cost lives at: query_block -> cost_info -> query_cost
                cost_str = (
                    parsed
                    .get("query_block", {})
                    .get("cost_info", {})
                    .get("query_cost", "0")
                )
                return float(cost_str)
            finally:
                cursor.close()
        except Exception:
            return 0.0

    @staticmethod
    def sanitize_for_mysql(sql: str) -> str:
        """
        Light post-processing to make Unparser-generated SQL strictly valid MySQL.

        The Unparser wraps everything in subqueries with AS aliases which is
        perfectly valid.  The only edge case is when the outermost node is a
        plain ScanNode that generates ``SELECT * FROM table AS alias`` — MySQL
        accepts this fine.  So currently this is a passthrough; kept as an
        extension point.
        """
        return sql.strip().rstrip(";")
