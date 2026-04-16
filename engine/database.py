"""
engine/database.py
------------------
Live MySQL Database Manager

Handles connecting to a real MySQL server, syncing the live schema into the
Catalog singleton, and providing a connection handle for the executor.

Design notes
~~~~~~~~~~~~
- Uses mysql-connector-python directly (no ORM) — lightweight and fast.
- The connection is NOT pooled; for a Streamlit demo app one connection is fine.
- All methods are safe to call even if a connection has not been established yet
  (they raise a clear RuntimeError instead of letting a NoneType crash propagate).
- .env credentials are read once at import time as defaults; the UI can override them.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional; credentials can come from the UI

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    MySQLError = Exception  # type: ignore[misc,assignment]


class DatabaseManager:
    """
    Manages a single MySQL connection and exposes schema-sync + query helpers.

    Usage::

        mgr = DatabaseManager(host="localhost", port=3306,
                              user="root", password="secret", database="olist_db")
        mgr.connect()              # raises on failure
        catalog = mgr.sync_schema_to_catalog(catalog)
        mgr.disconnect()
    """

    def __init__(
        self,
        host: str     = os.getenv("DB_HOST", "localhost"),
        port: int      = int(os.getenv("DB_PORT", "3306")),
        user: str      = os.getenv("DB_USER", "root"),
        password: str  = os.getenv("DB_PASSWORD", ""),
        database: str  = os.getenv("DB_NAME", ""),
    ) -> None:
        self.host     = host
        self.port     = port
        self.user     = user
        self.password = password
        self.database = database

        self._connection: Optional[Any] = None  # mysql.connector.CMySQLConnection

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """
        Open a connection to MySQL.  Raises ``RuntimeError`` on failure with a
        human-readable message suitable for ``st.error()``.
        """
        if not MYSQL_AVAILABLE:
            raise RuntimeError(
                "mysql-connector-python is not installed. "
                "Run: uv add mysql-connector-python"
            )
        try:
            self._connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connection_timeout=10,
                autocommit=True,
            )
        except MySQLError as exc:
            raise RuntimeError(f"MySQL connection failed: {exc}") from exc

    def disconnect(self) -> None:
        """Close the connection if open."""
        if self._connection and self._connection.is_connected():
            self._connection.close()
        self._connection = None

    @property
    def is_connected(self) -> bool:
        """True if a live connection exists."""
        return (
            self._connection is not None
            and self._connection.is_connected()
        )

    def _require_connection(self) -> None:
        if not self.is_connected:
            raise RuntimeError("No active database connection. Call connect() first.")

    # ------------------------------------------------------------------
    # Schema sync
    # ------------------------------------------------------------------

    def sync_schema_to_catalog(self, catalog) -> Tuple[Any, int]:
        """
        Query information_schema to pull real table stats and push them into
        the provided ``Catalog`` instance.

        Returns (updated_catalog, tables_imported_count).

        Queries
        ~~~~~~~
        - information_schema.TABLES  → TABLE_NAME, TABLE_ROWS
        - information_schema.COLUMNS → TABLE_NAME, COLUMN_NAME (ordered by ORDINAL_POSITION)

        Note: TABLE_ROWS is an *estimate* for InnoDB (updated by ANALYZE TABLE).
        We use max(1, TABLE_ROWS) to avoid zero-cardinality entries.
        """
        self._require_connection()
        cursor = self._connection.cursor(dictionary=True)

        try:
            # ── Fetch table row-count estimates ───────────────────────────
            cursor.execute(
                """
                SELECT TABLE_NAME, COALESCE(TABLE_ROWS, 1) AS TABLE_ROWS
                FROM   information_schema.TABLES
                WHERE  TABLE_SCHEMA = %s
                  AND  TABLE_TYPE   = 'BASE TABLE'
                ORDER  BY TABLE_NAME
                """,
                (self.database,),
            )
            table_rows: Dict[str, int] = {
                row["TABLE_NAME"]: max(1, int(row["TABLE_ROWS"]))
                for row in cursor.fetchall()
            }

            if not table_rows:
                return catalog, 0

            # ── Fetch column lists ─────────────────────────────────────────
            placeholders = ", ".join(["%s"] * len(table_rows))
            cursor.execute(
                f"""
                SELECT TABLE_NAME, COLUMN_NAME
                FROM   information_schema.COLUMNS
                WHERE  TABLE_SCHEMA  = %s
                  AND  TABLE_NAME IN ({placeholders})
                ORDER  BY TABLE_NAME, ORDINAL_POSITION
                """,
                (self.database, *table_rows.keys()),
            )
            table_cols: Dict[str, List[str]] = {}
            for row in cursor.fetchall():
                tbl = row["TABLE_NAME"]
                table_cols.setdefault(tbl, []).append(row["COLUMN_NAME"])

            # ── Push into catalog ──────────────────────────────────────────
            # Wipe the old catalog and re-populate from live schema.
            import pandas as pd
            rows = [
                {
                    "table":     tbl,
                    "row_count": table_rows[tbl],
                    "columns":   ", ".join(table_cols.get(tbl, [])),
                }
                for tbl in sorted(table_rows)
            ]
            df = pd.DataFrame(rows, columns=["table", "row_count", "columns"])
            catalog.sync_from_dataframe(df)
            return catalog, len(rows)

        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Raw query helper (used by executor)
    # ------------------------------------------------------------------

    def get_cursor(self):
        """Return a fresh cursor on the active connection."""
        self._require_connection()
        return self._connection.cursor()

    def get_dict_cursor(self):
        """Return a fresh dictionary cursor on the active connection."""
        self._require_connection()
        return self._connection.cursor(dictionary=True)

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        state = "connected" if self.is_connected else "disconnected"
        return f"DatabaseManager({self.host}:{self.port}/{self.database}, {state})"
