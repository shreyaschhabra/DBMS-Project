from __future__ import annotations

import os
import traceback
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional; credentials can come from the UI

# PyMySQL is the sole driver used here. It is a pure-Python MySQL client that
# natively supports caching_sha2_password (MySQL 8+ default) without requiring
# any C extensions or plugin installs.
# Install: pip install pymysql
try:
    import pymysql
    import pymysql.cursors
    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class DatabaseManager:
    def __init__(
        self,
        host: str     = os.getenv("DB_HOST", "localhost"),
        port: int      = int(os.getenv("DB_PORT", "3306")),
        user: str      = os.getenv("DB_USER", "root"),
        password: str  = os.getenv("DB_PASSWORD", ""),
        database: str  = os.getenv("DB_NAME", ""),
    ) -> None:
        if not PYMYSQL_AVAILABLE:
            raise ImportError(
                "pymysql is not installed. Run: pip install pymysql"
            )

        self.host     = host
        self.port     = port
        self.user     = user
        self.password = password
        self.database = database

        self._connection: Optional[Any] = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> dict:
        """
        Open a MySQL connection using PyMySQL (supports caching_sha2_password).
        Returns a dict with:
          {"status": "success"|"error", "message": str, "config": dict}
        Raises nothing — all errors are captured and returned.
        """
        host     = (self.host or "127.0.0.1").strip()
        port     = int(self.port) if self.port else 3306
        user     = (self.user or "root").strip()
        password = self.password or ""
        database = self.database.strip() if self.database and self.database.strip() else None

        debug_info = {
            "host":     host,
            "port":     port,
            "user":     user,
            "database": database,
        }

        print("Connecting to MySQL with config:", debug_info)

        try:
            self._safe_close()

            kw: Dict[str, Any] = dict(
                host            = host,
                port            = port,
                user            = user,
                password        = password,
                connect_timeout = 5,
                autocommit      = True,
                # Use DictCursor globally so all cursors return dicts by default.
                cursorclass     = pymysql.cursors.DictCursor,
            )
            if database:
                kw["database"] = database

            print("Connecting via pymysql (caching_sha2_password supported) ...")
            self._connection = pymysql.connect(**kw)

            # Validate the connection with a real round-trip query.
            cur = self._connection.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()

            current_db = self._query_current_db()
            msg = f"Connected successfully. Active database: {current_db or '(none)'}"
            print(msg)
            return {"status": "success", "message": msg, "config": debug_info}

        except Exception as exc:
            error_msg = f"Connection failed: {exc}"
            print("ERROR:", error_msg)
            traceback.print_exc()
            self._connection = None
            return {"status": "error", "message": error_msg, "config": debug_info}

    def _query_current_db(self) -> Optional[str]:
        """Return the name of the currently selected database, or None."""
        try:
            cur = self._connection.cursor()
            cur.execute("SELECT DATABASE();")
            row = cur.fetchone()
            cur.close()
            # mysql-connector returns a tuple; pymysql DictCursor returns a dict
            if isinstance(row, dict):
                return list(row.values())[0]
            return row[0] if row else None
        except Exception:
            return None

    def ensure_connected(self) -> dict:
        """
        Connect if not already connected, or reconnect if the connection dropped.
        Returns the same dict as connect().
        """
        if not self.is_connected:
            return self.connect()
        return {"status": "success", "message": "Already connected.", "config": {}}

    def disconnect(self) -> None:
        """Close the connection if open."""
        self._safe_close()

    def _safe_close(self) -> None:
        """Close and discard the connection without raising."""
        try:
            if self._connection is not None:
                self._connection.close()
        except Exception:
            pass
        finally:
            self._connection = None

    @property
    def is_connected(self) -> bool:
        """
        True only when a live connection exists.
        Validated with a real SELECT 1 round-trip.
        """
        if self._connection is None:
            return False
        try:
            cur = self._connection.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            return True
        except Exception:
            self._connection = None
            return False

    def _require_connection(self) -> None:
        """Raise a clear error if there is no active connection."""
        if not self.is_connected:
            raise RuntimeError(
                "No active database connection. "
                "Call connect() first and check that it returned status='success'."
            )

    # ------------------------------------------------------------------
    # Schema sync
    # ------------------------------------------------------------------

    def sync_schema_to_catalog(self, catalog) -> Tuple[Any, int]:
        """
        Read the live schema from information_schema and push it into *catalog*.
        Returns (catalog, number_of_tables_synced).
        """
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is required for sync_schema_to_catalog. Run: pip install pandas"
            )

        self._require_connection()
        cursor = self._connection.cursor()   # DictCursor set at connect time

        try:
            cursor.execute(
                """
                SELECT TABLE_NAME, COALESCE(TABLE_ROWS, 1) AS TABLE_ROWS
                FROM   information_schema.TABLES
                WHERE  TABLE_SCHEMA = DATABASE()
                  AND  TABLE_TYPE   = 'BASE TABLE'
                ORDER  BY TABLE_NAME
                """
            )
            table_rows: Dict[str, int] = {
                row["TABLE_NAME"]: max(1, int(row["TABLE_ROWS"]))
                for row in cursor.fetchall()
            }

            if not table_rows:
                return catalog, 0

            placeholders = ", ".join(["%s"] * len(table_rows))
            cursor.execute(
                f"""
                SELECT TABLE_NAME, COLUMN_NAME
                FROM   information_schema.COLUMNS
                WHERE  TABLE_SCHEMA  = DATABASE()
                  AND  TABLE_NAME IN ({placeholders})
                ORDER  BY TABLE_NAME, ORDINAL_POSITION
                """,
                tuple(table_rows.keys()),
            )
            table_cols: Dict[str, List[str]] = {}
            for row in cursor.fetchall():
                tbl = row["TABLE_NAME"]
                table_cols.setdefault(tbl, []).append(row["COLUMN_NAME"])

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
    # Raw query helpers (used by executor)
    # ------------------------------------------------------------------

    def get_cursor(self):
        """Return a fresh plain (tuple-row) cursor on the active connection."""
        self._require_connection()
        return self._connection.cursor(pymysql.cursors.Cursor)

    def get_dict_cursor(self):
        """Return a fresh dictionary cursor on the active connection."""
        self._require_connection()
        return self._connection.cursor(pymysql.cursors.DictCursor)

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        state = "connected" if self.is_connected else "disconnected"
        return f"DatabaseManager({self.host}:{self.port}/{self.database or '(no db)'}, {state})"