"""
engine/catalog.py
-----------------
Mock Database Catalog
Simulates a real database's system catalog (pg_stats, information_schema, etc.)
Provides metadata like row counts (cardinality) used by the Cost-Based Optimizer.
"""

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class TableStats:
    """
    Holds statistics for a single table.

    Attributes:
        name        : The table name.
        row_count   : Number of rows (cardinality) in the table.
        columns     : List of column names belonging to the table.
    """
    name: str
    row_count: int
    columns: List[str] = field(default_factory=list)


class Catalog:
    """
    Mock Database Statistics Catalog.

    Acts as a surrogate for a real DBMS's system catalog. Holds hardcoded
    metadata for three tables to drive the Cost-Based Optimizer (CBO).

    Tables:
        users     : 10,000 rows  | columns: id, name, city_id
        cities    :    100 rows  | columns: id, city_name, country_id
        countries :     10 rows  | columns: id, country_name
    """

    def __init__(self) -> None:
        # Register all tables with their statistics.
        self._tables: Dict[str, TableStats] = {
            "users": TableStats(
                name="users",
                row_count=10_000,
                columns=["id", "name", "city_id"],
            ),
            "cities": TableStats(
                name="cities",
                row_count=100,
                columns=["id", "city_name", "country_id"],
            ),
            "countries": TableStats(
                name="countries",
                row_count=10,
                columns=["id", "country_name"],
            ),
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_cardinality(self, table_name: str) -> int:
        """
        Return the row count (cardinality) for the given table.

        Parameters:
            table_name : Name of the table to query.

        Returns:
            Integer row count.

        Raises:
            KeyError if the table does not exist in the catalog.
        """
        table_name = table_name.lower()
        if table_name not in self._tables:
            raise KeyError(
                f"Table '{table_name}' not found in catalog. "
                f"Available tables: {list(self._tables.keys())}"
            )
        return self._tables[table_name].row_count

    def get_columns(self, table_name: str) -> List[str]:
        """
        Return the column list for the given table.

        Parameters:
            table_name : Name of the table to query.

        Returns:
            List of column name strings.
        """
        table_name = table_name.lower()
        if table_name not in self._tables:
            raise KeyError(f"Table '{table_name}' not found in catalog.")
        return self._tables[table_name].columns

    def get_all_stats(self) -> Dict[str, Dict]:
        """
        Return a dictionary representation of the entire catalog.

        Useful for rendering statistics in the Streamlit sidebar.

        Returns:
            A dict mapping table_name -> {"row_count": int, "columns": List[str]}.

        Example output::

            {
                "users":     {"row_count": 10000, "columns": ["id", "name", "city_id"]},
                "cities":    {"row_count":   100, "columns": ["id", "city_name", "country_id"]},
                "countries": {"row_count":    10, "columns": ["id", "country_name"]},
            }
        """
        return {
            name: {
                "row_count": stats.row_count,
                "columns": stats.columns,
            }
            for name, stats in self._tables.items()
        }

    def table_exists(self, table_name: str) -> bool:
        """Return True if the given table exists in the catalog."""
        return table_name.lower() in self._tables

    def __repr__(self) -> str:
        lines = ["Catalog("]
        for name, stats in self._tables.items():
            lines.append(
                f"  {name}: {stats.row_count:,} rows, cols={stats.columns}"
            )
        lines.append(")")
        return "\n".join(lines)
