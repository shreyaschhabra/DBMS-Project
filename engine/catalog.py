""" catalog.py """

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

    Acts as a surrogate for a real DBMS's system catalog. Holds metadata
    for tables to drive the Cost-Based Optimizer (CBO).

    Default tables:
        users     : 10,000 rows  | columns: id, name, city_id
        cities    :    100 rows  | columns: id, city_name, country_id
        countries :     10 rows  | columns: id, country_name

    Mutable API (new):
        add_table(name, row_count, columns)  — add or overwrite a table
        remove_table(name)                   — remove a table entry
        sync_from_dataframe(df)              — bulk update from a DataFrame
        to_dataframe()                       — export as a DataFrame
    """

    def __init__(self) -> None:
        # Default catalog: Olist Brazilian E-Commerce dataset
        # (row counts are approximate; synced from live DB when connected)
        self._tables: Dict[str, TableStats] = {
            "olist_orders_dataset": TableStats(
                name="olist_orders_dataset",
                row_count=99_441,
                columns=["order_id", "customer_id", "order_status",
                         "order_purchase_timestamp", "order_delivered_customer_date"],
            ),
            "olist_customers_dataset": TableStats(
                name="olist_customers_dataset",
                row_count=99_441,
                columns=["customer_id", "customer_unique_id",
                         "customer_zip_code_prefix", "customer_city", "customer_state"],
            ),
            "olist_order_items_dataset": TableStats(
                name="olist_order_items_dataset",
                row_count=112_650,
                columns=["order_id", "order_item_id", "product_id",
                         "seller_id", "price", "freight_value"],
            ),
            "olist_order_payments_dataset": TableStats(
                name="olist_order_payments_dataset",
                row_count=103_886,
                columns=["order_id", "payment_sequential", "payment_type",
                         "payment_installments", "payment_value"],
            ),
            "olist_products_dataset": TableStats(
                name="olist_products_dataset",
                row_count=32_951,
                columns=["product_id", "product_category_name", "product_name_lenght",
                         "product_photos_qty", "product_weight_g"],
            ),
            "olist_sellers_dataset": TableStats(
                name="olist_sellers_dataset",
                row_count=3_095,
                columns=["seller_id", "seller_zip_code_prefix",
                         "seller_city", "seller_state"],
            ),
            "olist_order_reviews_dataset": TableStats(
                name="olist_order_reviews_dataset",
                row_count=99_224,
                columns=["review_id", "order_id", "review_score",
                         "review_comment_title", "review_creation_date"],
            ),
            "product_category_name_translation": TableStats(
                name="product_category_name_translation",
                row_count=71,
                columns=["product_category_name", "product_category_name_english"],
            ),
        }

    # ------------------------------------------------------------------
    # Read-only Public API
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

        Returns:
            A dict mapping table_name -> {"row_count": int, "columns": List[str]}.
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

    # ------------------------------------------------------------------
    # Mutable Public API (new)
    # ------------------------------------------------------------------

    def add_table(self, name: str, row_count: int, columns: List[str]) -> None:
        """
        Add a new table to the catalog, or overwrite an existing one.

        Parameters:
            name      : Table name (will be stored lower-cased).
            row_count : Estimated row count (cardinality).
            columns   : List of column name strings.
        """
        name = name.strip().lower()
        if not name:
            return
        self._tables[name] = TableStats(
            name=name,
            row_count=max(1, int(row_count)),
            columns=[c.strip() for c in columns if c.strip()],
        )

    def remove_table(self, name: str) -> None:
        """
        Remove a table from the catalog.

        Parameters:
            name : Table name to remove. No-op if not found.
        """
        self._tables.pop(name.strip().lower(), None)

    def sync_from_dataframe(self, df) -> None:
        """
        Bulk-update the catalog from a pandas DataFrame.

        The DataFrame must have columns:
            table      (str)  — table name
            row_count  (int)  — row count / cardinality
            columns    (str)  — comma-separated list of column names

        Rows with an empty or whitespace-only 'table' value are skipped.
        The existing catalog is replaced entirely by the DataFrame contents.

        Parameters:
            df : pandas.DataFrame with the schema described above.
        """
        new_tables: Dict[str, TableStats] = {}
        for _, row in df.iterrows():
            name = str(row.get("table", "")).strip().lower()
            if not name:
                continue
            try:
                row_count = max(1, int(row.get("row_count", 1)))
            except (ValueError, TypeError):
                row_count = 1
            cols_raw = str(row.get("columns", "")).strip()
            cols = [c.strip() for c in cols_raw.split(",") if c.strip()]
            new_tables[name] = TableStats(
                name=name,
                row_count=row_count,
                columns=cols,
            )
        self._tables = new_tables

    def to_dataframe(self):
        """
        Export the catalog as a pandas DataFrame suitable for ``st.data_editor``.

        Returns a DataFrame with columns:
            table (str), row_count (int), columns (str — comma-separated)
        """
        import pandas as pd  # lazy import — only needed when called from Streamlit
        rows = [
            {
                "table": stats.name,
                "row_count": stats.row_count,
                "columns": ", ".join(stats.columns),
            }
            for stats in self._tables.values()
        ]
        return pd.DataFrame(rows, columns=["table", "row_count", "columns"])

    def __repr__(self) -> str:
        lines = ["Catalog("]
        for name, stats in self._tables.items():
            lines.append(
                f"  {name}: {stats.row_count:,} rows, cols={stats.columns}"
            )
        lines.append(")")
        return "\n".join(lines)
