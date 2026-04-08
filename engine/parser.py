"""
engine/parser.py
----------------
SQL → Logical Plan Parser

Uses the `sqlparse` library to tokenize and parse a raw SQL SELECT string,
then constructs an unoptimized logical relational-algebra tree from the AST.

Supported SQL grammar (subset):
    SELECT <col1>, <col2>, ...
    FROM   <table1>
    [JOIN  <table2> ON <condition>]*
    [WHERE <predicate>]

Output tree shape (before any optimization):
    ProjectNode
      └── SelectNode  (only if WHERE clause is present)
            └── JoinNode  (or ScanNode if a single table)
                  ├── ScanNode (left)
                  └── ScanNode (right)
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

import sqlparse
from sqlparse.sql import (
    Comparison,
    Identifier,
    IdentifierList,
    Parenthesis,
    Where,
)
from sqlparse.tokens import Keyword, DML, Punctuation

from engine.nodes import (
    JoinNode,
    PlanNode,
    ProjectNode,
    ScanNode,
    SelectNode,
)


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _clean(token_value: str) -> str:
    """Strip surrounding whitespace and optional SQL quoting."""
    return token_value.strip().strip('"').strip("'").strip("`")


def _extract_identifiers(id_list) -> List[str]:
    """
    Flatten an IdentifierList or single Identifier into a plain list of
    'table.column' or 'column' strings.
    """
    if isinstance(id_list, IdentifierList):
        return [_clean(ident.value) for ident in id_list.get_identifiers()]
    if isinstance(id_list, Identifier):
        return [_clean(id_list.value)]
    # Fallback: treat as a raw token value.
    return [_clean(id_list.value)]


def _normalize_sql(sql: str) -> str:
    """
    Normalise the SQL string for easier regex parsing:
    - Collapse consecutive whitespace / newlines to a single space.
    - Upper-case keywords so regex alternation is simpler.
    """
    return re.sub(r"\s+", " ", sql).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Main parser class
# ─────────────────────────────────────────────────────────────────────────────

class QueryParser:
    """
    Parse a SQL SELECT string and produce an unoptimized logical plan tree.

    Usage::

        parser = QueryParser()
        tree   = parser.parse("SELECT u.name FROM users u WHERE u.id > 5")

    The returned tree has the shape::

        ProjectNode
          └── SelectNode (if WHERE present)
                └── JoinNode | ScanNode
    """

    # Regex for capturing JOIN … ON clauses (handles multiple JOINs).
    _JOIN_RE = re.compile(
        r"(?:INNER\s+)?JOIN\s+(\w+)\s+ON\s+(.+?)(?=(?:INNER\s+)?JOIN\s+\w+\s+ON|WHERE|$)",
        re.IGNORECASE,
    )

    # Regex for the WHERE clause remainder.
    _WHERE_RE = re.compile(r"WHERE\s+(.+?)$", re.IGNORECASE)

    # Regex for FROM clause (first table, before any JOIN).
    _FROM_RE = re.compile(
        r"FROM\s+(\w+)(?:\s+(?:AS\s+)?\w+)?(?:\s|$)",
        re.IGNORECASE,
    )

    # Regex for SELECT column list.
    _SELECT_RE = re.compile(
        r"SELECT\s+(.+?)\s+FROM\b",
        re.IGNORECASE | re.DOTALL,
    )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse(self, sql: str) -> PlanNode:
        """
        Parse *sql* and return the root of the unoptimized logical plan tree.

        Parameters:
            sql : A SQL SELECT statement string.

        Returns:
            The root PlanNode of the logical plan.

        Raises:
            ValueError : If the SQL cannot be parsed or contains unsupported syntax.
        """
        normalised = _normalize_sql(sql)

        columns   = self._extract_columns(normalised)
        from_table = self._extract_from_table(normalised)
        joins      = self._extract_joins(normalised)
        where      = self._extract_where(normalised)

        # ── Build the leaf scan nodes ─────────────────────────────────
        base_plan: PlanNode = ScanNode(table_name=from_table)

        if joins:
            # Chain joins left-to-right: ((A JOIN B) JOIN C)
            join_conditions = []
            for join_table, join_cond in joins:
                right_scan = ScanNode(table_name=join_table)
                base_plan  = JoinNode(
                    left=base_plan,
                    right=right_scan,
                    condition=join_cond.strip(),
                )
                join_conditions.append(join_cond.strip())

        # ── Wrap in SelectNode if WHERE is present ───────────────────
        if where:
            base_plan = SelectNode(child=base_plan, predicate=where.strip())

        # ── Wrap everything in ProjectNode ───────────────────────────
        plan = ProjectNode(child=base_plan, columns=columns)

        return plan

    # ------------------------------------------------------------------
    # Private extraction helpers
    # ------------------------------------------------------------------

    def _extract_columns(self, sql: str) -> List[str]:
        """
        Extract the SELECT column list.

        Returns a list of column strings, e.g. ["users.name", "cities.city_name"].
        Falls back to ["*"] if parsing fails.
        """
        match = self._SELECT_RE.search(sql)
        if not match:
            return ["*"]
        raw = match.group(1).strip()
        # Split on commas, but beware of commas inside parentheses.
        cols = [c.strip() for c in self._split_by_comma(raw)]
        return cols if cols else ["*"]

    def _extract_from_table(self, sql: str) -> str:
        """
        Extract the primary (FROM) table name.

        Handles optional aliases: FROM users  or  FROM users u
        """
        match = self._FROM_RE.search(sql)
        if not match:
            raise ValueError(
                "Could not find a FROM clause in the SQL statement.\n"
                f"  SQL: {sql}"
            )
        return match.group(1).strip().lower()

    def _extract_joins(self, sql: str) -> List[Tuple[str, str]]:
        """
        Extract all JOIN … ON clauses.

        Returns a list of (table_name, condition) tuples in the order they
        appear in the SQL.
        """
        results: List[Tuple[str, str]] = []
        for match in self._JOIN_RE.finditer(sql):
            table = match.group(1).strip().lower()
            cond  = match.group(2).strip()
            # Strip trailing keywords the regex may have captured.
            cond = re.sub(r"\s+(WHERE|ORDER|GROUP|LIMIT|HAVING).*$", "", cond, flags=re.IGNORECASE).strip()
            results.append((table, cond))
        return results

    def _extract_where(self, sql: str) -> Optional[str]:
        """
        Extract the WHERE predicate string.

        Returns None if no WHERE clause is present.
        """
        match = self._WHERE_RE.search(sql)
        if not match:
            return None
        predicate = match.group(1).strip()
        # Remove any trailing ORDER BY / LIMIT / GROUP BY clauses.
        predicate = re.sub(
            r"\s+(ORDER|GROUP|LIMIT|HAVING)\b.*$",
            "",
            predicate,
            flags=re.IGNORECASE,
        ).strip()
        return predicate if predicate else None

    @staticmethod
    def _split_by_comma(text: str) -> List[str]:
        """
        Split *text* by top-level commas (i.e., not inside parentheses).
        """
        parts: List[str] = []
        depth = 0
        current: List[str] = []
        for ch in text:
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                current.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            parts.append("".join(current).strip())
        return parts

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def explain_parse(self, sql: str) -> str:
        """
        Return a human-readable breakdown of what the parser extracted.
        Useful for debugging and the Streamlit UI.
        """
        normalised = _normalize_sql(sql)
        lines = ["=== Parser Extraction Report ==="]
        try:
            cols  = self._extract_columns(normalised)
            lines.append(f"  SELECT columns : {cols}")
        except Exception as exc:
            lines.append(f"  SELECT columns : ERROR – {exc}")
        try:
            from_t = self._extract_from_table(normalised)
            lines.append(f"  FROM table     : {from_t}")
        except Exception as exc:
            lines.append(f"  FROM table     : ERROR – {exc}")
        joins = self._extract_joins(normalised)
        if joins:
            for tbl, cond in joins:
                lines.append(f"  JOIN           : {tbl} ON {cond}")
        else:
            lines.append("  JOINs          : (none)")
        where = self._extract_where(normalised)
        lines.append(f"  WHERE          : {where or '(none)'}")
        return "\n".join(lines)
