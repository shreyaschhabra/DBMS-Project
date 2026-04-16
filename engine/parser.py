"""
engine/parser.py
----------------
SQL → Logical Plan Parser

Uses the ``sqlglot`` library to parse a raw SQL SELECT string into an AST,
then traverses that AST to construct an unoptimized logical relational-algebra
tree.

Supported SQL grammar:
    [WITH <cte_name> AS (<inner_select>)]
    SELECT <col1>, <col2>, ... | <agg_func(col)>
    FROM   <table1> [AS <alias>]
    [INNER JOIN | LEFT [OUTER] JOIN | RIGHT [OUTER] JOIN <table2> [AS <alias>] ON <condition>]*
    [WHERE <predicate>]
    [GROUP BY <key1>, <key2>]
    [HAVING <predicate>]

Key behaviours
~~~~~~~~~~~~~~
**Join type detection**
    ``sqlglot`` exposes ``join.args["side"]`` to distinguish INNER, LEFT,
    RIGHT, etc.  This value is normalised to upper-case and stored in
    ``JoinNode.join_type``.

**AND splitting in WHERE**
    A WHERE clause composed entirely of AND-connected clauses is split into
    *individual* ``SelectNode``s stacked above the join tree.  Each split
    predicate can then be pushed down independently by the RBO, enabling finer-
    grained filter placement.

**OR blocks in WHERE**
    Any clause whose top-level connective is OR (including ``(A OR B) AND C``
    — the OR sub-expression) is kept as a *single atomic* ``SelectNode`` with
    ``is_or_block=True``.  The RBO applies special OR-pushdown logic to decide
    whether this block can be pushed to one side of the join.

**Alias resolution**
    All column references in condition strings (JOIN ON, WHERE, GROUP BY,
    HAVING, SELECT list) are resolved from aliases to **real table names**
    before being stored in plan nodes.  This keeps the RBO and CBO completely
    alias-agnostic; aliases survive only in ``ScanNode.alias`` and
    ``SubqueryNode.alias`` for display purposes.

Output tree shape (before any optimization):
    ProjectNode
      └── [AggregateNode]               (only if GROUP BY is present)
            └── [SelectNode …]          (one per AND-clause; OR-blocks atomic)
                  └── JoinNode …        (or ScanNode / SubqueryNode for a single source)
                         ├── ScanNode | SubqueryNode
                         └── ScanNode | SubqueryNode

CTE handling
~~~~~~~~~~~~
Each ``WITH <name> AS (<inner_select>)`` is parsed recursively into a full
``SubqueryNode`` whose ``alias`` equals the CTE name.  When the outer query
references the CTE name in its FROM or JOIN, a ``SubqueryNode`` is used as the
leaf instead of a ``ScanNode``.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

import sqlglot
import sqlglot.expressions as exp

from engine.nodes import (
    AggregateNode,
    JoinNode,
    PlanNode,
    ProjectNode,
    ScanNode,
    SelectNode,
    SubqueryNode,
)


# ─────────────────────────────────────────────────────────────────────────────
# Main parser class
# ─────────────────────────────────────────────────────────────────────────────

class QueryParser:
    """
    Parse a SQL SELECT string and produce an unoptimized logical plan tree.

    Usage::

        parser = QueryParser()
        tree   = parser.parse("SELECT u.name FROM users u WHERE u.id > 5")

    Supports: aliases, multi-table JOINs (INNER/LEFT/RIGHT), WHERE (AND-split /
    OR-block), GROUP BY, HAVING, CTEs.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse(self, sql: str) -> PlanNode:
        """
        Parse *sql* and return the root of the unoptimized logical plan tree.

        Parameters:
            sql : A SQL SELECT statement string (may include a WITH clause).

        Returns:
            The root PlanNode of the logical plan.

        Raises:
            ValueError : If the SQL cannot be parsed or is missing a FROM clause.
        """
        sql = sql.strip().rstrip(";")
        try:
            ast = sqlglot.parse_one(sql)
        except Exception as exc:
            raise ValueError(
                f"sqlglot could not parse the SQL: {exc}\n  SQL: {sql}"
            ) from exc

        if ast is None:
            raise ValueError(f"sqlglot returned None for: {sql!r}")

        # Step 1 — Build CTE registry from the WITH clause.
        cte_registry: Dict[str, SubqueryNode] = {}
        with_clause = ast.args.get("with_")
        if with_clause:
            for cte in with_clause.expressions:
                cte_name: str = cte.alias.lower()
                inner_select = cte.this
                if isinstance(inner_select, exp.Subquery):
                    inner_select = inner_select.this  # unwrap if needed
                inner_plan = self._parse_select(inner_select, cte_registry={})
                cte_registry[cte_name] = SubqueryNode(
                    child=inner_plan, alias=cte_name
                )

        # Step 2 — Parse the outer SELECT using the populated CTE registry.
        return self._parse_select(ast, cte_registry)

    def explain_parse(self, sql: str) -> str:
        """
        Return a human-readable breakdown of what the parser extracted.
        Useful for debugging and the Streamlit UI.

        Parameters:
            sql : A SQL SELECT statement string.

        Returns:
            Multi-line report string.
        """
        lines = ["=== Parser Extraction Report ==="]
        try:
            sql = sql.strip().rstrip(";")
            ast = sqlglot.parse_one(sql)

            # ── CTE names ───────────────────────────────────────────────
            cte_names: List[str] = []
            with_clause = ast.args.get("with_")
            if with_clause:
                for cte in with_clause.expressions:
                    cte_names.append(cte.alias.lower())
            if cte_names:
                lines.append(f"  CTEs           : {', '.join(cte_names)}")

            # ── Build alias map (for display in report) ──────────────────
            alias_map: Dict[str, str] = {}
            from_e = ast.args.get("from_")
            if from_e and isinstance(from_e.this, exp.Table):
                self._register_alias(from_e.this, alias_map, cte_names)
            for join in (ast.args.get("joins") or []):
                if isinstance(join.this, exp.Table):
                    self._register_alias(join.this, alias_map, cte_names)

            alias_display = {k: v for k, v in alias_map.items() if k != v}
            if alias_display:
                lines.append(
                    "  Alias map      : {"
                    + ", ".join(f"{k} -> {v}" for k, v in alias_display.items())
                    + "}"
                )
            else:
                lines.append("  Alias map      : (none)")

            # ── FROM ────────────────────────────────────────────────────
            if from_e and isinstance(from_e.this, exp.Table):
                t       = from_e.this
                suffix  = " (CTE)" if t.name.lower() in cte_names else ""
                alias_s = f" AS {t.alias}" if t.alias else ""
                lines.append(f"  FROM table     : {t.name.lower()}{alias_s}{suffix}")
            else:
                lines.append("  FROM table     : (unknown)")

            # ── JOINs (with join type) ───────────────────────────────────
            joins = ast.args.get("joins") or []
            if joins:
                for join in joins:
                    if isinstance(join.this, exp.Table):
                        t         = join.this
                        on_expr   = join.args.get("on")
                        cond      = (
                            self._resolve_aliases(on_expr.sql(), alias_map)
                            if on_expr else "(none)"
                        )
                        alias_s   = f" AS {t.alias}" if t.alias else ""
                        suffix    = " (CTE)" if t.name.lower() in cte_names else ""
                        jtype     = self._detect_join_type(join)
                        lines.append(
                            f"  {jtype} JOIN    : {t.name.lower()}{alias_s}{suffix} ON {cond}"
                        )
            else:
                lines.append("  JOINs          : (none)")

            # ── WHERE (show AND-split / OR-block info) ───────────────────
            where_e = ast.args.get("where")
            if where_e:
                clauses = self._split_where_clauses(where_e.this, alias_map)
                if len(clauses) > 1:
                    lines.append(f"  WHERE (AND split into {len(clauses)} clauses):")
                    for pred, is_or in clauses:
                        tag = " [OR-block]" if is_or else ""
                        lines.append(f"    - {pred}{tag}")
                else:
                    pred, is_or = clauses[0]
                    tag = " [OR-block]" if is_or else ""
                    lines.append(f"  WHERE          : {pred}{tag}")
            else:
                lines.append("  WHERE          : (none)")

            # ── GROUP BY ────────────────────────────────────────────────
            group_e = ast.args.get("group")
            if group_e:
                gb_cols = [
                    self._resolve_aliases(e.sql(), alias_map)
                    for e in group_e.expressions
                ]
                lines.append(f"  GROUP BY       : {', '.join(gb_cols)}")
            else:
                lines.append("  GROUP BY       : (none)")

            # ── HAVING ──────────────────────────────────────────────────
            having_e = ast.args.get("having")
            if having_e:
                lines.append(
                    f"  HAVING         : "
                    f"{self._resolve_aliases(having_e.this.sql(), alias_map)}"
                )
            else:
                lines.append("  HAVING         : (none)")

            # ── SELECT columns + aggregates ─────────────────────────────
            sel_exprs = ast.args.get("expressions") or []
            cols      = [self._resolve_aliases(e.sql(), alias_map) for e in sel_exprs]
            agg_funcs = []
            for se in sel_exprs:
                for agg in se.find_all(exp.AggFunc):
                    s = self._resolve_aliases(agg.sql(), alias_map)
                    if s not in agg_funcs:
                        agg_funcs.append(s)

            lines.append(f"  SELECT columns : {cols or ['*']}")
            if agg_funcs:
                lines.append(f"  Aggregates     : {', '.join(agg_funcs)}")

        except Exception as exc:
            lines.append(f"  ERROR          : {exc}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Core: parse a sqlglot Select node into a PlanNode tree
    # ------------------------------------------------------------------

    def _parse_select(
        self,
        select_node: exp.Select,
        cte_registry: Dict[str, SubqueryNode],
    ) -> PlanNode:
        """
        Traverse a sqlglot ``Select`` expression and build a logical plan tree.

        Parameters:
            select_node  : A sqlglot Select expression.
            cte_registry : Map of ``cte_name → SubqueryNode`` for the current scope.

        Returns:
            Root PlanNode of the unoptimized logical plan.
        """
        # ── Step A: Build alias map (alias → real table name) ───────────
        alias_map: Dict[str, str] = {}

        from_e = select_node.args.get("from_")
        if from_e and isinstance(from_e.this, exp.Table):
            self._register_alias(from_e.this, alias_map, list(cte_registry.keys()))

        for join in (select_node.args.get("joins") or []):
            if isinstance(join.this, exp.Table):
                self._register_alias(
                    join.this, alias_map, list(cte_registry.keys())
                )

        # ── Step B: Build FROM leaf ──────────────────────────────────────
        if from_e is None:
            raise ValueError("SQL statement has no FROM clause.")

        base_plan: PlanNode = self._make_leaf(from_e.this, alias_map, cte_registry)

        # ── Step C: Chain JOINs left-to-right (with join_type) ─────────
        for join in (select_node.args.get("joins") or []):
            right_leaf = self._make_leaf(join.this, alias_map, cte_registry)
            on_expr    = join.args.get("on")
            cond_str   = (
                self._resolve_aliases(on_expr.sql(), alias_map)
                if on_expr is not None
                else ""
            )
            join_type  = self._detect_join_type(join)
            base_plan  = JoinNode(
                left=base_plan,
                right=right_leaf,
                condition=cond_str,
                join_type=join_type,
            )

        # ── Step D: Split WHERE into stacked SelectNodes ────────────────
        # AND-connected clauses are split so each can be pushed independently.
        # OR-blocks are kept atomic (is_or_block=True).
        where_e = select_node.args.get("where")
        if where_e is not None:
            clauses = self._split_where_clauses(where_e.this, alias_map)
            # Wrap in *reverse* order so the first clause ends up outermost.
            for pred_str, is_or in reversed(clauses):
                base_plan = SelectNode(
                    child=base_plan,
                    predicate=pred_str,
                    is_or_block=is_or,
                )

        # ── Step E: Wrap in AggregateNode if GROUP BY present ───────────
        group_e  = select_node.args.get("group")
        having_e = select_node.args.get("having")

        if group_e is not None:
            group_cols: List[str] = [
                self._resolve_aliases(e.sql(), alias_map)
                for e in group_e.expressions
            ]
            agg_funcs: List[str] = []
            for se in select_node.args.get("expressions") or []:
                for agg in se.find_all(exp.AggFunc):
                    s = self._resolve_aliases(agg.sql(), alias_map)
                    if s not in agg_funcs:
                        agg_funcs.append(s)

            having_str: Optional[str] = None
            if having_e is not None:
                having_str = self._resolve_aliases(
                    having_e.this.sql(), alias_map
                )

            base_plan = AggregateNode(
                child=base_plan,
                group_by_cols=group_cols,
                aggregates=agg_funcs,
                having=having_str,
            )

        # ── Step F: Wrap everything in ProjectNode ──────────────────────
        sel_exprs = select_node.args.get("expressions") or []
        cols: List[str] = (
            [self._resolve_aliases(e.sql(), alias_map) for e in sel_exprs]
            if sel_exprs
            else ["*"]
        )
        return ProjectNode(child=base_plan, columns=cols)

    # ------------------------------------------------------------------
    # WHERE clause splitting
    # ------------------------------------------------------------------

    def _split_where_clauses(
        self,
        expr: exp.Expression,
        alias_map: Dict[str, str],
    ) -> List[tuple]:  # List[Tuple[str, bool]]
        """
        Recursively split a WHERE expression into a flat list of
        ``(predicate_string, is_or_block)`` tuples.

        Rules
        ~~~~~
        - ``AND`` nodes are split into their left and right children
          (recursively), enabling fine-grained independent pushdown.
        - Any other node (including ``OR``, ``Paren`` wrapping OR, or raw
          comparisons) is treated as a *single atomic clause*.  OR clauses
          are tagged with ``is_or_block=True``.

        Parameters:
            expr      : The sqlglot expression node for the WHERE condition.
            alias_map : Alias-to-real-name map for column resolution.

        Returns:
            A list of ``(predicate_str, is_or_block)`` tuples.
        """
        if isinstance(expr, exp.And):
            left_clauses  = self._split_where_clauses(expr.left,  alias_map)
            right_clauses = self._split_where_clauses(expr.right, alias_map)
            return left_clauses + right_clauses

        # Any non-AND expression is an atomic predicate clause.
        pred_str = self._resolve_aliases(expr.sql(), alias_map)
        is_or    = isinstance(expr, exp.Or) or (
            isinstance(expr, exp.Paren) and isinstance(expr.this, exp.Or)
        )
        return [(pred_str, is_or)]

    # ------------------------------------------------------------------
    # Join type detection
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_join_type(join: exp.Join) -> str:
        """
        Return the normalised join type string for a sqlglot ``Join`` node.

        sqlglot stores the join side in ``join.args["side"]``:
          - ``"LEFT"``  → ``"LEFT"``
          - ``"RIGHT"`` → ``"RIGHT"``
          - ``"FULL"``  → ``"FULL"``
          - ``None`` / ``""`` → ``"INNER"``  (default)

        Parameters:
            join : A sqlglot Join expression node.

        Returns:
            One of ``"INNER"``, ``"LEFT"``, ``"RIGHT"``, ``"FULL"``,
            ``"CROSS"``.
        """
        kind = (join.args.get("kind") or "").upper()
        side = (join.args.get("side") or "").upper()

        if kind == "CROSS":
            return "CROSS"
        if side in ("LEFT", "RIGHT", "FULL"):
            return side
        return "INNER"

    # ------------------------------------------------------------------
    # Alias helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _register_alias(
        tbl_expr: exp.Table,
        alias_map: Dict[str, str],
        cte_names: List[str],
    ) -> None:
        """
        Populate *alias_map* with ``alias → real_name`` and ``name → name``
        entries for each table or CTE reference.

        For a CTE reference (``FROM active_users a``), the alias maps to the
        CTE name (``a → active_users``).  For a real table (``FROM users u``),
        the alias maps to the catalog table name (``u → users``).

        Parameters:
            tbl_expr  : A sqlglot Table expression.
            alias_map : Mutable dict to populate.
            cte_names : List of known CTE names in scope.
        """
        tbl_name: str  = tbl_expr.name.lower()
        tbl_alias: str = tbl_expr.alias.lower() if tbl_expr.alias else ""

        # Self-map the real/CTE name so unaliased references pass through.
        alias_map[tbl_name] = tbl_name

        if tbl_alias and tbl_alias != tbl_name:
            alias_map[tbl_alias] = tbl_name

    @staticmethod
    def _make_leaf(
        tbl_expr: exp.Expression,
        alias_map: Dict[str, str],
        cte_registry: Dict[str, SubqueryNode],
    ) -> PlanNode:
        """
        Build the leaf plan node for a table expression.

        - If the table name resolves to a CTE → return its ``SubqueryNode``.
        - Otherwise → return a ``ScanNode`` with the real table name and alias.

        Parameters:
            tbl_expr     : A sqlglot Table (or similar) expression.
            alias_map    : Populated alias → real_name map.
            cte_registry : Known CTEs in scope.

        Returns:
            A ScanNode or SubqueryNode.
        """
        if not isinstance(tbl_expr, exp.Table):
            return ScanNode(table_name=tbl_expr.sql().lower())

        tbl_name:  str = tbl_expr.name.lower()
        tbl_alias: str = tbl_expr.alias.lower() if tbl_expr.alias else ""

        if tbl_name in cte_registry:
            return cte_registry[tbl_name]

        return ScanNode(
            table_name=tbl_name,
            alias=tbl_alias if tbl_alias else None,
        )

    # ------------------------------------------------------------------
    # Alias resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_aliases(sql_str: str, alias_map: Dict[str, str]) -> str:
        """
        Replace every ``alias.column`` token in *sql_str* with
        ``real_table.column`` using *alias_map*.

        This converts aliased references (e.g. ``u.id``) to their catalog
        equivalents (e.g. ``users.id``) so that downstream optimizers remain
        completely alias-agnostic.

        Only dotted tokens of the form ``word.word`` are replaced; plain
        identifiers and keywords are left untouched.

        Parameters:
            sql_str   : Input SQL fragment (condition, column, etc.)
            alias_map : Map of ``alias → real_table_name``.

        Returns:
            The input string with aliases resolved.
        """
        def _replace(match: re.Match) -> str:  # type: ignore[type-arg]
            prefix = match.group(1).lower()
            col    = match.group(2)
            real   = alias_map.get(prefix, prefix)
            return f"{real}.{col}"

        return re.sub(r"([A-Za-z_]\w*)\.([A-Za-z_]\w*)", _replace, sql_str)
