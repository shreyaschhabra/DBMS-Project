"""
engine/nodes.py
---------------
Relational Algebra Tree Nodes

Every node represents a single relational-algebra operation:
  - ScanNode      — full sequential scan of a base table
  - SelectNode    — filter rows by a predicate (WHERE / OR-block)
  - ProjectNode   — project a subset of columns (SELECT list)
  - JoinNode      — inner, left, or right join on a condition
  - AggregateNode — aggregate rows (GROUP BY / HAVING)
  - SubqueryNode  — derived table — a CTE or inline subquery

SQL Unparser — Flattening Strategy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The unparser collects the entire join spine into a ``_JoinPlan`` structure
(tables, join conditions, predicates) and renders it as a single flat query:

    SELECT cols
    FROM   t1
    JOIN   t2  ON t1.x = t2.y
    JOIN   t3  ON t2.z = t3.w
    WHERE  pred1 AND pred2

Rules:
  1. Project → JoinSpine  → SELECT cols FROM ... JOIN ... [WHERE ...]
  2. Project → Scan       → SELECT cols FROM table [WHERE pred_from_Select]
  3. Select  → Scan       → SELECT * FROM table WHERE pred
  4. bare Scan            → SELECT * FROM table

Subquery wrapping is ONLY emitted for:
  - SubqueryNode (CTE / derived table by definition)
  - AggregateNode over a complex child
  - Arms that are themselves a SubqueryNode / AggregateNode
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Optional, Set, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _next_alias(ctr: List[int]) -> str:
    ctr[0] += 1
    return f"subq_{ctr[0]}"


def _indent_block(sql: str, spaces: int = 4) -> str:
    pad = " " * spaces
    return "\n".join(pad + line for line in sql.splitlines())


# ─────────────────────────────────────────────────────────────────────────────
# Multi-table join plan collector
# ─────────────────────────────────────────────────────────────────────────────

class _JoinEntry(NamedTuple):
    """One JOIN arm."""
    table:     str          # base table name
    alias:     str          # alias (== table when no alias)
    join_type: str          # INNER | LEFT | RIGHT | FULL | CROSS
    condition: str          # ON predicate (empty for the first / FROM table)


class _JoinPlan(NamedTuple):
    """Fully-flattened join spine + filter predicates + select columns."""
    tables:      List[_JoinEntry]   # from/join tables in order
    predicates:  List[str]          # collected WHERE predicates
    cols:        str                # SELECT list ("*" or "a, b, c")


def _try_collect_join(node: "PlanNode", cols: str = "*") -> Optional[_JoinPlan]:
    """
    Recursively walk a subtree and collect its join spine into a ``_JoinPlan``.

    Returns ``None`` if the subtree contains a SubqueryNode, AggregateNode,
    or any other node that cannot be expressed flat.

    Recognised patterns (recursively):
      * ScanNode
      * SelectNode   → (recursive)
      * ProjectNode  → (recursive)   [cols are overridden by the outermost Project]
      * JoinNode     → left + right  (recursively)
    """
    return _collect(node, cols, [])


def _collect(
    node: "PlanNode",
    cols: str,
    preds: List[str],
) -> Optional[_JoinPlan]:
    """Internal recursive collector."""

    if isinstance(node, ScanNode):
        alias = node.alias or node.table_name
        entry = _JoinEntry(table=node.table_name, alias=alias,
                           join_type="INNER", condition="")
        return _JoinPlan(tables=[entry], predicates=list(preds), cols=cols)

    if isinstance(node, SelectNode):
        # Accumulate predicate, recurse into child
        new_preds = list(preds) + [node.predicate]
        return _collect(node.child, cols, new_preds)

    if isinstance(node, ProjectNode):
        # Only override cols if the outer caller hasn't specified one yet.
        # The outermost ProjectNode's list wins; inner pushed-down projections
        # are irrelevant to the final SELECT list (they just narrow scans).
        node_cols = ", ".join(node.columns) if node.columns else "*"
        effective_cols = node_cols if (cols == "*" and node_cols != "*") else cols
        return _collect(node.child, effective_cols, preds)

    if isinstance(node, JoinNode):
        # Collect left and right arms independently
        left_plan  = _collect(node.left,  cols, preds)
        right_plan = _collect(node.right, cols, [])
        if left_plan is None or right_plan is None:
            return None

        # Merge: left tables first, then right tables (first right entry gets
        # the join type + condition from the JoinNode).
        right_tables = list(right_plan.tables)
        if right_tables:
            first_right = right_tables[0]
            right_tables[0] = _JoinEntry(
                table=first_right.table,
                alias=first_right.alias,
                join_type=node.join_type,
                condition=node.condition,
            )

        merged_preds = left_plan.predicates + right_plan.predicates
        return _JoinPlan(
            tables=left_plan.tables + right_tables,
            predicates=merged_preds,
            cols=left_plan.cols,  # outermost ProjectNode controls cols
        )

    # SubqueryNode, AggregateNode, or unknown — cannot flatten
    return None


def _render_join_plan(plan: _JoinPlan) -> str:
    """
    Render a ``_JoinPlan`` as a flat MySQL SELECT statement.

    If any table alias differs from the table name an ``AS alias`` clause
    is appended.  Duplicate predicates are deduplicated.
    """
    lines: List[str] = [f"SELECT {plan.cols}"]

    for i, entry in enumerate(plan.tables):
        alias_clause = f" AS {entry.alias}" if entry.alias != entry.table else ""
        if i == 0:
            lines.append(f"FROM {entry.table}{alias_clause}")
        else:
            jk = f"{entry.join_type} JOIN" if entry.join_type != "INNER" else "JOIN"
            lines.append(f"{jk} {entry.table}{alias_clause}")
            if entry.condition:
                lines.append(f"ON {entry.condition}")

    # Deduplicate predicates while preserving order
    seen: Dict[str, None] = {}
    unique_preds = [p for p in plan.predicates if not (p in seen or seen.update({p: None}))]  # type: ignore[func-returns-value]
    if unique_preds:
        if len(unique_preds) == 1:
            lines.append(f"WHERE {unique_preds[0]}")
        else:
            where_body = "\n  AND ".join(f"({p})" for p in unique_preds)
            lines.append(f"WHERE {where_body}")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Simple single-table flatten (used for non-join branches)
# ─────────────────────────────────────────────────────────────────────────────

class _Flat(NamedTuple):
    table: str
    alias: str
    cols:  str
    where: str


def _try_flatten(node: "PlanNode") -> Optional[_Flat]:
    """
    Flatten *node* into a ``_Flat`` for single-table patterns (no join).
    Returns None if a join or complex node is encountered.
    """
    if isinstance(node, ScanNode):
        alias = node.alias or node.table_name
        return _Flat(table=node.table_name, alias=alias, cols="*", where="")

    if isinstance(node, SelectNode) and isinstance(node.child, ScanNode):
        scan  = node.child
        alias = scan.alias or scan.table_name
        return _Flat(table=scan.table_name, alias=alias, cols="*", where=node.predicate)

    if isinstance(node, ProjectNode) and isinstance(node.child, ScanNode):
        scan  = node.child
        alias = scan.alias or scan.table_name
        cols  = ", ".join(node.columns) if node.columns else "*"
        return _Flat(table=scan.table_name, alias=alias, cols=cols, where="")

    if (isinstance(node, ProjectNode)
            and isinstance(node.child, SelectNode)
            and isinstance(node.child.child, ScanNode)):
        sel   = node.child
        scan  = sel.child
        alias = scan.alias or scan.table_name
        cols  = ", ".join(node.columns) if node.columns else "*"
        return _Flat(table=scan.table_name, alias=alias, cols=cols, where=sel.predicate)

    if (isinstance(node, SelectNode)
            and isinstance(node.child, ProjectNode)
            and isinstance(node.child.child, ScanNode)):
        proj  = node.child
        scan  = proj.child
        alias = scan.alias or scan.table_name
        cols  = ", ".join(proj.columns) if proj.columns else "*"
        return _Flat(table=scan.table_name, alias=alias, cols=cols, where=node.predicate)

    if (isinstance(node, SelectNode)
            and isinstance(node.child, ProjectNode)
            and isinstance(node.child.child, SelectNode)
            and isinstance(node.child.child.child, ScanNode)):
        outer_sel = node
        proj      = node.child
        inner_sel = proj.child
        scan      = inner_sel.child
        alias     = scan.alias or scan.table_name
        cols      = ", ".join(proj.columns) if proj.columns else "*"
        combined  = f"({inner_sel.predicate}) AND ({outer_sel.predicate})"
        return _Flat(table=scan.table_name, alias=alias, cols=cols, where=combined)

    return None


def _flat_to_sql(flat: _Flat) -> str:
    alias_clause = f" AS {flat.alias}" if flat.alias != flat.table else ""
    sql = f"SELECT {flat.cols}\nFROM {flat.table}{alias_clause}"
    if flat.where:
        sql += f"\nWHERE {flat.where}"
    return sql


# ─────────────────────────────────────────────────────────────────────────────
# Subquery wrapper (only used when truly necessary)
# ─────────────────────────────────────────────────────────────────────────────

def _wrap(inner_sql: str, ctr: List[int]) -> Tuple[str, str]:
    """Return (``(\\n    inner\\n)``, alias) derived-table fragment."""
    alias    = _next_alias(ctr)
    indented = _indent_block(inner_sql)
    return f"(\n{indented}\n)", alias


# ─────────────────────────────────────────────────────────────────────────────
# Base class
# ─────────────────────────────────────────────────────────────────────────────

class PlanNode(ABC):
    """Abstract base for all relational-algebra plan nodes."""

    @abstractmethod
    def explain(self, depth: int = 0) -> str: ...

    @property
    @abstractmethod
    def source_tables(self) -> Set[str]: ...

    @abstractmethod
    def to_sql(self, ctr: Optional[List[int]] = None) -> str:
        """
        Recursively generate a SQL string for the subtree.

        Parameters
        ----------
        ctr : Mutable single-element list [int] for unique subquery alias
              generation.  Pass None on the first call.
        """

    @staticmethod
    def _indent(depth: int) -> str:
        if depth == 0:
            return ""
        return "│   " * (depth - 1) + "├── "

    @staticmethod
    def _last_indent(depth: int) -> str:
        if depth == 0:
            return ""
        return "│   " * (depth - 1) + "└── "


# ─────────────────────────────────────────────────────────────────────────────
# ScanNode
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScanNode(PlanNode):
    """Sequential table scan — leaf node of any plan tree."""

    table_name: str
    alias: Optional[str] = None

    @property
    def effective_name(self) -> str:
        return self.alias if self.alias else self.table_name

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        label  = f"{self.table_name} AS {self.alias}" if self.alias else self.table_name
        return f"{indent}SeqScan [ {label} ]\n"

    @property
    def source_tables(self) -> Set[str]:
        return {self.table_name.lower()}

    def to_sql(self, ctr: Optional[List[int]] = None) -> str:
        if ctr is None:
            ctr = [0]
        base = f"SELECT *\nFROM {self.table_name}"
        if self.alias and self.alias != self.table_name:
            base += f" AS {self.alias}"
        return base

    def __repr__(self) -> str:
        return f"ScanNode(table={self.table_name!r}, alias={self.alias!r})"


# ─────────────────────────────────────────────────────────────────────────────
# SelectNode
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SelectNode(PlanNode):
    """
    Selection / Filter (WHERE clause).

    Attributes
    ----------
    child       : Child plan node whose output is filtered.
    predicate   : SQL filter condition string.
    is_or_block : True when the top-level connective is OR.
    """

    child: PlanNode
    predicate: str
    is_or_block: bool = False

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        tag    = "OrFilter" if self.is_or_block else "Filter"
        return f"{indent}{tag} [ {self.predicate} ]\n" + self.child.explain(depth + 1)

    @property
    def source_tables(self) -> Set[str]:
        return self.child.source_tables

    def to_sql(self, ctr: Optional[List[int]] = None) -> str:
        if ctr is None:
            ctr = [0]

        # Try full join-spine flatten first
        plan = _try_collect_join(self)
        if plan is not None:
            return _render_join_plan(plan)

        # Single-table flatten
        flat = _try_flatten(self)
        if flat is not None:
            return _flat_to_sql(flat)

        # Complex child — wrap
        inner_sql = self.child.to_sql(ctr)
        src, alias = _wrap(inner_sql, ctr)
        return f"SELECT *\nFROM {src} AS {alias}\nWHERE {self.predicate}"

    def __repr__(self) -> str:
        return (
            f"SelectNode(predicate={self.predicate!r}, "
            f"is_or_block={self.is_or_block!r})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# ProjectNode
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ProjectNode(PlanNode):
    """
    Projection (SELECT column list).

    Attributes
    ----------
    child   : Child plan node.
    columns : Column names to include.  [``*``] = SELECT *.
    """

    child: PlanNode
    columns: List[str] = field(default_factory=lambda: ["*"])

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        cols   = ", ".join(self.columns) if self.columns else "*"
        return f"{indent}Project [ {cols} ]\n" + self.child.explain(depth + 1)

    @property
    def source_tables(self) -> Set[str]:
        return self.child.source_tables

    def to_sql(self, ctr: Optional[List[int]] = None) -> str:
        if ctr is None:
            ctr = [0]
        cols = ", ".join(self.columns) if self.columns else "*"

        # Try full join-spine flatten (Project sits above a join tree)
        plan = _try_collect_join(self)
        if plan is not None:
            return _render_join_plan(plan)

        # Single-table flatten
        flat = _try_flatten(self)
        if flat is not None:
            effective_cols = cols if cols != "*" else flat.cols
            alias_clause   = f" AS {flat.alias}" if flat.alias != flat.table else ""
            sql = f"SELECT {effective_cols}\nFROM {flat.table}{alias_clause}"
            if flat.where:
                sql += f"\nWHERE {flat.where}"
            return sql

        # Complex child — wrap
        inner_sql  = self.child.to_sql(ctr)
        src, alias = _wrap(inner_sql, ctr)
        return f"SELECT {cols}\nFROM {src} AS {alias}"

    def __repr__(self) -> str:
        return f"ProjectNode(columns={self.columns!r})"


# ─────────────────────────────────────────────────────────────────────────────
# JoinNode
# ─────────────────────────────────────────────────────────────────────────────

_VALID_JOIN_TYPES = frozenset({"INNER", "LEFT", "RIGHT", "FULL", "CROSS"})


@dataclass
class JoinNode(PlanNode):
    """
    Join of two sub-trees.

    Attributes
    ----------
    left      : Left child plan node.
    right     : Right child plan node.
    condition : Join predicate string.
    join_type : ``"INNER"`` | ``"LEFT"`` | ``"RIGHT"`` | ``"FULL"`` | ``"CROSS"``.
    """

    left: PlanNode
    right: PlanNode
    condition: str
    join_type: str = "INNER"

    def __post_init__(self) -> None:
        jt = self.join_type.upper()
        if jt not in _VALID_JOIN_TYPES:
            raise ValueError(
                f"Invalid join_type {self.join_type!r}. "
                f"Must be one of {sorted(_VALID_JOIN_TYPES)}."
            )
        self.join_type = jt

    @property
    def is_outer(self) -> bool:
        return self.join_type in ("LEFT", "RIGHT", "FULL")

    def explain(self, depth: int = 0) -> str:
        indent    = self._indent(depth)
        label     = f"{self.join_type.capitalize()}Join"
        header    = f"{indent}{label} [ ON {self.condition} ]\n"
        return header + self.left.explain(depth + 1) + self._explain_right(depth + 1)

    def _explain_right(self, depth: int) -> str:
        raw = self.right.explain(depth)
        if depth == 0:
            return raw
        old_prefix = "│   " * (depth - 1) + "├── "
        new_prefix = "│   " * (depth - 1) + "└── "
        return raw.replace(old_prefix, new_prefix, 1)

    @property
    def source_tables(self) -> Set[str]:
        return self.left.source_tables | self.right.source_tables

    def to_sql(self, ctr: Optional[List[int]] = None) -> str:
        """
        Attempt full join-spine flatten first (emits flat multi-table JOIN).
        Falls back to wrapping each arm in a derived-table subquery.
        """
        if ctr is None:
            ctr = [0]

        # Try flatten the whole join spine
        plan = _try_collect_join(self)
        if plan is not None:
            return _render_join_plan(plan)

        # Cannot flatten (e.g., arms contain SubqueryNode / AggregateNode)
        # — wrap each arm in a subquery, use SELECT * to preserve all columns
        left_inner  = self.left.to_sql(ctr)
        right_inner = self.right.to_sql(ctr)
        left_src,  left_alias  = _wrap(left_inner,  ctr)
        right_src, right_alias = _wrap(right_inner, ctr)
        join_kw = f"{self.join_type} JOIN" if self.join_type != "INNER" else "JOIN"
        return (
            f"SELECT *\n"
            f"FROM {left_src} AS {left_alias}\n"
            f"{join_kw} {right_src} AS {right_alias}\n"
            f"ON {self.condition}"
        )

    def __repr__(self) -> str:
        return (
            f"JoinNode(join_type={self.join_type!r}, condition={self.condition!r}, "
            f"left={self.left!r}, right={self.right!r})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# AggregateNode
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AggregateNode(PlanNode):
    """
    Aggregation — GROUP BY / aggregate functions / HAVING.

    Attributes
    ----------
    child         : Child plan node.
    group_by_cols : GROUP BY key expressions.
    aggregates    : Aggregate function strings.
    having        : Optional HAVING predicate.
    """

    child: PlanNode
    group_by_cols: List[str] = field(default_factory=list)
    aggregates: List[str]    = field(default_factory=list)
    having: Optional[str]    = None

    def explain(self, depth: int = 0) -> str:
        indent      = self._indent(depth)
        gb_str      = ", ".join(self.group_by_cols) if self.group_by_cols else "(none)"
        agg_str     = ", ".join(self.aggregates)    if self.aggregates    else "(none)"
        having_part = f" | HAVING {self.having}"    if self.having        else ""
        header = f"{indent}Aggregate [ GROUP BY {gb_str} | {agg_str}{having_part} ]\n"
        return header + self.child.explain(depth + 1)

    @property
    def source_tables(self) -> Set[str]:
        return self.child.source_tables

    def to_sql(self, ctr: Optional[List[int]] = None) -> str:
        if ctr is None:
            ctr = [0]

        select_parts = list(self.group_by_cols) + list(self.aggregates)
        select_str   = ", ".join(select_parts) if select_parts else "*"
        gb_str       = ", ".join(self.group_by_cols) if self.group_by_cols else ""

        # Try to flatten child into a single FROM block
        plan = _try_collect_join(self.child)
        if plan is not None:
            # Reuse join tables/predicates; override SELECT list
            lines_from_join = _render_join_plan(
                _JoinPlan(tables=plan.tables, predicates=plan.predicates, cols="*")
            ).splitlines()
            # Replace first line (SELECT *) with our aggregate SELECT
            lines_from_join[0] = f"SELECT {select_str}"
            sql = "\n".join(lines_from_join)
        else:
            flat = _try_flatten(self.child)
            if flat is not None:
                alias_clause = f" AS {flat.alias}" if flat.alias != flat.table else ""
                sql = f"SELECT {select_str}\nFROM {flat.table}{alias_clause}"
                if flat.where:
                    sql += f"\nWHERE {flat.where}"
            else:
                inner_sql  = self.child.to_sql(ctr)
                src, alias = _wrap(inner_sql, ctr)
                sql = f"SELECT {select_str}\nFROM {src} AS {alias}"

        if gb_str:
            sql += f"\nGROUP BY {gb_str}"
        if self.having:
            sql += f"\nHAVING {self.having}"
        return sql

    def __repr__(self) -> str:
        return (
            f"AggregateNode(group_by={self.group_by_cols!r}, "
            f"aggregates={self.aggregates!r}, having={self.having!r})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# SubqueryNode
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SubqueryNode(PlanNode):
    """
    Derived table — a named CTE or inline subquery used as a table source.

    Attributes
    ----------
    child : Full logical plan of the inner query.
    alias : Name by which the outer query references this subquery.
    """

    child: PlanNode
    alias: str

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        return f"{indent}Subquery [ {self.alias} ]\n" + self.child.explain(depth + 1)

    @property
    def source_tables(self) -> Set[str]:
        return {self.alias.lower()}

    def to_sql(self, ctr: Optional[List[int]] = None) -> str:
        """
        Attempt to flatten the inner child.  Either way wrap in a named
        derived table ``(…) AS alias``.
        """
        if ctr is None:
            ctr = [0]

        # Flatten inner child if possible
        plan = _try_collect_join(self.child)
        if plan is not None:
            inner_sql = _render_join_plan(plan)
        else:
            flat = _try_flatten(self.child)
            if flat is not None:
                inner_sql = _flat_to_sql(flat)
            else:
                inner_sql = self.child.to_sql(ctr)

        indented = _indent_block(inner_sql)
        return f"SELECT *\nFROM (\n{indented}\n) AS {self.alias}"

    def __repr__(self) -> str:
        return f"SubqueryNode(alias={self.alias!r})"
