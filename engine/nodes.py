"""
engine/nodes.py
---------------
Relational Algebra Tree Nodes

Defines the building blocks of a query execution plan tree.

Every node represents a single relational-algebra operation:
  - ScanNode      → full sequential scan of a base table
  - SelectNode    → filter rows by a predicate (WHERE / OR-block)
  - ProjectNode   → project a subset of columns (SELECT list)
  - JoinNode      → inner, left, or right join of two sub-trees on a condition
  - AggregateNode → aggregate rows (GROUP BY / HAVING)
  - SubqueryNode  → derived table — a CTE or inline subquery

Each node exposes:
  - explain(depth)  → returns an indented ASCII string of the subtree
  - source_tables   → property returning the set of base table names in the subtree
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Set


# ─────────────────────────────────────────────────────────────────────────────
# Base class
# ─────────────────────────────────────────────────────────────────────────────

class PlanNode(ABC):
    """
    Abstract base class for all relational-algebra plan nodes.

    Subclasses must implement:
        explain(depth)   → formatted string representation of the node tree
        source_tables    → set of base table names reachable from this node
    """

    @abstractmethod
    def explain(self, depth: int = 0) -> str:
        """
        Return a pretty-printed, indented string for the subtree rooted here.

        Parameters:
            depth : Current indentation depth (0 = root).
        """

    @property
    @abstractmethod
    def source_tables(self) -> Set[str]:
        """Return the set of effective table/alias names reachable from this node."""

    # ------------------------------------------------------------------
    # Helpers shared by all nodes
    # ------------------------------------------------------------------

    @staticmethod
    def _indent(depth: int) -> str:
        """Build the leading indentation string for the given depth."""
        if depth == 0:
            return ""
        return "│   " * (depth - 1) + "├── "

    @staticmethod
    def _last_indent(depth: int) -> str:
        """Indent string using a 'last child' connector (└──)."""
        if depth == 0:
            return ""
        return "│   " * (depth - 1) + "└── "


# ─────────────────────────────────────────────────────────────────────────────
# Concrete nodes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScanNode(PlanNode):
    """
    Sequential Table Scan.

    Reads every row from the named base table.  This is the leaf-node of any
    plan tree — every other operator sits above one or more ScanNodes.

    Attributes:
        table_name : Name of the base table to scan (always the catalog name).
        alias      : Optional query alias, e.g. ``u`` for ``FROM users u``.
                     If set, the rest of the tree references this table via the alias.
    """

    table_name: str
    alias: Optional[str] = None

    @property
    def effective_name(self) -> str:
        """Name the outer query uses to refer to this table (alias if set)."""
        return self.alias if self.alias else self.table_name

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        label = (
            f"{self.table_name} AS {self.alias}" if self.alias else self.table_name
        )
        return f"{indent}SeqScan [ {label} ]\n"

    @property
    def source_tables(self) -> Set[str]:
        """
        Returns the *real* catalog table name so RBO/CBO can look it up.
        When the optimizer needs the effective alias, use ``effective_name``.
        """
        return {self.table_name.lower()}

    def __repr__(self) -> str:
        return f"ScanNode(table={self.table_name!r}, alias={self.alias!r})"


# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SelectNode(PlanNode):
    """
    Selection / Filter (WHERE clause).

    Passes only those rows from its child that satisfy the given predicate.

    The ``is_or_block`` flag distinguishes between simple conjunctive predicates
    (which can be pushed down independently) and OR-compound predicates (which
    must be treated as an atomic, indivisible block by the optimizer).

    Attributes:
        child       : The child plan node whose output is filtered.
        predicate   : SQL string of the filter condition,
                      e.g. ``users.id > 500`` or ``users.id > 5 OR users.id < 2``.
        is_or_block : True when the top-level connective in *predicate* is OR.
                      The RBO will only push this node down if ALL referenced
                      tables are on the same side of the join.
    """

    child: PlanNode
    predicate: str
    is_or_block: bool = False

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        tag    = "OrFilter" if self.is_or_block else "Filter"
        header = f"{indent}{tag} [ {self.predicate} ]\n"
        return header + self.child.explain(depth + 1)

    @property
    def source_tables(self) -> Set[str]:
        return self.child.source_tables

    def __repr__(self) -> str:
        return (
            f"SelectNode(predicate={self.predicate!r}, "
            f"is_or_block={self.is_or_block!r})"
        )


# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ProjectNode(PlanNode):
    """
    Projection (SELECT column list).

    Removes unwanted columns from the output, keeping only those listed in
    ``columns``.

    Attributes:
        child   : The child plan node whose columns are projected.
        columns : List of column name strings to include in the output.
                  Use [``*``] to represent SELECT *.
    """

    child: PlanNode
    columns: List[str] = field(default_factory=lambda: ["*"])

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        cols = ", ".join(self.columns) if self.columns else "*"
        header = f"{indent}Project [ {cols} ]\n"
        return header + self.child.explain(depth + 1)

    @property
    def source_tables(self) -> Set[str]:
        return self.child.source_tables

    def __repr__(self) -> str:
        return f"ProjectNode(columns={self.columns!r})"


# ─────────────────────────────────────────────────────────────────────────────

# Valid join type literals — all normalised to upper-case in the parser.
_VALID_JOIN_TYPES = frozenset({"INNER", "LEFT", "RIGHT", "FULL", "CROSS"})


@dataclass
class JoinNode(PlanNode):
    """
    Join of two sub-trees.

    Supports INNER, LEFT (OUTER), RIGHT (OUTER), FULL (OUTER), and CROSS joins.
    The ``join_type`` attribute is used by:

    - ``PlanVisualizer`` — to render the correct label
      (``InnerJoin``, ``LeftJoin``, ``RightJoin``, …).
    - ``CostBasedOptimizer`` — to detect non-commutative joins and
      **disable reordering** for LEFT/RIGHT/FULL joins so that the
      original table ordering written in the query is preserved.

    Attributes:
        left      : Left child plan node.
        right     : Right child plan node.
        condition : Join predicate string, e.g. ``users.city_id = cities.id``.
        join_type : One of ``"INNER"``, ``"LEFT"``, ``"RIGHT"``, ``"FULL"``,
                    ``"CROSS"``.  Defaults to ``"INNER"``.
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
        """True when this join is non-commutative (LEFT, RIGHT, or FULL)."""
        return self.join_type in ("LEFT", "RIGHT", "FULL")

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        label  = f"{self.join_type.capitalize()}Join"
        header = f"{indent}{label} [ ON {self.condition} ]\n"
        left_str  = self.left.explain(depth + 1)
        right_str = self._explain_right(depth + 1)
        return header + left_str + right_str

    def _explain_right(self, depth: int) -> str:
        """Re-render the right child using the '└──' (last-child) connector."""
        raw = self.right.explain(depth)
        if depth == 0:
            return raw
        old_prefix = "│   " * (depth - 1) + "├── "
        new_prefix = "│   " * (depth - 1) + "└── "
        return raw.replace(old_prefix, new_prefix, 1)

    @property
    def source_tables(self) -> Set[str]:
        return self.left.source_tables | self.right.source_tables

    def __repr__(self) -> str:
        return (
            f"JoinNode(join_type={self.join_type!r}, condition={self.condition!r}, "
            f"left={self.left!r}, right={self.right!r})"
        )


# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AggregateNode(PlanNode):
    """
    Aggregation operator — GROUP BY / aggregate functions / HAVING.

    Sits above the join/scan sub-tree and below the top-level ProjectNode.
    Encodes GROUP BY keys, aggregate function expressions, and an optional
    HAVING predicate.

    IMPORTANT for RBO: A HAVING predicate (stored in ``having``) is a
    **post-aggregate** filter.  The RBO must **not** push any predicate that
    references aggregate outputs below this node.

    Attributes:
        child         : Child plan node whose rows are grouped.
        group_by_cols : List of GROUP BY key expression strings,
                        e.g. ``["cities.city_name"]``.
        aggregates    : List of aggregate function strings,
                        e.g. ``["COUNT(users.id)", "SUM(orders.amount)"]``.
        having        : Optional HAVING predicate string (post-aggregate filter),
                        e.g. ``"COUNT(users.id) > 5"``.
    """

    child: PlanNode
    group_by_cols: List[str] = field(default_factory=list)
    aggregates: List[str] = field(default_factory=list)
    having: Optional[str] = None

    def explain(self, depth: int = 0) -> str:
        indent  = self._indent(depth)
        gb_str  = ", ".join(self.group_by_cols) if self.group_by_cols else "(none)"
        agg_str = ", ".join(self.aggregates)     if self.aggregates    else "(none)"
        having_part = f" | HAVING {self.having}" if self.having else ""
        header = f"{indent}Aggregate [ GROUP BY {gb_str} | {agg_str}{having_part} ]\n"
        return header + self.child.explain(depth + 1)

    @property
    def source_tables(self) -> Set[str]:
        return self.child.source_tables

    def __repr__(self) -> str:
        return (
            f"AggregateNode(group_by={self.group_by_cols!r}, "
            f"aggregates={self.aggregates!r}, having={self.having!r})"
        )


# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SubqueryNode(PlanNode):
    """
    Derived table — a named CTE or inline subquery used as a table source.

    The outer query references this subquery's output rows by ``alias``.
    The full logical plan of the inner query is stored in ``child``.

    Key property: ``source_tables`` returns ``{alias}`` rather than the inner
    tables.  This ensures that the outer query's join-condition matcher
    (RBO / CBO) correctly attributes alias-prefixed column references
    (e.g. ``active_users.city_id``) to this node.

    Attributes:
        child : Full logical plan of the inner (CTE / subquery) query.
        alias : The name by which the outer query references this subquery.
    """

    child: PlanNode
    alias: str

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        header = f"{indent}Subquery [ {self.alias} ]\n"
        return header + self.child.explain(depth + 1)

    @property
    def source_tables(self) -> Set[str]:
        """
        Returns ``{alias}`` so that outer-query condition matching works
        against the alias (e.g. ``active_users``) rather than the inner tables.
        """
        return {self.alias.lower()}

    def __repr__(self) -> str:
        return f"SubqueryNode(alias={self.alias!r})"
