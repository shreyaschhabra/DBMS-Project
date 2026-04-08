"""
engine/nodes.py
---------------
Relational Algebra Tree Nodes
Defines the building blocks of a query execution plan tree.

Every node represents a single relational-algebra operation:
  - ScanNode     → full sequential scan of a base table
  - SelectNode   → filter rows by a predicate (WHERE)
  - ProjectNode  → project a subset of columns (SELECT list)
  - JoinNode     → inner join of two sub-trees on a condition

Each node exposes:
  - explain(depth)  → returns an indented, ASCII-art string of the subtree
  - source_tables   → property returning the set of base tables in the subtree
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
        """Return the set of base table names reachable from this node."""

    # ------------------------------------------------------------------
    # Helpers shared by all nodes
    # ------------------------------------------------------------------

    @staticmethod
    def _indent(depth: int) -> str:
        """Build the leading indentation string for the given depth."""
        if depth == 0:
            return ""
        # Each level adds a vertical-bar + spaces connector.
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

    Reads every row from the named base table. This is the leaf-node of any
    plan tree — every other operator sits above one or more ScanNodes.

    Attributes:
        table_name : Name of the base table to scan.
    """
    table_name: str

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        return f"{indent}📂 SeqScan [ {self.table_name} ]\n"

    @property
    def source_tables(self) -> Set[str]:
        return {self.table_name.lower()}

    def __repr__(self) -> str:
        return f"ScanNode(table={self.table_name!r})"


# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SelectNode(PlanNode):
    """
    Selection / Filter (WHERE clause).

    Passes only those rows from its child that satisfy the given predicate.

    Attributes:
        child     : The child plan node whose output is filtered.
        predicate : A string representation of the filter condition,
                    e.g. "users.id > 500".
    """
    child: PlanNode
    predicate: str

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        header = f"{indent}🔍 Filter [ {self.predicate} ]\n"
        child_str = self.child.explain(depth + 1)
        return header + child_str

    @property
    def source_tables(self) -> Set[str]:
        return self.child.source_tables

    def __repr__(self) -> str:
        return f"SelectNode(predicate={self.predicate!r})"


# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ProjectNode(PlanNode):
    """
    Projection (SELECT column list).

    Removes unwanted columns from the output, keeping only those listed in
    `columns`.

    Attributes:
        child   : The child plan node whose columns are projected.
        columns : List of column name strings to include in the output.
                  Use ["*"] to represent SELECT *.
    """
    child: PlanNode
    columns: List[str] = field(default_factory=lambda: ["*"])

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        cols = ", ".join(self.columns) if self.columns else "*"
        header = f"{indent}📋 Project [ {cols} ]\n"
        child_str = self.child.explain(depth + 1)
        return header + child_str

    @property
    def source_tables(self) -> Set[str]:
        return self.child.source_tables

    def __repr__(self) -> str:
        return f"ProjectNode(columns={self.columns!r})"


# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class JoinNode(PlanNode):
    """
    Inner Join of two sub-trees.

    Performs a nested-loop (logical) join of the left and right children on
    the specified equi-join condition.

    Attributes:
        left      : Left child plan node.
        right     : Right child plan node.
        condition : Join predicate string, e.g. "users.city_id = cities.id".
    """
    left: PlanNode
    right: PlanNode
    condition: str

    def explain(self, depth: int = 0) -> str:
        indent = self._indent(depth)
        header = f"{indent}🔗 InnerJoin [ ON {self.condition} ]\n"
        # Left uses regular connector; right uses "last-child" connector.
        left_str  = self.left.explain(depth + 1)
        right_str = self._explain_right(depth + 1)
        return header + left_str + right_str

    def _explain_right(self, depth: int) -> str:
        """
        Re-render the right child using the '└──' (last-child) connector at
        the root of that subtree so the ASCII art looks correct.
        """
        # Swap the connector for the immediate right child line only.
        raw = self.right.explain(depth)
        if depth == 0:
            return raw
        old_prefix = "│   " * (depth - 1) + "├── "
        new_prefix = "│   " * (depth - 1) + "└── "
        # Replace only the first occurrence (the root line of the subtree).
        return raw.replace(old_prefix, new_prefix, 1)

    @property
    def source_tables(self) -> Set[str]:
        return self.left.source_tables | self.right.source_tables

    def __repr__(self) -> str:
        return (
            f"JoinNode(condition={self.condition!r}, "
            f"left={self.left!r}, right={self.right!r})"
        )
