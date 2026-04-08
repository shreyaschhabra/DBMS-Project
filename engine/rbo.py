"""
engine/rbo.py
-------------
Rule-Based Optimizer (RBO)

Implements classical algebraic rewrite rules on the logical plan tree.
Rules are applied in a deterministic, priority order without any knowledge
of statistics.

Rules implemented
~~~~~~~~~~~~~~~~~
1. **Predicate Pushdown** – Move a SelectNode (WHERE filter) as *close to
   the data source* as possible, i.e., below joins and immediately above the
   relevant ScanNode.  This dramatically reduces the number of rows that
   flow through expensive join operations.

2. **Projection Pushdown** – Determine the exact set of columns required by
   the query (from the top-level ProjectNode, intermediate SelectNode
   predicates, and JoinNode conditions), then insert new, narrow ProjectNodes
   immediately above every ScanNode so that unused columns are dropped as
   early as possible.  In a real engine this shrinks the row-width that
   travels through every operator, saving RAM and network bandwidth.

Usage::

    from engine.rbo import RuleBasedOptimizer
    rbo          = RuleBasedOptimizer()
    tree         = rbo.optimize(logical_tree)
    pred_rules   = rbo.get_predicate_pushdown_rules()
    proj_rules   = rbo.get_projection_pushdown_rules()
    all_rules    = rbo.get_applied_rules()
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Set

from engine.catalog import Catalog
from engine.nodes import (
    JoinNode,
    PlanNode,
    ProjectNode,
    ScanNode,
    SelectNode,
)


class RuleBasedOptimizer:
    """
    Applies rule-based algebraic rewrite rules to a logical plan tree.

    The optimizer is **stateless between calls** — a fresh log of applied
    rules is maintained per ``optimize()`` invocation.

    Public attributes (after calling ``optimize()``)
    -------------------------------------------------
    predicate_pushdown_rules  : Rules fired by Predicate Pushdown.
    projection_pushdown_rules : Rules fired by Projection Pushdown.
    """

    def __init__(self, catalog: Optional[Catalog] = None) -> None:
        """
        Parameters
        ----------
        catalog : Optional[Catalog]
            If supplied, the Projection Pushdown rule can cross-reference the
            full column list for each table to enumerate which columns are
            *dropped*.  If ``None``, a fallback message is used instead.
        """
        self._catalog: Optional[Catalog] = catalog
        self._predicate_rules: List[str] = []
        self._projection_rules: List[str] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def optimize(self, root: PlanNode) -> PlanNode:
        """
        Walk the tree and apply all known rewrite rules in priority order:

        1. Predicate Pushdown
        2. Projection Pushdown

        Parameters
        ----------
        root : PlanNode
            Root node of the unoptimized logical plan.

        Returns
        -------
        PlanNode
            Root node of the optimized logical plan.
        """
        self._predicate_rules = []
        self._projection_rules = []

        # Pass 1 – Predicate Pushdown
        tree = self._apply_predicate_pushdown(root)

        # Pass 2 – Projection Pushdown
        tree = self._apply_projection_pushdown(tree)

        return tree

    def get_predicate_pushdown_rules(self) -> List[str]:
        """Return rules fired by Predicate Pushdown (most recent call)."""
        return list(self._predicate_rules)

    def get_projection_pushdown_rules(self) -> List[str]:
        """Return rules fired by Projection Pushdown (most recent call)."""
        return list(self._projection_rules)

    def get_applied_rules(self) -> List[str]:
        """Return *all* RBO rules fired (both passes combined)."""
        return self._predicate_rules + self._projection_rules

    # ------------------------------------------------------------------
    # Rule 1 – Predicate Pushdown
    # ------------------------------------------------------------------

    def _apply_predicate_pushdown(self, node: PlanNode) -> PlanNode:
        """
        Recursively traverse the plan tree and push SelectNodes downward.

        Core rule
        ~~~~~~~~~
        If we find  ``SelectNode(predicate, child=JoinNode(L, R))``
        and the predicate references only columns from table T, rewrite to::

            JoinNode(SelectNode(predicate, L), R)   # predicate belongs to L
            JoinNode(L, SelectNode(predicate, R))   # predicate belongs to R

        If the predicate spans both sides the SelectNode stays above the join.

        Returns
        -------
        PlanNode
            The (possibly rewritten) subtree.
        """
        # ── ProjectNode: recurse into child ──────────────────────────
        if isinstance(node, ProjectNode):
            node.child = self._apply_predicate_pushdown(node.child)
            return node

        # ── SelectNode above a JoinNode: attempt pushdown ────────────
        if isinstance(node, SelectNode) and isinstance(node.child, JoinNode):
            join_node: JoinNode = node.child
            predicate = node.predicate

            pred_tables = self._tables_in_expr(predicate)
            left_tables = join_node.left.source_tables
            right_tables = join_node.right.source_tables

            if pred_tables and pred_tables.issubset(left_tables):
                self._predicate_rules.append(
                    f"Predicate Pushdown: '{predicate}' pushed below JOIN "
                    f"→ applied to LEFT side ({', '.join(sorted(left_tables))})"
                )
                join_node.left = SelectNode(
                    child=join_node.left, predicate=predicate
                )
                join_node.left = self._apply_predicate_pushdown(join_node.left)
                join_node.right = self._apply_predicate_pushdown(join_node.right)
                return join_node

            elif pred_tables and pred_tables.issubset(right_tables):
                self._predicate_rules.append(
                    f"Predicate Pushdown: '{predicate}' pushed below JOIN "
                    f"→ applied to RIGHT side ({', '.join(sorted(right_tables))})"
                )
                join_node.right = SelectNode(
                    child=join_node.right, predicate=predicate
                )
                join_node.left = self._apply_predicate_pushdown(join_node.left)
                join_node.right = self._apply_predicate_pushdown(join_node.right)
                return join_node

            else:
                # Cross-predicate — recurse but keep SelectNode in place.
                join_node.left = self._apply_predicate_pushdown(join_node.left)
                join_node.right = self._apply_predicate_pushdown(join_node.right)
                return node

        # ── SelectNode above a ScanNode: already at the bottom ───────
        if isinstance(node, SelectNode) and isinstance(node.child, ScanNode):
            return node

        # ── SelectNode above other nodes: recurse ─────────────────────
        if isinstance(node, SelectNode):
            node.child = self._apply_predicate_pushdown(node.child)
            return node

        # ── JoinNode: recurse into both children ─────────────────────
        if isinstance(node, JoinNode):
            node.left = self._apply_predicate_pushdown(node.left)
            node.right = self._apply_predicate_pushdown(node.right)
            return node

        # ── ScanNode: leaf — nothing to do ───────────────────────────
        return node

    # ------------------------------------------------------------------
    # Rule 2 – Projection Pushdown
    # ------------------------------------------------------------------

    def _apply_projection_pushdown(self, root: PlanNode) -> PlanNode:
        """
        Insert narrow ``ProjectNode``s immediately above every ``ScanNode``
        so that only the columns actually needed by the query are read.

        Algorithm
        ~~~~~~~~~
        1. Collect the *globally required* column set from:
           - The top-level ``ProjectNode`` SELECT list.
           - All ``SelectNode`` predicate expressions.
           - All ``JoinNode`` ON-condition expressions.
        2. For each ``ScanNode`` encountered during DFS, determine which of
           its catalog columns appear in the global required set.
        3. Wrap the ``ScanNode`` with a ``ProjectNode`` listing only those
           columns (or the full set if none is matched, as a safe fallback).

        Parameters
        ----------
        root : PlanNode
            Root of the (predicate-pushed) plan tree.

        Returns
        -------
        PlanNode
            Root of the tree with projection nodes injected above scans.
        """
        # Step 1 – gather every column reference in the whole tree.
        required_cols: Set[str] = set()
        self._collect_required_columns(root, required_cols)

        # Step 2 – walk the tree and inject ProjectNodes above ScanNodes.
        return self._inject_projections(root, required_cols)

    def _collect_required_columns(
        self, node: PlanNode, required: Set[str]
    ) -> None:
        """
        Recursively harvest every ``table.column`` token referenced anywhere
        in the tree (PROJECT list, SELECT predicates, JOIN conditions).
        """
        if isinstance(node, ProjectNode):
            for col in node.columns:
                if col != "*":
                    required.update(self._dotted_columns(col))
            self._collect_required_columns(node.child, required)

        elif isinstance(node, SelectNode):
            required.update(self._dotted_columns(node.predicate))
            self._collect_required_columns(node.child, required)

        elif isinstance(node, JoinNode):
            required.update(self._dotted_columns(node.condition))
            self._collect_required_columns(node.left, required)
            self._collect_required_columns(node.right, required)

        elif isinstance(node, ScanNode):
            pass  # Leaf — no children.

    def _inject_projections(
        self, node: PlanNode, required: Set[str]
    ) -> PlanNode:
        """
        Walk the tree top-down; when a ``ScanNode`` is found, wrap it with a
        ``ProjectNode`` containing only the columns needed from that table.
        """
        if isinstance(node, ScanNode):
            needed = self._columns_for_table(node.table_name, required)
            if not needed:
                # Safety fallback: keep all columns rather than projecting
                # nothing (which would be semantically wrong).
                return node

            needed_str = ", ".join(sorted(needed))
            full_cols = self._all_catalog_columns(node.table_name)
            dropped = sorted(set(full_cols) - set(needed))
            dropped_str = (
                ", ".join(dropped) if dropped else "none"
            )
            self._projection_rules.append(
                f"Projection Pushdown on '{node.table_name}': "
                f"keep [{needed_str}], drop [{dropped_str}]"
            )
            return ProjectNode(child=node, columns=sorted(needed))

        if isinstance(node, ProjectNode):
            node.child = self._inject_projections(node.child, required)
            return node

        if isinstance(node, SelectNode):
            node.child = self._inject_projections(node.child, required)
            return node

        if isinstance(node, JoinNode):
            node.left = self._inject_projections(node.left, required)
            node.right = self._inject_projections(node.right, required)
            return node

        return node  # Unknown node type — pass through.

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _tables_in_expr(expr: str) -> Set[str]:
        """
        Return the set of *table names* referenced in *expr* via
        ``table.column`` dotted notation.

        Examples::
            "users.id > 500"            → {"users"}
            "users.city_id = cities.id" → {"users", "cities"}
        """
        matches = re.findall(r"([A-Za-z_]\w*)\.(?:[A-Za-z_]\w*)", expr)
        return {t.lower() for t in matches}

    @staticmethod
    def _dotted_columns(expr: str) -> Set[str]:
        """
        Return all ``table.column`` tokens found in *expr*, lower-cased.

        Examples::
            "users.name, cities.city_name" → {"users.name", "cities.city_name"}
            "users.id > 500"               → {"users.id"}
        """
        matches = re.findall(
            r"([A-Za-z_]\w*\.[A-Za-z_]\w*)", expr
        )
        return {m.lower() for m in matches}

    @staticmethod
    def _columns_for_table(table_name: str, required: Set[str]) -> List[str]:
        """
        Filter *required* to only those columns belonging to *table_name*.

        A column belongs to a table when it is expressed as ``table.column``
        and the ``table`` part matches *table_name*.

        Returns a list of bare column names (without the table prefix).
        """
        table_lower = table_name.lower()
        result: List[str] = []
        for ref in required:
            parts = ref.split(".", 1)
            if len(parts) == 2 and parts[0] == table_lower:
                result.append(parts[1])
        return result

    def _all_catalog_columns(self, table_name: str) -> List[str]:
        """
        Return the full column list for *table_name* from the catalog.

        Falls back to an empty list if no catalog is available or the table
        is not found.
        """
        if self._catalog is None:
            return []
        try:
            return self._catalog.get_columns(table_name)
        except KeyError:
            return []
