"""
engine/cbo.py
-------------
Cost-Based Optimizer (CBO)

Uses table cardinality statistics from the Catalog to choose the *cheapest*
join ordering when multiple tables are joined.

Cost Model
~~~~~~~~~~
A simplified, yet realistic, cost model based on the *intermediate result
size* of each join:

    cost(A JOIN B) = cardinality(A) * cardinality(B)

For a chain of three tables A, B, C the optimizer evaluates every possible
left-deep ordering and picks the one with the minimum total cost:

    Ordering 1: (A ⋈ B) ⋈ C   cost = |A|*|B| + (|A|*|B|)*|C|
    Ordering 2: (A ⋈ C) ⋈ B   cost = |A|*|C| + (|A|*|C|)*|B|
    Ordering 3: (B ⋈ A) ⋈ C   cost = |B|*|A| + (|B|*|A|)*|C|
    ...etc.

The optimizer also correctly:
  - Preserves all join conditions from the original tree.
  - Wraps ScanNodes in their pushed-down SelectNodes if present.
  - Returns both the rewritten tree and a detailed cost report.

Usage::

    from engine.cbo import CostBasedOptimizer
    from engine.catalog import Catalog

    cbo    = CostBasedOptimizer(catalog=Catalog())
    result = cbo.optimize(rbo_tree)
    print(result.cost)          # integer
    print(result.cost_report)   # human-readable breakdown
    print(result.plan.explain())# final physical plan
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from engine.catalog import Catalog
from engine.nodes import (
    JoinNode,
    PlanNode,
    ProjectNode,
    ScanNode,
    SelectNode,
)


# ─────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CBOResult:
    """
    Returned by `CostBasedOptimizer.optimize()`.

    Attributes:
        plan        : Root node of the physical plan (with optimal join order).
        cost        : Total estimated cost (integer row multiplications).
        cost_report : Multi-line human-readable cost breakdown for display.
        ordering    : List of table names in the chosen join order.
    """
    plan: PlanNode
    cost: int
    cost_report: str
    ordering: List[str] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _TableInfo:
    """
    Internal representation of a table's role in the join.

    Attributes:
        name       : Table name (lower-case).
        scan       : The ScanNode for this table.
        filter     : Optional SelectNode wrapping the scan (pushed-down WHERE).
        cardinality: Row count from the catalog.
    """
    name: str
    scan: ScanNode
    filter: Optional[SelectNode]
    cardinality: int

    @property
    def root_node(self) -> PlanNode:
        """Return the filter node if present, otherwise the raw scan."""
        if self.filter is not None:
            return self.filter
        return self.scan


# ─────────────────────────────────────────────────────────────────────────────
# Main optimizer
# ─────────────────────────────────────────────────────────────────────────────

class CostBasedOptimizer:
    """
    Cost-Based Optimizer: chooses the cheapest join ordering.

    Supports query trees with 2 or 3 tables (suitable for the project scope).
    Falls back gracefully to the original order for single-table queries.
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def optimize(self, root: PlanNode) -> CBOResult:
        """
        Inspect the tree, extract tables and join conditions, enumerate all
        possible join orderings, compute costs, and return the cheapest plan.

        Parameters:
            root : Root of the RBO-optimized logical plan.

        Returns:
            A :class:`CBOResult` with the physical plan and cost information.
        """
        # Peel off the top-level ProjectNode so we work on the join sub-tree.
        project_node: Optional[ProjectNode] = None
        inner = root
        if isinstance(root, ProjectNode):
            project_node = root
            inner = root.child

        # Collect table infos and join conditions from the tree.
        table_infos, join_conditions = self._extract_plan_components(inner)

        # If 0 or 1 tables, no join reordering needed.
        if len(table_infos) <= 1:
            return CBOResult(
                plan=root,
                cost=0,
                cost_report="Single table query — no join reordering needed.",
                ordering=[t.name for t in table_infos],
            )

        # Enumerate all permutations of the tables.
        best_cost    = float("inf")
        best_order   = None
        cost_lines   = ["Join Ordering Cost Analysis:", "─" * 48]

        for perm in itertools.permutations(table_infos):
            cost, breakdown = self._compute_order_cost(list(perm))
            label = " ⋈ ".join(t.name for t in perm)
            cost_lines.append(f"  ({label})")
            cost_lines.append(f"    ↳ cost = {cost:,}  [{breakdown}]")
            if cost < best_cost:
                best_cost  = cost
                best_order = list(perm)

        cost_lines.append("─" * 48)
        best_label = " ⋈ ".join(t.name for t in best_order)
        cost_lines.append(f"✔ Best ordering: ({best_label})")
        cost_lines.append(f"✔ Minimum cost : {best_cost:,} row-multiplications")

        # Build the optimized physical plan for the best ordering.
        physical_inner = self._build_join_tree(best_order, join_conditions)

        # Re-attach the ProjectNode if it existed.
        if project_node is not None:
            project_node.child = physical_inner
            final_plan = project_node
        else:
            final_plan = physical_inner

        return CBOResult(
            plan=final_plan,
            cost=int(best_cost),
            cost_report="\n".join(cost_lines),
            ordering=[t.name for t in best_order],
        )

    # ------------------------------------------------------------------
    # Tree component extraction
    # ------------------------------------------------------------------

    def _extract_plan_components(
        self, node: PlanNode
    ) -> Tuple[List[_TableInfo], List[str]]:
        """
        Walk the plan tree and collect:
          - A list of _TableInfo objects (one per base table).
          - A list of join condition strings.

        We flatten the tree top-down so that pushed-down SelectNodes are
        correctly associated with their ScanNode.
        """
        table_infos: List[_TableInfo] = []
        join_conditions: List[str]    = []
        self._collect(node, table_infos, join_conditions, pending_filter=None)
        return table_infos, join_conditions

    def _collect(
        self,
        node: PlanNode,
        table_infos: List[_TableInfo],
        join_conditions: List[str],
        pending_filter: Optional[SelectNode],
    ) -> None:
        """
        Recursive DFS to collect tables and join conditions.
        """
        if isinstance(node, ScanNode):
            cardinality = self._safe_cardinality(node.table_name)
            table_infos.append(
                _TableInfo(
                    name=node.table_name.lower(),
                    scan=node,
                    filter=pending_filter,
                    cardinality=cardinality,
                )
            )

        elif isinstance(node, SelectNode):
            # This is a pushed-down filter — pass it as the pending filter
            # for the child ScanNode (or continue recursing).
            self._collect(node.child, table_infos, join_conditions, pending_filter=node)

        elif isinstance(node, JoinNode):
            if node.condition not in join_conditions:
                join_conditions.append(node.condition)
            self._collect(node.left,  table_infos, join_conditions, None)
            self._collect(node.right, table_infos, join_conditions, None)

        elif isinstance(node, ProjectNode):
            self._collect(node.child, table_infos, join_conditions, None)

    # ------------------------------------------------------------------
    # Cost computation
    # ------------------------------------------------------------------

    def _compute_order_cost(
        self, order: List[_TableInfo]
    ) -> Tuple[int, str]:
        """
        Compute the total cost of joining tables in *order* left-to-right.

        Cost model:
            intermediate = cardinality(T1)
            for each subsequent table Ti:
                step_cost    = intermediate * cardinality(Ti)
                total_cost  += step_cost
                intermediate = step_cost   (the join result size)

        Returns (total_cost, breakdown_string).
        """
        intermediate = order[0].cardinality
        total        = 0
        steps        = [str(order[0].cardinality)]

        for info in order[1:]:
            step      = intermediate * info.cardinality
            total    += step
            intermediate = step
            steps.append(f"*{info.cardinality}={step:,}")

        breakdown = " ".join(steps)
        return total, breakdown

    # ------------------------------------------------------------------
    # Physical plan builder
    # ------------------------------------------------------------------

    def _build_join_tree(
        self,
        order: List[_TableInfo],
        join_conditions: List[str],
    ) -> PlanNode:
        """
        Build a left-deep join tree for the given table ordering.

        Conditions are assigned greedily: a condition is attached to the
        first join that references both its tables.

        Parameters:
            order           : Tables in the chosen join order.
            join_conditions : All join condition strings from the original tree.

        Returns:
            A PlanNode (ScanNode or chain of JoinNodes) representing the
            physical join plan.
        """
        if len(order) == 1:
            return order[0].root_node

        # Track which tables have been introduced so far.
        introduced: List[str] = [order[0].name]
        current_node: PlanNode = order[0].root_node
        used_conditions: set = set()

        for info in order[1:]:
            introduced.append(info.name)
            # Find the best matching join condition for this step.
            matched_cond = self._find_condition(
                introduced, join_conditions, used_conditions
            )
            used_conditions.add(matched_cond or "")
            current_node = JoinNode(
                left=current_node,
                right=info.root_node,
                condition=matched_cond or f"{order[0].name} ⋈ {info.name}",
            )

        return current_node

    @staticmethod
    def _find_condition(
        tables: List[str],
        conditions: List[str],
        used: set,
    ) -> Optional[str]:
        """
        Find a join condition that references at least two of the tables in
        *tables* (the ones introduced so far), and hasn't been used yet.

        Falls back to the first unused condition if no perfect match.
        """
        import re
        best = None
        for cond in conditions:
            if cond in used:
                continue
            # Extract tables mentioned in this condition.
            dotted = re.findall(r"([A-Za-z_]\w*)\.(?:[A-Za-z_]\w*)", cond)
            mentioned = {t.lower() for t in dotted}
            if mentioned and mentioned.issubset(set(tables)):
                return cond
        # Fallback: return any unused condition.
        for cond in conditions:
            if cond not in used:
                return cond
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _safe_cardinality(self, table_name: str) -> int:
        """
        Return the cardinality for *table_name*, defaulting to 1 if the
        table is not in the catalog (prevents division-by-zero / crashes).
        """
        try:
            return self._catalog.get_cardinality(table_name)
        except KeyError:
            return 1
