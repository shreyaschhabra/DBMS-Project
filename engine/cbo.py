""" cbo.py """

from __future__ import annotations

import itertools
import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from engine.catalog import Catalog
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
# Result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CBOResult:
    """
    Returned by `CostBasedOptimizer.optimize()`.

    Attributes:
        plan             : Root node of the physical plan (with optimal join order).
        cost             : Total estimated cost (integer row multiplications).
        cost_report      : Multi-line human-readable cost breakdown for display.
        ordering         : List of table names in the chosen join order.
        reorder_disabled : True when reordering was suppressed due to outer joins.
    """
    plan: PlanNode
    cost: int
    cost_report: str
    ordering: List[str] = field(default_factory=list)
    reorder_disabled: bool = False
    residual_filters: List[SelectNode] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Internal table-info helper
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _TableInfo:
    """
    Internal representation of a table source's role in the join.

    Attributes:
        name        : Effective table name (real catalog name, or CTE alias).
        scan        : The leaf PlanNode for this source (ScanNode or SubqueryNode).
        filter      : Optional SelectNode wrapping the scan (pushed-down WHERE).
        cardinality : Row count estimate.  For SubqueryNodes this defaults to 1.
        join_type   : The join type used to attach this table to the left side.
                      ``"FROM"`` for the first (driving) table.
    """
    name: str
    scan: PlanNode            # ScanNode or SubqueryNode
    filter: Optional[SelectNode]
    cardinality: int
    join_type: str = "FROM"   # "FROM" | "INNER" | "LEFT" | "RIGHT" | "FULL"

    @property
    def is_outer(self) -> bool:
        """True when this table is joined via a non-commutative outer join."""
        return self.join_type in ("LEFT", "RIGHT", "FULL")

    @property
    def root_node(self) -> PlanNode:
        """Return the filter node if present, otherwise the raw scan/subquery."""
        if self.filter is not None:
            return self.filter
        return self.scan


# ─────────────────────────────────────────────────────────────────────────────
# Main optimizer
# ─────────────────────────────────────────────────────────────────────────────

class CostBasedOptimizer:
    """
    Cost-Based Optimizer: chooses the cheapest join ordering.

    Supports query trees with 1–N tables (suitable for the project scope).
    Falls back gracefully to the original order for single-table queries and
    for queries containing outer joins (LEFT/RIGHT/FULL), where reordering
    would produce mathematically incorrect results.

    AggregateNode / ProjectNode handling
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    The CBO peels off the outermost ``ProjectNode`` and, if present,
    ``AggregateNode`` before analysing the join sub-tree.  After building the
    optimally-ordered physical join tree, the wrappers are re-attached in the
    correct layering order (join core → AggregateNode → ProjectNode).
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
        # ── Peel wrapper nodes to expose the join core ──────────────────
        project_node: Optional[ProjectNode]   = None
        agg_node:     Optional[AggregateNode] = None
        inner: PlanNode = root

        if isinstance(inner, ProjectNode):
            project_node = inner
            inner = inner.child

        if isinstance(inner, AggregateNode):
            agg_node = inner
            inner    = inner.child

        # ── Collect table infos, join conditions, and residual filters ──
        table_infos, join_conditions, residual_filters = self._extract_plan_components(inner)

        # ── Short-circuit for 0 or 1 tables ────────────────────────────
        if len(table_infos) <= 1:
            return CBOResult(
                plan=root,
                cost=0,
                cost_report="Single table query — no join reordering needed.",
                ordering=[t.name for t in table_infos],
                reorder_disabled=False,
                residual_filters=residual_filters,
            )

        # ── Outer-join safety check ──────────────────────────────────────
        # If ANY join in the tree is a non-commutative outer join, reordering
        # is mathematically unsafe.  Preserve the original query order and
        # report the estimated cost of the as-written plan only.
        has_outer = any(t.is_outer for t in table_infos)
        if has_outer:
            original_cost, breakdown = self._compute_order_cost(table_infos, join_conditions)
            outer_tables = [t.name for t in table_infos if t.is_outer]
            cost_lines = [
                "Join Ordering Cost Analysis:",
                "─" * 48,
                f"  Outer joins detected: {', '.join(outer_tables)}",
                "  LEFT/RIGHT/FULL joins are non-commutative — reordering",
                "  would change NULL-padding semantics and produce incorrect",
                "  results.  Table order from the query is preserved.",
                "─" * 48,
                f"  Original order cost: {original_cost:,}  [{breakdown}]",
                "─" * 48,
                "Reorder disabled : outer join safety",
                f"Estimated cost   : {original_cost:,} row-multiplications",
            ]
            # Return the original tree unchanged (just re-attach wrappers).
            physical_inner = inner  # keep original join order
            # Re-wrap any cross-join SelectNodes that RBO left above the join.
            for sf in residual_filters:
                sf.child = physical_inner
                physical_inner = sf
            if agg_node is not None:
                agg_node.child = physical_inner
                physical_inner = agg_node
            if project_node is not None:
                project_node.child = physical_inner
                final_plan: PlanNode = project_node
            else:
                final_plan = physical_inner
            return CBOResult(
                plan=final_plan,
                cost=int(original_cost),
                cost_report="\n".join(cost_lines),
                ordering=[t.name for t in table_infos],
                reorder_disabled=True,
                residual_filters=residual_filters,
            )

        # ── Enumerate all orderings and pick the cheapest ───────────────
        best_cost:  float            = float("inf")
        best_order: List[_TableInfo] = []
        cost_lines = ["Join Ordering Cost Analysis:", "─" * 48]

        for perm in itertools.permutations(table_infos):
            cost, breakdown = self._compute_order_cost(list(perm), join_conditions)
            label = " \u22c8 ".join(t.name for t in perm)
            cost_lines.append(f"  ({label})")
            cost_lines.append(f"    -> cost = {cost:,}  [{breakdown}]")
            if cost < best_cost:
                best_cost  = cost
                best_order = list(perm)

        cost_lines.append("─" * 48)
        best_label = " \u22c8 ".join(t.name for t in best_order)
        cost_lines.append(f"Best ordering : ({best_label})")
        cost_lines.append(f"Minimum cost  : {best_cost:,} row-multiplications")

        # ── Build the physical join tree for the best ordering ──────────
        physical_inner = self._build_join_tree(best_order, join_conditions)

        # ── Re-wrap residual cross-join SelectNodes above the join core ─
        # These are predicates that the RBO couldn't push below any join
        # (e.g. cross-table OR predicates).  They must sit above the joins.
        for sf in residual_filters:
            sf.child = physical_inner
            physical_inner = sf

        # ── Re-attach wrappers (AggregateNode then ProjectNode) ─────────
        if agg_node is not None:
            agg_node.child = physical_inner
            physical_inner = agg_node

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
            reorder_disabled=False,
            residual_filters=residual_filters,
        )

    # ------------------------------------------------------------------
    # Tree component extraction
    # ------------------------------------------------------------------

    def _extract_plan_components(
        self, node: PlanNode
    ) -> Tuple[List[_TableInfo], List[str], List[SelectNode]]:
        """
        Walk the plan tree and collect:
          - A list of _TableInfo objects (one per base table / subquery source).
          - A list of join condition strings.
          - A list of residual SelectNodes (cross-join predicates that the RBO
            left above a JoinNode because they span multiple tables; they must
            be re-wrapped around the final physical join core).

        We flatten the tree top-down so that pushed-down SelectNodes are
        correctly associated with their ScanNode or SubqueryNode.
        """
        table_infos:      List[_TableInfo]  = []
        join_conditions:  List[str]         = []
        residual_filters: List[SelectNode]  = []
        self._collect(node, table_infos, join_conditions, pending_filter=None,
                      residual_filters=residual_filters)
        return table_infos, join_conditions, residual_filters

    def _collect(
        self,
        node: PlanNode,
        table_infos: List[_TableInfo],
        join_conditions: List[str],
        pending_filter: Optional[SelectNode],
        residual_filters: Optional[List[SelectNode]] = None,
    ) -> None:
        """
        Recursive DFS to collect table sources and join conditions.

        SelectNodes that are *directly above a JoinNode* and reference columns
        from multiple tables (cross-join predicates that the RBO couldn't push
        down) are captured as ``residual_filters`` instead of ``pending_filter``
        so that they can be re-applied above the reassembled join core.

        The ``join_type`` stored in each ``_TableInfo`` reflects the type of
        join used to attach that table to the left side.  This is how the CBO
        detects outer joins without scanning every JoinNode separately.
        """
        if residual_filters is None:
            residual_filters = []

        if isinstance(node, ScanNode):
            cardinality = self._safe_cardinality(node.table_name)
            table_infos.append(
                _TableInfo(
                    name=node.table_name.lower(),
                    scan=node,
                    filter=pending_filter,
                    cardinality=cardinality,
                    join_type="FROM",       # updated by caller for JOIN nodes
                )
            )

        elif isinstance(node, SubqueryNode):
            cardinality = self._safe_cardinality(node.alias)
            table_infos.append(
                _TableInfo(
                    name=node.alias.lower(),
                    scan=node,
                    filter=pending_filter,
                    cardinality=cardinality,
                    join_type="FROM",
                )
            )

        elif isinstance(node, SelectNode):
            # Determine whether this SelectNode is a pushed-down leaf filter
            # (child is a ScanNode/SubqueryNode) or a cross-join residual
            # (child is a JoinNode — meaning the RBO couldn't push it down).
            if isinstance(node.child, JoinNode):
                # Cross-join residual — collect it and recurse into the join.
                import copy as _copy
                residual = _copy.copy(node)  # shallow copy — child will be reset
                residual_filters.append(residual)
                self._collect(
                    node.child, table_infos, join_conditions,
                    pending_filter=None, residual_filters=residual_filters,
                )
            else:
                # Standard pushed-down filter — associate with its child source.
                self._collect(
                    node.child, table_infos, join_conditions,
                    pending_filter=node, residual_filters=residual_filters,
                )

        elif isinstance(node, JoinNode):
            if node.condition not in join_conditions:
                join_conditions.append(node.condition)
            # Left child: use default join_type (already set).
            self._collect(node.left, table_infos, join_conditions,
                          None, residual_filters)
            # Right child: tag with the join's type so outer-join detection works.
            right_start = len(table_infos)
            self._collect(node.right, table_infos, join_conditions,
                          None, residual_filters)
            # Retroactively tag every _TableInfo added from the right side.
            for i in range(right_start, len(table_infos)):
                table_infos[i] = _TableInfo(
                    name=table_infos[i].name,
                    scan=table_infos[i].scan,
                    filter=table_infos[i].filter,
                    cardinality=table_infos[i].cardinality,
                    join_type=node.join_type,
                )

        elif isinstance(node, ProjectNode):
            # Leaf-level ProjectNode (RBO projection pushdown): the child is a
            # ScanNode or SubqueryNode.  Treat the whole ProjectNode as the scan
            # so the projection is preserved in the physical plan.
            if isinstance(node.child, (ScanNode, SubqueryNode)):
                inner = node.child
                if isinstance(inner, ScanNode):
                    cardinality = self._safe_cardinality(inner.table_name)
                    tbl_name    = inner.table_name.lower()
                else:
                    cardinality = self._safe_cardinality(inner.alias)
                    tbl_name    = inner.alias.lower()
                table_infos.append(
                    _TableInfo(
                        name=tbl_name,
                        scan=node,           # ProjectNode IS the leaf subtree
                        filter=pending_filter,
                        cardinality=cardinality,
                        join_type="FROM",
                    )
                )
            else:
                # Top-level ProjectNode wrapping a join tree or aggregate —
                # recurse through it (it gets peeled/re-attached in optimize()).
                self._collect(node.child, table_infos, join_conditions,
                              None, residual_filters)

        elif isinstance(node, AggregateNode):
            self._collect(node.child, table_infos, join_conditions,
                          None, residual_filters)

    # ------------------------------------------------------------------
    # Cost computation
    # ------------------------------------------------------------------

    def _compute_order_cost(
        self, order: List[_TableInfo], join_conditions: List[str]
    ) -> Tuple[int, str]:
        """
        Compute the total cost of joining tables in *order* left-to-right.
        """
        intermediate = order[0].cardinality
        total        = 0
        steps        = [str(order[0].cardinality)]

        introduced = [order[0].name]
        used_conds = set()

        for info in order[1:]:
            introduced.append(info.name)
            matched_cond = self._find_condition(introduced, join_conditions, used_conds, strict=True)
            
            if matched_cond:
                used_conds.add(matched_cond)
                step         = int(intermediate * info.cardinality * 0.1)
                total       += step
                intermediate = max(intermediate, info.cardinality)
                steps.append(f"*{info.cardinality}={step:,}")
            else:
                step         = intermediate * info.cardinality
                total       += step
                intermediate = step
                steps.append(f"*(Cross){info.cardinality}={step:,}")

        return total, " ".join(steps)

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

        Note: This method is only called when no outer joins are present
        (reordering has been deemed safe).  JoinNodes built here always use
        ``join_type="INNER"``.

        Parameters:
            order           : Tables in the chosen join order.
            join_conditions : All join condition strings from the original tree.

        Returns:
            A PlanNode (single source or chain of JoinNodes).
        """
        if len(order) == 1:
            return order[0].root_node

        introduced:   List[str] = [order[0].name]
        current_node: PlanNode  = order[0].root_node
        used_conditions: set    = set()

        for info in order[1:]:
            introduced.append(info.name)
            matched_cond = self._find_condition(
                introduced, join_conditions, used_conditions
            )
            used_conditions.add(matched_cond or "")
            current_node = JoinNode(
                left=current_node,
                right=info.root_node,
                condition=matched_cond or f"{order[0].name} \u22c8 {info.name}",
                join_type="INNER",
            )

        return current_node

    @staticmethod
    def _find_condition(
        tables: List[str],
        conditions: List[str],
        used: set,
        strict: bool = False,
    ) -> Optional[str]:
        """
        Find a join condition that references at least two of the currently
        introduced tables and hasn't been used yet.

        The match uses dotted-notation token extraction so that conditions like
        ``active_users.city_id = cities.id`` correctly resolve to the virtual
        CTE table name ``active_users``.

        Falls back to the first unused condition if no perfect match.
        """
        tables_set = set(tables)

        for cond in conditions:
            if cond in used:
                continue
            dotted    = re.findall(r"([A-Za-z_]\w*)\.(?:[A-Za-z_]\w*)", cond)
            mentioned = {t.lower() for t in dotted}
            if mentioned and mentioned.issubset(tables_set):
                return cond

        # Fallback: return any unused condition.
        if not strict:
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
        table is not in the catalog (e.g. CTE virtual tables, unknown sources).
        """
        try:
            return self._catalog.get_cardinality(table_name)
        except KeyError:
            return 1