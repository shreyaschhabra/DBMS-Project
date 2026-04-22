""" rbo.py """

from __future__ import annotations

import re
from typing import List, Optional, Set

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


class RuleBasedOptimizer:
    """
    Applies rule-based algebraic rewrite rules to a logical plan tree.

    The optimizer is **stateless between calls** — a fresh log of applied
    rules is maintained per ``optimize()`` invocation.

    Public attributes (after calling ``optimize()``)
    -------------------------------------------------
    _predicate_rules  : Rules fired by Predicate Pushdown.
    _projection_rules : Rules fired by Projection Pushdown.
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

        tree = self._apply_predicate_pushdown(root)
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

        Core rule (AND clause — ``is_or_block=False``)
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        If we find ``SelectNode(predicate, child=JoinNode(L, R))``
        and the predicate references only columns from table T, rewrite to::

            JoinNode(SelectNode(predicate, L), R)   # predicate belongs to L
            JoinNode(L, SelectNode(predicate, R))   # predicate belongs to R

        If the predicate spans both sides the SelectNode stays above the join.

        OR-block rule (``is_or_block=True``)
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        An OR-compound predicate (e.g. ``users.id > 5 OR users.id < 2``) can
        only be pushed below a JoinNode if ALL table references in the
        expression belong exclusively to one side.  If the OR spans tables on
        both sides of the join it MUST remain above the join.

        Outer-join safety rule
        ~~~~~~~~~~~~~~~~~~~~~~
        For a LEFT JOIN: a predicate that references ONLY the right-side table
        cannot be pushed to the right subtree — doing so would eliminate NULL-
        padded rows, changing the semantics from LEFT JOIN to effectively an
        INNER JOIN.  Similarly for RIGHT JOIN / left-side predicates.

        AggregateNode barrier
        ~~~~~~~~~~~~~~~~~~~~~
        A predicate that references an aggregate function (``COUNT``, ``SUM``,
        ``AVG``, ``MIN``, ``MAX``) is a HAVING-style post-aggregate filter.
        It must stay **above** the AggregateNode that produced the aggregated
        values.

        SubqueryNode barrier
        ~~~~~~~~~~~~~~~~~~~~
        Predicates are never pushed into a SubqueryNode from outside.  The
        inner plan is already independently optimized.

        Returns
        -------
        PlanNode
            The (possibly rewritten) subtree.
        """
        # ── ProjectNode: recurse into child ──────────────────────────────
        if isinstance(node, ProjectNode):
            node.child = self._apply_predicate_pushdown(node.child)
            return node

        # ── AggregateNode: recurse into child ────────────────────────────
        if isinstance(node, AggregateNode):
            node.child = self._apply_predicate_pushdown(node.child)
            return node

        # ── SelectNode ─────────────────────────────────────────────────────
        if isinstance(node, SelectNode):
            return self._push_select(node)

        # ── JoinNode: recurse into both children ──────────────────────────
        if isinstance(node, JoinNode):
            node.left  = self._apply_predicate_pushdown(node.left)
            node.right = self._apply_predicate_pushdown(node.right)
            return node

        # ── SubqueryNode: recurse into inner plan ─────────────────────────
        if isinstance(node, SubqueryNode):
            node.child = self._apply_predicate_pushdown(node.child)
            return node

        # ── ScanNode / leaf: nothing to do ───────────────────────────────
        return node

    def _push_select(self, node: SelectNode) -> PlanNode:
        """
        Attempt to push *node* (a SelectNode) downward through its child.

        Handles the AggregateNode barrier, SubqueryNode barrier, outer-join
        safety, plain AND-clause pushdown, and OR-block pushdown.
        """
        predicate = node.predicate
        is_or     = node.is_or_block

        # ── AggregateNode barrier ────────────────────────────────────────
        if isinstance(node.child, AggregateNode):
            if self._references_aggregate(predicate):
                # Post-aggregate (HAVING-style) — keep above AggregateNode.
                node.child = self._apply_predicate_pushdown(node.child)
                return node
            else:
                # Pre-aggregate — push through AggregateNode into its child.
                agg_node: AggregateNode = node.child
                agg_node.child = SelectNode(
                    child=agg_node.child,
                    predicate=predicate,
                    is_or_block=is_or,
                )
                agg_node.child = self._apply_predicate_pushdown(agg_node.child)
                return agg_node

        # ── SubqueryNode barrier ─────────────────────────────────────────
        if isinstance(node.child, SubqueryNode):
            return node

        # ── JoinNode: attempt pushdown ────────────────────────────────────
        if isinstance(node.child, JoinNode):
            return self._push_through_join(node, node.child)

        # ── Already at ScanNode level — nothing deeper to push ───────────
        if isinstance(node.child, ScanNode):
            return node

        # ── Any other child: recurse ──────────────────────────────────────
        node.child = self._apply_predicate_pushdown(node.child)
        return node

    def _push_through_join(
        self, select_node: SelectNode, join_node: JoinNode
    ) -> PlanNode:
        """
        Decide whether *select_node* can be pushed below *join_node*.

        Returns the (possibly rewritten) subtree.
        """
        predicate    = select_node.predicate
        is_or        = select_node.is_or_block
        pred_tables  = self._tables_in_expr(predicate)
        left_tables  = join_node.left.source_tables
        right_tables = join_node.right.source_tables

        # ── OR-block: can only push if ALL referenced tables are on ONE side
        if is_or:
            if pred_tables and pred_tables.issubset(left_tables):
                # All OR-terms are from the left side — safe to push left.
                if join_node.is_outer and join_node.join_type == "RIGHT":
                    # RIGHT JOIN: right table is preserved; left is NOT.
                    # Filtering on left inside a RIGHT JOIN is safe.
                    pass  # fall through to push logic below
                self._predicate_rules.append(
                    f"OR Predicate Pushdown: '{predicate}' (OR-block, single-table) "
                    f"pushed below {join_node.join_type} JOIN "
                    f"-> LEFT side ({', '.join(sorted(left_tables))})"
                )
                join_node.left = SelectNode(
                    child=join_node.left, predicate=predicate, is_or_block=True
                )
                join_node.left  = self._apply_predicate_pushdown(join_node.left)
                join_node.right = self._apply_predicate_pushdown(join_node.right)
                return join_node

            elif pred_tables and pred_tables.issubset(right_tables):
                if join_node.is_outer and join_node.join_type == "LEFT":
                    # LEFT JOIN: right table is the "optional" side.
                    # Pushing a filter onto the right would convert it to inner.
                    self._predicate_rules.append(
                        f"OR Predicate NOT pushed: '{predicate}' targets RIGHT side "
                        f"of a LEFT JOIN — pushing would eliminate NULL-padded rows, "
                        f"changing LEFT JOIN semantics to INNER JOIN."
                    )
                    join_node.left  = self._apply_predicate_pushdown(join_node.left)
                    join_node.right = self._apply_predicate_pushdown(join_node.right)
                    return select_node
                self._predicate_rules.append(
                    f"OR Predicate Pushdown: '{predicate}' (OR-block, single-table) "
                    f"pushed below {join_node.join_type} JOIN "
                    f"-> RIGHT side ({', '.join(sorted(right_tables))})"
                )
                join_node.right = SelectNode(
                    child=join_node.right, predicate=predicate, is_or_block=True
                )
                join_node.left  = self._apply_predicate_pushdown(join_node.left)
                join_node.right = self._apply_predicate_pushdown(join_node.right)
                return join_node

            else:
                # OR spans multiple tables — MUST stay above the join.
                involved = ", ".join(sorted(pred_tables)) if pred_tables else "unknown"
                self._predicate_rules.append(
                    f"OR Predicate NOT pushed: '{predicate}' references tables "
                    f"[{involved}] on both sides of the {join_node.join_type} JOIN — "
                    f"OR conditions spanning multiple tables cannot be decomposed "
                    f"and must remain above the join."
                )
                join_node.left  = self._apply_predicate_pushdown(join_node.left)
                join_node.right = self._apply_predicate_pushdown(join_node.right)
                return select_node  # keep SelectNode above join

        # ── AND clause: standard pushdown logic ───────────────────────────
        if pred_tables and pred_tables.issubset(left_tables):
            # Check outer-join safety for LEFT/RIGHT joins.
            if join_node.is_outer and join_node.join_type == "RIGHT":
                # RIGHT JOIN preserves right rows; left is filtered at join time.
                # It IS safe to push a left-table filter below a RIGHT JOIN.
                pass  # fall through to push
            self._predicate_rules.append(
                f"Predicate Pushdown: '{predicate}' pushed below "
                f"{join_node.join_type} JOIN "
                f"-> LEFT side ({', '.join(sorted(left_tables))})"
            )
            join_node.left = SelectNode(
                child=join_node.left, predicate=predicate, is_or_block=False
            )
            join_node.left  = self._apply_predicate_pushdown(join_node.left)
            join_node.right = self._apply_predicate_pushdown(join_node.right)
            return join_node

        elif pred_tables and pred_tables.issubset(right_tables):
            # Outer-join safety: cannot push right-table filter below a LEFT JOIN.
            if join_node.is_outer and join_node.join_type == "LEFT":
                self._predicate_rules.append(
                    f"Predicate NOT pushed: '{predicate}' targets RIGHT side "
                    f"of a LEFT JOIN — pushing would eliminate NULL-padded rows, "
                    f"converting LEFT JOIN semantics to INNER JOIN."
                )
                join_node.left  = self._apply_predicate_pushdown(join_node.left)
                join_node.right = self._apply_predicate_pushdown(join_node.right)
                return select_node

            self._predicate_rules.append(
                f"Predicate Pushdown: '{predicate}' pushed below "
                f"{join_node.join_type} JOIN "
                f"-> RIGHT side ({', '.join(sorted(right_tables))})"
            )
            join_node.right = SelectNode(
                child=join_node.right, predicate=predicate, is_or_block=False
            )
            join_node.left  = self._apply_predicate_pushdown(join_node.left)
            join_node.right = self._apply_predicate_pushdown(join_node.right)
            return join_node

        else:
            # Cross-table predicate — keep SelectNode above join, recurse.
            join_node.left  = self._apply_predicate_pushdown(join_node.left)
            join_node.right = self._apply_predicate_pushdown(join_node.right)
            return select_node

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
           - All ``AggregateNode`` ``group_by_cols``, ``aggregates``, and ``having``.
        2. For each ``ScanNode`` encountered during DFS, determine which of
           its catalog columns appear in the global required set.
        3. Wrap the ``ScanNode`` with a ``ProjectNode`` listing only those
           columns (or leave it unwrapped if none matched, as a safe fallback).

        Parameters
        ----------
        root : PlanNode
            Root of the (predicate-pushed) plan tree.

        Returns
        -------
        PlanNode
            Root of the tree with projection nodes injected above scans.
        """
        required_cols: Set[str] = set()
        self._collect_required_columns(root, required_cols)
        return self._inject_projections(root, required_cols)

    def _collect_required_columns(
        self, node: PlanNode, required: Set[str]
    ) -> None:
        """
        Recursively harvest every ``table.column`` token referenced anywhere
        in the tree (PROJECT list, SELECT predicates, JOIN conditions,
        AGGREGATE keys and functions).
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
            self._collect_required_columns(node.left,  required)
            self._collect_required_columns(node.right, required)

        elif isinstance(node, AggregateNode):
            for col in node.group_by_cols:
                required.update(self._dotted_columns(col))
            for agg in node.aggregates:
                required.update(self._dotted_columns(agg))
            if node.having:
                required.update(self._dotted_columns(node.having))
            self._collect_required_columns(node.child, required)

        elif isinstance(node, SubqueryNode):
            # Do NOT recurse into the inner plan — it is independently
            # optimized; projections are not pushed across subquery boundaries.
            pass

        elif isinstance(node, ScanNode):
            pass  # Leaf — no children.

    def _inject_projections(
        self, node: PlanNode, required: Set[str]
    ) -> PlanNode:
        """
        Walk the tree top-down; when a ``ScanNode`` is found, wrap it with a
        ``ProjectNode`` containing only the columns needed from that table.

        SubqueryNodes and AggregateNodes are passed through unchanged (no
        external projections are injected into derived tables or across
        aggregate boundaries).
        """
        if isinstance(node, ScanNode):
            needed = self._columns_for_table(node.table_name, required)
            if not needed:
                return node
            needed_str  = ", ".join(sorted(needed))
            full_cols   = self._all_catalog_columns(node.table_name)
            dropped     = sorted(set(full_cols) - set(needed))
            dropped_str = ", ".join(dropped) if dropped else "none"
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
            node.left  = self._inject_projections(node.left,  required)
            node.right = self._inject_projections(node.right, required)
            return node

        if isinstance(node, AggregateNode):
            node.child = self._inject_projections(node.child, required)
            return node

        if isinstance(node, SubqueryNode):
            return node  # Never inject projections into a subquery from outside.

        return node  # Unknown node type — pass through.

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _tables_in_expr(expr: str) -> Set[str]:
        """
        Return the set of *table names* referenced in *expr* via
        ``table.column`` dotted notation.

        Since the parser resolves all aliases to real table names (or CTE names)
        before storing condition strings, these prefixes already match the
        ``source_tables`` of each plan node.

        Examples::
            "users.id > 500"                         -> {"users"}
            "users.city_id = cities.id"              -> {"users", "cities"}
            "users.id > 5 OR users.id < 2"           -> {"users"}
            "users.id > 5 OR cities.id = 1"          -> {"users", "cities"}
            "active_users.city_id = cities.id"       -> {"active_users", "cities"}
        """
        matches = re.findall(r"([A-Za-z_]\w*)\.(?:[A-Za-z_]\w*)", expr)
        return {t.lower() for t in matches}

    @staticmethod
    def _dotted_columns(expr: str) -> Set[str]:
        """
        Return all ``table.column`` tokens found in *expr*, lower-cased.

        Examples::
            "users.name, cities.city_name" -> {"users.name", "cities.city_name"}
            "users.id > 500"               -> {"users.id"}
        """
        matches = re.findall(r"([A-Za-z_]\w*\.[A-Za-z_]\w*)", expr)
        return {m.lower() for m in matches}

    @staticmethod
    def _references_aggregate(expr: str) -> bool:
        """
        Return True if *expr* contains an aggregate function call
        (``COUNT``, ``SUM``, ``AVG``, ``MIN``, ``MAX``).

        Used to identify HAVING-style predicates that must stay above the
        AggregateNode barrier.

        Examples::
            "COUNT(users.id) > 5" -> True
            "users.id > 500"      -> False
        """
        return bool(
            re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", expr, re.IGNORECASE)
        )

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
        is not found (e.g. it is a CTE virtual table).
        """
        if self._catalog is None:
            return []
        try:
            return self._catalog.get_columns(table_name)
        except KeyError:
            return []