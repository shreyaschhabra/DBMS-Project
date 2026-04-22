""" visualizer.py """

from __future__ import annotations

from typing import List

from engine.nodes import (
    AggregateNode,
    JoinNode,
    PlanNode,
    ProjectNode,
    ScanNode,
    SelectNode,
    SubqueryNode,
)


class PlanVisualizer:
    """
    Renders a PlanNode tree as a formatted ASCII string.

    Internals
    ---------
    The renderer walks the tree recursively, threading a *prefix* string that
    encodes the branch context (``│`` pipe characters for open branches,
    spaces for closed ones).  This produces correctly-aligned connectors at
    every level without a two-pass approach.

    No emoji are used — the output is strictly ASCII + box-drawing so it
    renders correctly in monospace containers regardless of font support.
    """

    # Box-drawing characters used for the tree layout.
    _PIPE   = "│   "   # vertical continuation line
    _TEE    = "├── "   # non-last sibling connector
    _CORNER = "└── "   # last sibling connector
    _BLANK  = "    "   # continuation under a last-sibling branch

    def render(self, root: PlanNode) -> str:
        """
        Render the plan tree rooted at *root* to a multi-line string.

        Parameters:
            root : The root PlanNode of the plan tree.

        Returns:
            A formatted string with box-drawing characters.
        """
        lines: List[str] = []
        self._render_node(root, prefix="", is_last=True, lines=lines)
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal recursive renderer
    # ------------------------------------------------------------------

    def _render_node(
        self,
        node: PlanNode,
        prefix: str,
        is_last: bool,
        lines: List[str],
    ) -> None:
        """
        Recursively render *node* and all its children.

        Parameters:
            node    : Current node to render.
            prefix  : Accumulated indentation prefix from parent levels.
            is_last : True if this node is the last child of its parent.
            lines   : Accumulated output lines (mutated in place).
        """
        connector    = self._CORNER if is_last else self._TEE
        child_prefix = prefix + (self._BLANK if is_last else self._PIPE)

        # ── SeqScan ──────────────────────────────────────────────────
        if isinstance(node, ScanNode):
            label = (
                f"{node.table_name} AS {node.alias}"
                if node.alias
                else node.table_name
            )
            lines.append(f"{prefix}{connector}SeqScan [ {label} ]")

        # ── Filter (WHERE / OR-block) ─────────────────────────────────
        elif isinstance(node, SelectNode):
            tag = "OrFilter" if node.is_or_block else "Filter"
            lines.append(f"{prefix}{connector}{tag} [ {node.predicate} ]")
            self._render_node(node.child, child_prefix, is_last=True, lines=lines)

        # ── Project ───────────────────────────────────────────────────
        elif isinstance(node, ProjectNode):
            cols = ", ".join(node.columns) if node.columns else "*"
            lines.append(f"{prefix}{connector}Project [ {cols} ]")
            self._render_node(node.child, child_prefix, is_last=True, lines=lines)

        # ── Join (INNER / LEFT / RIGHT / FULL / CROSS) ────────────────
        elif isinstance(node, JoinNode):
            # Capitalise so it reads: InnerJoin, LeftJoin, RightJoin, etc.
            label = f"{node.join_type.capitalize()}Join"
            lines.append(
                f"{prefix}{connector}{label} [ ON {node.condition} ]"
            )
            # Left child is NOT last (right comes after).
            self._render_node(node.left,  child_prefix, is_last=False, lines=lines)
            # Right child IS last.
            self._render_node(node.right, child_prefix, is_last=True,  lines=lines)

        # ── Aggregate (GROUP BY / HAVING) ─────────────────────────────
        elif isinstance(node, AggregateNode):
            gb_str  = ", ".join(node.group_by_cols) if node.group_by_cols else "(none)"
            agg_str = ", ".join(node.aggregates)     if node.aggregates    else "(none)"
            having_part = f" | HAVING {node.having}" if node.having else ""
            lines.append(
                f"{prefix}{connector}Aggregate [ GROUP BY {gb_str} | {agg_str}{having_part} ]"
            )
            self._render_node(node.child, child_prefix, is_last=True, lines=lines)

        # ── Subquery (CTE / inline subquery) ──────────────────────────
        elif isinstance(node, SubqueryNode):
            lines.append(f"{prefix}{connector}Subquery [ {node.alias} ]")
            self._render_node(node.child, child_prefix, is_last=True, lines=lines)

        # ── Unknown node type — graceful fallback ─────────────────────
        else:
            lines.append(f"{prefix}{connector}{type(node).__name__}")

    # ------------------------------------------------------------------
    # Additional utilities
    # ------------------------------------------------------------------

    def render_comparison(
        self,
        label_a: str,
        root_a: PlanNode,
        label_b: str,
        root_b: PlanNode,
    ) -> str:
        """
        Render two plan trees side-by-side with labels.

        Useful for showing "Before Optimization" vs "After Optimization".

        Parameters:
            label_a : Header label for the first tree.
            root_a  : Root node of the first tree.
            label_b : Header label for the second tree.
            root_b  : Root node of the second tree.

        Returns:
            A formatted string with both trees, labelled and separated.
        """
        sep    = "=" * 60
        tree_a = self.render(root_a)
        tree_b = self.render(root_b)
        return (
            f"{sep}\n"
            f"  {label_a}\n"
            f"{sep}\n"
            f"{tree_a}\n\n"
            f"{sep}\n"
            f"  {label_b}\n"
            f"{sep}\n"
            f"{tree_b}"
        )

    @staticmethod
    def node_summary(root: PlanNode) -> str:
        """
        Return a one-line summary of the plan: list of operator types from
        root to the first leaf.

        Example: ``"Project -> Aggregate -> LeftJoin -> Scan(users)"``
        """
        parts: List[str] = []
        node: PlanNode | None = root
        while node is not None:
            if isinstance(node, ProjectNode):
                parts.append("Project")
                node = node.child
            elif isinstance(node, AggregateNode):
                parts.append("Aggregate")
                node = node.child
            elif isinstance(node, SelectNode):
                tag = "OrFilter" if node.is_or_block else "Filter"
                parts.append(tag)
                node = node.child
            elif isinstance(node, JoinNode):
                parts.append(f"{node.join_type.capitalize()}Join")
                node = node.left   # Follow left branch for summary
            elif isinstance(node, SubqueryNode):
                parts.append(f"Subquery({node.alias})")
                break
            elif isinstance(node, ScanNode):
                parts.append(f"Scan({node.table_name})")
                break
            else:
                parts.append(type(node).__name__)
                break
        return " -> ".join(parts)