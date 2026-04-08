"""
engine/visualizer.py
--------------------
Plan Tree Visualizer

Provides a utility class `PlanVisualizer` that converts a PlanNode tree into
a rich, multi-line ASCII-art string suitable for display in terminals or
web interfaces (e.g., Streamlit `st.code` blocks).

The output uses box-drawing characters to make the tree structure clear:

    📋 Project [ users.name, countries.country_name ]
    └── 🔗 InnerJoin [ ON cities.country_id = countries.id ]
         ├── 🔗 InnerJoin [ ON users.city_id = cities.id ]
         │    ├── 🔍 Filter [ users.id > 500 ]
         │    │    └── 📂 SeqScan [ users ]
         │    └── 📂 SeqScan [ cities ]
         └── 📂 SeqScan [ countries ]

Usage::

    from engine.visualizer import PlanVisualizer
    vis    = PlanVisualizer()
    output = vis.render(plan_root)
    print(output)
"""

from __future__ import annotations

from typing import List

from engine.nodes import (
    JoinNode,
    PlanNode,
    ProjectNode,
    ScanNode,
    SelectNode,
)


class PlanVisualizer:
    """
    Renders a PlanNode tree as a formatted ASCII string.

    Internals
    ---------
    The renderer walks the tree recursively, passing a *prefix* string that
    encodes the "branch" context (using │ pipe characters for open branches
    and spaces for closed ones).  This allows us to produce correctly aligned
    connectors at every level without a two-pass approach.
    """

    # Box-drawing characters used for the tree layout.
    _PIPE    = "│   "   # vertical continuation
    _TEE     = "├── "   # non-last sibling connector
    _CORNER  = "└── "   # last sibling connector
    _BLANK   = "    "   # continuation under a last sibling

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
        connector = self._CORNER if is_last else self._TEE

        if isinstance(node, ScanNode):
            lines.append(f"{prefix}{connector}📂 SeqScan [ {node.table_name} ]")

        elif isinstance(node, SelectNode):
            lines.append(f"{prefix}{connector}🔍 Filter [ {node.predicate} ]")
            child_prefix = prefix + (self._BLANK if is_last else self._PIPE)
            self._render_node(node.child, child_prefix, is_last=True, lines=lines)

        elif isinstance(node, ProjectNode):
            cols = ", ".join(node.columns) if node.columns else "*"
            lines.append(f"{prefix}{connector}📋 Project [ {cols} ]")
            child_prefix = prefix + (self._BLANK if is_last else self._PIPE)
            self._render_node(node.child, child_prefix, is_last=True, lines=lines)

        elif isinstance(node, JoinNode):
            lines.append(
                f"{prefix}{connector}🔗 InnerJoin [ ON {node.condition} ]"
            )
            child_prefix = prefix + (self._BLANK if is_last else self._PIPE)
            # Left child is NOT last (right comes after it).
            self._render_node(node.left,  child_prefix, is_last=False, lines=lines)
            # Right child IS last.
            self._render_node(node.right, child_prefix, is_last=True,  lines=lines)

        else:
            # Unknown node type — render a generic representation.
            lines.append(f"{prefix}{connector}⚙ {type(node).__name__}")

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
        sep   = "═" * 60
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

        Example: "Project → Filter → Join → Scan"
        """
        parts: List[str] = []
        node = root
        while node is not None:
            if isinstance(node, ProjectNode):
                parts.append("Project")
                node = node.child
            elif isinstance(node, SelectNode):
                parts.append("Filter")
                node = node.child
            elif isinstance(node, JoinNode):
                parts.append("Join")
                node = node.left   # Follow left branch for summary
            elif isinstance(node, ScanNode):
                parts.append(f"Scan({node.table_name})")
                break
            else:
                parts.append(type(node).__name__)
                break
        return " → ".join(parts)
