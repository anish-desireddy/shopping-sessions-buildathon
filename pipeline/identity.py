"""Union-find over user IDs (rules.md, "Linked IDs").

Links join two IDs into one person and joining is transitive, in any file, in
either direction, however many links apart. Each person is named by the
byte-wise smallest of their IDs, including IDs that never appear in an event or
a click.

Byte-wise order and Python string order agree for valid UTF-8: UTF-8 preserves
code-point order in its byte sequences, so ``min()`` over decoded IDs is the
byte-wise minimum the rules require. IDs are never normalized (no case folding,
no trimming), so ``U1`` and ``u1`` stay distinct unless a link joins them.

The name is computed from the final components, not from the union order, so
the result does not depend on the order links are read.
"""
from __future__ import annotations

from typing import Dict, Iterable, List

__all__ = ["UnionFind"]


class UnionFind:
    """Disjoint sets of user IDs with path compression and union by rank."""

    __slots__ = ("_parent", "_rank")

    def __init__(self, items: Iterable[str] = ()) -> None:
        self._parent: Dict[str, str] = {}
        self._rank: Dict[str, int] = {}
        for item in items:
            self.add(item)

    def add(self, item: str) -> None:
        """Register ``item`` as its own set if it is not known yet."""
        if item not in self._parent:
            self._parent[item] = item
            self._rank[item] = 0

    def __contains__(self, item: object) -> bool:
        return item in self._parent

    def __len__(self) -> int:
        return len(self._parent)

    def find(self, item: str) -> str:
        """Representative of ``item``'s set, compressing the path."""
        parent = self._parent
        root = item
        while parent[root] != root:
            root = parent[root]
        while parent[item] != root:
            parent[item], item = root, parent[item]
        return root

    def union(self, left: str, right: str) -> bool:
        """Join two IDs' sets. Returns False if they were already joined."""
        self.add(left)
        self.add(right)
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return False
        rank = self._rank
        if rank[left_root] < rank[right_root]:
            left_root, right_root = right_root, left_root
        self._parent[right_root] = left_root
        if rank[left_root] == rank[right_root]:
            rank[left_root] += 1
        return True

    def components(self) -> Dict[str, List[str]]:
        """Map each representative to its member IDs."""
        groups: Dict[str, List[str]] = {}
        for item in self._parent:
            groups.setdefault(self.find(item), []).append(item)
        return groups

    def names(self) -> Dict[str, str]:
        """Map every known ID to the name of its person (smallest ID)."""
        smallest: Dict[str, str] = {}
        for item in self._parent:
            root = self.find(item)
            current = smallest.get(root)
            if current is None or item < current:
                smallest[root] = item
        return {item: smallest[self.find(item)] for item in self._parent}
