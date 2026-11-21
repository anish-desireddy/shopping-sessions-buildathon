"""Unit tests for pipeline/identity.py against rules.md, "Linked IDs"."""
import random
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))

import csvio  # noqa: E402
from identity import UnionFind  # noqa: E402

FIXTURES = ROOT / "fixtures"


def fixture_links(case):
    """Link pairs as the links stage sees them: CSV-valid, both sides non-empty."""
    pairs = []
    for path in sorted((FIXTURES / case / "input" / "links").glob("*.csv")):
        for left, right in csvio.iter_rows(path, csvio.LINK_FIELDS):
            if left and right:
                pairs.append((left, right))
    return pairs


def names_for(pairs):
    uf = UnionFind()
    for left, right in pairs:
        uf.union(left, right)
    return uf.names()


class UnionFindTest(unittest.TestCase):
    def test_unlinked_id_is_its_own_person(self):
        uf = UnionFind(["u1", "u2"])
        self.assertEqual(uf.names(), {"u1": "u1", "u2": "u2"})

    def test_transitive_join(self):
        uf = UnionFind()
        uf.union("u3", "u9")
        uf.union("u9", "u2")
        self.assertEqual(uf.find("u3"), uf.find("u2"))
        self.assertEqual(
            sorted(sorted(members) for members in uf.components().values()),
            [["u2", "u3", "u9"]],
        )
        self.assertEqual(uf.names(), {"u3": "u2", "u9": "u2", "u2": "u2"})

    def test_direction_and_duplicates_do_not_matter(self):
        left = names_for([("a", "b"), ("c", "b")])
        right = names_for([("b", "a"), ("b", "c"), ("a", "b")])
        self.assertEqual(left, right)
        self.assertEqual(left, {"a": "a", "b": "a", "c": "a"})

    def test_self_link_changes_nothing(self):
        self.assertEqual(names_for([("u6", "u6")]), {"u6": "u6"})

    def test_name_is_smallest_even_if_never_used_in_events(self):
        self.assertEqual(
            names_for([("u3", "u9"), ("u9", "u2")]),
            {"u2": "u2", "u3": "u2", "u9": "u2"},
        )

    def test_name_is_bytewise_smallest(self):
        # "U7" < "u2": upper case sorts before lower case.
        self.assertEqual(names_for([("u9", "U7")])["u9"], "U7")
        self.assertEqual(names_for([("u9", "U7")])["U7"], "U7")

    def test_ids_are_case_sensitive(self):
        uf = UnionFind(["U1", "u1"])
        self.assertEqual(len(uf.components()), 2)
        uf.union("U1", "u1")
        self.assertEqual(uf.names()["u1"], "U1")

    def test_control_tab_sorts_before_letters(self):
        self.assertEqual(names_for([("user_id_a", "A\tB")])["user_id_a"], "A\tB")

    def test_union_reports_whether_it_joined(self):
        uf = UnionFind()
        self.assertTrue(uf.union("a", "b"))
        self.assertFalse(uf.union("b", "a"))

    def test_order_independence(self):
        pairs = [
            ("u1", "u2"),
            ("u2", "u3"),
            ("u9", "u3"),
            ("U1", "u9"),
            ("u4", "u5"),
        ]
        expected = names_for(pairs)
        rng = random.Random(1)
        for _ in range(50):
            shuffled = list(pairs)
            rng.shuffle(shuffled)
            shuffled = [(b, a) if rng.random() < 0.5 else (a, b) for a, b in shuffled]
            self.assertEqual(names_for(shuffled), expected)
        self.assertEqual(expected["u1"], "U1")

    def test_find_compresses_without_changing_roots(self):
        uf = UnionFind()
        for index in range(20):
            uf.union(f"u{index}", f"u{index + 1}")
        root = uf.find("u20")
        for index in range(21):
            self.assertEqual(uf.find(f"u{index}"), root)
        self.assertEqual(len(uf.components()), 1)


class FixtureLinksTest(unittest.TestCase):
    """The links fixtures, resolved to person names."""

    def test_l1(self):
        self.assertEqual(names_for(fixture_links("L1")), {"u2": "u1", "u1": "u1"})

    def test_l2_chains_across_files(self):
        names = names_for(fixture_links("L2"))
        self.assertEqual(names["u3"], "U7")
        self.assertEqual(names["u9"], "U7")
        self.assertEqual(names["u2"], "U7")
        self.assertEqual(names["u10"], "u10")

    def test_l3_malformed_and_quoted_links(self):
        names = names_for(fixture_links("L3"))
        # u2, u3 and u5 appear only in events, never in a surviving link.
        self.assertEqual(
            set(names), {"u1", "u4", "user_id_a", "user_id_b", "A\tB", "u6"}
        )
        for linked in ("u1", "u4", "user_id_a", "user_id_b"):
            self.assertEqual(names[linked], "A\tB")
        self.assertEqual(names["u6"], "u6")

    def test_l4(self):
        self.assertEqual(names_for(fixture_links("L4")), {"u2": "u1", "u1": "u1"})


if __name__ == "__main__":
    unittest.main()
