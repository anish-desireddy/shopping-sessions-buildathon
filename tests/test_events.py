"""Tests for pipeline/events.py and pipeline/user_daily.py.

Fixture expectations are read from ``fixtures/*/expected`` and never rewritten.
"""
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT / "scripts"))

import check_events  # noqa: E402
import events  # noqa: E402
import user_daily  # noqa: E402

FIXTURES = ROOT / "fixtures"
EVENT_HEADER = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
MANIFEST = '{"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}'
IN_WINDOW = "2026-10-03T10:00:00Z"
INGEST = "2026-10-03T10:01:00Z"


def ev(
    event_id,
    user,
    ts=IN_WINDOW,
    ingest=INGEST,
    kind="view",
    product="p1",
    price="",
):
    return f"{event_id},{user},{ts},{ingest},{kind},{product},{price}"


def run_stage(event_files, link_rows=(), products=("p1,shoes", "p2,books")):
    """Build a /data-shaped directory in a temp dir and run the event stage."""
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp)
        for sub in ("events", "clicks", "links"):
            (data / sub).mkdir()
        for index, rows in enumerate(event_files):
            (data / "events" / f"part-{index:05d}.csv").write_text(
                EVENT_HEADER + "\n" + "".join(row + "\n" for row in rows),
                encoding="utf-8",
            )
        (data / "clicks" / "part-00000.csv").write_text(
            "click_id,user_id,campaign_id,ts\n", encoding="utf-8"
        )
        (data / "links" / "part-00000.csv").write_text(
            "user_id_a,user_id_b\n" + "".join(row + "\n" for row in link_rows),
            encoding="utf-8",
        )
        (data / "products.csv").write_text(
            "product_id,category\n" + "".join(row + "\n" for row in products),
            encoding="utf-8",
        )
        (data / "campaigns.csv").write_text(
            "campaign_id,channel\nc_1,search\nc_2,social\n", encoding="utf-8"
        )
        (data / "manifest.json").write_text(MANIFEST, encoding="utf-8")
        return events.run_event_stage(data)


def user_rows(result):
    return [
        (
            row.user_id,
            user_daily.iso_day(row.day),
            row.sessions,
            row.events,
            row.revenue_cents,
            row.top_category,
        )
        for row in user_daily.build_user_daily(result.sessions)
    ]


def session_summary(result):
    return [
        (session.person, session.day, session.events, session.revenue_cents)
        for session in result.sessions
    ]


class DedupTest(unittest.TestCase):
    def test_dedup_is_global_across_files(self):
        result = run_stage(
            [
                [ev("e1", "u1", ingest="2026-10-03T10:05:00Z")],
                [ev("e1", "u2", ingest="2026-10-03T10:01:00Z")],
            ]
        )
        self.assertEqual(user_rows(result), [("u2", "2026-10-03", 1, 1, 0, None)])

    def test_invalid_winning_copy_blocks_the_valid_copy(self):
        # E5: the earliest copy has a bad ts, and the later valid copy must not
        # rescue it, even though the valid copy is listed first.
        result = run_stage(
            [
                [
                    ev("e2", "u1", ingest="2026-10-03T10:05:00Z"),
                    ev(
                        "e2",
                        "u1",
                        ts="2026-10-03 10:00:00",
                        ingest="2026-10-03T10:01:00Z",
                    ),
                ]
            ]
        )
        self.assertEqual(user_rows(result), [])
        self.assertEqual(result.stats.invalid_ts, 1)

    def test_malformed_copy_cannot_block_a_valid_copy(self):
        result = run_stage(
            [
                [
                    # Six fields: malformed CSV, dropped before dedup.
                    "e2,u1,2026-10-03T10:00:00Z,2026-10-03T10:00:30Z,view,p1",
                    ev("e2", "u1", ingest="2026-10-03T10:05:00Z"),
                ]
            ]
        )
        self.assertEqual(user_rows(result), [("u1", "2026-10-03", 1, 1, 0, None)])

    def test_equal_ingest_ties_keep_exactly_one_copy(self):
        row = ev("e1", "u1")
        result = run_stage([[row, row]])
        self.assertEqual(user_rows(result), [("u1", "2026-10-03", 1, 1, 0, None)])

    def test_empty_event_id_is_dropped_before_dedup(self):
        result = run_stage([[ev("", "u1"), ev("e1", "u1")]])
        self.assertEqual(user_rows(result), [("u1", "2026-10-03", 1, 1, 0, None)])
        self.assertEqual(result.stats.empty_event_id, 1)


class ValidationTest(unittest.TestCase):
    def test_window_start_inclusive_end_exclusive(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", ts="2026-09-30T23:59:59Z",
                       ingest="2026-10-01T00:00:59Z"),
                    ev("e2", "u1", ts="2026-10-15T00:00:00Z",
                       ingest="2026-10-15T00:01:00Z"),
                    ev("e3", "u1", ts="2026-10-14T23:59:59Z",
                       ingest="2026-10-15T00:00:59Z"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-14", 1, 1, 0, None)]
        )
        self.assertEqual(result.stats.out_of_window, 2)

    def test_late_boundary(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", ingest="2026-10-04T10:00:00Z"),  # exactly 24h
                    ev("e2", "u1", ingest="2026-10-04T10:00:01Z"),  # over 24h
                    ev("e3", "u1", ingest="2026-10-03T09:00:00Z"),  # before ts
                ]
            ]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 1, 2, 0, None)]
        )
        self.assertEqual(result.stats.late, 1)

    def test_event_type_is_case_and_space_sensitive(self):
        invalid = ("Purchase", "view ", "View", "purchase ", "")
        rows = [
            ev(f"e{index}", "u1", kind=kind)
            for index, kind in enumerate(invalid)
        ]
        result = run_stage([rows])
        self.assertEqual(user_rows(result), [])
        self.assertEqual(result.stats.invalid_type, len(invalid))

        valid = run_stage(
            [["e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,cart,p1,"]]
        )
        self.assertEqual(user_rows(valid), [("u1", "2026-10-03", 1, 1, 0, None)])

    def test_user_and_product_must_be_non_empty(self):
        result = run_stage(
            [[ev("e1", ""), ev("e2", "u1", product="")]]
        )
        self.assertEqual(user_rows(result), [])
        self.assertEqual(result.stats.invalid_user, 1)
        self.assertEqual(result.stats.invalid_product, 1)

    def test_purchase_price_range_and_form(self):
        for price, revenue in (
            ("0250", 250),
            ("1", 1),
            ("10000000", 10_000_000),
        ):
            with self.subTest(price=price):
                result = run_stage(
                    [[ev("e1", "u1", kind="purchase", price=price)]]
                )
                self.assertEqual(
                    user_rows(result),
                    [("u1", "2026-10-03", 1, 1, revenue, "shoes")],
                )
        for price in (
            "",
            "0",
            "000",
            "10000001",
            "19.99",
            "-5",
            " 250",
            "250 ",
            "\uff12\uff15\uff10",  # full-width 250
            "\u0662\u0665\u0660",  # Arabic-Indic 250
            "1e6",
            "25 0",
        ):
            with self.subTest(price=price):
                result = run_stage(
                    [[ev("e1", "u1", kind="purchase", price=price)]]
                )
                self.assertEqual(user_rows(result), [])
                self.assertEqual(result.stats.invalid_price, 1)

    def test_view_and_cart_ignore_the_price_field(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", kind="view", price="-5"),
                    ev("e2", "u1", kind="cart", price="19.99"),
                    ev("e3", "u1", kind="view", price="1000000000000"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 1, 3, 0, None)]
        )

    def test_unknown_product_is_a_category(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", kind="purchase", product="p9", price="2000"),
                    ev("e2", "u1", kind="purchase", product="p1", price="1500"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 1, 2, 3500, "unknown")]
        )

    def test_product_id_with_a_space_is_a_different_product(self):
        result = run_stage(
            [[ev("e1", "u1", kind="purchase", product=" p1", price="400")]]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 1, 1, 400, "unknown")]
        )


class SessionTest(unittest.TestCase):
    def test_gap_of_exactly_30_minutes_is_one_session(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", ts="2026-10-03T10:00:00Z"),
                    ev("e2", "u1", ts="2026-10-03T10:30:00Z",
                       ingest="2026-10-03T10:31:00Z"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 1, 2, 0, None)]
        )

    def test_gap_of_30_minutes_plus_1ms_splits(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", ts="1791021600000"),
                    ev("e2", "u1", ts="1791023400001",
                       ingest="2026-10-03T10:31:00Z"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 2, 2, 0, None)]
        )

    def test_equal_timestamps_share_a_session(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1"),
                    ev("e2", "u1"),
                    ev("e3", "u1", ts="2026-10-03T10:40:00Z",
                       ingest="2026-10-03T10:41:00Z"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 2, 3, 0, None)]
        )

    def test_session_keeps_the_day_of_its_start(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", ts="2026-10-03T23:50:00Z",
                       ingest="2026-10-03T23:51:00Z"),
                    ev("e2", "u1", ts="2026-10-04T00:10:00Z",
                       ingest="2026-10-04T00:11:00Z",
                       kind="purchase", price="1000"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 1, 2, 1000, "shoes")]
        )

    def test_several_sessions_on_one_day(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", ts="2026-10-03T09:00:00Z",
                       ingest="2026-10-03T09:01:00Z"),
                    ev("e2", "u1", ts="2026-10-03T09:10:00Z",
                       ingest="2026-10-03T09:11:00Z"),
                    ev("e3", "u1", ts="2026-10-03T14:00:00Z",
                       ingest="2026-10-03T14:01:00Z"),
                    ev("e4", "u1", ts="2026-10-04T08:00:00Z",
                       ingest="2026-10-04T08:01:00Z"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result),
            [
                ("u1", "2026-10-03", 2, 3, 0, None),
                ("u1", "2026-10-04", 1, 1, 0, None),
            ],
        )

    def test_top_category_tie_goes_to_the_smaller_name(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", kind="purchase", price="500"),
                    ev("e2", "u1", ts="2026-10-03T10:05:00Z",
                       ingest="2026-10-03T10:06:00Z",
                       kind="purchase", product="p2", price="500"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 1, 2, 1000, "books")]
        )

    def test_category_totals_span_a_day_not_a_session(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", kind="purchase", price="300"),
                    ev("e2", "u1", ts="2026-10-03T12:00:00Z",
                       ingest="2026-10-03T12:01:00Z",
                       kind="purchase", product="p2", price="400"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result),
            [("u1", "2026-10-03", 2, 2, 700, "books")],
        )

    def test_revenue_counts_purchases_only(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1", price="999"),
                    ev("e2", "u1", ts="2026-10-03T10:05:00Z",
                       ingest="2026-10-03T10:06:00Z",
                       kind="purchase", price="150"),
                ]
            ]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 1, 2, 150, "shoes")]
        )


class LinkedIdsTest(unittest.TestCase):
    def test_linked_ids_share_sessions(self):
        result = run_stage(
            [
                [
                    ev("e1", "u2", ts="2026-10-03T10:00:00Z"),
                    ev("e2", "u1", ts="2026-10-03T10:20:00Z",
                       ingest="2026-10-03T10:21:00Z"),
                ]
            ],
            link_rows=["u1,u2"],
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 1, 2, 0, None)]
        )

    def test_session_gaps_are_measured_across_linked_ids(self):
        result = run_stage(
            [
                [
                    ev("e1", "u2", ts="2026-10-03T10:00:00Z"),
                    ev("e2", "u1", ts="2026-10-03T10:40:00Z",
                       ingest="2026-10-03T10:41:00Z"),
                ]
            ],
            link_rows=["u1,u2"],
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 2, 2, 0, None)]
        )

    def test_person_is_named_by_a_linked_id_with_no_events(self):
        result = run_stage(
            [[ev("e1", "u2")]], link_rows=["u1,u2"]
        )
        self.assertEqual(
            user_rows(result), [("u1", "2026-10-03", 1, 1, 0, None)]
        )

    def test_link_with_an_empty_side_is_dropped(self):
        result = run_stage(
            [
                [
                    ev("e1", "u1"),
                    ev("e2", "u2", ts="2026-10-03T10:05:00Z",
                       ingest="2026-10-03T10:06:00Z"),
                ]
            ],
            link_rows=["u1,"],
        )
        self.assertEqual(
            user_rows(result),
            [
                ("u1", "2026-10-03", 1, 1, 0, None),
                ("u2", "2026-10-03", 1, 1, 0, None),
            ],
        )


class ESeriesFixtureTest(unittest.TestCase):
    """E1-E20 compared against the hand-written expected user_daily tables."""

    def test_e_series_user_daily_matches_expected(self):
        for number in range(1, 21):
            case = f"E{number}"
            with self.subTest(case=case):
                ok, expected, actual = check_events.compare_case(FIXTURES / case)
                self.assertTrue(
                    ok, f"{case}: expected {expected} but got {actual}"
                )


if __name__ == "__main__":
    unittest.main()
