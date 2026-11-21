"""Tests for pipeline/clicks.py: dedup, validation, credit and assists.

Fixture expectations come from fixtures/*/expected and are never rewritten.
"""
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT / "scripts"))

import check_attribution  # noqa: E402
import clicks  # noqa: E402
import events  # noqa: E402
import user_daily  # noqa: E402

FIXTURES = ROOT / "fixtures"
EVENT_HEADER = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
CLICK_HEADER = "click_id,user_id,campaign_id,ts"
MANIFEST = '{"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}'

START = "2026-10-03T10:00:00Z"
START_MS = 1791021600000
INGEST = "2026-10-03T10:01:00Z"
SEVEN_DAYS_BEFORE = "2026-09-26T10:00:00Z"
SEVEN_DAYS_BEFORE_MS = 1790416800000
SEVEN_DAYS_PLUS_1 = 1790416799999
START_PLUS_1 = 1791021600001


def ev(event_id, user, ts=START, ingest=INGEST, kind="view", product="p1", price=""):
    return f"{event_id},{user},{ts},{ingest},{kind},{product},{price}"


def clk(click_id, user, campaign, ts):
    return f"{click_id},{user},{campaign},{ts}"


def run_pipeline(
    event_rows,
    click_rows=(),
    link_rows=(),
    click_files=None,
    products=("p1,shoes", "p2,books"),
    campaigns=("c_1,search", "c_2,social"),
):
    """Build a /data-shaped directory and run the event + attribution stages."""
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp)
        for sub in ("events", "clicks", "links"):
            (data / sub).mkdir()
        (data / "events" / "part-00000.csv").write_text(
            EVENT_HEADER + "\n" + "".join(row + "\n" for row in event_rows),
            encoding="utf-8",
        )
        files = [list(click_rows)] if click_files is None else click_files
        for index, rows in enumerate(files):
            (data / "clicks" / f"part-{index:05d}.csv").write_text(
                CLICK_HEADER + "\n" + "".join(row + "\n" for row in rows),
                encoding="utf-8",
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
            "campaign_id,channel\n" + "".join(row + "\n" for row in campaigns),
            encoding="utf-8",
        )
        (data / "manifest.json").write_text(MANIFEST, encoding="utf-8")
        event_result = events.run_event_stage(data)
        return clicks.run_attribution_stage(event_result, data)


def credit_rows(result):
    return sorted(
        (
            attribution.session.person,
            user_daily.iso_day(attribution.session.day),
            attribution.campaign_id,
            attribution.channel,
        )
        for attribution in result.attributions
    )


def assists_of(result, person, start_ms=START_MS):
    for attribution in result.attributions:
        if attribution.session.person == person and attribution.session.start_ms == start_ms:
            return attribution.assists
    raise AssertionError(f"no session for {person} at {start_ms}")


class ClickDedupTest(unittest.TestCase):
    def test_duplicate_click_is_kept_once(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", "2026-10-03T09:00:00Z"),
             clk("k1", "u1", "c_1", "2026-10-03T09:00:00Z")],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "c_1", "search")]
        )
        self.assertEqual(result.stats.duplicate_copies, 1)
        self.assertEqual(result.stats.unique_clicks, 1)
        self.assertEqual(result.stats.clicks_kept, 1)

    def test_dedup_is_global_across_files(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            click_files=[
                [clk("k1", "u1", "c_1", "2026-10-03T09:00:00Z")],
                [clk("k1", "u1", "c_1", "2026-10-03T09:00:00Z")],
            ],
        )
        self.assertEqual(result.stats.unique_clicks, 1)
        self.assertEqual(result.stats.clicks_kept, 1)

    def test_dedup_keeps_the_first_copy_even_when_it_is_invalid(self):
        # Out of contract (copies are identical by the rules), so this pins our
        # deterministic choice: the first copy is the one that stands or falls.
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_999", "2026-10-03T09:00:00Z"),
             clk("k1", "u1", "c_1", "2026-10-03T09:00:00Z")],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "organic", "organic")]
        )
        self.assertEqual(result.stats.unknown_campaign, 1)
        self.assertEqual(result.stats.clicks_kept, 0)

    def test_empty_click_id_is_dropped(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("", "u1", "c_1", "2026-10-03T09:00:00Z")],
        )
        self.assertEqual(result.stats.empty_click_id, 1)
        self.assertEqual(result.stats.unique_clicks, 0)
        self.assertEqual(assists_of(result, "u1"), ())


class ClickValidationTest(unittest.TestCase):
    def test_empty_user_id_is_dropped(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "", "c_1", "2026-10-03T09:00:00Z")],
        )
        self.assertEqual(result.stats.invalid_user, 1)
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "organic", "organic")]
        )

    def test_unknown_campaign_is_dropped(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_999", "2026-10-03T09:00:00Z")],
        )
        self.assertEqual(result.stats.unknown_campaign, 1)
        self.assertEqual(assists_of(result, "u1"), ())

    def test_invalid_timestamp_is_dropped(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", "2026-10-03 09:00:00")],
        )
        self.assertEqual(result.stats.invalid_ts, 1)
        self.assertEqual(result.stats.clicks_kept, 0)

    def test_click_window_start_is_inclusive(self):
        # The window starts exactly seven days before window_start.
        session_at_window_start = "2026-10-01T00:00:00Z"
        result = run_pipeline(
            [ev("e1", "u1", ts=session_at_window_start,
                ingest="2026-10-01T01:00:00Z")],
            [clk("k1", "u1", "c_1", "2026-09-24T00:00:00Z")],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-01", "c_1", "search")]
        )

    def test_click_1ms_before_the_click_window_is_dropped(self):
        result = run_pipeline(
            [ev("e1", "u1", ts="2026-10-01T00:00:00Z",
                ingest="2026-10-01T01:00:00Z")],
            [clk("k1", "u1", "c_1", "1790207999999")],
        )
        self.assertEqual(result.stats.out_of_window, 1)
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-01", "organic", "organic")]
        )

    def test_click_at_window_end_is_dropped(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", "2026-10-15T00:00:00Z")],
        )
        self.assertEqual(result.stats.out_of_window, 1)
        self.assertEqual(result.stats.clicks_kept, 0)


class CreditTest(unittest.TestCase):
    def test_latest_click_wins(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", "2026-10-02T10:00:00Z"),
             clk("k2", "u1", "c_2", "2026-10-03T08:00:00Z")],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "c_2", "social")]
        )
        self.assertEqual(assists_of(result, "u1"), ("c_1", "c_2"))

    def test_tie_goes_to_the_smallest_click_id(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k2", "u1", "c_2", "2026-10-03T09:00:00Z"),
             clk("k1", "u1", "c_1", "2026-10-03T09:00:00Z")],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "c_1", "search")]
        )

    def test_click_at_session_start_counts(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", START)],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "c_1", "search")]
        )

    def test_click_1ms_after_session_start_does_not_count(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", str(START_PLUS_1))],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "organic", "organic")]
        )
        self.assertEqual(assists_of(result, "u1"), ())

    def test_click_exactly_seven_days_before_counts(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", SEVEN_DAYS_BEFORE)],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "c_1", "search")]
        )
        self.assertEqual(assists_of(result, "u1"), ("c_1",))

    def test_click_seven_days_plus_1ms_before_does_not_count(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", str(SEVEN_DAYS_PLUS_1))],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "organic", "organic")]
        )
        self.assertEqual(assists_of(result, "u1"), ())
        # The click itself is still valid: it is inside the click window.
        self.assertEqual(result.stats.clicks_kept, 1)

    def test_one_click_credits_many_sessions(self):
        result = run_pipeline(
            [
                ev("e1", "u1"),
                ev("e2", "u1", ts="2026-10-04T08:00:00Z",
                   ingest="2026-10-04T08:01:00Z"),
            ],
            [clk("k1", "u1", "c_1", "2026-10-03T09:00:00Z")],
        )
        self.assertEqual(
            credit_rows(result),
            [
                ("u1", "2026-10-03", "c_1", "search"),
                ("u1", "2026-10-04", "c_1", "search"),
            ],
        )

    def test_click_after_a_session_start_credits_only_later_sessions(self):
        result = run_pipeline(
            [
                ev("e1", "u1"),
                ev("e2", "u1", ts="2026-10-03T10:25:00Z",
                   ingest="2026-10-03T10:26:00Z"),
                ev("e3", "u1", ts="2026-10-03T11:30:00Z",
                   ingest="2026-10-03T11:31:00Z"),
            ],
            [clk("k1", "u1", "c_1", "2026-10-03T10:20:00Z")],
        )
        self.assertEqual(
            credit_rows(result),
            [
                ("u1", "2026-10-03", "c_1", "search"),
                ("u1", "2026-10-03", "organic", "organic"),
            ],
        )


class AssistTest(unittest.TestCase):
    def test_every_qualifying_campaign_assists_once(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [
                clk("k1", "u1", "c_1", SEVEN_DAYS_BEFORE),
                clk("k2", "u1", "c_2", "2026-10-03T09:00:00Z"),
                clk("k3", "u1", "c_2", "2026-10-03T09:30:00Z"),
                clk("k4", "u1", "c_1", str(START_PLUS_1)),
            ],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "c_2", "social")]
        )
        self.assertEqual(assists_of(result, "u1"), ("c_1", "c_2"))

    def test_duplicate_clicks_from_one_campaign_assist_once(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [
                clk("k2", "u1", "c_2", "2026-10-03T09:00:00Z"),
                clk("k3", "u1", "c_2", "2026-10-03T09:30:00Z"),
            ],
        )
        self.assertEqual(assists_of(result, "u1"), ("c_2",))

    def test_credited_campaign_is_included_in_assists(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", "2026-10-03T09:00:00Z")],
        )
        self.assertIn("c_1", assists_of(result, "u1"))

    def test_no_clicks_means_organic_and_no_assists(self):
        result = run_pipeline([ev("e1", "u1")])
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "organic", "organic")]
        )
        self.assertEqual(assists_of(result, "u1"), ())

    def test_campaign_can_assist_without_credit(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", "2026-10-02T10:00:00Z")],
        )
        self.assertIn("c_1", assists_of(result, "u1"))

    def test_click_just_outside_the_assist_window_does_not_assist(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u1", "c_1", str(SEVEN_DAYS_PLUS_1))],
        )
        self.assertEqual(assists_of(result, "u1"), ())

    def test_linked_id_click_credits_and_assists(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [clk("k1", "u2", "c_1", "2026-10-03T09:00:00Z")],
            link_rows=["u1,u2"],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "c_1", "search")]
        )
        self.assertEqual(assists_of(result, "u1"), ("c_1",))

    def test_latest_click_across_linked_ids_wins(self):
        result = run_pipeline(
            [ev("e1", "u1")],
            [
                clk("k1", "u2", "c_2", "2026-10-03T09:00:00Z"),
                clk("k2", "u1", "c_1", "2026-10-03T08:00:00Z"),
            ],
            link_rows=["u1,u2"],
        )
        self.assertEqual(
            credit_rows(result), [("u1", "2026-10-03", "c_2", "social")]
        )
        self.assertEqual(assists_of(result, "u1"), ("c_1", "c_2"))


class AttributionInvariantTest(unittest.TestCase):
    def test_every_credited_non_organic_session_is_assisted_by_its_campaign(self):
        for number in range(1, 13):
            case = f"A{number}"
            with self.subTest(case=case):
                result = _load_fixture(case)
                for attribution in result.attributions:
                    if attribution.campaign_id == clicks.ORGANIC:
                        continue
                    self.assertIn(attribution.campaign_id, attribution.assists)


def _load_fixture(case):
    data = FIXTURES / case / "input"
    event_result = events.run_event_stage(data)
    return clicks.run_attribution_stage(event_result, data)


class ASeriesFixtureTest(unittest.TestCase):
    """A1-A12 compared on campaign/day credit and assist counts."""

    def test_a_series_attribution_matches_expected(self):
        for number in range(1, 13):
            case = f"A{number}"
            with self.subTest(case=case):
                ok, expected, actual = check_attribution.compare_case(
                    FIXTURES / case
                )
                self.assertTrue(
                    ok, f"{case}: expected {expected} but got {actual}"
                )


class StreamAttributionTest(unittest.TestCase):
    def test_stream_matches_materialized_attribution(self):
        data = FIXTURES / "A11" / "input"
        event_result = events.run_event_stage(data)
        materialized = clicks.run_attribution_stage(event_result, data)

        collected = []
        stream = clicks.run_attribution_stream(
            event_result, data, collected.append
        )

        def key(attribution):
            return (
                attribution.session.person,
                attribution.session.start_ms,
                attribution.campaign_id,
                attribution.channel,
                attribution.assists,
            )

        self.assertEqual(
            sorted(map(key, materialized.attributions)),
            sorted(map(key, collected)),
        )
        for name in (
            "sessions_credited",
            "sessions_organic",
            "assist_links",
            "max_assists",
            "clicks_kept",
        ):
            self.assertEqual(
                getattr(stream.stats, name),
                getattr(materialized.stats, name),
                name,
            )


class OrganicInvariantTest(unittest.TestCase):
    def test_organic_sessions_have_no_assists(self):
        for number in range(1, 13):
            case = f"A{number}"
            with self.subTest(case=case):
                for attribution in _load_fixture(case).attributions:
                    if attribution.campaign_id == clicks.ORGANIC:
                        self.assertEqual(attribution.assists, ())


if __name__ == "__main__":
    unittest.main()
