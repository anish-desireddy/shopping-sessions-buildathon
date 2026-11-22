"""Targeted regression cases for the opt-in fused click path.

Each case writes a tiny /data-shaped fixture, runs the Rust binary with
SHOPPING_FUSED_CLICKS=0 and =1 under the same W2/P4 configuration, and
requires identical user_daily, campaign_daily and click counter output.

The cases deliberately include copies in separate click files so the
range-order first-copy rule is exercised, not just the within-file rule.
"""
import csv
import os
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / "rust" / "shopping-pipeline" / "target" / "release" / "shopping-pipeline"
WINDOW_START = int(datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp() * 1000)
WINDOW_END = int(datetime(2026, 10, 15, tzinfo=timezone.utc).timestamp() * 1000)
EVENT_HEADER = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
CLICK_HEADER = "click_id,user_id,campaign_id,ts"
START_TS = "2026-10-03T09:00:00Z"
LATER_TS = "2026-10-03T09:30:00Z"
SESSION_TS = "2026-10-03T10:00:00Z"


def _rows(path, day_index):
    if not path.exists():
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as handle:
        for row in csv.reader(handle):
            if not row:
                continue
            row = list(row)
            if row[day_index].isdigit():
                day = int(row[day_index])
                row[day_index] = (
                    datetime(1970, 1, 1) + timedelta(days=day)
                ).date().isoformat()
            rows.append(tuple(row))
    return sorted(rows)


def _attr_line(stderr):
    for line in stderr.splitlines():
        if line.startswith("[rust-attribution]"):
            return line
    raise AssertionError(f"no attribution counters in:\n{stderr}")


def _run(data, out, fused):
    out.mkdir(parents=True, exist_ok=True)
    scratch = out / "scratch"
    scratch.mkdir()
    env = dict(
        os.environ,
        RUST_WORKERS="2",
        SHOPPING_CLICK_WORKERS="2",
        SHOPPING_PARALLEL_CLICKS="1",
        SHOPPING_INMEMORY_PARTS="1",
        SHOPPING_PARALLEL_SESSIONIZE="1",
        SHOPPING_SESSION_WORKERS="2",
        SHOPPING_COMPACT_EVENTS="1",
        SHOPPING_FUSED_CLICKS="1" if fused else "0",
    )
    user_csv = out / "user_daily.csv"
    campaign_csv = out / "campaign_daily.csv"
    command = [
        str(BINARY),
        "--links", str(data / "links"),
        "--products", str(data / "products.csv"),
        "--events", str(data / "events"),
        "--parts-out", str(scratch / "parts"),
        "--clicks", str(data / "clicks"),
        "--campaigns", str(data / "campaigns.csv"),
        "--start-ms", str(WINDOW_START),
        "--end-ms", str(WINDOW_END),
        "--partitions", "4",
        "--workers", "2",
        "--user-daily-csv", str(user_csv),
        "--campaigns-csv", str(campaign_csv),
    ]
    completed = subprocess.run(command, env=env, capture_output=True, text=True)
    if completed.returncode != 0:
        raise AssertionError(completed.stderr[-4000:])
    return _rows(user_csv, 1), _rows(campaign_csv, 2), _attr_line(completed.stderr)


def _build(tmp, click_files):
    data = Path(tmp) / "data"
    for sub in ("events", "clicks", "links"):
        (data / sub).mkdir(parents=True)
    (data / "events" / "part-00000.csv").write_text(
        EVENT_HEADER
        + "\n"
        + f"e1,u1,{SESSION_TS},2026-10-03T10:01:00Z,view,p1,\n",
        encoding="utf-8",
    )
    for index, rows in enumerate(click_files):
        (data / "clicks" / f"part-{index:05d}.csv").write_text(
            CLICK_HEADER + "\n" + "".join(row + "\n" for row in rows),
            encoding="utf-8",
        )
    (data / "links" / "part-00000.csv").write_text(
        "user_id_a,user_id_b\n", encoding="utf-8"
    )
    (data / "products.csv").write_text(
        "product_id,category\np1,shoes\n", encoding="utf-8"
    )
    (data / "campaigns.csv").write_text(
        "campaign_id,channel\nc_1,search\nc_2,social\n", encoding="utf-8"
    )
    (data / "manifest.json").write_text(
        '{"window_start":"2026-10-01T00:00:00Z",'
        '"window_end":"2026-10-15T00:00:00Z"}',
        encoding="utf-8",
    )
    return data


def _click(click_id, campaign, ts=START_TS, user="u1"):
    return f"{click_id},{user},{campaign},{ts}"


@unittest.skipUnless(BINARY.is_file(), "Rust release binary is not built")
class ClickFusionRegressionTest(unittest.TestCase):
    def _compare(self, click_files):
        with tempfile.TemporaryDirectory() as tmp:
            data = _build(tmp, click_files)
            old = _run(data, Path(tmp) / "legacy", False)
            fused = _run(data, Path(tmp) / "fused", True)
        self.assertEqual(old, fused)
        return old

    def test_duplicate_within_one_worker_range(self):
        rows = self._compare([[
            _click("k1", "c_1"),
            _click("k1", "c_1"),
        ]])
        self.assertIn(("c_1", "search", "2026-10-03", "1", "0", "0", "1"), rows[1])

    def test_duplicate_across_worker_ranges(self):
        rows = self._compare([
            [_click("k1", "c_1")],
            [_click("k1", "c_1")],
        ])
        self.assertIn(("c_1", "search", "2026-10-03", "1", "0", "0", "1"), rows[1])

    def test_first_copy_invalid_then_later_valid(self):
        rows = self._compare([
            [_click("k1", "c_999")],
            [_click("k1", "c_1")],
        ])
        self.assertEqual(
            rows[1],
            [("organic", "organic", "2026-10-03", "1", "0", "0", "0")],
        )

    def test_first_copy_valid_then_later_invalid(self):
        rows = self._compare([
            [_click("k1", "c_1")],
            [_click("k1", "c_999")],
        ])
        self.assertIn(("c_1", "search", "2026-10-03", "1", "0", "0", "1"), rows[1])

    def test_differing_duplicate_copies_use_the_first(self):
        rows = self._compare([
            [_click("k1", "c_1")],
            [_click("k1", "c_2")],
        ])
        self.assertIn(("c_1", "search", "2026-10-03", "1", "0", "0", "1"), rows[1])
        self.assertNotIn("c_2", [row[0] for row in rows[1]])

    def test_empty_click_id_is_dropped(self):
        rows = self._compare([[_click("", "c_1")]])
        self.assertEqual(
            rows[1],
            [("organic", "organic", "2026-10-03", "1", "0", "0", "0")],
        )

    def test_malformed_click_record_is_dropped(self):
        rows = self._compare([["k1,u1,c_1"]])
        self.assertEqual(
            rows[1],
            [("organic", "organic", "2026-10-03", "1", "0", "0", "0")],
        )

    def test_multiple_campaigns_for_one_person(self):
        rows = self._compare([[
            _click("k1", "c_1"),
            _click("k2", "c_2", LATER_TS),
        ]])
        self.assertIn(("c_2", "social", "2026-10-03", "1", "0", "0", "1"), rows[1])
        self.assertIn(("c_1", "search", "2026-10-03", "0", "0", "0", "1"), rows[1])

    def test_organic_fallback_when_no_clicks(self):
        rows = self._compare([[]])
        self.assertEqual(
            rows[1],
            [("organic", "organic", "2026-10-03", "1", "0", "0", "0")],
        )
        self.assertIn(("u1", "2026-10-03", "1", "1", "0", ""), rows[0])

    def test_multiple_clicks_for_one_person(self):
        rows = self._compare([[
            _click("k1", "c_1"),
            _click("k2", "c_1", LATER_TS),
            _click("k3", "c_2", "2026-10-03T09:40:00Z"),
        ]])
        self.assertIn(("c_2", "social", "2026-10-03", "1", "0", "0", "1"), rows[1])
        self.assertIn(("c_1", "search", "2026-10-03", "0", "0", "0", "1"), rows[1])


if __name__ == "__main__":
    unittest.main()
