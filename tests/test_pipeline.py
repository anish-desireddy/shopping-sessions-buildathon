"""End-to-end pipeline test: fixture in, Parquet out, exact tables compared.

Skipped when DuckDB is not installed locally; the container always has it.
"""
import csv
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))

try:
    import duckdb
except ImportError:  # pragma: no cover - local convenience only
    duckdb = None

TABLES = ("user_daily", "campaign_daily")


def _expected(table, case):
    path = ROOT / "fixtures" / case / "expected" / f"{table}.csv"
    rows = []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        next(reader)
        for row in reader:
            if not row:
                continue
            if table == "user_daily":
                rows.append(
                    (row[0], row[1], int(row[2]), int(row[3]), int(row[4]),
                     row[5] or None)
                )
            else:
                rows.append(
                    (row[0], row[1], row[2], int(row[3]), int(row[4]),
                     int(row[5]), int(row[6]))
                )
    return sorted(rows)


def _actual(out_dir, table):
    day_index = 1 if table == "user_daily" else 2
    path = Path(out_dir) / table / "part-0.parquet"
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT * FROM read_parquet('{path}')"
        ).fetchall()
        schema = {
            name: kind
            for name, kind, *_ in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path}')"
            ).fetchall()
        }
    finally:
        con.close()
    normalized = []
    for row in rows:
        row = list(row)
        row[day_index] = row[day_index].isoformat()  # DATE -> YYYY-MM-DD
        normalized.append(tuple(row))
    return sorted(normalized), schema


@unittest.skipUnless(duckdb is not None, "duckdb is not installed")
class PipelineEndToEndTest(unittest.TestCase):
    def run_case(self, case):
        import pipeline

        data = ROOT / "fixtures" / case / "input"
        with tempfile.TemporaryDirectory() as out, tempfile.TemporaryDirectory() as scratch:
            saved = {
                key: os.environ.get(key)
                for key in ("DATA_DIR", "OUTPUT_DIR", "SCRATCH_DIR", "NCPU", "MEM_GB")
            }
            os.environ.update(
                DATA_DIR=str(data),
                OUTPUT_DIR=out,
                SCRATCH_DIR=scratch,
                NCPU="2",
                MEM_GB="2",
            )
            try:
                pipeline.main()
            finally:
                for key, value in saved.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
            for table in TABLES:
                self.assertTrue(
                    (Path(out) / table / "part-0.parquet").is_file(),
                    f"{table} parquet missing for {case}",
                )
                actual, schema = _actual(out, table)
                self.assertEqual(
                    actual,
                    _expected(table, case),
                    f"{case} {table} mismatch",
                )
                self.assertEqual(schema["day"], "DATE")
                expected_columns = (
                    pipeline.USER_COLUMNS
                    if table == "user_daily"
                    else pipeline.CAMPAIGN_COLUMNS
                )
                self.assertEqual(
                    list(schema), [name for name, _ in expected_columns]
                )

    def test_a11_full_fixture(self):
        self.run_case("A11")

    def test_e5_empty_user_daily_is_valid_parquet(self):
        self.run_case("E5")

    def test_partitioned_event_dedup_matches_python_path(self):
        import events

        data = ROOT / "fixtures" / "A11" / "input"
        python_result = events.run_event_stage(data)
        with tempfile.TemporaryDirectory() as scratch:
            partitioned = events.run_event_stage(data, scratch=scratch)

        def sessions(result):
            return [
                (
                    session.person,
                    session.start_ms,
                    session.day,
                    session.events,
                    session.revenue_cents,
                    session.purchases,
                    session.categories,
                )
                for session in result.sessions
            ]

        self.assertEqual(
            sorted(sessions(partitioned)), sorted(sessions(python_result))
        )
        self.assertEqual(
            partitioned.stats.summary(), python_result.stats.summary()
        )


if __name__ == "__main__":
    unittest.main()
