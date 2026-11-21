#!/usr/bin/env python3
"""Targeted fixture runner for the event stage (Phase 2).

The published harness (``scripts/run_fixtures.py``) compares *both* output
tables for every fixture, so it cannot pass until click attribution (Phase 3)
and campaign aggregation (Phase 4) exist. This script runs the event stage alone
and compares only ``user_daily``, which depends solely on events, links and
products -- exactly the surface Phase 2 implements.

Usage:
  python3 scripts/check_events.py                # every E-series case
  python3 scripts/check_events.py E3 E16         # selected cases
  python3 scripts/check_events.py --all          # every fixture directory
  python3 scripts/check_events.py --timing E16   # with stage timings on stderr
"""
import csv
import sys
from pathlib import Path
from typing import List, Optional, Tuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "pipeline"))

import events  # noqa: E402
import user_daily  # noqa: E402

Row = Tuple[str, str, int, int, int, Optional[str]]


def _key(row: Row) -> Tuple[str, str]:
    return (row[0], row[1])


def expected_rows(path: Path) -> List[Row]:
    """Read an expected user_daily CSV into comparable tuples."""
    rows: List[Row] = []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            return rows
        for row in reader:
            if not row:
                continue
            top = row[5] if len(row) > 5 else ""
            rows.append(
                (
                    row[0],
                    row[1],
                    int(row[2]),
                    int(row[3]),
                    int(row[4]),
                    top if top != "" else None,
                )
            )
    rows.sort(key=_key)
    return rows


def actual_rows(result: events.EventResult) -> List[Row]:
    rows: List[Row] = [
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
    rows.sort(key=_key)
    return rows


def compare_case(
    case_dir: Path, log: bool = False
) -> Tuple[bool, List[Row], List[Row]]:
    """Run the event stage on one fixture and diff user_daily against expected."""
    result = events.run_event_stage(Path(case_dir) / "input", log=log)
    expected = expected_rows(Path(case_dir) / "expected" / "user_daily.csv")
    actual = actual_rows(result)
    return expected == actual, expected, actual


def main(argv: List[str]) -> int:
    fixtures = HERE.parent / "fixtures"
    log = False
    every_case = False
    cases: List[str] = []
    index = 0
    while index < len(argv):
        argument = argv[index]
        if argument == "--timing":
            log = True
        elif argument == "--all":
            every_case = True
        elif argument == "--fixtures":
            index += 1
            if index >= len(argv):
                print("--fixtures needs a directory", file=sys.stderr)
                return 2
            fixtures = Path(argv[index])
        elif argument.startswith("--"):
            print(f"unknown option {argument}", file=sys.stderr)
            return 2
        else:
            cases.append(argument)
        index += 1
    if not fixtures.is_dir():
        print(f"no fixtures directory: {fixtures}", file=sys.stderr)
        return 2
    if not cases:
        cases = sorted(
            path.name
            for path in fixtures.iterdir()
            if path.is_dir() and (every_case or path.name.startswith("E"))
        )

    failed = []
    for case in cases:
        ok, expected, actual = compare_case(fixtures / case, log=log)
        if ok:
            print(f"ok   {case}")
            continue
        failed.append(case)
        print(f"FAIL {case}")
        for row in expected:
            if row not in actual:
                print(f"  missing {row}")
        for row in actual:
            if row not in expected:
                print(f"  extra   {row}")
    print(f"\n{len(cases) - len(failed)}/{len(cases)} fixtures matched on user_daily")
    if failed:
        print("failed: " + " ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
