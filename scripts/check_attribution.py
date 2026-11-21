#!/usr/bin/env python3
"""Targeted fixture runner for the click/attribution stage (Phase 3).

The published harness compares both output tables, so it cannot pass until the
campaign_daily aggregation exists (Phase 4). This script runs events and
attribution and compares the columns attribution alone determines:

    campaign_id, channel, day, sessions (credited), assisted_sessions

``purchasing_sessions`` and ``revenue_cents`` come from sessions during Phase 4
aggregation and are deliberately not checked here. The grouping below exists
only in this checker; no production campaign_daily aggregation is implemented.

Usage:
  python3 scripts/check_attribution.py             # every A-series case
  python3 scripts/check_attribution.py A4 A10      # selected cases
  python3 scripts/check_attribution.py --all       # every fixture directory
  python3 scripts/check_attribution.py --timing A11
"""
import csv
import sys
from pathlib import Path
from typing import Dict, List, Tuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "pipeline"))

import clicks  # noqa: E402
import events  # noqa: E402
import user_daily  # noqa: E402

Row = Tuple[str, str, str, int, int]


def _key(row: Row) -> Tuple[str, str]:
    return (row[0], row[2])


def expected_rows(path: Path) -> List[Row]:
    """The attribution-determined columns of an expected campaign_daily CSV."""
    rows: List[Row] = []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            return rows
        for row in reader:
            if not row:
                continue
            rows.append((row[0], row[1], row[2], int(row[3]), int(row[6])))
    rows.sort(key=_key)
    return rows


def attribution_rows(result: clicks.AttributionResult) -> List[Row]:
    credited: Dict[Tuple[str, int], int] = {}
    assisted: Dict[Tuple[str, int], int] = {}
    for attribution in result.attributions:
        day = attribution.session.day
        key = (attribution.campaign_id, day)
        credited[key] = credited.get(key, 0) + 1
        for campaign in attribution.assists:
            assist_key = (campaign, day)
            assisted[assist_key] = assisted.get(assist_key, 0) + 1

    rows: List[Row] = []
    for campaign, day in set(credited) | set(assisted):
        channel = (
            clicks.ORGANIC
            if campaign == clicks.ORGANIC
            else result.campaign_channels[campaign]
        )
        rows.append(
            (
                campaign,
                channel,
                user_daily.iso_day(day),
                credited.get((campaign, day), 0),
                assisted.get((campaign, day), 0),
            )
        )
    rows.sort(key=_key)
    return rows


def compare_case(
    case_dir: Path, log: bool = False
) -> Tuple[bool, List[Row], List[Row]]:
    """Run event + attribution stages on one fixture and diff the two columns."""
    data = Path(case_dir) / "input"
    event_result = events.run_event_stage(data, log=log)
    result = clicks.run_attribution_stage(event_result, data, log=log)
    expected = expected_rows(Path(case_dir) / "expected" / "campaign_daily.csv")
    actual = attribution_rows(result)
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
            if path.is_dir() and (every_case or path.name.startswith("A"))
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
    print(f"\n{len(cases) - len(failed)}/{len(cases)} fixtures matched on attribution")
    if failed:
        print("failed: " + " ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
