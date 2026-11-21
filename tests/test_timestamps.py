"""Unit tests for pipeline/timestamps.py against rules.md, "Timestamps".

``utc_ms`` below is an independent implementation (datetime), used to
cross-check the arithmetic in the module.
"""
import random
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))

import csvio  # noqa: E402
import timestamps  # noqa: E402

FIXTURES = ROOT / "fixtures"


def utc_ms(year, month, day, hour=0, minute=0, second=0):
    moment = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    return int(moment.timestamp() * 1000)


def fixture_events(case):
    rows = []
    for path in sorted((FIXTURES / case / "input" / "events").glob("*.csv")):
        rows.extend(csvio.iter_rows(path, csvio.EVENT_FIELDS))
    return rows


class ValidFormsTest(unittest.TestCase):
    def test_f1(self):
        self.assertEqual(
            timestamps.parse_timestamp("2026-10-03T23:58:10Z"),
            utc_ms(2026, 10, 3, 23, 58, 10),
        )

    def test_f2_equals_f1(self):
        self.assertEqual(
            timestamps.parse_timestamp("2026-10-03T19:58:10-04:00"),
            timestamps.parse_timestamp("2026-10-03T23:58:10Z"),
        )

    def test_f2_positive_offset(self):
        self.assertEqual(
            timestamps.parse_timestamp("2026-10-04T01:30:00+02:00"),
            utc_ms(2026, 10, 3, 23, 30, 0),
        )

    def test_f2_negative_offset_changes_the_date_backwards(self):
        self.assertEqual(
            timestamps.parse_timestamp("2026-10-03T00:30:00-05:00"),
            utc_ms(2026, 10, 3, 5, 30, 0),
        )

    def test_f2_minus_zero_is_utc(self):
        self.assertEqual(
            timestamps.parse_timestamp("2026-10-03T12:00:00-00:00"),
            utc_ms(2026, 10, 3, 12, 0, 0),
        )
        self.assertEqual(
            timestamps.parse_timestamp("2026-10-03T12:00:00+00:00"),
            utc_ms(2026, 10, 3, 12, 0, 0),
        )

    def test_offset_range_edges(self):
        self.assertEqual(
            timestamps.parse_timestamp("2026-10-04T09:59:00+23:59"),
            utc_ms(2026, 10, 3, 10, 0, 0),
        )

    def test_f3(self):
        self.assertEqual(timestamps.parse_timestamp("1791071890000"), 1791071890000)
        self.assertEqual(timestamps.parse_timestamp("0"), 0)
        self.assertEqual(timestamps.parse_timestamp("86400000"), 86400000)

    def test_f3_leading_zeros(self):
        self.assertEqual(
            timestamps.parse_timestamp("0001791071890000"), 1791071890000
        )

    def test_f3_too_long_is_a_number_not_malformed(self):
        digits = "179102160000000000000"
        self.assertEqual(timestamps.parse_timestamp(digits), int(digits))

    def test_f3_of_length_20_is_not_parsed_as_a_date(self):
        digits = "1" * 20
        self.assertEqual(timestamps.parse_timestamp(digits), int(digits))

    def test_leap_days(self):
        self.assertEqual(
            timestamps.parse_timestamp("2028-02-29T00:00:00Z"),
            utc_ms(2028, 2, 29),
        )
        self.assertEqual(
            timestamps.parse_timestamp("2000-02-29T00:00:00Z"),
            utc_ms(2000, 2, 29),
        )
        self.assertIsNone(timestamps.parse_timestamp("2027-02-29T00:00:00Z"))
        self.assertIsNone(timestamps.parse_timestamp("1900-02-29T00:00:00Z"))

    def test_year_zero(self):
        # Year 0 is leap (divisible by 400); it is 366 days before 0001-01-01,
        # which is -719162 days from the epoch.
        self.assertEqual(
            timestamps.parse_timestamp("0000-01-01T00:00:00Z"),
            (-719162 - 366) * 86400 * 1000,
        )

    def test_year_nine_nine_nine_nine(self):
        self.assertEqual(
            timestamps.parse_timestamp("9999-12-31T23:59:59Z"),
            utc_ms(9999, 12, 31, 23, 59, 59),
        )


class InvalidFormsTest(unittest.TestCase):
    CASES = [
        "",
        "2026-10-03 10:00:00Z",  # space instead of T
        "2026-10-03t10:00:00z",  # lowercase
        "2026-10-03T10:00:00",  # no zone
        "2026-10-03T10:00:00.500Z",  # fractional seconds
        "2026-10-03T10:00:00.000Z",
        "2026-10-03T10:00:00+0200",  # offset without colon
        "2026-10-03T10:00:00+24:00",  # offset hour out of range
        "2026-10-03T10:00:00+05:60",  # offset minute out of range
        "2026-10-03T10:00:00+2:00",  # short offset
        "2026-10-03T10:00:00Z\r",  # CR from a CRLF line
        "2026-10-03T24:00:00Z",  # hour 24
        "2026-10-03T10:00:60Z",  # second 60
        "2026-10-03T10:60:00Z",
        "2026-13-03T10:00:00Z",  # month 13
        "2026-00-03T10:00:00Z",
        "2026-10-00T10:00:00Z",
        "2026-10-32T10:00:00Z",
        "2026-02-30T10:00:00Z",
        "2026-04-31T10:00:00Z",
        "1791071890000.0",  # non-digit in F3
        "1.79e12",
        "-5",
        "+5",
        " 1791071890000",
        "1791071890000 ",
        "1791071890000Z",
        "\u0661\u0667\u0669\u0661\u0660\u0667\u0661\u0668\u0669\u0660\u0660\u0660\u0660",  # Arabic-Indic
        "\uff12026-10-03T10:00:00Z",  # full-width digit in the year
        "\u00b2",  # superscript two: isdigit() but not ASCII
        "2026-1-03T10:00:00Z",
        "2026-10-3T10:00:00Z",
        "02026-10-03T10:00:00Z",
        "2026-10-03T10:00:00ZZ",
        "2026-10-03T10:00:00z",
        "2026-10-03T10:00:00+",
    ]

    def test_invalid_forms(self):
        for value in self.CASES:
            with self.subTest(value=value):
                self.assertIsNone(timestamps.parse_timestamp(value))


class CivilDaysTest(unittest.TestCase):
    def test_known_days(self):
        self.assertEqual(timestamps.civil_from_days(0), (1970, 1, 1))
        self.assertEqual(timestamps.civil_from_days(-1), (1969, 12, 31))
        self.assertEqual(timestamps.civil_from_days(20729), (2026, 10, 3))
        self.assertEqual(timestamps.civil_from_days(-719528), (0, 1, 1))

    def test_round_trip_with_the_parser(self):
        # Every four-digit year in nine-day steps; midnight is 0 ms into the day.
        for day in range(-719528, 2932897, 9):
            year, month, date = timestamps.civil_from_days(day)
            text = f"{year:04d}-{month:02d}-{date:02d}T00:00:00Z"
            self.assertEqual(timestamps.parse_timestamp(text), day * 86400000)

    def test_very_long_f3_is_converted_exactly(self):
        # Python 3.11+ caps int(str) at 4300 digits; the parser must not crash.
        expected = (
            timestamps.parse_timestamp("1" * 4000) * 10**1000
            + int("1" * 1000)
        )
        self.assertEqual(timestamps.parse_timestamp("1" * 5000), expected)


class AgainstDatetimeTest(unittest.TestCase):
    def test_f1_round_trip(self):
        rng = random.Random(20261003)
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        for _ in range(2000):
            moment = start + timedelta(seconds=rng.randrange(0, 4 * 365 * 86400))
            text = moment.strftime("%Y-%m-%dT%H:%M:%SZ")
            with self.subTest(text=text):
                self.assertEqual(
                    timestamps.parse_timestamp(text), int(moment.timestamp() * 1000)
                )

    def test_f2_round_trip(self):
        rng = random.Random(7)
        base = datetime(2026, 1, 15, 6, 30, 15, tzinfo=timezone.utc)
        for _ in range(2000):
            hours = rng.randrange(0, 24)
            minutes = rng.randrange(0, 60)
            sign = rng.choice("+-")
            offset = timedelta(hours=hours, minutes=minutes)
            if sign == "-":
                offset = -offset
            local = (base + offset).replace(tzinfo=None)
            text = local.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{hours:02d}:{minutes:02d}"
            with self.subTest(text=text):
                self.assertEqual(
                    timestamps.parse_timestamp(text),
                    int(base.timestamp() * 1000),
                )

    def test_f3_round_trip(self):
        rng = random.Random(99)
        for _ in range(2000):
            value = rng.randrange(0, 4102444800000)
            with self.subTest(value=value):
                self.assertEqual(
                    timestamps.parse_timestamp(str(value)), value
                )
                self.assertEqual(
                    timestamps.parse_timestamp("000" + str(value)), value
                )


class FixtureTimestampTest(unittest.TestCase):
    """Timestamp rules as they appear in the 36 fixtures."""

    def test_e7_offset_converts_the_date(self):
        values = [
            timestamps.parse_timestamp(row[2])
            for row in fixture_events("E7")
        ]
        self.assertIn(utc_ms(2026, 10, 3, 23, 30, 0), values)

    def test_e12_window_edges_are_valid_timestamps(self):
        values = [
            timestamps.parse_timestamp(row[2]) for row in fixture_events("E12")
        ]
        self.assertIn(utc_ms(2026, 9, 30, 23, 59, 59), values)
        self.assertIn(utc_ms(2026, 10, 15, 0, 0, 0), values)
        self.assertIn(utc_ms(2026, 10, 14, 23, 59, 59), values)

    def test_e17_bad_offsets(self):
        parsed = {
            row[1]: timestamps.parse_timestamp(row[2])
            for row in fixture_events("E17")
        }
        self.assertIsNone(parsed["ua"])  # +24:00
        self.assertIsNone(parsed["ub"])  # +05:60
        self.assertIsNone(parsed["ue"])  # +0200
        self.assertEqual(parsed["uc"], utc_ms(2026, 10, 3, 10, 0, 0))  # +23:59
        self.assertEqual(parsed["ud"], utc_ms(2026, 10, 3, 12, 0, 0))  # -00:00

    def test_e18_digit_forms(self):
        parsed = {
            row[0]: timestamps.parse_timestamp(row[2])
            for row in fixture_events("E18")
        }
        self.assertEqual(parsed["e1"], 1791021600000)  # leading zeros
        self.assertEqual(parsed["e2"], 86400000)  # outside the window, still valid
        self.assertIsNone(parsed["e3"])  # Arabic-Indic digits
        self.assertEqual(parsed["e4"], 179102160000000000000)  # too long: a number
        self.assertIsNone(parsed["e8"])  # full-width digit in the year


if __name__ == "__main__":
    unittest.main()
