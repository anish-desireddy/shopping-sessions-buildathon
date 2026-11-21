"""Unit tests for pipeline/csvio.py against the dialect in rules.md."""
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))

import csvio  # noqa: E402

FIXTURES = ROOT / "fixtures"


def fixture_rows(case, folder, nfields):
    """Every parsed row of a fixture folder, plus counts, read file by file."""
    stats = csvio.RowStats()
    rows = []
    for path in sorted((FIXTURES / case / "input" / folder).glob("*.csv")):
        rows.extend(csvio.iter_rows(path, nfields, stats=stats))
    return rows, stats


class SplitFieldsTest(unittest.TestCase):
    def test_plain_fields(self):
        self.assertEqual(csvio.split_fields(b"a,b,c"), [b"a", b"b", b"c"])

    def test_quotes_are_syntax(self):
        self.assertEqual(csvio.split_fields(b'"a","b"'), [b"a", b"b"])
        self.assertEqual(csvio.split_fields(b'"a,b"x'), None)  # comma splits first

    def test_quoted_empty_is_null(self):
        self.assertEqual(csvio.split_fields(b'""'), [b""])

    def test_empty_field_is_null(self):
        self.assertEqual(csvio.split_fields(b","), [b"", b""])
        self.assertEqual(csvio.split_fields(b"a,,c"), [b"a", b"", b"c"])

    def test_spaces_are_part_of_the_value(self):
        self.assertEqual(csvio.split_fields(b" u1 , u1"), [b" u1 ", b" u1"])
        self.assertEqual(csvio.split_fields(b'" u1"'), [b" u1"])

    def test_carriage_return_is_an_ordinary_character(self):
        self.assertEqual(csvio.split_fields(b"a,1000\r"), [b"a", b"1000\r"])
        self.assertEqual(csvio.split_fields(b"e3\r,u1"), [b"e3\r", b"u1"])

    def test_malformed_lines(self):
        for line in (
            b"",
            b'"',
            b'"a',
            b'a"',
            b'a"b',
            b'"a"b',
            b'"a"b"',
            b'""a""',
            b'"a""b"',
        ):
            with self.subTest(line=line):
                self.assertIsNone(csvio.split_fields(line))


class IterRowsTest(unittest.TestCase):
    def read(self, data, nfields, skip_header=True):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "t.csv"
            path.write_bytes(data)
            stats = csvio.RowStats()
            rows = list(
                csvio.iter_rows(path, nfields, skip_header=skip_header, stats=stats)
            )
            return rows, stats

    def test_header_is_skipped(self):
        rows, stats = self.read(b"a,b\n1,2\n", 2)
        self.assertEqual(rows, [("1", "2")])
        self.assertEqual((stats.rows, stats.malformed), (1, 0))

    def test_only_newline_ends_a_line(self):
        rows, _ = self.read(b"a,b\r\nc,d\n", 2, skip_header=False)
        self.assertEqual(rows, [("a", "b\r"), ("c", "d")])

    def test_trailing_newline_is_not_an_empty_line(self):
        rows, stats = self.read(b"a,b\n1,2\n", 2)
        self.assertEqual(rows, [("1", "2")])
        self.assertEqual(stats.malformed, 0)

    def test_empty_interior_line_is_malformed(self):
        rows, stats = self.read(b"a,b\n1,2\n\n3,4\n", 2)
        self.assertEqual(rows, [("1", "2"), ("3", "4")])
        self.assertEqual(stats.malformed, 1)

    def test_last_line_without_newline_counts(self):
        rows, _ = self.read(b"a,b\n1,2", 2)
        self.assertEqual(rows, [("1", "2")])

    def test_wrong_field_count_is_dropped(self):
        rows, stats = self.read(b"a,b\n1,2,3\n4,5\n", 2)
        self.assertEqual(rows, [("4", "5")])
        self.assertEqual(stats.malformed, 1)

    def test_header_only_and_empty_files(self):
        self.assertEqual(self.read(b"a,b\n", 2)[0], [])
        self.assertEqual(self.read(b"", 2)[0], [])

    def test_bad_header_is_loud(self):
        with self.assertRaises(csvio.CsvError):
            self.read(b"a,b,c\n1,2\n", 2)

    def test_invalid_utf8_is_malformed(self):
        rows, stats = self.read(b"a,b\n1,\xff\n2,3\n", 2)
        self.assertEqual(rows, [("2", "3")])
        self.assertEqual(stats.malformed, 1)

    def test_header_may_be_quoted(self):
        rows, _ = self.read(b'"a","b"\n1,2\n', 2)
        self.assertEqual(rows, [("1", "2")])

    def test_skip_header_false_reads_every_line(self):
        rows, _ = self.read(b"1,2\n", 2, skip_header=False)
        self.assertEqual(rows, [("1", "2")])


class FixtureParsingTest(unittest.TestCase):
    """The dialect rules as they actually appear in the 36 fixtures."""

    def test_e20_crlf_keeps_carriage_return(self):
        rows, stats = fixture_rows("E20", "events", csvio.EVENT_FIELDS)
        self.assertEqual(stats.malformed, 0)
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            rows[0],
            (
                "e1",
                "u1",
                "2026-10-03T10:00:00Z",
                "2026-10-03T12:00:00Z",
                "view",
                "p1",
                "\r",  # the empty price field still holds the CR
            ),
        )
        self.assertEqual(rows[1][6], "1000\r")  # bad price, dropped later
        self.assertEqual(rows[3][0], "e3\r")  # a different event_id from "e3"
        clicks, click_stats = fixture_rows("E20", "clicks", csvio.CLICK_FIELDS)
        self.assertEqual(click_stats.malformed, 0)
        self.assertEqual(clicks[0][3], "2026-10-03T09:00:00Z\r")

    def test_e15_quotes_are_not_part_of_the_value(self):
        rows, _ = fixture_rows("E15", "events", csvio.EVENT_FIELDS)
        self.assertEqual([row[1] for row in rows], ["u1", "u1"])

    def test_e13_parser_keeps_three_rows_and_drops_the_six_field_line(self):
        # The empty event_id and the bad type/ts are semantic, not CSV, errors;
        # only the line with six fields is malformed CSV.
        rows, stats = fixture_rows("E13", "events", csvio.EVENT_FIELDS)
        self.assertEqual(stats.malformed, 1)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][0], "")

    def test_l3_link_lines(self):
        rows, stats = fixture_rows("L3", "links", csvio.LINK_FIELDS)
        self.assertEqual(stats.malformed, 4)  # 3 fields, stray quote, blank, 1 field
        self.assertEqual(
            rows,
            [
                ("u1", ""),
                ("", "u3"),
                ("u4", "u1"),
                ("u4", "u1"),
                ("u6", "u6"),
                ("user_id_a", "user_id_b"),
                ("u4", "user_id_b"),
                ("A\tB", "user_id_a"),
            ],
        )

    def test_every_fixture_file_parses(self):
        cases = sorted(path for path in FIXTURES.iterdir() if path.is_dir())
        self.assertEqual(len(cases), 36)
        total = 0
        for case in cases:
            for folder, nfields in (
                ("events", csvio.EVENT_FIELDS),
                ("clicks", csvio.CLICK_FIELDS),
                ("links", csvio.LINK_FIELDS),
            ):
                input_dir = case / "input" / folder
                if not input_dir.is_dir():
                    continue
                stats = csvio.RowStats()
                for path in sorted(input_dir.glob("*.csv")):
                    for _ in csvio.iter_rows(path, nfields, stats=stats):
                        pass
                total += stats.rows
        self.assertGreater(total, 0)


if __name__ == "__main__":
    unittest.main()
