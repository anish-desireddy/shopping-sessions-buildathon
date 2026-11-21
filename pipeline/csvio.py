"""Byte-exact reader for the competition's CSV dialect (rules.md, "CSV dialect").

The dialect, restated so this module can be audited against the rules:

* UTF-8, comma separated. Only ``\\n`` ends a line. ``\\r`` is an ordinary
  character of the field it sits in, so in a CRLF file the last field of every
  line keeps a trailing ``\\r``.
* Every file has a header row, and only the *first physical line* is a header:
  a later line that looks exactly like one is data (fixture L3).
* Any field may be wrapped in one pair of double quotes, and the quotes are
  syntax, not part of the value; ``""`` is an empty field. No value holds a
  comma, a quote or a newline, quoted or not, so splitting on ``","`` is exact.
* An empty field means null. Because no production path can yield an empty
  non-null value, null is represented as the empty string (``NULL``).
* A line with the wrong number of fields, a quote that does not wrap a whole
  field (including a doubled ``""`` escape inside a field), and an empty line
  are malformed; the line is dropped.
* Spaces are ordinary characters and are never trimmed.

The parser works on bytes and decodes each accepted row as UTF-8. Bytes that
are not valid UTF-8 count as a malformed line: the input contract says these
files are UTF-8, so this only guards against garbage.

DuckDB's CSV reader is deliberately not used: it sniffs dialects, repairs or
rejects malformed rows its own way, and translates ``\\r\\n`` (see the
reconnaissance report and fixtures E13/E15/E20).
"""
from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Tuple

__all__ = ["NULL", "CsvError", "RowStats", "split_fields", "iter_rows"]

#: An empty field means null, and nothing else can produce an empty value.
NULL = ""

#: Field counts of the three input tables, header included.
EVENT_FIELDS = 7
CLICK_FIELDS = 4
LINK_FIELDS = 2


class CsvError(ValueError):
    """A file cannot be read at all (missing or unusable header)."""


class RowStats:
    """Mutable counters for one reader pass; used for stderr diagnostics."""

    __slots__ = ("rows", "malformed")

    def __init__(self) -> None:
        self.rows = 0
        self.malformed = 0

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"RowStats(rows={self.rows}, malformed={self.malformed})"


def _split_text(text: str) -> Optional[List[str]]:
    """Split one decoded line into unquoted fields; ``None`` if malformed."""
    fields = text.split(",")
    parsed: List[str] = []
    for field in fields:
        if field.startswith('"'):
            # A quote is only syntax when it wraps the entire field exactly
            # once. The interior may not contain another quote, so the
            # conventional "" escape is not part of this dialect.
            if len(field) < 2 or not field.endswith('"'):
                return None
            inner = field[1:-1]
            if '"' in inner:
                return None
            parsed.append(inner)
        elif '"' in field:
            return None
        else:
            parsed.append(field)
    return parsed


def split_fields(line: bytes) -> Optional[List[bytes]]:
    """Split one line (without its ``\\n``) into raw field bytes.

    Returns ``None`` when the line is malformed under the dialect rules. An
    accepted field is returned unquoted; ``b""`` means null.

    ``iter_rows`` decodes each line once and calls ``_split_text`` directly,
    because decoding every field separately was the hottest part of the parse.
    This byte-level wrapper is the reference form the dialect tests assert on.
    """
    if not line:
        # An empty line is malformed, not a row of one null field.
        return None
    try:
        text = line.decode("utf-8")
    except UnicodeDecodeError:
        return None
    fields = _split_text(text)
    if fields is None:
        return None
    return [field.encode("utf-8") for field in fields]


def iter_rows(
    path,
    nfields: int,
    skip_header: bool = True,
    stats: Optional[RowStats] = None,
) -> Iterator[Tuple[str, ...]]:
    """Yield every well-formed data row of ``path`` as decoded strings.

    Malformed rows are silently dropped (the rules say to drop them); pass a
    ``RowStats`` to count them. Binary iteration splits on ``\\n`` only, which
    is exactly the dialect's line rule, and keeps ``\\r`` in the field.
    """
    with open(path, "rb") as handle:
        if skip_header:
            header = handle.readline()
            if header == b"":
                return  # empty file: nothing to read
            if header.endswith(b"\n"):
                header = header[:-1]
            try:
                header_text = header.decode("utf-8")
            except UnicodeDecodeError:
                header_text = None
            fields = None if header_text is None else _split_text(header_text)
            if fields is None or len(fields) != nfields:
                raise CsvError(
                    f"{path}: header is not {nfields} fields: {header[:120]!r}"
                )
        for raw in handle:
            if raw.endswith(b"\n"):
                raw = raw[:-1]
            if not raw:
                # An empty line is malformed, not a row of one null field.
                if stats is not None:
                    stats.malformed += 1
                continue
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                if stats is not None:
                    stats.malformed += 1
                continue
            fields = _split_text(text)
            if fields is None or len(fields) != nfields:
                if stats is not None:
                    stats.malformed += 1
                continue
            if stats is not None:
                stats.rows += 1
            yield tuple(fields)


def format_field(value) -> str:
    """One field for a scratch CSV written by :func:`write_rows`.

    ``None`` is an unquoted empty field (NULL on read). Everything else stays
    unquoted unless it holds a comma, quote or line break. Input values cannot
    hold those (rules.md, CSV dialect), but quoting keeps the file valid if one
    does. DuckDB reads even a quoted empty field as NULL; that is safe here
    because no value written through this path has a legitimate empty string.
    """
    if value is None:
        return ""
    text = str(value)
    if text and not any(ch in text for ch in ',"\n\r'):
        return text
    return '"' + text.replace('"', '""') + '"'


def write_rows(path, rows) -> None:
    """Write rows as a headerless scratch CSV (no trailing newline)."""
    with open(path, "w", encoding="utf-8", newline="") as handle:
        first = True
        for row in rows:
            if first:
                first = False
            else:
                handle.write("\n")
            handle.write(",".join(format_field(value) for value in row))
