"""Strict timestamp parsing for ``ts`` values (rules.md, "Timestamps").

Three forms are accepted, all converted to integer milliseconds since the Unix
epoch in UTC:

* F1 ``YYYY-MM-DDTHH:MM:SSZ`` -- UTC, whole seconds (exactly 20 characters).
* F2 ``YYYY-MM-DDTHH:MM:SS+HH:MM`` / ``-HH:MM`` -- offset hours 00..23 and
  minutes 00..59, with the colon. ``-00:00`` is UTC (exactly 25 characters).
* F3 an epoch-millisecond number: ASCII digits only, leading zeros allowed,
  any length. A value too long for a 64-bit integer is still a *valid* number
  that merely falls outside the window, so it is returned as a Python int.

Everything else returns ``None``: empty, fractional seconds, a lowercase
``z``/``t``, a space instead of ``T``, a missing zone, any non-ASCII-digit
character in F3 (``1791071890000.0``, ``1.79e12``, ``-5``), an impossible
calendar value, or an out-of-range offset.

Digit checks are ASCII-only on purpose. ``str.isdigit()`` is true for
Arabic-Indic and full-width digits, and ``int()`` happily converts them, so
``value.isascii()`` is checked before any digit test.

Calendars are converted with the civil-from-days algorithm (Howard Hinnant's
``days_from_civil``), not with ``datetime``: it is exact for every four-digit
year including ``0000``, avoids platform ``timestamp()`` limits, and is much
faster on tens of millions of rows.
"""
from __future__ import annotations

from typing import Optional, Tuple

__all__ = [
    "parse_timestamp",
    "is_leap_year",
    "days_in_month",
    "civil_from_days",
]

_MS_PER_SECOND = 1000
_SECONDS_PER_DAY = 86400
_MONTH_DAYS = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

#: Python 3.11+ refuses ``int(str)`` beyond 4300 digits by default. F3 values
#: have no length limit, so long ones are converted exactly in chunks.
_INT_CHUNK = 4000


def is_leap_year(year: int) -> bool:
    """Proleptic Gregorian leap rule."""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def days_in_month(year: int, month: int) -> int:
    """Length of ``month`` (1..12) in ``year``."""
    if month == 2 and is_leap_year(year):
        return 29
    return _MONTH_DAYS[month - 1]


def _days_from_civil(year: int, month: int, day: int) -> int:
    """Days from 1970-01-01 to ``year-month-day`` (proleptic Gregorian)."""
    year -= month <= 2
    era = year // 400
    year_of_era = year - era * 400  # [0, 399]
    day_of_year = (153 * (month + (-3 if month > 2 else 9)) + 2) // 5 + day - 1
    day_of_era = (
        year_of_era * 365
        + year_of_era // 4
        - year_of_era // 100
        + day_of_year
    )
    return era * 146097 + day_of_era - 719468


def civil_from_days(days: int) -> Tuple[int, int, int]:
    """Inverse of :func:`_days_from_civil`: (year, month, day) of a day count."""
    days += 719468
    era = days // 146097
    day_of_era = days - era * 146097  # [0, 146096]
    year_of_era = (
        day_of_era
        - day_of_era // 1460
        + day_of_era // 36524
        - day_of_era // 146096
    ) // 365
    year = year_of_era + era * 400
    day_of_year = day_of_era - (
        365 * year_of_era + year_of_era // 4 - year_of_era // 100
    )
    month_part = (5 * day_of_year + 2) // 153
    day = day_of_year - (153 * month_part + 2) // 5 + 1
    month = month_part + (3 if month_part < 10 else -9)
    return year + (month <= 2), month, day


def _digits_to_int(value: str) -> int:
    """Exact ``int(value)`` for a run of ASCII digits of any length."""
    if len(value) <= _INT_CHUNK:
        return int(value)
    result = 0
    for start in range(0, len(value), _INT_CHUNK):
        chunk = value[start : start + _INT_CHUNK]
        result = result * 10 ** len(chunk) + int(chunk)
    return result


def _ascii_int(value: str, start: int, end: int) -> Optional[int]:
    """The ASCII-digit integer in ``value[start:end]``, else ``None``."""
    part = value[start:end]
    if not part.isdigit():  # safe: the caller proved the string is ASCII
        return None
    return int(part)


#: ``YYYY-MM-DD`` -> days since 1970-01-01. A dataset's dates repeat across
#: millions of rows, so the calendar work is done once per distinct date. The
#: cache is cleared if hostile input ever makes it large.
_DATE_DAYS: Dict[str, int] = {}
_DATE_CACHE_LIMIT = 4096


def _date_days(prefix: str) -> Optional[int]:
    """Days since the epoch for a checked ``YYYY-MM-DD`` prefix."""
    days = _DATE_DAYS.get(prefix)
    if days is not None:
        return days
    if len(prefix) != 10 or prefix[4] != "-" or prefix[7] != "-":
        return None
    year = _ascii_int(prefix, 0, 4)
    month = _ascii_int(prefix, 5, 7)
    day = _ascii_int(prefix, 8, 10)
    if year is None or month is None or day is None:
        return None
    if not 1 <= month <= 12:
        return None
    if not 1 <= day <= days_in_month(year, month):
        return None
    days = _days_from_civil(year, month, day)
    if len(_DATE_DAYS) >= _DATE_CACHE_LIMIT:
        _DATE_DAYS.clear()
    _DATE_DAYS[prefix] = days
    return days


def _time_ms(
    days: int, value: str, offset_seconds: int
) -> Optional[int]:
    """Combine a valid date with ``HH:MM:SS`` at offset 11 and the offset."""
    hour = _ascii_int(value, 11, 13)
    minute = _ascii_int(value, 14, 16)
    second = _ascii_int(value, 17, 19)
    if hour is None or minute is None or second is None:
        return None
    if hour > 23 or minute > 59 or second > 59:
        return None
    total_seconds = (
        days * _SECONDS_PER_DAY + hour * 3600 + minute * 60 + second - offset_seconds
    )
    return total_seconds * _MS_PER_SECOND


def parse_timestamp(value: str) -> Optional[int]:
    """Epoch milliseconds for a valid F1/F2/F3 ``ts``, else ``None``."""
    if not value or not value.isascii():
        return None
    # F3 first: a run of digits is always a number, whatever its length.
    if value.isdigit():
        return _digits_to_int(value)
    length = len(value)
    if length == 20:
        if (
            value[4] == "-"
            and value[7] == "-"
            and value[10] == "T"
            and value[13] == ":"
            and value[16] == ":"
            and value[19] == "Z"
        ):
            days = _date_days(value[:10])
            if days is None:
                return None
            return _time_ms(days, value, 0)
        return None
    if length == 25:
        if (
            value[4] == "-"
            and value[7] == "-"
            and value[10] == "T"
            and value[13] == ":"
            and value[16] == ":"
            and value[19] in "+-"
            and value[22] == ":"
        ):
            days = _date_days(value[:10])
            if days is None:
                return None
            offset_hours = _ascii_int(value, 20, 22)
            offset_minutes = _ascii_int(value, 23, 25)
            if offset_hours is None or offset_minutes is None:
                return None
            if offset_hours > 23 or offset_minutes > 59:
                return None
            sign = 1 if value[19] == "+" else -1
            return _time_ms(
                days, value, sign * (offset_hours * 3600 + offset_minutes * 60)
            )
        return None
    return None
