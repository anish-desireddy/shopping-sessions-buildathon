"""``user_daily`` aggregation from sessions (rules.md, "Outputs").

One row per person and day with at least one session:

* ``sessions`` -- sessions starting that day;
* ``events`` -- surviving events in those sessions;
* ``revenue_cents`` -- summed purchase price in those sessions, 0 if none;
* ``top_category`` -- the category with the highest summed purchase price in
  the day's sessions, ties going to the byte-wise smallest category, and null
  only when the day has no purchases. ``unknown`` competes like any category.

This is the only output table the event stage can produce; it is aggregated from
:class:`pipeline.events.Session` records so Phase 4 can reuse it unchanged once
attribution exists.
"""
from __future__ import annotations

from typing import Dict, List, NamedTuple, Optional, Tuple

import timestamps

__all__ = ["UserDaily", "build_user_daily", "iso_day"]

_MS_PER_DAY = 86_400_000


class UserDaily(NamedTuple):
    user_id: str
    day: int  # days since 1970-01-01, UTC
    sessions: int
    events: int
    revenue_cents: int
    top_category: Optional[str]


def iso_day(day: int) -> str:
    """``YYYY-MM-DD`` for a day count since the epoch."""
    year, month, date = timestamps.civil_from_days(day)
    return f"{year:04d}-{month:02d}-{date:02d}"


def build_user_daily(sessions: List) -> List[UserDaily]:
    """Aggregate sessions into ``user_daily`` rows, sorted by (person, day)."""
    # (person, day) -> [sessions, events, revenue, {category: cents} | None]
    grouped: Dict[Tuple[str, int], list] = {}
    for session in sessions:
        key = (session.person, session.day)
        aggregate = grouped.get(key)
        if aggregate is None:
            aggregate = [0, 0, 0, None]
            grouped[key] = aggregate
        aggregate[0] += 1
        aggregate[1] += session.events
        aggregate[2] += session.revenue_cents
        if session.categories:
            categories = aggregate[3]
            if categories is None:
                categories = {}
                aggregate[3] = categories
            for category, cents in session.categories:
                categories[category] = categories.get(category, 0) + cents

    rows: List[UserDaily] = []
    for (person, day), aggregate in grouped.items():
        categories = aggregate[3]
        if categories is None:
            top_category = None
        else:
            # Highest summed price, then byte-wise smallest category on a tie.
            top_category = min(
                categories.items(), key=lambda item: (-item[1], item[0])
            )[0]
        rows.append(
            UserDaily(
                person,
                day,
                aggregate[0],
                aggregate[1],
                aggregate[2],
                top_category,
            )
        )
    rows.sort(key=_key)
    return rows


def _key(row: UserDaily) -> Tuple[str, int]:
    return (row.user_id, row.day)
