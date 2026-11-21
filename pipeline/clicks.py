"""Click stage (rules.md, "Clicks") and last-click attribution.

Order, exactly as the rules give it:

1. **Parse** every click file, dropping CSV-malformed lines and rows whose
   ``click_id`` is empty.
2. **Dedup** globally on ``click_id``. Copies are identical by contract, so any
   one may be kept; this keeps the first one read. Dedup happens before
   validation, so an invalid copy is not filtered out ahead of its duplicates.
3. **Validate** the kept copy: non-empty ``user_id``, valid ``ts`` inside
   ``[window_start - 7 days, window_end)``, and a ``campaign_id`` that exists in
   ``campaigns.csv``.
4. **Map** user IDs to persons with the same Union-Find names as events.

Attribution, per session:

* **Credit**: among the person's valid clicks with ``click.ts <= session start``
  and ``session_start - click.ts <= 7 days``, take the latest ``ts``; on a tie
  take the smallest ``click_id`` byte by byte. No such click means
  ``organic``/``organic``. One click may credit many sessions.
* **Assists**: every campaign with at least one of the person's valid clicks in
  ``[session_start - 7 days, session_start]``, once per campaign per session,
  including the crediting campaign. ``organic`` never assists.

Cost: credit is a bisect over the person's clicks sorted by ``(ts, click_id)``;
assists are a sliding window over the same list as the person's sessions advance
in start order. That is O(clicks + sessions) per person with no session × click
product and no per-session scanning of the 7-day window.

The result keeps credit and assists as separate fields so Phase 4 can aggregate
each without re-deriving either.
"""
from __future__ import annotations

import sys
import time
from bisect import bisect_left, bisect_right
from pathlib import Path
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
)

import csvio
import timestamps
from events import Session

__all__ = [
    "LOOKBACK_MS",
    "ORGANIC",
    "ClickCopy",
    "Click",
    "SessionAttribution",
    "ClickStats",
    "AttributionResult",
    "read_campaigns",
    "dedup_clicks",
    "validate_clicks",
    "attribute_sessions",
    "stream_attributions",
    "run_attribution_stage",
    "run_attribution_stream",
]

#: A click may credit or assist a session at most this far before its start.
LOOKBACK_MS = 604_800_000
#: Campaign and channel used when no click qualifies; never in campaigns.csv.
ORGANIC = "organic"


class ClickCopy(NamedTuple):
    """One de-duplicated click copy: the winner for its ``click_id``."""

    click_id: str
    user_id: str
    campaign_id: str
    ts_ms: Optional[int]


class Click(NamedTuple):
    """A validated click, already resolved to a person."""

    click_id: str
    person: str
    ts_ms: int
    campaign_id: str


class SessionAttribution(NamedTuple):
    """Attribution of one session; credit and assists stay separate."""

    session: Session
    campaign_id: str  # ORGANIC when no click qualified
    channel: str  # campaigns.csv channel, or ORGANIC
    click_id: Optional[str]  # the crediting click, None for organic
    click_ts: Optional[int]
    assists: Tuple[str, ...]  # distinct campaign IDs, sorted; never organic


class ClickStats:
    """Counters for the click and attribution stages."""

    __slots__ = (
        "click_rows",
        "csv_malformed",
        "empty_click_id",
        "duplicate_copies",
        "unique_clicks",
        "invalid_user",
        "invalid_ts",
        "out_of_window",
        "unknown_campaign",
        "clicks_kept",
        "persons_with_clicks",
        "campaigns",
        "sessions_credited",
        "sessions_organic",
        "assist_links",
        "max_assists",
    )

    def __init__(self) -> None:
        for name in self.__slots__:
            setattr(self, name, 0)

    def summary(self) -> str:
        return (
            f"rows={self.click_rows} csv_malformed={self.csv_malformed} "
            f"empty_id={self.empty_click_id} unique={self.unique_clicks} "
            f"duplicates={self.duplicate_copies} kept={self.clicks_kept} "
            f"dropped(user={self.invalid_user},ts={self.invalid_ts},"
            f"window={self.out_of_window},campaign={self.unknown_campaign}) "
            f"persons={self.persons_with_clicks} credited={self.sessions_credited} "
            f"organic={self.sessions_organic} assists={self.assist_links} "
            f"max_assists={self.max_assists}"
        )


class AttributionResult(NamedTuple):
    """Sessions together with their credit and assists, aligned by index."""

    sessions: List[Session]
    attributions: List[SessionAttribution]
    campaign_channels: Dict[str, str]
    stats: ClickStats


class AttributionStream(NamedTuple):
    """What the streaming entry point returns once every session is consumed."""

    campaign_channels: Dict[str, str]
    stats: ClickStats


def _log(enabled: bool, message: str) -> None:
    if enabled:
        print(message, file=sys.stderr, flush=True)


def read_campaigns(
    path: Path, stats: Optional[ClickStats] = None
) -> Dict[str, str]:
    """campaign_id -> channel from the clean campaigns table."""
    campaigns: Dict[str, str] = {}
    row_stats = csvio.RowStats()
    for campaign_id, channel in csvio.iter_rows(path, 2, stats=row_stats):
        if campaign_id:
            campaigns[campaign_id] = channel
    if stats is not None:
        stats.campaigns = len(campaigns)
    return campaigns


def dedup_clicks(
    click_paths: Iterable[Path], stats: Optional[ClickStats] = None
) -> Dict[str, ClickCopy]:
    """Global dedup on ``click_id``; the first copy read is kept.

    Only CSV-malformed rows and empty ``click_id`` are dropped here. No semantic
    check runs before dedup, matching the event stage.
    """
    winners: Dict[str, ClickCopy] = {}
    intern = sys.intern
    row_stats = csvio.RowStats()
    for path in click_paths:
        for row in csvio.iter_rows(path, csvio.CLICK_FIELDS, stats=row_stats):
            click_id = row[0]
            if not click_id:
                if stats is not None:
                    stats.empty_click_id += 1
                continue
            if click_id in winners:
                if stats is not None:
                    stats.duplicate_copies += 1
                continue
            winners[click_id] = ClickCopy(
                click_id,
                intern(row[1]),
                intern(row[2]),
                timestamps.parse_timestamp(row[3]),
            )
    if stats is not None:
        stats.click_rows += row_stats.rows
        stats.csv_malformed += row_stats.malformed
        stats.unique_clicks += len(winners)
    return winners


def validate_clicks(
    copies: Iterable[ClickCopy],
    click_window_start_ms: int,
    window_end_ms: int,
    campaigns: Dict[str, str],
    person_names: Dict[str, str],
    stats: Optional[ClickStats] = None,
) -> "Iterable[Click]":
    """Validate kept copies and resolve them to persons.

    The click window is ``[window_start - 7 days, window_end)``: a click exactly
    seven days before the window is in, one at ``window_end`` is out.
    """
    for copy in copies:
        if not copy.user_id:
            if stats is not None:
                stats.invalid_user += 1
            continue
        ts_ms = copy.ts_ms
        if ts_ms is None:
            if stats is not None:
                stats.invalid_ts += 1
            continue
        if ts_ms < click_window_start_ms or ts_ms >= window_end_ms:
            if stats is not None:
                stats.out_of_window += 1
            continue
        if copy.campaign_id not in campaigns:
            if stats is not None:
                stats.unknown_campaign += 1
            continue
        if stats is not None:
            stats.clicks_kept += 1
        yield Click(
            copy.click_id,
            person_names.get(copy.user_id, copy.user_id),
            ts_ms,
            copy.campaign_id,
        )


def attribute_sessions(
    sessions: Sequence[Session],
    clicks: Iterable[Click],
    campaign_channels: Dict[str, str],
    stats: Optional[ClickStats] = None,
) -> List[SessionAttribution]:
    """Credit and assist every session; output is aligned with ``sessions``.

    This keeps one object per session, which the Phase 3 tests and fixture
    checkers compare against. The pipeline itself uses
    :func:`stream_attributions`, which never holds more than one attribution.
    """
    by_person = _group_clicks(clicks)

    sessions_by_person: Dict[str, List[int]] = {}
    for index, session in enumerate(sessions):
        bucket = sessions_by_person.get(session.person)
        if bucket is None:
            sessions_by_person[session.person] = [index]
        else:
            bucket.append(index)

    attributions: List[Optional[SessionAttribution]] = [None] * len(sessions)
    for person, indices in sessions_by_person.items():
        indices.sort(key=_start_of(sessions))
        person_sessions = [sessions[index] for index in indices]
        for index, attribution in zip(
            indices,
            _iter_person_attributions(
                person_sessions,
                by_person.get(person) or (),
                campaign_channels,
                stats,
            ),
        ):
            attributions[index] = attribution

    if stats is not None:
        stats.persons_with_clicks = len(by_person)
        for bucket in by_person.values():
            bucket.clear()
    resolved: List[SessionAttribution] = []
    for position, attribution in enumerate(attributions):
        if attribution is None:  # every session is covered above; be loud
            raise RuntimeError(f"session {position} was not attributed")
        resolved.append(attribution)
    return resolved


def stream_attributions(
    sessions: Iterable[Session],
    clicks: Iterable[Click],
    campaign_channels: Dict[str, str],
    consume: Callable[[SessionAttribution], None],
    stats: Optional[ClickStats] = None,
) -> None:
    """Attribute sessions one at a time, handing each to ``consume``.

    Sessions are grouped by person and sorted by start, because the assist
    window slides forward. Grouping stores references to the session objects
    that already exist; beyond that, no per-session attribution is retained, so
    ``campaign_daily`` can fold each one into counters and drop it.
    """
    by_person = _group_clicks(clicks)
    by_person_sessions: Dict[str, List[Session]] = {}
    for session in sessions:
        bucket = by_person_sessions.get(session.person)
        if bucket is None:
            by_person_sessions[session.person] = [session]
        else:
            bucket.append(session)

    for person, person_sessions in by_person_sessions.items():
        person_sessions.sort(key=_session_start)
        for attribution in _iter_person_attributions(
            person_sessions,
            by_person.get(person) or (),
            campaign_channels,
            stats,
        ):
            consume(attribution)

    if stats is not None:
        stats.persons_with_clicks = len(by_person)
    by_person_sessions.clear()
    for bucket in by_person.values():
        bucket.clear()


def _group_clicks(clicks: Iterable[Click]) -> Dict[str, List[Click]]:
    """Group valid clicks by person, each group sorted by (ts, click_id)."""
    by_person: Dict[str, List[Click]] = {}
    for click in clicks:
        bucket = by_person.get(click.person)
        if bucket is None:
            by_person[click.person] = [click]
        else:
            bucket.append(click)
    for bucket in by_person.values():
        bucket.sort(key=_click_order)
    return by_person


def _session_start(session: Session) -> int:
    return session.start_ms


def _click_order(click: Click) -> Tuple[int, str]:
    return (click.ts_ms, click.click_id)


def _start_of(sessions: Sequence[Session]):
    def key(index: int) -> int:
        return sessions[index].start_ms

    return key


def _iter_person_attributions(
    sessions: Sequence[Session],
    clicks: Sequence[Click],
    campaign_channels: Dict[str, str],
    stats: Optional[ClickStats],
) -> Iterator[SessionAttribution]:
    """Yield one person's attributions; ``sessions`` must be start-sorted.

    Sliding window for assists, bisect for credit. Keeping this a generator is
    what lets the pipeline aggregate each session and immediately discard it.
    """
    click_times = [click.ts_ms for click in clicks]
    left = 0
    right = 0
    active: Dict[str, int] = {}
    for session in sessions:
        start_ms = session.start_ms
        limit_ms = start_ms - LOOKBACK_MS

        while right < len(clicks) and clicks[right].ts_ms <= start_ms:
            campaign = clicks[right].campaign_id
            active[campaign] = active.get(campaign, 0) + 1
            right += 1
        while left < right and clicks[left].ts_ms < limit_ms:
            campaign = clicks[left].campaign_id
            remaining = active[campaign] - 1
            if remaining:
                active[campaign] = remaining
            else:
                del active[campaign]
            left += 1

        assists = tuple(sorted(active))
        position = bisect_right(click_times, start_ms) - 1
        if position >= 0 and start_ms - click_times[position] <= LOOKBACK_MS:
            # Latest ts wins; the first click of that ts block has the smallest
            # click_id because clicks are sorted by (ts, click_id).
            first = bisect_left(click_times, click_times[position])
            winner = clicks[first]
            attribution = SessionAttribution(
                session,
                winner.campaign_id,
                campaign_channels[winner.campaign_id],
                winner.click_id,
                winner.ts_ms,
                assists,
            )
            if stats is not None:
                stats.sessions_credited += 1
        else:
            attribution = SessionAttribution(
                session, ORGANIC, ORGANIC, None, None, assists
            )
            if stats is not None:
                stats.sessions_organic += 1
        if stats is not None:
            stats.assist_links += len(assists)
            if len(assists) > stats.max_assists:
                stats.max_assists = len(assists)
        yield attribution


def _read_click_inputs(event_result, data_dir, stats, log):
    """Read campaigns and clicks once; return validated-click inputs."""
    data = Path(data_dir)
    started = time.perf_counter()
    campaign_channels = read_campaigns(data / "campaigns.csv", stats)
    copies = dedup_clicks(sorted((data / "clicks").glob("*.csv")), stats)
    after_dedup = time.perf_counter()
    _log(
        log,
        f"[attribution-stage] read+dedup {after_dedup - started:.3f}s "
        f"unique_clicks={stats.unique_clicks} campaigns={stats.campaigns}",
    )
    validated = validate_clicks(
        copies.values(),
        event_result.window_start_ms - LOOKBACK_MS,
        event_result.window_end_ms,
        campaign_channels,
        event_result.person_names,
        stats,
    )
    return campaign_channels, copies, validated, started, after_dedup


def _log_click_totals(started, after_dedup, stats, log) -> None:
    finished = time.perf_counter()
    _log(
        log,
        f"[attribution-stage] validate+attribute {finished - after_dedup:.3f}s "
        f"kept={stats.clicks_kept}",
    )
    _log(
        log,
        f"[attribution-stage] total {finished - started:.3f}s {stats.summary()}",
    )


def run_attribution_stage(
    event_result, data_dir, log: bool = False
) -> AttributionResult:
    """Run clicks and attribution on a ``/data``-shaped directory.

    ``event_result`` supplies the sessions, the person names and the window, so
    links and the manifest are read only once for the whole pipeline.
    """
    stats = ClickStats()
    campaign_channels, copies, validated, started, after_dedup = (
        _read_click_inputs(event_result, data_dir, stats, log)
    )
    attributions = attribute_sessions(
        event_result.sessions, validated, campaign_channels, stats
    )
    copies.clear()
    _log_click_totals(started, after_dedup, stats, log)
    return AttributionResult(
        event_result.sessions, attributions, campaign_channels, stats
    )


def run_attribution_stream(
    event_result, data_dir, consume, log: bool = False
) -> AttributionStream:
    """Same stages as :func:`run_attribution_stage`, but consumable in place.

    Returns the campaign channels (needed for assisted-only ``campaign_daily``
    rows) and the stage statistics, not a list of attributions.
    """
    stats = ClickStats()
    campaign_channels, copies, validated, started, after_dedup = (
        _read_click_inputs(event_result, data_dir, stats, log)
    )
    stream_attributions(
        event_result.sessions, validated, campaign_channels, consume, stats
    )
    copies.clear()
    _log_click_totals(started, after_dedup, stats, log)
    return AttributionStream(campaign_channels, stats)
