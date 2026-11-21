"""Event stage (rules.md, "Events, in this order").

The order below is load-bearing and is exactly the order the rules give:

1. **Parse** every event file, dropping only CSV-malformed lines and rows whose
   ``event_id`` is empty.
2. **Dedup** globally on ``event_id``, keeping the copy with the smallest
   ``ingest_ts``. This is one dataset-wide operation, never per file, and it
   happens before any semantic validation: an invalid winning copy drops the
   event and a later valid copy does not rescue it (fixture E5). A
   CSV-malformed copy was already dropped in step 1, so it can never win.
3. **Validate** the kept copy: non-empty ``user_id``, valid ``ts`` inside
   ``[window_start, window_end)``, exact ``event_type``, non-empty
   ``product_id``, and for purchases a digits-only price from 1 to 10,000,000.
4. **Drop late events**: ``ingest_ts - ts > 24 h``; exactly 24 h stays, and an
   ``ingest_ts`` before ``ts`` stays.
5. **Map** user IDs to persons through Union-Find over ``links/``.
6. **Sessionize** each person's events, sorted by ``ts``, starting a new
   session when the gap is over 30 minutes. Equal timestamps share a session.
7. **Day** is the UTC date of the session start; every event and purchase in
   the session counts on that day, even after midnight.
8. **Category** comes from ``products.csv``; an absent product is ``unknown``.

Parsing each copy's ``ts`` while reading is representation only: nothing is
dropped or reordered by it, and the winning copy's parsed-or-``None`` ``ts`` is
what validation sees, so E5 still behaves exactly as the rules require.

Every row still gets stored once in the dedup map, every surviving event once in
its person's list, and every session once in the output. That is the correctness
baseline; the dedup map is the dominant memory user at full size and is the
first thing to replace when Phase 6 measures the pipeline.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, NamedTuple, Optional, Sequence, Tuple

import csvio
import timestamps
from identity import UnionFind

__all__ = [
    "GAP_MS",
    "LATE_MS",
    "MAX_PRICE_CENTS",
    "UNKNOWN_CATEGORY",
    "EventCopy",
    "Session",
    "StageStats",
    "EventResult",
    "build_person_names",
    "read_products",
    "dedup_events",
    "dedup_events_partitioned",
    "validate_copies",
    "sessionize_person",
    "build_sessions",
    "run_event_stage",
]

#: A new session starts only when the gap is *over* 30 minutes.
GAP_MS = 1_800_000
#: An event is late only when ``ingest_ts - ts`` is *over* 24 hours.
LATE_MS = 86_400_000
MAX_PRICE_CENTS = 10_000_000
#: Category for a product that is not in products.csv; no category uses this name.
UNKNOWN_CATEGORY = "unknown"

EVENT_TYPES = frozenset(("view", "cart", "purchase"))
_MS_PER_DAY = 86_400_000


class EventCopy(NamedTuple):
    """One de-duplicated event copy: the winner for its ``event_id``."""

    ingest_ms: int
    user_id: str
    ts_ms: Optional[int]
    event_type: str
    product_id: str
    price: str


class Session(NamedTuple):
    """One session of one person."""

    person: str
    start_ms: int
    day: int  # days since 1970-01-01, UTC
    events: int
    revenue_cents: int
    purchases: int
    #: (category, cents) pairs sorted by category, or None without purchases.
    categories: Optional[Tuple[Tuple[str, int], ...]]


class StageStats:
    """Counters for the event stage; logged as stage diagnostics."""

    __slots__ = (
        "event_rows",
        "csv_malformed",
        "empty_event_id",
        "invalid_ingest",
        "unique_events",
        "duplicate_copies",
        "invalid_user",
        "invalid_ts",
        "out_of_window",
        "invalid_type",
        "invalid_product",
        "invalid_price",
        "late",
        "events_kept",
        "links",
        "link_malformed",
        "link_empty_side",
        "product_malformed",
        "persons",
        "sessions",
    )

    def __init__(self) -> None:
        for name in self.__slots__:
            setattr(self, name, 0)

    def summary(self) -> str:
        return (
            f"rows={self.event_rows} csv_malformed={self.csv_malformed} "
            f"empty_id={self.empty_event_id} unique={self.unique_events} "
            f"duplicates={self.duplicate_copies} kept={self.events_kept} "
            f"dropped(user={self.invalid_user},ts={self.invalid_ts},"
            f"window={self.out_of_window},type={self.invalid_type},"
            f"product={self.invalid_product},price={self.invalid_price},"
            f"late={self.late}) persons={self.persons} sessions={self.sessions}"
        )


class EventResult(NamedTuple):
    sessions: List[Session]
    person_names: Dict[str, str]
    window_start_ms: int
    window_end_ms: int
    stats: StageStats


def _log(enabled: bool, message: str) -> None:
    if enabled:
        print(message, file=sys.stderr, flush=True)


def build_person_names(
    link_paths: Iterable[Path], stats: Optional[StageStats] = None
) -> Dict[str, str]:
    """Every linked ID mapped to its person's name (byte-wise smallest ID)."""
    union_find = UnionFind()
    row_stats = csvio.RowStats()
    for path in link_paths:
        for left, right in csvio.iter_rows(
            path, csvio.LINK_FIELDS, stats=row_stats
        ):
            if stats is not None:
                stats.links += 1
            if not left or not right:
                if stats is not None:
                    stats.link_empty_side += 1
                continue
            union_find.union(left, right)
    if stats is not None:
        stats.link_malformed += row_stats.malformed
    return union_find.names()


def read_products(
    path: Path, stats: Optional[StageStats] = None
) -> Dict[str, str]:
    """product_id -> category from the clean products table."""
    products: Dict[str, str] = {}
    row_stats = csvio.RowStats()
    for product_id, category in csvio.iter_rows(path, 2, stats=row_stats):
        if product_id:
            products[product_id] = category
    if stats is not None:
        stats.product_malformed += row_stats.malformed
    return products


def dedup_events(
    event_paths: Iterable[Path], stats: Optional[StageStats] = None
) -> Dict[str, EventCopy]:
    """Global dedup on ``event_id``: the copy with the smallest ``ingest_ts``.

    Rows are only dropped here for CSV malformation (by the reader) or an empty
    ``event_id``; no semantic check runs before dedup. ``ingest_ts`` is
    guaranteed valid F1 by the rules; a violation is counted and dropped rather
    than crashing the run, since no ordering or lateness can be defined for it.
    """
    winners: Dict[str, EventCopy] = {}
    row_stats = csvio.RowStats()
    for path in event_paths:
        for row in csvio.iter_rows(
            path, csvio.EVENT_FIELDS, stats=row_stats
        ):
            _dedup_update(winners, row, stats)
    if stats is not None:
        stats.event_rows += row_stats.rows
        stats.csv_malformed += row_stats.malformed
        stats.unique_events += len(winners)
    return winners


#: Event dedup partitions; each holds at most 1/PARTITION_COUNT of the ids.
PARTITION_COUNT = 32


def _dedup_update(winners: Dict[str, EventCopy], row, stats) -> None:
    """Fold one parsed copy into the winners map for its partition.

    Only malformed CSV rows (already dropped by the reader) and empty ids never
    enter; no semantic check runs before dedup, so an invalid winning copy still
    stands and a later valid copy cannot rescue it.
    """
    event_id = row[0]
    if not event_id:
        if stats is not None:
            stats.empty_event_id += 1
        return
    ingest_ms = timestamps.parse_timestamp(row[3])
    if ingest_ms is None:
        if stats is not None:
            stats.invalid_ingest += 1
        return
    previous = winners.get(event_id)
    if previous is not None:
        if stats is not None:
            stats.duplicate_copies += 1
        if previous.ingest_ms <= ingest_ms:
            return
    # Interning keeps the ~2M repeated user IDs and the 50k product IDs as
    # single objects instead of one object per row.
    winners[event_id] = EventCopy(
        ingest_ms,
        sys.intern(row[1]),
        timestamps.parse_timestamp(row[2]),
        sys.intern(row[4]),
        sys.intern(row[5]),
        sys.intern(row[6]),
    )


def _first_field(raw: bytes) -> str:
    """First field of a raw line, quote-aware, for partition hashing."""
    if raw[:1] == b'"':
        end = raw.find(b'"', 1)
        if end < 0:
            return ""
        field = raw[1:end]
    else:
        end = raw.find(b",")
        field = raw if end < 0 else raw[:end]
    try:
        return field.decode("utf-8")
    except UnicodeDecodeError:
        return ""


def _partition_events(
    event_paths, scratch: str, stats: Optional[StageStats]
) -> List[str]:
    """Copy every raw event line into one of PARTITION_COUNT scratch files.

    The partition is chosen by ``event_id``, so all copies of an id land in the
    same file and can be deduplicated there without a global dictionary. Lines
    are copied as raw bytes, so quoting, ``\\r`` and malformed rows reach the
    real parser exactly as they were read.
    """
    paths = [
        os.path.join(scratch, f"event-part-{index:02d}.csv")
        for index in range(PARTITION_COUNT)
    ]
    handles = [open(path, "wb", buffering=1 << 20) for path in paths]
    try:
        for path in event_paths:
            with open(path, "rb") as handle:
                header = handle.readline()
                if header == b"":
                    continue
                if header.endswith(b"\n"):
                    header = header[:-1]
                fields = csvio.split_fields(header)
                if fields is None or len(fields) != csvio.EVENT_FIELDS:
                    raise csvio.CsvError(
                        f"{path}: header is not {csvio.EVENT_FIELDS} fields: "
                        f"{header[:120]!r}"
                    )
                for raw in handle:
                    if raw.endswith(b"\n"):
                        raw = raw[:-1]
                    if not raw:
                        # An empty line is malformed and never reaches a
                        # partition; count it here so diagnostics stay exact.
                        if stats is not None:
                            stats.csv_malformed += 1
                        continue
                    out = handles[hash(_first_field(raw)) % PARTITION_COUNT]
                    out.write(raw)
                    out.write(b"\n")
    finally:
        for handle in handles:
            handle.close()
    return paths


def dedup_events_partitioned(
    event_paths,
    scratch: str,
    window_start_ms: int,
    window_end_ms: int,
    products: Dict[str, str],
    person_names: Dict[str, str],
    stats: Optional[StageStats] = None,
) -> Dict[str, List[Tuple[int, int, Optional[str]]]]:
    """Dedup, validate and group events with bounded memory.

    Pass 1 partitions raw lines by ``event_id`` hash. Pass 2 dedups one
    partition at a time (smallest ``ingest_ts`` wins), validates the winners and
    appends them to the shared per-person lists, so at most
    ``1/PARTITION_COUNT`` of the dedup entries exist at once. Dedup still
    precedes validation for every id: all copies of an id are in one partition,
    so its winner is final before it is validated.
    """
    paths = _partition_events(event_paths, scratch, stats)
    by_person: Dict[str, List[Tuple[int, int, Optional[str]]]] = {}
    try:
        for path in paths:
            winners: Dict[str, EventCopy] = {}
            row_stats = csvio.RowStats()
            for row in csvio.iter_rows(
                path, csvio.EVENT_FIELDS, skip_header=False, stats=row_stats
            ):
                _dedup_update(winners, row, stats)
            if stats is not None:
                stats.event_rows += row_stats.rows
                stats.csv_malformed += row_stats.malformed
                stats.unique_events += len(winners)
            _group_by_person(
                winners.values(),
                window_start_ms,
                window_end_ms,
                products,
                person_names,
                stats,
                by_person,
            )
            winners.clear()
    finally:
        for path in paths:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
    return by_person


def _parse_price(text: str) -> Optional[int]:
    """Purchase price: ASCII digits only, value 1..10,000,000; else None."""
    if not text or not text.isascii() or not text.isdigit():
        return None
    digits = text.lstrip("0")
    if not digits or len(digits) > 8:
        # 0/000 is out of range; 9+ significant digits are over 10,000,000.
        return None
    cents = int(digits)
    return cents if cents <= MAX_PRICE_CENTS else None


def validate_copies(
    copies: Iterable[EventCopy],
    window_start_ms: int,
    window_end_ms: int,
    products: Dict[str, str],
    person_names: Dict[str, str],
    stats: Optional[StageStats] = None,
) -> Iterator[Tuple[str, int, int, Optional[str]]]:
    """Validate kept copies, drop late ones, and yield session-ready events.

    Yields ``(person, ts_ms, revenue_cents, category)`` where ``category`` is
    None for views and carts (their price is ignored whatever it holds) and the
    product's category for purchases.
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
        if ts_ms < window_start_ms or ts_ms >= window_end_ms:
            if stats is not None:
                stats.out_of_window += 1
            continue
        event_type = copy.event_type
        if event_type not in EVENT_TYPES:
            if stats is not None:
                stats.invalid_type += 1
            continue
        if not copy.product_id:
            if stats is not None:
                stats.invalid_product += 1
            continue
        if event_type == "purchase":
            cents = _parse_price(copy.price)
            if cents is None:
                if stats is not None:
                    stats.invalid_price += 1
                continue
            category: Optional[str] = products.get(
                copy.product_id, UNKNOWN_CATEGORY
            )
        else:
            cents = 0
            category = None
        if copy.ingest_ms - ts_ms > LATE_MS:
            if stats is not None:
                stats.late += 1
            continue
        if stats is not None:
            stats.events_kept += 1
        yield person_names.get(copy.user_id, copy.user_id), ts_ms, cents, category


def sessionize_person(
    person: str, events: List[Tuple[int, int, Optional[str]]]
) -> List[Session]:
    """Split one person's ``(ts, revenue, category)`` events into sessions.

    ``events`` is sorted in place by ``ts``. Equal timestamps give a zero gap
    and therefore share a session, and the gap is measured against the previous
    event's timestamp, so equal timestamps can never open a session.
    """
    events.sort(key=_first)
    sessions: List[Session] = []
    start_ms = previous_ms = events[0][0]
    count = 0
    revenue = 0
    purchases = 0
    categories: Optional[Dict[str, int]] = None
    for ts_ms, cents, category in events:
        if ts_ms - previous_ms > GAP_MS:
            sessions.append(
                _session(
                    person, start_ms, count, revenue, purchases, categories
                )
            )
            start_ms = ts_ms
            count = 0
            revenue = 0
            purchases = 0
            categories = None
        previous_ms = ts_ms
        count += 1
        if category is not None:
            purchases += 1
            revenue += cents
            if categories is None:
                categories = {category: cents}
            else:
                categories[category] = categories.get(category, 0) + cents
    sessions.append(
        _session(person, start_ms, count, revenue, purchases, categories)
    )
    return sessions


def _first(event: Tuple[int, int, Optional[str]]) -> int:
    return event[0]


def _session(
    person: str,
    start_ms: int,
    count: int,
    revenue: int,
    purchases: int,
    categories: Optional[Dict[str, int]],
) -> Session:
    return Session(
        person,
        start_ms,
        start_ms // _MS_PER_DAY,
        count,
        revenue,
        purchases,
        None if categories is None else tuple(sorted(categories.items())),
    )


def build_sessions(
    by_person: Dict[str, List[Tuple[int, int, Optional[str]]]],
    stats: Optional[StageStats] = None,
) -> List[Session]:
    """Sessionize every person; ``by_person`` is consumed."""
    if stats is not None:
        stats.persons = len(by_person)
    sessions: List[Session] = []
    for person, events in by_person.items():
        sessions.extend(sessionize_person(person, events))
    if stats is not None:
        stats.sessions = len(sessions)
    return sessions


def _group_by_person(
    copies: Iterable[EventCopy],
    window_start_ms: int,
    window_end_ms: int,
    products: Dict[str, str],
    person_names: Dict[str, str],
    stats: Optional[StageStats],
    by_person: Optional[Dict[str, List[Tuple[int, int, Optional[str]]]]] = None,
) -> Dict[str, List[Tuple[int, int, Optional[str]]]]:
    """Validate copies and collect surviving events per person."""
    if by_person is None:
        by_person = {}
    for person, ts_ms, cents, category in validate_copies(
        copies,
        window_start_ms,
        window_end_ms,
        products,
        person_names,
        stats,
    ):
        bucket = by_person.get(person)
        if bucket is None:
            by_person[person] = [(ts_ms, cents, category)]
        else:
            bucket.append((ts_ms, cents, category))
    return by_person


def run_event_stage(data_dir, log: bool = False, scratch=None) -> EventResult:
    """Run the whole event stage on a ``/data``-shaped directory.

    With a ``scratch`` directory, dedup is hash-partitioned through raw-line
    scratch files (bounded memory); without one it uses a single in-memory
    Python dictionary, which is what most unit tests and diagnostic scripts
    exercise.
    """
    data = Path(data_dir)
    started = time.perf_counter()

    with open(data / "manifest.json", encoding="utf-8") as handle:
        manifest = json.load(handle)
    window_start_ms = timestamps.parse_timestamp(manifest["window_start"])
    window_end_ms = timestamps.parse_timestamp(manifest["window_end"])
    if window_start_ms is None or window_end_ms is None:
        raise ValueError(
            f"unusable manifest window: {manifest['window_start']!r} .. "
            f"{manifest['window_end']!r}"
        )

    stats = StageStats()
    person_names = build_person_names(
        sorted((data / "links").glob("*.csv")), stats
    )
    products = read_products(data / "products.csv", stats)
    precedent = time.perf_counter()
    _log(
        log,
        f"[event-stage] links+products {precedent - started:.3f}s "
        f"ids={len(person_names)} products={len(products)}",
    )

    event_paths = sorted((data / "events").glob("*.csv"))
    partitioned = scratch is not None
    if partitioned:
        by_person = dedup_events_partitioned(
            event_paths,
            scratch,
            window_start_ms,
            window_end_ms,
            products,
            person_names,
            stats,
        )
        after_dedup = precedent
    else:
        copied = dedup_events(event_paths, stats)
        after_dedup = time.perf_counter()
        _log(
            log,
            f"[event-stage] read+dedup {after_dedup - precedent:.3f}s "
            f"unique_events={stats.unique_events}",
        )
        by_person = _group_by_person(
            copied.values(),
            window_start_ms,
            window_end_ms,
            products,
            person_names,
            stats,
        )
        copied.clear()

    after_validate = time.perf_counter()
    if partitioned:
        _log(
            log,
            f"[event-stage] dedup+validate+group "
            f"{after_validate - precedent:.3f}s unique_events="
            f"{stats.unique_events} kept={stats.events_kept} "
            f"persons={len(by_person)}",
        )
    else:
        _log(
            log,
            f"[event-stage] validate+group {after_validate - after_dedup:.3f}s "
            f"kept={stats.events_kept} persons={len(by_person)}",
        )

    sessions = build_sessions(by_person, stats)
    by_person.clear()
    finished = time.perf_counter()
    _log(
        log,
        f"[event-stage] sessionize {finished - after_validate:.3f}s "
        f"sessions={stats.sessions}",
    )
    _log(log, f"[event-stage] total {finished - started:.3f}s {stats.summary()}")

    return EventResult(
        sessions, person_names, window_start_ms, window_end_ms, stats
    )
