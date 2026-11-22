"""Parallel execution of the existing event hash partitions (Phase 6 experiment).

The sequential path in :mod:`events` already partitions raw lines by
``event_id``.  Every copy of an id is therefore in exactly one partition, which
makes per-partition dedup globally correct: this module runs those partitions in
forked worker processes, performs the *same* dedup and validation there, and
writes compact binary records for the parent to merge by person.

What is parallel: parsing, ``event_id`` dedup, timestamp/product/price/late
validation.  What is not: links and products (read once before the workers),
the global per-person merge, and sessionization, which must see all of a
person's events even when they came from different event partitions.

The binary record keeps strings out of the IPC path: ``person`` is written once
per surviving event as length-prefixed UTF-8, followed by ``ts`` (i64),
``revenue_cents`` (i32) and a small category id (i32, -1 for views/carts).
"""
from __future__ import annotations

import multiprocessing
import os
import resource
import struct
import sys
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Optional, Tuple

import csvio
import events

__all__ = ["dedup_events_parallel", "LAST_PEAKS_MB"]

_REC = struct.Struct("<qii")
_LEN = struct.Struct("<I")

#: Counters a worker can produce; all are additive across partitions.
_COUNTERS = (
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
)

#: Per-worker peak RSS in MiB, filled by the last call.  The aggregate is the
#: parent peak plus these, an upper bound on simultaneous process memory.
LAST_PEAKS_MB: List[float] = []

_CTX: dict = {}


def _peak_rss_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return peak / (1024 * 1024)
    return peak / 1024


def _process(task):
    """Dedup, validate and encode one contiguous chunk of partitions."""
    index, part_paths, out_path = task
    local = events.StageStats()
    category_ids = _CTX["category_ids"]
    person_names = _CTX["person_names"]
    products = _CTX["products"]
    window_start = _CTX["window_start_ms"]
    window_end = _CTX["window_end_ms"]
    pack = _REC.pack
    written = bytearray()
    handle = open(out_path, "wb", buffering=1 << 20)
    try:
        for path in part_paths:
            winners: Dict[str, events.EventCopy] = {}
            row_stats = csvio.RowStats()
            for row in csvio.iter_rows(
                path,
                csvio.EVENT_FIELDS,
                skip_header=False,
                stats=row_stats,
            ):
                events._dedup_update(winners, row, local)
            local.event_rows += row_stats.rows
            local.csv_malformed += row_stats.malformed
            local.unique_events += len(winners)
            for copy in winners.values():
                # Exact copy of events.validate_copies, including the order of
                # the checks and which counter each rejection increments.
                if not copy.user_id:
                    local.invalid_user += 1
                    continue
                ts_ms = copy.ts_ms
                if ts_ms is None:
                    local.invalid_ts += 1
                    continue
                if ts_ms < window_start or ts_ms >= window_end:
                    local.out_of_window += 1
                    continue
                event_type = copy.event_type
                if event_type not in events.EVENT_TYPES:
                    local.invalid_type += 1
                    continue
                if not copy.product_id:
                    local.invalid_product += 1
                    continue
                if event_type == "purchase":
                    cents = events._parse_price(copy.price)
                    if cents is None:
                        local.invalid_price += 1
                        continue
                    category = category_ids[
                        products.get(copy.product_id, events.UNKNOWN_CATEGORY)
                    ]
                else:
                    cents = 0
                    category = -1
                if copy.ingest_ms - ts_ms > events.LATE_MS:
                    local.late += 1
                    continue
                local.events_kept += 1
                name = person_names.get(copy.user_id, copy.user_id).encode("utf-8")
                written += _LEN.pack(len(name))
                written += name
                written += pack(ts_ms, cents, category)
            winners.clear()
            if len(written) >= (1 << 20):
                handle.write(written)
                written.clear()
    finally:
        handle.write(written)
        handle.close()
    return (
        index,
        _peak_rss_mb(),
        {field: getattr(local, field) for field in _COUNTERS},
    )


def dedup_events_parallel(
    event_paths,
    scratch: str,
    window_start_ms: int,
    window_end_ms: int,
    products: Dict[str, str],
    person_names: Dict[str, str],
    stats: Optional[events.StageStats] = None,
    workers: int = 1,
    log: bool = False,
) -> Dict[str, List[Tuple[int, int, Optional[str]]]]:
    """Partition, then dedup/validate/group with ``workers`` processes.

    Partitioning stays in the parent so all copies of an ``event_id`` still meet
    in one file.  Each worker owns whole partitions and applies the sequential
    rules unchanged; only the resulting records cross the process boundary.
    """
    paths = events._partition_events(event_paths, scratch, stats)
    if not paths:
        return {}
    count = max(1, min(workers, len(paths)))
    size = (len(paths) + count - 1) // count
    chunks = [paths[i * size : (i + 1) * size] for i in range(count)]
    categories = sorted(set(products.values()) | {events.UNKNOWN_CATEGORY})
    _CTX.clear()
    _CTX.update(
        {
            "category_ids": {name: i for i, name in enumerate(categories)},
            "person_names": person_names,
            "products": products,
            "window_start_ms": window_start_ms,
            "window_end_ms": window_end_ms,
        }
    )
    out_paths = [
        os.path.join(scratch, f"event-worker-{index}.bin")
        for index in range(count)
    ]
    tasks = [
        (index, chunks[index], out_paths[index])
        for index in range(count)
        if chunks[index]
    ]
    totals = {field: 0 for field in _COUNTERS}
    global LAST_PEAKS_MB
    LAST_PEAKS_MB = []
    by_person: Dict[str, List[Tuple[int, int, Optional[str]]]] = {}
    try:
        context = multiprocessing.get_context("fork")
        with ProcessPoolExecutor(
            max_workers=len(tasks), mp_context=context
        ) as pool:
            results = list(pool.map(_process, tasks))
        for _index, peak, counters in results:
            LAST_PEAKS_MB.append(peak)
            for field, value in counters.items():
                totals[field] += value

        name_cache: Dict[bytes, str] = {}
        unpack = _REC.unpack_from
        read_len = _LEN.unpack_from
        for out_path in out_paths:
            try:
                with open(out_path, "rb") as handle:
                    data = handle.read()
            except FileNotFoundError:
                continue
            position = 0
            end = len(data)
            while position < end:
                length = read_len(data, position)[0]
                position += _LEN.size
                raw_name = data[position : position + length]
                position += length
                name = name_cache.get(raw_name)
                if name is None:
                    name = sys.intern(raw_name.decode("utf-8"))
                    name_cache[raw_name] = name
                ts_ms, cents, category_id = unpack(data, position)
                position += _REC.size
                category = None if category_id < 0 else categories[category_id]
                bucket = by_person.get(name)
                if bucket is None:
                    by_person[name] = [(ts_ms, cents, category)]
                else:
                    bucket.append((ts_ms, cents, category))
    finally:
        for path in paths:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        for path in out_paths:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

    if stats is not None:
        for field, value in totals.items():
            setattr(stats, field, getattr(stats, field) + value)
    events._log(
        log,
        f"[event-stage] parallel workers={len(tasks)} "
        f"worker_peak_sum={sum(LAST_PEAKS_MB):.0f}MB",
    )
    return by_person
