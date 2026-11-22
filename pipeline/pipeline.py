#!/usr/bin/env python3
"""Shopping Sessions: the complete pipeline.

    events/*.csv   --+
    clicks/*.csv   --+--> parse, dedup, validate, sessionize, attribute
    links/*.csv    --+         |
    products.csv   ------------+--> /output/user_daily/*.parquet
    campaigns.csv  ------------+--> /output/campaign_daily/*.parquet
    manifest.json  ------------+

The event and click stages own the rules and their order (see ``events.py`` and
``clicks.py``); this file only sequences them, aggregates, and writes Parquet.

Memory: the global event deduplication dictionary is the largest structure at
full size, so ``events.run_event_stage`` receives the scratch directory and
dedups in hash partitions of raw lines instead: all copies of an event_id share
one partition, so only 1/32 of the dedup entries exist at once. DuckDB is used
only to write the two small output tables.

Aggregation is streamed: ``clicks.run_attribution_stream`` hands each session's
credit and assists to a ``CampaignDailyBuilder`` that keeps one counter entry
per ``(campaign, day)`` output row. No per-session attribution list and no
session x click product is ever built.

Writing: DuckDB's Python ``executemany`` inserts about 1,600 rows/s here, which
would be ~an hour at full size, so aggregated rows go through a CSV in
``/scratch`` and one ``COPY`` per table instead. That keeps ``/output`` free of
temporary files and uses DuckDB only for the typed Parquet write.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import json
import os
import resource
import sys
import tempfile
import time
from pathlib import Path

import duckdb

import campaign_daily
import clicks
import csvio
import events
import user_daily

#: File order and explicit DuckDB types; header=false, so nothing is inferred.
USER_COLUMNS = (
    ("user_id", "VARCHAR"),
    ("day", "INTEGER"),
    ("sessions", "BIGINT"),
    ("events", "BIGINT"),
    ("revenue_cents", "BIGINT"),
    ("top_category", "VARCHAR"),
)
USER_SELECT = (
    "user_id, (DATE '1970-01-01' + day) AS day, sessions, events, "
    "revenue_cents, top_category"
)
USER_EMPTY_SELECT = (
    "SELECT NULL::VARCHAR AS user_id, NULL::DATE AS day, "
    "NULL::BIGINT AS sessions, NULL::BIGINT AS events, "
    "NULL::BIGINT AS revenue_cents, NULL::VARCHAR AS top_category WHERE false"
)

CAMPAIGN_COLUMNS = (
    ("campaign_id", "VARCHAR"),
    ("channel", "VARCHAR"),
    ("day", "INTEGER"),
    ("sessions", "BIGINT"),
    ("purchasing_sessions", "BIGINT"),
    ("revenue_cents", "BIGINT"),
    ("assisted_sessions", "BIGINT"),
)
CAMPAIGN_SELECT = (
    "campaign_id, channel, (DATE '1970-01-01' + day) AS day, sessions, "
    "purchasing_sessions, revenue_cents, assisted_sessions"
)
CAMPAIGN_EMPTY_SELECT = (
    "SELECT NULL::VARCHAR AS campaign_id, NULL::VARCHAR AS channel, "
    "NULL::DATE AS day, NULL::BIGINT AS sessions, "
    "NULL::BIGINT AS purchasing_sessions, NULL::BIGINT AS revenue_cents, "
    "NULL::BIGINT AS assisted_sessions WHERE false"
)


def _log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _peak_rss_mb() -> float:
    """Peak resident set size; Linux reports KiB, macOS bytes."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return peak / (1024 * 1024)
    return peak / 1024


def _columns_sql(columns) -> str:
    return (
        "{"
        + ", ".join(f"'{name}': '{kind}'" for name, kind in columns)
        + "}"
    )


def _copy_csv(con, csv_path: str, columns, select_list: str, out_path: str) -> None:
    con.execute(
        f"COPY (SELECT {select_list} FROM read_csv('{csv_path}', "
        f"header=false, columns={_columns_sql(columns)})) "
        f"TO '{out_path}' (FORMAT parquet)"
    )


def _write_user_daily(con, rows, out_dir: str, scratch: str) -> None:
    out_path = os.path.join(out_dir, "user_daily", "part-0.parquet")
    started = time.perf_counter()
    if not rows:
        # An empty table still has to be a valid Parquet file with this schema.
        # A typed empty SELECT is used rather than an empty scratch CSV, which
        # DuckDB's reader cannot sniff.
        con.execute(
            f"COPY ({USER_EMPTY_SELECT}) TO '{out_path}' (FORMAT parquet)"
        )
    else:
        csv_path = os.path.join(scratch, "user_daily.csv")
        try:
            csvio.write_rows(csv_path, rows)
            _copy_csv(con, csv_path, USER_COLUMNS, USER_SELECT, out_path)
        finally:
            _remove(csv_path)
    _log(
        f"[pipeline] write user_daily {time.perf_counter() - started:.3f}s "
        f"rows={len(rows)}"
    )


def _write_user_daily_csv(con, csv_path: str, out_dir: str) -> None:
    """Write user_daily from the Rust CSV handoff, without Python row objects."""
    out_path = os.path.join(out_dir, "user_daily", "part-0.parquet")
    started = time.perf_counter()
    if os.path.getsize(csv_path) == 0:
        con.execute(
            f"COPY ({USER_EMPTY_SELECT}) TO '{out_path}' (FORMAT parquet)"
        )
    else:
        _copy_csv(con, csv_path, USER_COLUMNS, USER_SELECT, out_path)
    _log(f"[pipeline] write user_daily {time.perf_counter() - started:.3f}s")


def _write_campaign_daily_csv(con, csv_path: str, out_dir: str) -> None:
    out_path = os.path.join(out_dir, "campaign_daily", "part-0.parquet")
    started = time.perf_counter()
    if os.path.getsize(csv_path) == 0:
        con.execute(
            f"COPY ({CAMPAIGN_EMPTY_SELECT}) TO '{out_path}' (FORMAT parquet)"
        )
    else:
        _copy_csv(
            con, csv_path, CAMPAIGN_COLUMNS, CAMPAIGN_SELECT, out_path
        )
    _log(f"[pipeline] write campaign_daily {time.perf_counter() - started:.3f}s")


def _write_campaign_daily(con, rows, out_dir: str, scratch: str) -> None:
    out_path = os.path.join(out_dir, "campaign_daily", "part-0.parquet")
    started = time.perf_counter()
    if not rows:
        con.execute(
            f"COPY ({CAMPAIGN_EMPTY_SELECT}) TO '{out_path}' (FORMAT parquet)"
        )
    else:
        csv_path = os.path.join(scratch, "campaign_daily.csv")
        try:
            csvio.write_rows(csv_path, rows)
            _copy_csv(
                con, csv_path, CAMPAIGN_COLUMNS, CAMPAIGN_SELECT, out_path
            )
        finally:
            _remove(csv_path)
    _log(
        f"[pipeline] write campaign_daily "
        f"{time.perf_counter() - started:.3f}s rows={len(rows)}"
    )


def _remove(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def _use_scratch(preferred: str) -> str:
    """Return a writable scratch directory.

    The scorer always mounts a writable ``/scratch``. A local run (for example
    ``scripts/run_fixtures.py``, which does not set ``SCRATCH_DIR``) may not
    have one, so fall back to a private temporary directory instead of failing.
    """
    probe = os.path.join(preferred, f".write-probe-{os.getpid()}")
    try:
        os.makedirs(preferred, exist_ok=True)
        with open(probe, "w"):
            pass
        os.remove(probe)
        return preferred
    except OSError:
        fallback = tempfile.mkdtemp(prefix="shopping-sessions-")
        _log(f"[pipeline] scratch {preferred} is not writable; using {fallback}")
        return fallback


def _connect(scratch: str, ncpu: int, mem_gb: int):
    """In-memory DuckDB configured to spill instead of growing without bound.

    A quarter of the container's memory is plenty for the dedup sort and the
    two small output writes; leaving the rest for Python is what keeps the
    process inside the 24 GB limit.
    """
    con = duckdb.connect()
    con.execute(f"SET threads = {ncpu}")
    con.execute(f"SET memory_limit = '{max(1, int(mem_gb * 0.25))}GB'")
    temp_dir = os.path.join(scratch, "duckdb_tmp")
    os.makedirs(temp_dir, exist_ok=True)
    con.execute(f"SET temp_directory = '{temp_dir}'")
    con.execute("SET preserve_insertion_order = false")
    return con


def main() -> None:
    data_dir = os.environ.get("DATA_DIR", "/data")
    out_dir = os.environ.get("OUTPUT_DIR", "/output")
    scratch = os.environ.get("SCRATCH_DIR", "/scratch")
    ncpu = int(os.environ.get("NCPU", os.cpu_count() or 1))
    mem_gb = int(os.environ.get("MEM_GB", "8"))
    event_workers = int(os.environ.get("EVENT_WORKERS", "1"))

    started = time.perf_counter()
    # Both output folders must exist even if a table ends up empty.
    for table in ("user_daily", "campaign_daily"):
        os.makedirs(os.path.join(out_dir, table), exist_ok=True)
    scratch = _use_scratch(scratch)
    with open(os.path.join(data_dir, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    _log(f"window: {manifest['window_start']} to {manifest['window_end']}")

    # 1-7: read manifest/links/lookups, parse + dedup + validate events,
    # sessionize. Dedup is hash-partitioned, so the 60M-entry Python dict never
    # exists; only one partition's winners are resident at a time.
    backend = os.environ.get("PIPELINE_BACKEND", "python")
    if backend in ("rust", "rust-all"):
        import rust_bridge

        (
            sessions,
            rust_rows,
            window_start_ms,
            window_end_ms,
            rust_stats,
            rust_user_csv,
            rust_campaign_csv,
        ) = rust_bridge.run_stage(data_dir, scratch, backend, log=True)
        person_names = (
            events.build_person_names(
                sorted((Path(data_dir) / "links").glob("*.csv")), rust_stats
            )
            if backend == "rust"
            else {}
        )
        event_result = events.EventResult(
            sessions, person_names, window_start_ms, window_end_ms, rust_stats
        )
    else:
        event_result = events.run_event_stage(
            data_dir, log=True, scratch=scratch, workers=event_workers
        )
        rust_user_csv = rust_campaign_csv = None
    _log(f"[pipeline] peak after events {_peak_rss_mb():.0f}MB")

    if backend == "rust-all":
        con = _connect(scratch, ncpu, mem_gb)
        try:
            _write_user_daily_csv(con, rust_user_csv, out_dir)
            _write_campaign_daily_csv(con, rust_campaign_csv, out_dir)
        finally:
            con.close()
            _remove(rust_user_csv)
            _remove(rust_campaign_csv)
        _log(
            f"[pipeline] total {time.perf_counter() - started:.3f}s "
            f"peak_rss={_peak_rss_mb():.0f}MB"
        )
        return

    user_rows = user_daily.build_user_daily(event_result.sessions)
    _log(
        f"[pipeline] user_daily aggregate "
        f"{time.perf_counter() - started:.3f}s rows={len(user_rows)}"
    )

    con = _connect(scratch, ncpu, mem_gb)
    try:
        # Write user_daily first, then free its rows before attribution so the
        # session list and the user aggregates are not resident together with
        # the attribution scratch.
        _write_user_daily(con, user_rows, out_dir, scratch)
        del user_rows

        # 8-9: stream credit + assists into campaign counters (no list), or
        # consume the Rust attribution rows for the hybrid backend.
        if backend == "rust-all":
            campaign_rows = rust_rows
        else:
            builder = campaign_daily.CampaignDailyBuilder()
            stream = clicks.run_attribution_stream(
                event_result, data_dir, builder.add, log=True
            )
            campaign_rows = builder.rows(stream.campaign_channels)
        _log(
            f"[pipeline] campaign_daily rows={len(campaign_rows)} "
            f"peak={_peak_rss_mb():.0f}MB"
        )
        del event_result

        _write_campaign_daily(con, campaign_rows, out_dir, scratch)
    finally:
        con.close()

    _log(
        f"[pipeline] total {time.perf_counter() - started:.3f}s "
        f"peak_rss={_peak_rss_mb():.0f}MB"
    )


if __name__ == "__main__":
    main()
