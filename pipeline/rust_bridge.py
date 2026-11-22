"""Opt-in bridge to the compiled Rust event/attribution pipeline.

The Rust binary consumes the same raw ``event_id`` partitions and writes
little-endian records for sessions and (optionally) campaign_daily rows.
Python keeps user_daily aggregation and both DuckDB Parquet writes.
"""
from __future__ import annotations

import json
import os
import resource
import shutil
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

import campaign_daily
import events
import timestamps

__all__ = ["binary_path", "run_stage", "read_sessions", "read_campaigns"]

_U32 = struct.Struct("<I")
_U16 = struct.Struct("<H")
_I32 = struct.Struct("<i")
_I64 = struct.Struct("<q")


def binary_path() -> str:
    """Locate the binary: explicit env, then local build, then image path."""
    explicit = os.environ.get("RUST_BIN")
    if explicit:
        return explicit
    root = Path(__file__).resolve().parents[1]
    candidates = (
        root / "rust" / "shopping-pipeline" / "target" / "release" / "shopping-pipeline",
        Path("/app/bin/shopping-pipeline"),
    )
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    raise FileNotFoundError(
        "Rust binary not found; set RUST_BIN or build rust/shopping-pipeline"
    )


def run_stage(data_dir, scratch: str, backend: str, log: bool = False):
    """Run Rust events (+ attribution for ``rust-all``) on ``data_dir``."""
    data = Path(data_dir)
    with open(data / "manifest.json", encoding="utf-8") as handle:
        manifest = json.load(handle)
    window_start = timestamps.parse_timestamp(manifest["window_start"])
    window_end = timestamps.parse_timestamp(manifest["window_end"])
    if window_start is None or window_end is None:
        raise ValueError("unusable manifest window")

    stats = events.StageStats()
    partition_count = int(os.environ.get("PARTITION_COUNT", "32"))
    workers = max(1, min(8, int(os.environ.get("RUST_WORKERS", os.cpu_count() or 1))))
    parts_dir = os.path.join(scratch, f"rust-parts-{os.getpid()}")
    os.makedirs(parts_dir, exist_ok=True)
    sessions_path = os.path.join(scratch, f"rust-sessions-{os.getpid()}.bin")
    user_csv = campaign_csv = None
    started = time.perf_counter()
    paths = []
    try:
        command = [
            binary_path(),
            "--links",
            str(data / "links"),
            "--products",
            str(data / "products.csv"),
            "--start-ms",
            str(window_start),
            "--end-ms",
            str(window_end),
            "--partitions",
            str(partition_count),
            "--workers",
            str(workers),
        ]
        if backend == "rust-all":
            # Rust owns raw partitioning and emits the two already-aggregated
            # tables as CSV, so Python never constructs millions of Session or
            # UserDaily objects. DuckDB still writes the Parquet files.
            user_csv = os.path.join(scratch, f"rust-user-daily-{os.getpid()}.csv")
            campaign_csv = os.path.join(
                scratch, f"rust-campaign-daily-{os.getpid()}.csv"
            )
            command += [
                "--events",
                str(data / "events"),
                "--parts-out",
                parts_dir,
                "--clicks",
                str(data / "clicks"),
                "--campaigns",
                str(data / "campaigns.csv"),
                "--user-daily-csv",
                user_csv,
                "--campaigns-csv",
                campaign_csv,
            ]
        else:
            paths = events._partition_events(
                sorted((data / "events").glob("*.csv")),
                parts_dir,
                stats,
                count=partition_count,
            )
            command += ["--parts", parts_dir, "--sessions-out", sessions_path]
        subprocess.run(
            command,
            check=True,
            stderr=sys.stderr if log else subprocess.DEVNULL,
        )
        if backend == "rust-all":
            sessions, rows = [], []
        else:
            sessions = read_sessions(sessions_path)
            rows = []
            stats.sessions = len(sessions)
            stats.persons = len({session.person for session in sessions})
            stats.events_kept = sum(session.events for session in sessions)
        if log:
            usage = resource.getrusage(resource.RUSAGE_CHILDREN)
            child = usage.ru_maxrss
            child_mb = child / (1024 * 1024) if sys.platform == "darwin" else child / 1024
            child_cpu = usage.ru_utime + usage.ru_stime
            child_wall = time.perf_counter() - started
            result_bytes = 0
            for path in (sessions_path, user_csv, campaign_csv):
                if path and os.path.exists(path):
                    result_bytes += os.path.getsize(path)
            partition_bytes = 0
            for root, _, files in os.walk(parts_dir):
                for name in files:
                    partition_bytes += os.path.getsize(os.path.join(root, name))
            print(
                f"[rust-pipeline] {backend} {time.perf_counter() - started:.3f}s "
                f"partitions={partition_count} workers={workers} "
                f"sessions={len(sessions)} rows={len(rows)} "
                f"child_peak={child_mb:.0f}MB child_cpu={child_cpu:.2f}s "
                f"child_wall={child_wall:.2f}s result_bytes={result_bytes} "
                f"partition_bytes={partition_bytes}",
                file=sys.stderr,
                flush=True,
            )
        return (
            sessions,
            rows,
            window_start,
            window_end,
            stats,
            user_csv,
            campaign_csv,
        )
    finally:
        for path in paths:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        for path in (sessions_path,):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        # The CSV files are returned to the caller; the caller removes them
        # after DuckDB has copied them to Parquet.
        shutil.rmtree(parts_dir, ignore_errors=True)


def _read(data: bytes, position: int, struct_type) -> Tuple[tuple, int]:
    values = struct_type.unpack_from(data, position)
    return values, position + struct_type.size


def _read_bytes(data: bytes, position: int) -> Tuple[bytes, int]:
    (length,), position = _read(data, position, _U32)
    return data[position : position + length], position + length


def read_sessions(path: str) -> List[events.Session]:
    """Decode the Rust sessions file into the existing Session namedtuples."""
    data = Path(path).read_bytes()
    position = 0
    (category_count,), position = _read(data, position, _U32)
    categories: List[str] = []
    for _ in range(category_count):
        raw, position = _read_bytes(data, position)
        categories.append(raw.decode("utf-8"))
    (session_count,), position = _read(data, position, _U32)
    sessions: List[events.Session] = []
    intern = sys.intern
    for _ in range(session_count):
        raw, position = _read_bytes(data, position)
        person = intern(raw.decode("utf-8"))
        (start,), position = _read(data, position, _I64)
        (day,), position = _read(data, position, _I32)
        (count,), position = _read(data, position, _U32)
        (revenue,), position = _read(data, position, _I64)
        (purchases,), position = _read(data, position, _U32)
        (ncat,), position = _read(data, position, _U16)
        pairs = []
        for _ in range(ncat):
            (category,), position = _read(data, position, _U16)
            (cents,), position = _read(data, position, _I64)
            pairs.append((categories[category], cents))
        sessions.append(
            events.Session(
                person,
                start,
                day,
                count,
                revenue,
                purchases,
                tuple(pairs) if pairs else None,
            )
        )
    return sessions


def read_campaigns(path: str) -> List[campaign_daily.CampaignDaily]:
    """Decode the Rust campaign_daily aggregate rows."""
    data = Path(path).read_bytes()
    position = 0
    (count,), position = _read(data, position, _U32)
    rows: List[campaign_daily.CampaignDaily] = []
    for _ in range(count):
        campaign, position = _read_bytes(data, position)
        channel, position = _read_bytes(data, position)
        (day,), position = _read(data, position, _I32)
        values = []
        for _ in range(4):
            (value,), position = _read(data, position, _I64)
            values.append(value)
        rows.append(
            campaign_daily.CampaignDaily(
                campaign.decode("utf-8"),
                channel.decode("utf-8"),
                day,
                values[0],
                values[1],
                values[2],
                values[3],
            )
        )
    return rows
