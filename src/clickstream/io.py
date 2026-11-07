"""Shared I/O helpers: DuckDB connections, date partitions, atomic writes, fingerprints."""

from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

from clickstream.config import DUCKDB_MEMORY_LIMIT, DUCKDB_TEMP_DIR


def connect(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """DuckDB connection with UTC session timezone (all dates in this project are UTC days)."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order=true")
    # Let large aggregations spill to disk instead of failing at the memory limit.
    con.execute(f"SET temp_directory='{DUCKDB_TEMP_DIR}'")
    if threads:
        con.execute(f"SET threads={threads}")
    return con


def partition_dir(table: Path, day: date, key: str = "event_date") -> Path:
    return table / f"{key}={day.isoformat()}"


def list_partitions(table: Path, key: str = "event_date") -> list[date]:
    if not table.exists():
        return []
    out = []
    for p in table.iterdir():
        if p.is_dir() and p.name.startswith(f"{key}=") and any(p.glob("*.parquet")):
            out.append(date.fromisoformat(p.name.split("=", 1)[1]))
    return sorted(out)


def parquet_glob(path: Path) -> str:
    return str(path / "*.parquet")


def day_bounds(day: date) -> tuple[datetime, datetime]:
    from datetime import UTC

    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return start, start + timedelta(days=1)


def date_range(start: date, end: date) -> Iterator[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


@contextmanager
def atomic_dir(target: Path) -> Iterator[Path]:
    """Yield a temp dir next to `target`; on success replace `target` with it in one rename.

    If the block raises (e.g. a data-quality error), `target` is left untouched.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".tmp-{target.name}-{uuid.uuid4().hex[:8]}"
    tmp.mkdir()
    try:
        yield tmp
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    old = None
    if target.exists():
        old = target.parent / f".old-{target.name}-{uuid.uuid4().hex[:8]}"
        target.rename(old)
    tmp.rename(target)
    if old is not None:
        shutil.rmtree(old, ignore_errors=True)


def fingerprint_files(paths: list[Path]) -> str:
    """sha256 over (name, bytes) of the given files, in sorted order."""
    h = hashlib.sha256()
    for p in sorted(paths):
        h.update(p.name.encode())
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
    return h.hexdigest()


FP_SIDECAR = ".fingerprint"


def fingerprint_dir(d: Path) -> str | None:
    """Content fingerprint of a partition directory's parquet files.

    Cached in a sidecar keyed by each file's (name, size, mtime_ns, inode). Writes in this
    project always create new files via rename, so a changed file never keeps its stat key.
    """
    files = sorted(d.glob("*.parquet")) if d.exists() else []
    if not files:
        return None
    stat_key = repr([(p.name, s.st_size, s.st_mtime_ns, s.st_ino) for p in files for s in [p.stat()]])
    sidecar = d / FP_SIDECAR
    try:
        cached_key, cached_fp = sidecar.read_text().split("\n", 1)
        if cached_key == stat_key:
            return cached_fp
    except (FileNotFoundError, ValueError):
        pass
    fp = fingerprint_files(files)
    with suppress(OSError):
        sidecar.write_text(f"{stat_key}\n{fp}")
    return fp


def fingerprint_values(*parts: object) -> str:
    """Fingerprint of plain values via canonical JSON (dict key order doesn't matter)."""
    payload = json.dumps(parts, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def sql_str(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"
