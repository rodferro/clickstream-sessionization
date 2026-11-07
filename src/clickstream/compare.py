"""Run several sessionizers over the same silver days and check they agree exactly.

Each engine chains its *own* carry-over from day to day (starting from the pipeline's stored
carry for the day before the range, or empty), so a divergence on one day would propagate and
be caught, rather than being masked by a shared state.
"""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from clickstream.config import Paths, gap_seconds
from clickstream.io import day_bounds, fingerprint_dir, partition_dir
from clickstream.sessionize import empty_carry, run_engine
from clickstream.sessions import load_carry, load_events


def _diff_rows(a: pa.Table, b: pa.Table) -> int:
    if a.num_rows != b.num_rows:
        return abs(a.num_rows - b.num_rows) + min(a.num_rows, b.num_rows)
    if a.num_rows == 0:
        return 0
    if a.schema != b.schema:
        return a.num_rows
    same = None
    for col in a.column_names:
        x, y = a[col], b[col]
        eq = pc.or_(pc.fill_null(pc.equal(x, y), False), pc.and_(pc.is_null(x), pc.is_null(y)))
        same = eq if same is None else pc.and_(same, eq)
    return a.num_rows - pc.sum(same.cast(pa.int64())).as_py()


def compare(paths: Paths, days: list[date], engines: list[str], log=print) -> dict:
    gap_s = gap_seconds()
    prev = days[0] - timedelta(days=1)
    start_carry = (
        load_carry(paths, prev) if fingerprint_dir(partition_dir(paths.carry, prev)) else empty_carry()
    )
    carries = {e: start_carry for e in engines}
    base = engines[0]
    result = {
        "engines": engines,
        "gap_seconds": gap_s,
        "days": [],
        "rows_compared": 0,
        "sessions_started": 0,
        "mismatched_rows": {e: 0 for e in engines[1:]},
        "mismatched_carry_rows": {e: 0 for e in engines[1:]},
        "seconds": {e: 0.0 for e in engines},
    }
    for day in days:
        events = load_events(paths, day)
        _, end = day_bounds(day)
        start, _ = day_bounds(day)
        outs = {}
        for e in engines:
            t = time.perf_counter()
            outs[e] = run_engine(e, events, carries[e], end, gap_s)
            result["seconds"][e] += time.perf_counter() - t
            carries[e] = outs[e][1]
        a0, c0 = outs[base]
        day_rec = {"day": day.isoformat(), "rows": a0.num_rows}
        for e in engines[1:]:
            a, c = outs[e]
            ma, mc = _diff_rows(a0, a), _diff_rows(c0, c)
            result["mismatched_rows"][e] += ma
            result["mismatched_carry_rows"][e] += mc
            day_rec[f"mismatch_{e}"] = ma + mc
        result["rows_compared"] += a0.num_rows
        starts = pc.greater_equal(a0["session_start"], pa.scalar(start, a0["session_start"].type))
        result["sessions_started"] += len(pc.unique(a0.filter(starts)["session_id"]))
        result["days"].append(day_rec)
        log(
            f"compare {day}: {a0.num_rows:,} rows, "
            + ", ".join(f"{k}={v}" for k, v in day_rec.items() if k.startswith("mismatch"))
        )
    result["seconds"] = {k: round(v, 2) for k, v in result["seconds"].items()}
    result["agree"] = all(v == 0 for v in result["mismatched_rows"].values()) and all(
        v == 0 for v in result["mismatched_carry_rows"].values()
    )
    return result


def write_result(result: dict, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1) + "\n")
