"""Three independent sessionizers behind one interface.

    sessionize_day(events, carry, day_end, gap_s) -> (assignments, carry_out)

events:      Arrow table [user_id, event_time, source_file, source_row] for one UTC day
carry:       Arrow table CARRY_SCHEMA, sessions still open at the end of the previous day
assignments: Arrow table [source_file, source_row, session_id, session_start], sorted by
             (source_file, source_row)
carry_out:   Arrow table CARRY_SCHEMA sorted by user_id, sessions that can continue past day_end

`duckdb` and `polars` are the production engines; `python` is the standard-library reference
(reference.py). The Arrow <-> tuple conversion for it lives here so reference.py stays stdlib-only.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

import pyarrow as pa

from clickstream.sessionize import duckdb_impl, polars_impl, reference

TS = pa.timestamp("us", tz="UTC")
CARRY_SCHEMA = pa.schema(
    [("user_id", pa.int64()), ("session_id", pa.string()), ("session_start", TS), ("last_event_time", TS)]
)
ASSIGNMENT_SCHEMA = pa.schema(
    [
        ("source_file", pa.string()),
        ("source_row", pa.int64()),
        ("session_id", pa.string()),
        ("session_start", TS),
    ]
)


def empty_carry() -> pa.Table:
    return CARRY_SCHEMA.empty_table()


def _python_day(
    events: pa.Table, carry: pa.Table, day_end: datetime, gap_s: int
) -> tuple[pa.Table, pa.Table]:
    def us(col: pa.ChunkedArray) -> list[int]:
        return col.cast(pa.int64()).to_pylist()

    rows = zip(
        events["user_id"].to_pylist(),
        us(events["event_time"]),
        events["source_file"].to_pylist(),
        events["source_row"].to_pylist(),
        strict=True,
    )
    carry_in = {
        u: reference.OpenSession(sid, start, last)
        for u, sid, start, last in zip(
            carry["user_id"].to_pylist(),
            carry["session_id"].to_pylist(),
            us(carry["session_start"]),
            us(carry["last_event_time"]),
            strict=True,
        )
    }
    end_us = int(day_end.timestamp()) * reference.US
    assigned, carry_out = reference.sessionize_batch(rows, carry_in, end_us, gap_s)
    assigned.sort(key=lambda r: (r[0], r[1]))
    files, rows_, sids, starts = zip(*assigned, strict=True) if assigned else ((), (), (), ())
    assignments = pa.table(
        [
            pa.array(files, pa.string()),
            pa.array(rows_, pa.int64()),
            pa.array(sids, pa.string()),
            pa.array(starts, pa.int64()).cast(TS),
        ],
        schema=ASSIGNMENT_SCHEMA,
    )
    users = sorted(carry_out)
    carry_tbl = pa.table(
        [
            pa.array(users, pa.int64()),
            pa.array([carry_out[u].session_id for u in users], pa.string()),
            pa.array([carry_out[u].start_us for u in users], pa.int64()).cast(TS),
            pa.array([carry_out[u].last_us for u in users], pa.int64()).cast(TS),
        ],
        schema=CARRY_SCHEMA,
    )
    return assignments, carry_tbl


Sessionizer = Callable[[pa.Table, pa.Table, datetime, int], tuple[pa.Table, pa.Table]]

ENGINES: dict[str, Sessionizer] = {
    "duckdb": duckdb_impl.sessionize_day,
    "polars": polars_impl.sessionize_day,
    "python": _python_day,
}


def run_engine(name: str, events: pa.Table, carry: pa.Table, day_end: datetime, gap_s: int):
    """Run an engine and normalise its output types so engines can be compared column by column."""
    assignments, carry_out = ENGINES[name](events, carry, day_end, gap_s)
    return assignments.select(ASSIGNMENT_SCHEMA.names).cast(ASSIGNMENT_SCHEMA), carry_out.select(
        CARRY_SCHEMA.names
    ).cast(CARRY_SCHEMA)
