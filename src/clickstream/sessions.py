"""silver events -> sessionized events, one UTC day at a time, in date order.

Day D needs: silver(D) and carry(D-1), the sessions still open at the end of D-1. It writes
sessionized_events(D) and carry(D). Earlier days are never rewritten. If silver(D-1) exists but
has not been sessionized yet, we stop: days must be processed in order.

The session id is "<user_id>-<start epoch seconds>": it depends only on the data, not on which
run or engine produced it, so re-runs are idempotent and engines can be compared row by row.
"""

from __future__ import annotations

from datetime import date, timedelta

import pyarrow as pa

from clickstream.config import Paths, gap_seconds
from clickstream.io import (
    atomic_dir,
    connect,
    day_bounds,
    fingerprint_dir,
    fingerprint_values,
    list_partitions,
    parquet_glob,
    partition_dir,
    sql_str,
)
from clickstream.quality import DQReport
from clickstream.sessionize import CARRY_SCHEMA, empty_carry, run_engine
from clickstream.state import Manifest

SESSION_VERSION = "sessions-v1"


def load_events(paths: Paths, day: date) -> pa.Table:
    con = connect()
    src = sql_str(parquet_glob(partition_dir(paths.silver, day)))
    return con.execute(
        f"SELECT user_id, event_time, source_file, source_row FROM read_parquet({src}, hive_partitioning=false)"
    ).to_arrow_table()


def load_carry(paths: Paths, day: date) -> pa.Table:
    con = connect()
    src = sql_str(parquet_glob(partition_dir(paths.carry, day)))
    return (
        con.execute(f"SELECT * FROM read_parquet({src}, hive_partitioning=false)")
        .to_arrow_table()
        .cast(CARRY_SCHEMA)
    )


def carry_in(paths: Paths, day: date) -> tuple[pa.Table, str | None]:
    prev = day - timedelta(days=1)
    if prev not in list_partitions(paths.silver):
        return empty_carry(), None
    fp = fingerprint_dir(partition_dir(paths.carry, prev))
    if fp is None:
        raise RuntimeError(f"sessionize {prev} before {day}: days must be processed in order")
    return load_carry(paths, prev), fp


def _output_fp(paths: Paths, day: date) -> str | None:
    fps = [
        fingerprint_dir(partition_dir(paths.sessionized, day)),
        fingerprint_dir(partition_dir(paths.carry, day)),
    ]
    return None if None in fps else fingerprint_values(*fps)


def sessionize_day(
    paths: Paths, day: date, manifest: Manifest, engine: str = "duckdb", force: bool = False, log=print
) -> bool:
    key = day.isoformat()
    gap_s = gap_seconds()
    carry, carry_fp = carry_in(paths, day)
    silver_dir = partition_dir(paths.silver, day)
    in_fp = fingerprint_values(fingerprint_dir(silver_dir), carry_fp, gap_s, SESSION_VERSION)
    if not force and manifest.is_current("sessions", key, in_fp, _output_fp(paths, day)):
        return False

    start, end = day_bounds(day)
    assignments, carry_out = run_engine(engine, load_events(paths, day), carry, end, gap_s)

    con = connect()
    con.register("a", assignments)
    con.register("ci", carry)
    con.register("co", carry_out)
    con.execute(
        f"""CREATE TEMP TABLE se AS
            SELECT s.*, a.session_id, a.session_start
            FROM read_parquet({sql_str(parquet_glob(silver_dir))}, hive_partitioning=false) s
            LEFT JOIN a USING (source_file, source_row)"""
    )

    def q(sql: str, params: dict | None = None):
        return con.execute(sql, params or {}).fetchone()[0]

    report = DQReport("sessions", key)
    n_silver = q(
        f"SELECT count(*) FROM read_parquet({sql_str(parquet_glob(silver_dir))}, hive_partitioning=false)"
    )
    n_se = q("SELECT count(*) FROM se")
    report.error(
        "every_event_has_one_session",
        n_se == n_silver == len(assignments) and q("SELECT count(*) FROM se WHERE session_id IS NULL") == 0,
        {"silver": n_silver, "sessionized": n_se, "assignments": len(assignments)},
    )
    report.error(
        "session_id_belongs_to_user",
        q("SELECT count(*) FROM se WHERE split_part(session_id, '-', 1)::BIGINT <> user_id") == 0,
    )
    report.error(
        "session_start_not_after_event", q("SELECT count(*) FROM se WHERE session_start > event_time") == 0
    )
    report.error(
        "new_sessions_start_at_first_event",
        q(
            """SELECT count(*) FROM (SELECT session_id, min(session_start) st, min(event_time) mt FROM se GROUP BY 1)
               WHERE st >= $s AND st <> mt""",
            {"s": start},
        )
        == 0,
    )
    report.error(
        "continued_sessions_come_from_carry",
        q(
            """SELECT count(*) FROM (SELECT DISTINCT session_id, session_start FROM se WHERE session_start < $s) x
               ANTI JOIN ci USING (session_id, session_start)""",
            {"s": start},
        )
        == 0,
    )
    gaps = con.execute(
        f"""SELECT count(*) FILTER (prev_sid = session_id AND gap_us > {gap_s} * 1000000) AS gap_inside_session_too_big,
                   count(*) FILTER (prev_sid <> session_id AND gap_us <= {gap_s} * 1000000) AS split_without_gap
            FROM (
              SELECT se.session_id,
                     coalesce(lag(se.session_id) OVER w, ci.session_id) AS prev_sid,
                     epoch_us(se.event_time) - epoch_us(coalesce(lag(se.event_time) OVER w, ci.last_event_time)) AS gap_us
              FROM se LEFT JOIN ci ON ci.user_id = se.user_id
              WINDOW w AS (PARTITION BY se.user_id ORDER BY se.event_time, se.source_file, se.source_row)
            ) WHERE prev_sid IS NOT NULL"""
    ).fetchone()
    report.error("no_gap_over_threshold_inside_session", gaps[0] == 0, gaps[0])
    report.error("no_session_split_without_gap", gaps[1] == 0, gaps[1])
    report.error(
        "carry_out_within_gap_of_day_end",
        q(
            f"SELECT count(*) FROM co WHERE epoch_us(last_event_time) < epoch_us($e) - {gap_s} * 1000000",
            {"e": end},
        )
        == 0,
    )
    report.metric("engine", engine)
    report.metric("events", n_se)
    report.metric("sessions_touching_day", q("SELECT count(DISTINCT session_id) FROM se"))
    report.metric(
        "sessions_started",
        q("SELECT count(DISTINCT session_id) FROM se WHERE session_start >= $s", {"s": start}),
    )
    report.metric(
        "sessions_continued_from_previous_day",
        q("SELECT count(DISTINCT session_id) FROM se WHERE session_start < $s", {"s": start}),
    )
    report.metric("carry_in", len(carry))
    report.metric("carry_out", len(carry_out))

    with (
        atomic_dir(partition_dir(paths.sessionized, day)) as t_se,
        atomic_dir(partition_dir(paths.carry, day)) as t_c,
    ):
        con.execute(
            f"COPY (SELECT * FROM se ORDER BY user_id, event_time, source_file, source_row) "
            f"TO {sql_str(t_se / 'part-0.parquet')} (FORMAT parquet, COMPRESSION zstd)"
        )
        con.execute(
            f"COPY (SELECT * FROM co ORDER BY user_id) TO {sql_str(t_c / 'part-0.parquet')} (FORMAT parquet, COMPRESSION zstd)"
        )
        report.finish(paths.dq, log)

    manifest.record("sessions", key, in_fp, _output_fp(paths, day), engine=engine)
    return True


def build(
    paths: Paths, days: list[date] | None = None, engine: str = "duckdb", force: bool = False, log=print
) -> list[date]:
    manifest = Manifest(paths.manifest)
    days = days if days is not None else list_partitions(paths.silver)
    built = []
    for day in sorted(days):
        if sessionize_day(paths, day, manifest, engine=engine, force=force, log=log):
            built.append(day)
            log(f"sessions: built {day} ({engine})")
    log(f"sessions: {len(built)} built, {len(days) - len(built)} up to date")
    return built
