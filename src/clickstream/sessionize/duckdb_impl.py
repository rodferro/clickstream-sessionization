"""Sessionizer in DuckDB SQL: lag() over each user's events, flag gaps > threshold, running sum."""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from clickstream.io import connect

ORDER = "e.event_time, e.source_file, e.source_row"


def sessionize_day(
    events: pa.Table, carry: pa.Table, day_end: datetime, gap_s: int
) -> tuple[pa.Table, pa.Table]:
    con = connect()
    con.register("ev", events)
    con.register("carry", carry)
    con.execute(
        f"""
        CREATE TEMP TABLE a AS
        WITH x AS (
          SELECT e.user_id, e.event_time, e.source_file, e.source_row,
                 c.session_id AS carry_id, c.session_start AS carry_start,
                 coalesce(lag(e.event_time) OVER w, c.last_event_time) AS prev_t
          FROM ev e LEFT JOIN carry c ON c.user_id = e.user_id
          WINDOW w AS (PARTITION BY e.user_id ORDER BY {ORDER})
        ), y AS (
          SELECT *, CASE WHEN prev_t IS NULL OR epoch_us(event_time) - epoch_us(prev_t) > {gap_s} * 1000000
                         THEN 1 ELSE 0 END AS is_new
          FROM x
        ), z AS (
          SELECT *, sum(is_new) OVER (PARTITION BY user_id ORDER BY event_time, source_file, source_row
                                      ROWS UNBOUNDED PRECEDING) AS grp
          FROM y
        ), s AS (
          -- grp = 0: the user's first events today continue yesterday's open session
          SELECT *, CASE WHEN grp = 0 THEN carry_start
                         ELSE min(event_time) OVER (PARTITION BY user_id, grp) END AS session_start
          FROM z
        )
        SELECT source_file, source_row, user_id, event_time,
               CASE WHEN grp = 0 THEN carry_id
                    ELSE user_id::VARCHAR || '-' || (epoch_us(session_start) // 1000000)::VARCHAR END AS session_id,
               session_start
        FROM s
        """
    )
    assignments = con.execute(
        "SELECT source_file, source_row, session_id, session_start FROM a ORDER BY source_file, source_row"
    ).to_arrow_table()
    carry_out = con.execute(
        f"""SELECT user_id, arg_max(session_id, session_start) AS session_id,
                   max(session_start) AS session_start, max(event_time) AS last_event_time
            FROM a GROUP BY user_id
            HAVING epoch_us(max(event_time)) >= epoch_us($end) - {gap_s} * 1000000
            ORDER BY user_id""",
        {"end": day_end},
    ).to_arrow_table()
    return assignments, carry_out
