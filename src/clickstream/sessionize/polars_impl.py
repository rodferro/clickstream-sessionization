"""Sessionizer in Polars: sort, diff within user, cumulative sum of 'new session' flags."""

from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pyarrow as pa


def sessionize_day(
    events: pa.Table, carry: pa.Table, day_end: datetime, gap_s: int
) -> tuple[pa.Table, pa.Table]:
    ev = pl.from_arrow(events)
    c = pl.from_arrow(carry).rename(
        {"session_id": "carry_id", "session_start": "carry_start", "last_event_time": "carry_last"}
    )
    gap_us = gap_s * 1_000_000
    df = (
        ev.join(c, on="user_id", how="left")
        .sort("user_id", "event_time", "source_file", "source_row")
        .with_columns(
            pl.coalesce(pl.col("event_time").shift(1).over("user_id"), pl.col("carry_last")).alias("prev_t")
        )
        .with_columns(
            (
                pl.col("prev_t").is_null()
                | ((pl.col("event_time") - pl.col("prev_t")).dt.total_microseconds() > gap_us)
            )
            .cast(pl.Int64)
            .cum_sum()
            .over("user_id")
            .alias("grp")
        )
        .with_columns(
            pl.when(pl.col("grp") == 0)
            .then(pl.col("carry_start"))
            .otherwise(pl.col("event_time").min().over("user_id", "grp"))
            .alias("session_start")
        )
        .with_columns(
            pl.when(pl.col("grp") == 0)
            .then(pl.col("carry_id"))
            .otherwise(
                pl.concat_str(
                    pl.col("user_id").cast(pl.String),
                    pl.lit("-"),
                    pl.col("session_start").dt.epoch("s").cast(pl.String),
                )
            )
            .alias("session_id")
        )
    )
    assignments = df.select("source_file", "source_row", "session_id", "session_start").sort(
        "source_file", "source_row"
    )
    horizon = day_end - timedelta(seconds=gap_s)
    carry_out = (
        df.group_by("user_id")
        .agg(
            pl.col("session_id").sort_by("session_start").last(),
            pl.col("session_start").max(),
            pl.col("event_time").max().alias("last_event_time"),
        )
        .filter(pl.col("last_event_time") >= horizon)
        .sort("user_id")
    )
    return assignments.to_arrow(), carry_out.to_arrow()
