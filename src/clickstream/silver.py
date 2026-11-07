"""bronze -> silver, one UTC day at a time.

Rules, in order:
1. Quarantine (row moved to silver/quarantine with a reason): null user_id / product_id /
   event_time / price, event_type outside the known set, negative price, event_time outside the
   partition day.
2. Drop exact duplicates over the 9 source columns, keeping the first copy by (source_file,
   source_row). Counted per event_type; purchase duplicates and their price sum are reported
   separately because they would inflate revenue.
3. Keep but flag: zero price (`is_zero_price`), null user_session (not used for sessionization).
4. Per user-day bot flags (thresholds in config) go to silver/user_day_flags.

Rows are written sorted by (user_id, event_time, source_file, source_row).
"""

from __future__ import annotations

import statistics
from datetime import date

from clickstream import config
from clickstream.config import Paths
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
from clickstream.state import Manifest

SRC = ", ".join(config.SOURCE_COLUMNS)
RULES_VERSION = "silver-v1"


def _input_fp(paths: Paths, day: date) -> str | None:
    fp = fingerprint_dir(partition_dir(paths.bronze, day))
    if fp is None:
        return None
    return fingerprint_values(
        fp,
        RULES_VERSION,
        config.EVENT_TYPES,
        config.BOT_MAX_EVENTS_PER_MINUTE,
        config.BOT_MAX_EVENTS_PER_DAY,
    )


def _output_dirs(paths: Paths, day: date):
    return [partition_dir(t, day) for t in (paths.silver, paths.quarantine, paths.bot_flags)]


def _output_fp(paths: Paths, day: date) -> str | None:
    fps = [fingerprint_dir(d) for d in _output_dirs(paths, day)]
    return None if any(f is None for f in fps) else fingerprint_values(*fps)


def build_day(paths: Paths, day: date, manifest: Manifest, force: bool = False, log=print) -> bool:
    """Build silver for one day. Returns True if (re)built, False if skipped as current."""
    key = day.isoformat()
    in_fp = _input_fp(paths, day)
    if in_fp is None:
        raise FileNotFoundError(f"no bronze data for {key}")
    if not force and manifest.is_current("silver", key, in_fp, _output_fp(paths, day)):
        return False

    start, end = day_bounds(day)
    con = connect()
    con.execute(
        f"CREATE TEMP TABLE b AS SELECT * FROM read_parquet({sql_str(parquet_glob(partition_dir(paths.bronze, day)))}, hive_partitioning=false)"
    )
    types = ", ".join(sql_str(t) for t in config.EVENT_TYPES)
    con.execute(
        f"""CREATE TEMP TABLE c AS SELECT *,
          CASE WHEN event_time IS NULL THEN 'null_event_time'
               WHEN user_id IS NULL THEN 'null_user_id'
               WHEN product_id IS NULL THEN 'null_product_id'
               WHEN event_type IS NULL OR event_type NOT IN ({types}) THEN 'invalid_event_type'
               WHEN price IS NULL THEN 'null_price'
               WHEN price < 0 THEN 'negative_price'
               WHEN event_time < $s OR event_time >= $e THEN 'outside_partition_day'
          END AS quarantine_reason
        FROM b""",
        {"s": start, "e": end},
    )
    con.execute(
        f"""CREATE TEMP TABLE v AS SELECT * EXCLUDE (quarantine_reason),
              row_number() OVER (PARTITION BY {SRC} ORDER BY source_file, source_row) AS copy_no
            FROM c WHERE quarantine_reason IS NULL"""
    )
    con.execute(
        f"""CREATE TEMP TABLE s AS SELECT {SRC}, source_file, source_row,
              split_part(category_code, '.', 1) AS category_top,
              price = 0 AS is_zero_price
            FROM v WHERE copy_no = 1"""
    )
    con.execute(
        f"""CREATE TEMP TABLE flags AS
            WITH per_min AS (
              SELECT user_id, date_trunc('minute', event_time) m, count(*) n FROM s GROUP BY ALL
            )
            SELECT user_id, sum(n)::BIGINT AS events, max(n)::BIGINT AS max_events_per_minute
            FROM per_min GROUP BY user_id
            HAVING max(n) >= {config.BOT_MAX_EVENTS_PER_MINUTE} OR sum(n) >= {config.BOT_MAX_EVENTS_PER_DAY}"""
    )

    report = DQReport("silver", key)

    def q(sql: str, params: dict | None = None):
        return con.execute(sql, params or {}).fetchone()[0]

    bronze_rows = q("SELECT count(*) FROM b")
    quarantined = dict(
        con.execute(
            "SELECT quarantine_reason, count(*) FROM c WHERE quarantine_reason IS NOT NULL GROUP BY 1"
        ).fetchall()
    )
    dups_by_type = dict(
        con.execute("SELECT event_type, count(*) FROM v WHERE copy_no > 1 GROUP BY 1 ORDER BY 1").fetchall()
    )
    dup_rows = sum(dups_by_type.values())
    purchase_dup_revenue = q(
        "SELECT coalesce(sum(price), 0) FROM v WHERE copy_no > 1 AND event_type = 'purchase'"
    )
    silver_rows = q("SELECT count(*) FROM s")
    n_quarantined = sum(quarantined.values())

    report.error(
        "rows_reconcile",
        bronze_rows == silver_rows + n_quarantined + dup_rows,
        {"bronze": bronze_rows, "silver": silver_rows, "quarantined": n_quarantined, "duplicates": dup_rows},
        "bronze = silver + quarantined + duplicates removed",
    )
    report.error(
        "no_null_keys",
        q(
            "SELECT count(*) FROM s WHERE user_id IS NULL OR product_id IS NULL OR event_time IS NULL OR event_type IS NULL"
        )
        == 0,
    )
    report.error("no_exact_duplicates", q(f"SELECT count(*) - count(DISTINCT ({SRC})) FROM s") == 0)
    report.error(
        "events_inside_day",
        q("SELECT count(*) FROM s WHERE event_time < $s OR event_time >= $e", {"s": start, "e": end}) == 0,
    )
    report.warn(
        "quarantine_rate",
        n_quarantined <= config.WARN_QUARANTINE_RATE * max(bronze_rows, 1),
        n_quarantined / max(bronze_rows, 1),
        f"threshold {config.WARN_QUARANTINE_RATE}",
    )
    report.warn(
        "duplicate_rate",
        dup_rows <= config.WARN_DUPLICATE_RATE * max(bronze_rows, 1),
        dup_rows / max(bronze_rows, 1),
        f"threshold {config.WARN_DUPLICATE_RATE}",
    )
    unknown_types = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT event_type FROM c WHERE quarantine_reason = 'invalid_event_type'"
        ).fetchall()
    ]
    report.warn("no_unknown_event_types", not unknown_types, unknown_types)
    previous = [
        manifest.get("silver", d.isoformat())["silver_rows"]
        for d in list_partitions(paths.silver)
        if d < day and manifest.get("silver", d.isoformat()) and (day - d).days <= 7
    ]
    if previous:
        median = statistics.median(previous)
        report.warn(
            "daily_volume_vs_trailing_median",
            abs(silver_rows - median) <= config.WARN_DAILY_VOLUME_DEVIATION * median,
            {"rows": silver_rows, "trailing_median": median},
            f"allowed deviation {config.WARN_DAILY_VOLUME_DEVIATION:.0%}",
        )

    report.metric("bronze_rows", bronze_rows)
    report.metric("silver_rows", silver_rows)
    report.metric("quarantined_by_reason", quarantined)
    report.metric("duplicates_removed", dup_rows)
    report.metric("duplicates_removed_by_event_type", dups_by_type)
    report.metric("purchase_duplicates_removed", dups_by_type.get("purchase", 0))
    report.metric("purchase_duplicate_revenue_removed", round(purchase_dup_revenue, 2))
    report.metric(
        "events_by_type",
        dict(con.execute("SELECT event_type, count(*) FROM s GROUP BY 1 ORDER BY 1").fetchall()),
    )
    report.metric("zero_price_rows", q("SELECT count(*) FROM s WHERE is_zero_price"))
    report.metric("null_user_session_rows", q("SELECT count(*) FROM s WHERE user_session IS NULL"))
    report.metric("bot_users_flagged", q("SELECT count(*) FROM flags"))
    report.metric("bot_events_flagged", q("SELECT coalesce(sum(events), 0) FROM flags"))
    report.metric("users", q("SELECT count(DISTINCT user_id) FROM s"))

    silver_dir, quarantine_dir, flags_dir = _output_dirs(paths, day)
    with atomic_dir(silver_dir) as t_s, atomic_dir(quarantine_dir) as t_q, atomic_dir(flags_dir) as t_f:
        con.execute(
            f"COPY (SELECT * FROM s ORDER BY user_id, event_time, source_file, source_row) "
            f"TO {sql_str(t_s / 'part-0.parquet')} (FORMAT parquet, COMPRESSION zstd)"
        )
        con.execute(
            "COPY (SELECT * FROM c WHERE quarantine_reason IS NOT NULL "
            f"ORDER BY source_file, source_row) TO {sql_str(t_q / 'part-0.parquet')} (FORMAT parquet, COMPRESSION zstd)"
        )
        con.execute(
            f"COPY (SELECT * FROM flags ORDER BY user_id) TO {sql_str(t_f / 'part-0.parquet')} (FORMAT parquet, COMPRESSION zstd)"
        )
        written = con.execute(
            f"SELECT count(*) FROM read_parquet({sql_str(t_s / 'part-0.parquet')}, hive_partitioning=false)"
        ).fetchone()[0]
        report.error(
            "written_rows_match", written == silver_rows, {"written": written, "expected": silver_rows}
        )
        report.finish(paths.dq, log)

    manifest.record("silver", key, in_fp, _output_fp(paths, day), silver_rows=silver_rows)
    return True


def build(paths: Paths, days: list[date] | None = None, force: bool = False, log=print) -> list[date]:
    manifest = Manifest(paths.manifest)
    days = days if days is not None else list_partitions(paths.bronze)
    built = []
    for day in days:
        if build_day(paths, day, manifest, force=force, log=log):
            built.append(day)
            log(f"silver: built {day}")
    log(f"silver: {len(built)} built, {len(days) - len(built)} up to date")
    return built
