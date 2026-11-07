"""sessionized events -> gold tables.

Partitioned by session_date (UTC date of the session's first event):
  sessions        one row per session
  session_items   one row per (session, top-level category, brand) with view/cart/purchase flags
  daily_metrics   two rows per date: population 'all' and 'non_bot'
  conversion_daily  non-bot sessions / carted / purchased / revenue per top-level category and
                  per brand. Additive across dates (a session lives in exactly one date), so the
                  conversion marts are sums over it rather than a scan of every session item.

Global marts (recomputed when any partition changes, from gold only, excluding suspect bots):
  conversion_by_category, conversion_by_brand, duration_by_segment

Sessions that start on S and continue past midnight have events in later sessionized
partitions. The carry-over state says exactly which: S+1 is read if carry(S) holds a session
that started on S, S+2 if carry(S+1) still does, and so on. So adding day D rebuilds gold for D
and for the start dates of sessions carried into D (normally just D-1), nothing older.
"""

from __future__ import annotations

from datetime import date, timedelta

from clickstream import config
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
from clickstream.state import Manifest

GOLD_VERSION = "gold-v2"
PARTITIONED = ("sessions", "session_items", "daily_metrics", "conversion_daily")
MARTS = ("conversion_by_category", "conversion_by_brand", "duration_by_segment")


def price_band_sql(col: str) -> str:
    edges = config.PRICE_BAND_EDGES
    parts = [f"WHEN {col} IS NULL THEN 'unknown'"]
    lo = 0
    for hi in edges:
        parts.append(f"WHEN {col} < {hi} THEN '{lo}-{hi}'")
        lo = hi
    parts.append(f"ELSE '{lo}+'")
    return "CASE " + " ".join(parts) + " END"


def dependencies(paths: Paths, session_date: date, available: set[date]) -> list[date]:
    """Sessionized days that hold events of sessions starting on `session_date`."""
    start, end = day_bounds(session_date)
    days = [session_date]
    d = session_date
    con = connect()
    while True:
        carry = partition_dir(paths.carry, d)
        if fingerprint_dir(carry) is None:
            break
        n = con.execute(
            f"SELECT count(*) FROM read_parquet({sql_str(parquet_glob(carry))}, hive_partitioning=false) WHERE session_start >= $s AND session_start < $e",
            {"s": start, "e": end},
        ).fetchone()[0]
        d = d + timedelta(days=1)
        if n == 0 or d not in available:
            break
        days.append(d)
    return days


def affected_dates(paths: Paths, days: list[date]) -> list[date]:
    """Session dates whose gold output can change when sessionized `days` change:
    the days themselves plus the start dates of sessions carried into them."""
    out = set(days)
    con = connect()
    for d in days:
        carry = partition_dir(paths.carry, d - timedelta(days=1))
        if fingerprint_dir(carry) is not None:
            rows = con.execute(
                f"SELECT DISTINCT session_start::DATE FROM read_parquet({sql_str(parquet_glob(carry))}, hive_partitioning=false)"
            ).fetchall()
            out.update(r[0] for r in rows)
    return sorted(out)


def _input_fp(paths: Paths, deps: list[date]) -> str:
    parts = [
        (
            d.isoformat(),
            fingerprint_dir(partition_dir(paths.sessionized, d)),
            fingerprint_dir(partition_dir(paths.bot_flags, d)),
        )
        for d in deps
    ]
    return fingerprint_values(GOLD_VERSION, gap_seconds(), config.PRICE_BAND_EDGES, parts)


def _output_fp(paths: Paths, session_date: date) -> str | None:
    fps = [
        fingerprint_dir(partition_dir(paths.gold_table(t), session_date, "session_date")) for t in PARTITIONED
    ]
    return None if None in fps else fingerprint_values(*fps)


def build_date(
    paths: Paths, session_date: date, manifest: Manifest, available: set[date], force=False, log=print
) -> bool:
    key = session_date.isoformat()
    deps = dependencies(paths, session_date, available)
    in_fp = _input_fp(paths, deps)
    if not force and manifest.is_current("gold", key, in_fp, _output_fp(paths, session_date)):
        return False

    start, end = day_bounds(session_date)
    con = connect()
    files = ", ".join(sql_str(parquet_glob(partition_dir(paths.sessionized, d))) for d in deps)
    con.execute(
        f"CREATE TEMP TABLE e AS SELECT * FROM read_parquet([{files}], hive_partitioning=false) WHERE session_start >= $s AND session_start < $e",
        {"s": start, "e": end},
    )
    flag_sql = " UNION ALL ".join(
        f"SELECT user_id, DATE '{d.isoformat()}' AS event_date FROM read_parquet({sql_str(parquet_glob(partition_dir(paths.bot_flags, d)))}, hive_partitioning=false)"
        for d in deps
    )
    con.execute(f"CREATE TEMP TABLE f AS {flag_sql}")
    order = "ORDER BY event_time, source_file, source_row"
    con.execute(
        f"""CREATE TEMP TABLE s AS
        SELECT session_id, any_value(e.user_id) AS user_id, DATE '{key}' AS session_date,
          min(session_start) AS session_start, max(event_time) AS session_end,
          (epoch_us(max(event_time)) - epoch_us(min(session_start))) // 1000000 AS duration_s,
          count(*) AS n_events,
          count(*) FILTER (event_type = 'view') AS n_views,
          count(*) FILTER (event_type = 'cart') AS n_carts,
          count(*) FILTER (event_type = 'remove_from_cart') AS n_remove_from_cart,
          count(*) FILTER (event_type = 'purchase') AS n_purchases,
          count(DISTINCT product_id) AS n_distinct_products,
          coalesce(sum(price::DECIMAL(18, 2)) FILTER (event_type = 'purchase'), 0) AS revenue,
          count(*) FILTER (event_type = 'cart') > 0 AS reached_cart,
          count(*) FILTER (event_type = 'purchase') > 0 AS reached_purchase,
          max(event_time)::DATE <> min(session_start)::DATE AS crosses_midnight,
          count(DISTINCT event_time::DATE) AS n_event_days,
          count(DISTINCT user_session) AS n_user_sessions,
          first(product_id {order}) AS entry_product_id,
          first(category_top {order}) AS entry_category_top,
          first(brand {order}) AS entry_brand,
          first(price {order}) AS entry_price,
          {price_band_sql(f"first(price {order})")} AS entry_price_band,
          bool_or(f.user_id IS NOT NULL) AS suspect_bot,
          min(event_time) AS first_event_time
        FROM e LEFT JOIN f ON f.user_id = e.user_id AND f.event_date = e.event_time::DATE
        GROUP BY session_id"""
    )
    con.execute(
        """CREATE TEMP TABLE i AS
        SELECT e.session_id, coalesce(category_top, 'unknown') AS category_top, coalesce(brand, 'unknown') AS brand,
          bool_or(event_type = 'view') AS viewed, bool_or(event_type = 'cart') AS carted,
          bool_or(event_type = 'purchase') AS purchased,
          coalesce(sum(price::DECIMAL(18, 2)) FILTER (event_type = 'purchase'), 0) AS revenue, count(*) AS n_events,
          any_value(s.suspect_bot) AS suspect_bot
        FROM e JOIN s USING (session_id) GROUP BY ALL"""
    )
    con.execute(
        f"""CREATE TEMP TABLE m AS
        SELECT DATE '{key}' AS session_date, p.population,
          count(*) AS sessions, count(DISTINCT user_id) AS users, sum(n_events) AS events,
          quantile_cont(duration_s, 0.5) AS median_duration_s, quantile_cont(duration_s, 0.9) AS p90_duration_s,
          avg(n_events) AS mean_events_per_session,
          avg((n_events = 1)::INT) AS single_event_share,
          count(*) FILTER (n_views > 0) AS sessions_with_view,
          count(*) FILTER (reached_cart) AS sessions_with_cart,
          count(*) FILTER (reached_purchase) AS sessions_with_purchase,
          count(*) FILTER (reached_cart AND reached_purchase) AS sessions_with_cart_and_purchase,
          sum(revenue) AS revenue,
          count(*) FILTER (crosses_midnight) AS sessions_crossing_midnight
        FROM s, (VALUES ('all'), ('non_bot')) p(population)
        WHERE p.population = 'all' OR NOT s.suspect_bot
        GROUP BY ALL ORDER BY population"""
    )

    con.execute(
        f"""CREATE TEMP TABLE cd AS
        WITH sc AS (
          SELECT session_id, 'category_top' AS dim_type, category_top AS dim_value,
                 bool_or(carted) AS c, bool_or(purchased) AS p, sum(revenue) AS r
          FROM i WHERE NOT suspect_bot GROUP BY 1, 2, 3
          UNION ALL
          SELECT session_id, 'brand', brand, bool_or(carted), bool_or(purchased), sum(revenue)
          FROM i WHERE NOT suspect_bot GROUP BY 1, 2, 3
        )
        SELECT DATE '{key}' AS session_date, dim_type, dim_value, count(*) AS sessions,
               count(*) FILTER (c) AS sessions_carted, count(*) FILTER (p) AS sessions_purchased,
               sum(r) AS revenue
        FROM sc GROUP BY ALL ORDER BY dim_type, dim_value"""
    )

    def q(sql: str):
        return con.execute(sql).fetchone()[0]

    report = DQReport("gold", key)
    n_e = q("SELECT count(*) FROM e")
    report.error("session_events_reconcile", q("SELECT coalesce(sum(n_events), 0) FROM s") == n_e, n_e)
    report.error("item_events_reconcile", q("SELECT coalesce(sum(n_events), 0) FROM i") == n_e)
    report.error("session_id_unique", q("SELECT count(*) - count(DISTINCT session_id) FROM s") == 0)
    report.error(
        "session_starts_at_its_first_event",
        q("SELECT count(*) FROM s WHERE first_event_time <> session_start") == 0,
        detail="fails if part of a session was lost across a day boundary",
    )
    report.error("non_negative_duration", q("SELECT count(*) FROM s WHERE duration_s < 0") == 0)
    report.error(
        "event_type_counts_add_up",
        q("SELECT count(*) FROM s WHERE n_views + n_carts + n_remove_from_cart + n_purchases <> n_events")
        == 0,
    )
    report.metric("dependencies", [d.isoformat() for d in deps])
    report.metric("sessions", q("SELECT count(*) FROM s"))
    report.metric("suspect_bot_sessions", q("SELECT count(*) FROM s WHERE suspect_bot"))
    report.metric("max_event_days_per_session", q("SELECT max(n_event_days) FROM s"))

    outs = [partition_dir(paths.gold_table(t), session_date, "session_date") for t in PARTITIONED]
    with (
        atomic_dir(outs[0]) as t_s,
        atomic_dir(outs[1]) as t_i,
        atomic_dir(outs[2]) as t_m,
        atomic_dir(outs[3]) as t_cd,
    ):
        con.execute(
            f"COPY (SELECT * EXCLUDE (first_event_time) FROM s ORDER BY user_id, session_start) TO {sql_str(t_s / 'part-0.parquet')} (FORMAT parquet, COMPRESSION zstd)"
        )
        con.execute(
            f"COPY (SELECT * FROM i ORDER BY session_id, category_top, brand) TO {sql_str(t_i / 'part-0.parquet')} (FORMAT parquet, COMPRESSION zstd)"
        )
        con.execute(
            f"COPY (SELECT * FROM m) TO {sql_str(t_m / 'part-0.parquet')} (FORMAT parquet, COMPRESSION zstd)"
        )
        con.execute(
            f"COPY (SELECT * FROM cd) TO {sql_str(t_cd / 'part-0.parquet')} (FORMAT parquet, COMPRESSION zstd)"
        )
        report.finish(paths.dq, log)
    manifest.record(
        "gold", key, in_fp, _output_fp(paths, session_date), dependencies=[d.isoformat() for d in deps]
    )
    return True


def build_marts(paths: Paths, manifest: Manifest, force: bool = False, log=print) -> bool:
    dates = list_partitions(paths.gold_table("sessions"), "session_date")
    if not dates:
        return False
    in_fp = fingerprint_values(
        GOLD_VERSION, config.MIN_SESSIONS_PER_BRAND, [manifest.get("gold", d.isoformat()) for d in dates]
    )
    marts_dir = paths.gold_table("marts")
    out_fp = fingerprint_dir(marts_dir)
    if not force and manifest.is_current("marts", "all", in_fp, out_fp):
        return False
    con = connect()
    sessions = sql_str(str(paths.gold_table("sessions") / "*" / "*.parquet"))
    conv = sql_str(str(paths.gold_table("conversion_daily") / "*" / "*.parquet"))
    con.execute(
        f"""CREATE VIEW s AS SELECT duration_s, n_events, entry_category_top, entry_brand, entry_price_band
            FROM read_parquet({sessions}, hive_partitioning=false) WHERE NOT suspect_bot"""
    )
    con.execute(f"CREATE VIEW cd AS SELECT * FROM read_parquet({conv}, hive_partitioning=false)")

    def conversion(dim: str, having: str = "") -> str:
        return f"""
        SELECT dim_value AS {dim}, sum(sessions) AS sessions, sum(sessions_carted) AS sessions_carted,
          sum(sessions_purchased) AS sessions_purchased,
          sum(sessions_carted) / sum(sessions) AS cart_rate,
          sum(sessions_purchased) / sum(sessions) AS purchase_rate,
          sum(revenue) AS revenue
        FROM cd WHERE dim_type = '{dim}' GROUP BY dim_value {having} ORDER BY sessions DESC, {dim}"""

    def durations(segment_type: str, expr: str, having: str = "") -> str:
        # Exact quantiles can't spill, so: one segment type per query, one list-valued
        # quantile state per group, INTEGER durations.
        return f"""
        SELECT '{segment_type}' AS segment_type, coalesce({expr}::VARCHAR, 'unknown') AS segment,
          count(*) AS sessions,
          quantile_cont(duration_s::INTEGER, [0.5, 0.9]) AS q_all,
          quantile_cont(duration_s::INTEGER, [0.5, 0.9]) FILTER (n_events > 1) AS q_multi,
          avg((n_events = 1)::INT) AS single_event_share
        FROM s GROUP BY 2 {having}"""

    min_b = config.MIN_SESSIONS_PER_BRAND
    segments = [
        ("category_top", "entry_category_top", ""),
        ("brand", "entry_brand", f"HAVING count(*) >= {min_b}"),
        ("price_band", "entry_price_band", ""),
    ]
    con.execute("SET threads=2")  # fewer concurrent quantile states
    con.execute(
        """CREATE TEMP TABLE dur (segment_type VARCHAR, segment VARCHAR, sessions BIGINT,
           q_all DOUBLE[], q_multi DOUBLE[], single_event_share DOUBLE)"""
    )
    for seg in segments:
        con.execute(f"INSERT INTO dur {durations(*seg)}")
    queries = {
        "conversion_by_category": conversion("category_top"),
        "conversion_by_brand": conversion("brand", f"HAVING sum(sessions) >= {min_b}"),
        "duration_by_segment": """
        SELECT segment_type, segment, sessions,
          q_all[1] AS median_duration_s, q_all[2] AS p90_duration_s,
          q_multi[1] AS median_duration_s_multi_event, q_multi[2] AS p90_duration_s_multi_event,
          single_event_share
        FROM dur ORDER BY segment_type, sessions DESC, segment""",
    }
    with atomic_dir(marts_dir) as tmp:
        for name, sql in queries.items():
            con.execute(
                f"COPY ({sql}) TO {sql_str(tmp / f'{name}.parquet')} (FORMAT parquet, COMPRESSION zstd)"
            )
    manifest.record("marts", "all", in_fp, fingerprint_dir(marts_dir))
    log("gold: marts rebuilt")
    return True


def build(paths: Paths, dates: list[date] | None = None, force: bool = False, log=print) -> list[date]:
    manifest = Manifest(paths.manifest)
    available = set(list_partitions(paths.sessionized))
    dates = dates if dates is not None else sorted(available)
    built = []
    for d in dates:
        if build_date(paths, d, manifest, available, force=force, log=log):
            built.append(d)
            log(f"gold: built {d}")
    log(f"gold: {len(built)} built, {len(dates) - len(built)} up to date")
    build_marts(paths, manifest, force=force or bool(built), log=log)
    return built
