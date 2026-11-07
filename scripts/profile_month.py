"""Profile one month of raw events from bronze (before any cleaning).

    python scripts/profile_month.py --month 2019-10 > reports/profile_2019-10.txt

Reads bronze (typed, every CSV row, with source_row) so file-order checks are possible.
All times UTC. Numbers in the README's "Data" section come from these outputs.
"""

from __future__ import annotations

import argparse
import time

from clickstream.config import MONTH_FILES, Paths
from clickstream.io import connect, sql_str


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, choices=sorted(MONTH_FILES))
    ap.add_argument("--memory", default="3GB")
    args = ap.parse_args()
    paths = Paths.default()
    stem = MONTH_FILES[args.month].removesuffix(".csv")
    con = connect(threads=4)
    con.execute(f"SET memory_limit='{args.memory}'")
    con.execute(
        f"CREATE VIEW ev AS SELECT * FROM read_parquet({sql_str(str(paths.bronze / '*' / (stem + '.parquet')))}, hive_partitioning=false)"
    )

    def q(title: str, sql: str) -> None:
        t = time.perf_counter()
        print(f"\n## {title}")
        rel = con.sql(sql)
        cols = rel.columns
        rows = rel.fetchall()
        widths = [max(len(str(c)), *(len(_fmt(r[i])) for r in rows)) for i, c in enumerate(cols)]
        print("  ".join(str(c).rjust(w) for c, w in zip(cols, widths, strict=True)))
        for r in rows:
            print("  ".join(_fmt(v).rjust(w) for v, w in zip(r, widths, strict=True)))
        print(f"({time.perf_counter() - t:.1f}s)")

    print(f"# Profile of {MONTH_FILES[args.month]} (bronze, before cleaning)")
    q(
        "rows, range (UTC), cardinalities",
        """SELECT count(*) AS rows, min(event_time)::VARCHAR AS min_t, max(event_time)::VARCHAR AS max_t,
             count(DISTINCT user_id) AS users, count(DISTINCT user_session) AS user_sessions,
             count(DISTINCT product_id) AS products, count(DISTINCT category_id) AS categories,
             count(DISTINCT brand) AS brands FROM ev""",
    )
    q(
        "event types",
        "SELECT event_type, count(*) AS n, round(100 * count(*) / sum(count(*)) OVER (), 3) AS pct FROM ev GROUP BY 1 ORDER BY 2 DESC",
    )
    q(
        "nulls per column",
        """SELECT count(*) FILTER (category_code IS NULL) AS category_code, count(*) FILTER (brand IS NULL) AS brand,
             count(*) FILTER (user_session IS NULL) AS user_session, count(*) FILTER (price IS NULL) AS price,
             count(*) FILTER (user_id IS NULL OR product_id IS NULL OR event_time IS NULL OR event_type IS NULL) AS any_key
           FROM ev""",
    )
    q(
        "exact duplicate rows (all 9 source columns), extra copies by event type",
        """SELECT event_type, sum(n - 1) AS extra_rows, count(*) AS groups, max(n) AS max_copies FROM (
             SELECT event_type, count(*) AS n FROM ev
             GROUP BY event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
             HAVING n > 1) GROUP BY 1 ORDER BY 2 DESC""",
    )
    q(
        "out-of-order rows in file order (event_time earlier than the previous row / user's previous row)",
        """SELECT count(*) FILTER (event_time < prev_any) AS global_out_of_order,
                  count(*) FILTER (event_time < prev_user) AS per_user_out_of_order FROM (
             SELECT event_time, lag(event_time) OVER (ORDER BY source_row) AS prev_any,
                    lag(event_time) OVER (PARTITION BY user_id ORDER BY source_row) AS prev_user FROM ev)""",
    )
    q(
        "same user, same second",
        "SELECT count(*) AS groups, sum(n) AS rows FROM (SELECT count(*) AS n FROM ev GROUP BY user_id, event_time HAVING n > 1)",
    )
    q(
        "events per user",
        """SELECT quantile_cont(n, [0.5, 0.9, 0.99]) AS p50_p90_p99, max(n) AS max, count(*) FILTER (n = 1) AS single_event_users
           FROM (SELECT count(*) AS n FROM ev GROUP BY user_id)""",
    )
    q(
        "user_session ids shared by more than one user",
        "SELECT count(*) AS ids FROM (SELECT user_session FROM ev WHERE user_session IS NOT NULL GROUP BY 1 HAVING count(DISTINCT user_id) > 1)",
    )
    q(
        "price",
        """SELECT min(price) AS min, quantile_cont(price, [0.5, 0.99]) AS p50_p99, max(price) AS max,
             count(*) FILTER (price = 0) AS zero, count(*) FILTER (price < 0) AS negative FROM ev""",
    )
    con.execute(
        """CREATE TEMP TABLE gaps AS
           SELECT (epoch_us(event_time) - epoch_us(lag(event_time) OVER w)) / 1e6 AS gap_s,
                  user_session IS DISTINCT FROM lag(user_session) OVER w AS id_changed
           FROM ev WINDOW w AS (PARTITION BY user_id ORDER BY event_time, source_row)"""
    )
    q(
        "gaps between a user's consecutive events (cumulative share)",
        """SELECT count(*) AS gaps,
             round(100 * avg((gap_s < 60)::INT), 2) AS pct_lt_1m, round(100 * avg((gap_s < 300)::INT), 2) AS pct_lt_5m,
             round(100 * avg((gap_s <= 1800)::INT), 2) AS pct_le_30m, round(100 * avg((gap_s <= 3600)::INT), 2) AS pct_le_60m,
             round(100 * avg((gap_s >= 86400)::INT), 2) AS pct_ge_1d
           FROM gaps WHERE gap_s IS NOT NULL""",
    )
    q(
        "gaps from 5 to 120 min, 5-minute bins",
        """SELECT (floor(gap_s / 300) * 5)::INT AS bin_start_min, count(*) AS gaps FROM gaps
           WHERE gap_s >= 300 AND gap_s < 7500 GROUP BY 1 ORDER BY 1""",
    )
    q(
        "user_session id changes vs gap",
        """SELECT count(*) FILTER (id_changed AND gap_s < 60) AS changes_after_lt_1m,
                  count(*) FILTER (id_changed AND gap_s = 0) AS changes_same_second,
                  count(*) FILTER (NOT id_changed AND gap_s > 3600) AS kept_after_gt_1h,
                  count(*) FILTER (NOT id_changed AND gap_s > 86400) AS kept_after_gt_1d
           FROM gaps WHERE gap_s IS NOT NULL""",
    )


def _fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:,.2f}"
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, list):
        return "[" + ", ".join(_fmt(x) for x in v) + "]"
    return str(v)


if __name__ == "__main__":
    main()
