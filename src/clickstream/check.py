"""Cross-layer reconciliation over everything that has been built, plus a DQ report summary.

Per-partition checks run inline when each partition is built (see quality.py). This command
re-verifies the totals across layers and fails if any partition's latest DQ report failed.
"""

from __future__ import annotations

from clickstream.config import Paths
from clickstream.io import connect, sql_str
from clickstream.quality import DQReport, load_reports


def _count(con, table_glob: str) -> int:
    try:
        return con.execute(
            f"SELECT count(*) FROM read_parquet({sql_str(table_glob)}, hive_partitioning=false)"
        ).fetchone()[0]
    except Exception:  # no files yet
        return 0


def run_checks(paths: Paths, log=print) -> DQReport:
    con = connect()
    bronze = _count(con, str(paths.bronze / "*" / "*.parquet"))
    silver = _count(con, str(paths.silver / "*" / "*.parquet"))
    quarantine = _count(con, str(paths.quarantine / "*" / "*.parquet"))
    sessionized = _count(con, str(paths.sessionized / "*" / "*.parquet"))
    silver_reports = load_reports(paths.dq, "silver")
    dups = sum(r["metrics"].get("duplicates_removed", 0) for r in silver_reports)
    gold_events = 0
    if (paths.gold_table("sessions")).exists():
        gold_events = con.execute(
            f"SELECT coalesce(sum(n_events), 0) FROM read_parquet({sql_str(str(paths.gold_table('sessions') / '*' / '*.parquet'))}, hive_partitioning=false)"
        ).fetchone()[0]

    report = DQReport("check", "all")
    report.error(
        "bronze_equals_silver_plus_quarantine_plus_duplicates",
        bronze == silver + quarantine + dups,
        {"bronze": bronze, "silver": silver, "quarantine": quarantine, "duplicates": dups},
    )
    report.error(
        "all_silver_sessionized", sessionized == silver, {"silver": silver, "sessionized": sessionized}
    )
    report.error(
        "gold_sessions_cover_all_events",
        gold_events == sessionized,
        {"gold_events": gold_events, "sessionized": sessionized},
    )
    failed = [
        f"{r['stage']}/{r['key']}"
        for r in load_reports(paths.dq)
        if r["status"] == "fail" and r["stage"] != "check"
    ]
    report.error("no_failed_partition_reports", not failed, failed)
    report.metric("partition_reports", len(load_reports(paths.dq)))
    report.finish(paths.dq, log)
    log(
        f"check: OK (bronze {bronze:,}, silver {silver:,}, quarantine {quarantine:,}, duplicates {dups:,}, sessionized {sessionized:,})"
    )
    return report
