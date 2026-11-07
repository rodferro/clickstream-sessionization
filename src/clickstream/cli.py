"""Command-line interface.

    clickstream download --month 2019-10
    clickstream ingest --month 2019-10            (or --file path/to.csv)
    clickstream build --layer silver [--start D --end D] [--force]
    clickstream sessions [--engine duckdb|polars|python] [--start D --end D] [--force]
    clickstream build --layer gold [--start D --end D] [--force]
    clickstream check
    clickstream compare-engines --engines duckdb,polars[,python] [--start D --end D] [--out file]
    clickstream report [--months 2019-10]
    clickstream run --months 2019-10[,2019-11] [--engine duckdb]   (download..gold for those months)

Exit codes: 0 ok, 1 usage/runtime error, 2 data-quality failure.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

from clickstream import bronze, check, compare, gold, silver
from clickstream import sessions as sessions_stage
from clickstream.config import MONTH_FILES, Paths
from clickstream.io import date_range, list_partitions
from clickstream.quality import DataQualityError
from clickstream.sessionize import ENGINES


def _days(available: list[date], start: str | None, end: str | None) -> list[date]:
    lo = date.fromisoformat(start) if start else None
    hi = date.fromisoformat(end) if end else None
    return [d for d in available if (lo is None or d >= lo) and (hi is None or d <= hi)]


def _month_days(month: str) -> list[date]:
    y, m = map(int, month.split("-"))
    first = date(y, m, 1)
    last = date(y + (m == 12), m % 12 + 1, 1)
    return [d for d in date_range(first, last) if d < last]


def _csv_for_month(paths: Paths, month: str, fetch: bool) -> Path:
    from clickstream.download import download, month_file

    csv = paths.raw / month_file(month)
    if not csv.exists():
        if not fetch:
            raise SystemExit(f"{csv} not found; run `clickstream download --month {month}` first")
        download(month, paths.raw)
    return csv


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="clickstream", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    def rng(sp):
        sp.add_argument("--start", help="first UTC date (YYYY-MM-DD)")
        sp.add_argument("--end", help="last UTC date (YYYY-MM-DD)")

    sp = sub.add_parser("download", help="download a month from Kaggle into data/raw")
    sp.add_argument("--month", required=True, choices=sorted(MONTH_FILES))

    sp = sub.add_parser("ingest", help="raw CSV -> bronze")
    g = sp.add_mutually_exclusive_group(required=True)
    g.add_argument("--month", choices=sorted(MONTH_FILES))
    g.add_argument("--file", type=Path)

    sp = sub.add_parser("build", help="build silver or gold")
    sp.add_argument("--layer", required=True, choices=["silver", "gold"])
    sp.add_argument("--force", action="store_true", help="rebuild even if inputs are unchanged")
    sp.add_argument("--skip-marts", action="store_true", help="gold: don't recompute the global marts")
    rng(sp)

    sp = sub.add_parser("sessions", help="silver -> sessionized events (in date order)")
    sp.add_argument("--engine", default="duckdb", choices=sorted(ENGINES))
    sp.add_argument("--force", action="store_true")
    rng(sp)

    sub.add_parser("check", help="cross-layer reconciliation + DQ summary")

    sp = sub.add_parser("compare-engines", help="check sessionizers agree exactly")
    sp.add_argument("--engines", default="duckdb,polars")
    sp.add_argument("--out", type=Path)
    rng(sp)

    sp = sub.add_parser("report", help="write reports/analysis.md and figures")
    sp.add_argument("--months", default=None, help="comma-separated months, default: all built")

    sp = sub.add_parser("run", help="download (if needed), ingest, silver, sessions, gold")
    sp.add_argument("--months", default="2019-10", help="comma-separated, e.g. 2019-10,2019-11")
    sp.add_argument("--file", type=Path, help="ingest this CSV instead of Kaggle months (e.g. the sample)")
    sp.add_argument("--engine", default="duckdb", choices=sorted(ENGINES))

    args = p.parse_args(argv)
    paths = Paths.default()
    t0 = time.perf_counter()
    try:
        if args.cmd == "download":
            from clickstream.download import download

            print(download(args.month, paths.raw))
        elif args.cmd == "ingest":
            bronze.ingest(args.file or _csv_for_month(paths, args.month, fetch=False), paths)
        elif args.cmd == "build" and args.layer == "silver":
            silver.build(paths, _days(list_partitions(paths.bronze), args.start, args.end), force=args.force)
        elif args.cmd == "sessions":
            sessions_stage.build(
                paths, _days(list_partitions(paths.silver), args.start, args.end), args.engine, args.force
            )
        elif args.cmd == "build" and args.layer == "gold":
            available = list_partitions(paths.sessionized)
            days = _days(available, args.start, args.end)
            dates = gold.affected_dates(paths, days) if (args.start or args.end) else None
            gold.build(paths, dates, force=args.force, marts=not args.skip_marts)
        elif args.cmd == "check":
            check.run_checks(paths)
        elif args.cmd == "compare-engines":
            engines = args.engines.split(",")
            days = _days(list_partitions(paths.silver), args.start, args.end)
            result = compare.compare(paths, days, engines)
            out = (
                args.out or paths.reports / f"engine_agreement_{'_'.join(engines)}_{days[0]}_{days[-1]}.json"
            )
            compare.write_result(result, out)
            print(
                f"{'AGREE' if result['agree'] else 'DISAGREE'}: {result['rows_compared']:,} rows, "
                f"{result['sessions_started']:,} sessions, mismatches {result['mismatched_rows']} -> {out}"
            )
            if not result["agree"]:
                return 2
        elif args.cmd == "report":
            from clickstream import report

            report.write_report(paths, args.months.split(",") if args.months else None)
        elif args.cmd == "run":
            if args.file:
                bronze.ingest(args.file, paths)
                days = None
            else:
                months = args.months.split(",")
                for m in months:
                    bronze.ingest(_csv_for_month(paths, m, fetch=True), paths)
                wanted = {d for m in months for d in _month_days(m)}
                days = [d for d in list_partitions(paths.bronze) if d in wanted]
            silver.build(paths, days)
            sessions_stage.build(
                paths,
                None if days is None else [d for d in list_partitions(paths.silver) if d in set(days)],
                args.engine,
            )
            gold.build(paths)
            check.run_checks(paths)
    except DataQualityError as e:
        print(f"DATA QUALITY FAILURE: {e}", file=sys.stderr)
        return 2
    print(f"done in {time.perf_counter() - t0:.1f}s", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
