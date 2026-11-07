"""Benchmark the pipeline end to end, one subprocess per stage and day, on a fresh data root.

    python scripts/benchmark.py --months 2019-10,2019-11

Measures wall time and peak resident memory (ru_maxrss of each child process, via wait4):
  1. default one-month run: `clickstream run --months <first month>` in a single process
  2. day by day over all months: ingest per month, then silver / sessions / gold per day
     (gold without marts), then marts once
  3. sessionization engines on the same inputs, per day: DuckDB vs Polars (compute only)

Writes reports/benchmark.json (every measurement), reports/benchmark.md (tables) and
reports/figures/benchmark_memory.png. Raw CSVs are reused from data/raw via a symlink.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from datetime import date, timedelta
from importlib.metadata import version
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CLI = [sys.executable, "-m", "clickstream.cli"]


def run(cmd: list[str], env: dict) -> tuple[float, float]:
    """Run a child process; return (seconds, peak RSS in MB). Raises on failure."""
    t = time.perf_counter()
    p = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    _, status, rusage = os.wait4(p.pid, 0)
    secs = time.perf_counter() - t
    err = p.stderr.read().decode() if p.stderr else ""
    if os.waitstatus_to_exitcode(status) != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed:\n{err[-3000:]}")
    rss = rusage.ru_maxrss / (
        1024 * 1024 if sys.platform == "darwin" else 1024
    )  # bytes on macOS, KB on Linux
    return secs, rss


def month_days(month: str) -> list[date]:
    y, m = map(int, month.split("-"))
    d, out = date(y, m, 1), []
    while d.month == m:
        out.append(d)
        d += timedelta(days=1)
    return out


def engine_child(engine: str, day: str) -> None:
    """Child mode: load one day's silver events + previous carry, run one engine, write nothing."""
    from clickstream.config import Paths, gap_seconds
    from clickstream.io import day_bounds
    from clickstream.sessionize import run_engine
    from clickstream.sessions import carry_in, load_events

    paths = Paths.default()
    d = date.fromisoformat(day)
    carry, _ = carry_in(paths, d)
    _, end = day_bounds(d)
    run_engine(engine, load_events(paths, d), carry, end, gap_seconds())


def fresh_root(root: Path, raw: Path) -> None:
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    (root / "raw").symlink_to(raw.resolve())


def machine() -> dict:
    mem = None
    if sys.platform == "darwin":
        mem = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"]).decode()) // 2**30
        cpu = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"]).decode().strip()
    else:
        cpu = platform.processor()
    return {
        "platform": platform.platform(),
        "cpu": cpu,
        "cpus": os.cpu_count(),
        "ram_gb": mem,
        "python": platform.python_version(),
        "duckdb": version("duckdb"),
        "polars": version("polars"),
        "duckdb_memory_limit": os.environ.get("CLICKSTREAM_DUCKDB_MEMORY", "2GB"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", default="2019-10,2019-11")
    ap.add_argument("--root", type=Path, default=REPO / "data" / "bench")
    ap.add_argument("--raw", type=Path, default=REPO / "data" / "raw")
    ap.add_argument("--out", type=Path, default=REPO / "reports")
    ap.add_argument(
        "--max-days", type=int, default=None, help="smoke test: only the first N days of each month"
    )
    args = ap.parse_args()
    months = args.months.split(",")
    env = {
        **os.environ,
        "CLICKSTREAM_DATA": str(args.root),
        "CLICKSTREAM_REPORTS": str(args.root / "reports"),
    }
    rows: list[dict] = []

    def record(scope: str, stage: str, secs: float, rss: float, **extra) -> None:
        rows.append(
            {"scope": scope, "stage": stage, "seconds": round(secs, 3), "peak_rss_mb": round(rss, 1), **extra}
        )
        print(
            f"{scope:<10} {stage:<16} {extra.get('day', extra.get('month', '')):<12} {secs:7.2f}s {rss:8.0f} MB",
            flush=True,
        )

    # 1. default single-process run of one month
    if args.max_days is None:
        fresh_root(args.root, args.raw)
        secs, rss = run([*CLI, "run", "--months", months[0]], env)
        record("default", "run", secs, rss, month=months[0])

    # 2. day by day
    fresh_root(args.root, args.raw)
    for m in months:
        secs, rss = run([*CLI, "ingest", "--month", m], env)
        record("daily", "ingest", secs, rss, month=m)
        for d in month_days(m)[: args.max_days]:
            day = d.isoformat()
            for stage, cmd in [
                ("silver", ["build", "--layer", "silver", "--start", day, "--end", day]),
                ("sessions", ["sessions", "--start", day, "--end", day]),
                ("gold", ["build", "--layer", "gold", "--start", day, "--end", day, "--skip-marts"]),
            ]:
                secs, rss = run([*CLI, *cmd], env)
                record("daily", stage, secs, rss, month=m, day=day)
            for engine in ("duckdb", "polars"):
                secs, rss = run([sys.executable, __file__, "_engine", engine, day], env)
                record("engine", engine, secs, rss, month=m, day=day)
    secs, rss = run([*CLI, "build", "--layer", "gold"], env)
    record("daily", "marts", secs, rss)
    if args.max_days is None:  # reconciliation needs every day built
        secs, rss = run([*CLI, "check"], env)
        record("daily", "check", secs, rss)

    result = {"machine": machine(), "months": months, "measurements": rows}
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "benchmark.json").write_text(json.dumps(result, indent=1) + "\n")
    (args.out / "benchmark.md").write_text(summary_markdown(result))
    plot(result, args.out / "figures" / "benchmark_memory.png")
    print((args.out / "benchmark.md").read_text())


def summary_markdown(result: dict) -> str:
    rows = result["measurements"]
    mc = result["machine"]
    lines = [
        "# Benchmark",
        "",
        f"Generated by `scripts/benchmark.py`. Machine: {mc['cpu']}, {mc['cpus']} cores, {mc['ram_gb']} GB RAM, "
        f"{mc['platform']}; Python {mc['python']}, DuckDB {mc['duckdb']}, Polars {mc['polars']}, "
        f"DuckDB memory limit {mc['duckdb_memory_limit']}. Peak memory is the peak RSS of each child process.",
        "",
        "## Default run (one month, single process)",
        "",
        "| Command | Time (s) | Peak memory (MB) |",
        "|---|---:|---:|",
    ]
    for r in rows:
        if r["scope"] == "default":
            lines.append(
                f"| `clickstream run --months {r['month']}` | {r['seconds']:.1f} | {r['peak_rss_mb']:.0f} |"
            )
    lines += [
        "",
        "## Day by day, per stage and month",
        "",
        "| Month | Stage | Runs | Total time (s) | Median time per run (s) | Max time per run (s) | Median peak (MB) | Max peak (MB) |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for m in result["months"]:
        for stage in ("ingest", "silver", "sessions", "gold"):
            sel = [r for r in rows if r["scope"] == "daily" and r["stage"] == stage and r.get("month") == m]
            if sel:
                t = [r["seconds"] for r in sel]
                p = [r["peak_rss_mb"] for r in sel]
                lines.append(
                    f"| {m} | {stage} | {len(sel)} | {sum(t):.1f} | {statistics.median(t):.2f} | {max(t):.2f} | "
                    f"{statistics.median(p):.0f} | {max(p):.0f} |"
                )
    for stage in ("marts", "check"):
        for r in rows:
            if r["scope"] == "daily" and r["stage"] == stage:
                lines.append(
                    f"| all | {stage} | 1 | {r['seconds']:.1f} | | | {r['peak_rss_mb']:.0f} | {r['peak_rss_mb']:.0f} |"
                )
    lines += [
        "",
        "## Sessionization engines (per day, compute only, same inputs)",
        "",
        "| Month | Engine | Days | Total time (s) | Median time per day (s) | Median peak (MB) | Max peak (MB) |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for m in result["months"]:
        for e in ("duckdb", "polars"):
            sel = [r for r in rows if r["scope"] == "engine" and r["stage"] == e and r.get("month") == m]
            if sel:
                t = [r["seconds"] for r in sel]
                p = [r["peak_rss_mb"] for r in sel]
                lines.append(
                    f"| {m} | {e} | {len(sel)} | {sum(t):.1f} | {statistics.median(t):.2f} | "
                    f"{statistics.median(p):.0f} | {max(p):.0f} |"
                )
    lines += ["", "![Peak memory per day](figures/benchmark_memory.png)", ""]
    return "\n".join(lines)


def plot(result: dict, out: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from clickstream.charts import PALETTE, style_axes

    rows = [r for r in result["measurements"] if r.get("day")]
    days = sorted({r["day"] for r in rows})
    series = [
        ("daily", "silver"),
        ("daily", "sessions"),
        ("daily", "gold"),
        ("engine", "duckdb"),
        ("engine", "polars"),
    ]
    fig, ax = plt.subplots(figsize=(9, 4.2), dpi=150)
    for i, (scope, stage) in enumerate(series):
        by_day = {r["day"]: r["peak_rss_mb"] for r in rows if r["scope"] == scope and r["stage"] == stage}
        label = f"{stage} stage" if scope == "daily" else f"{stage} engine only"
        ax.plot(range(len(days)), [by_day.get(d) for d in days], color=PALETTE[i], lw=2, label=label)
    ticks = [i for i, d in enumerate(days) if d.endswith("-01") or d.endswith("-15")]
    ax.set_xticks(ticks, [days[i] for i in ticks])
    ax.set_ylabel("Peak memory per process (MB)")
    ax.set_ylim(bottom=0)
    ax.set_title("Peak memory per process follows daily volume, not data already processed", loc="left")
    style_axes(ax)
    ax.legend(frameon=False, ncol=3, loc="lower center", bbox_to_anchor=(0.5, -0.32), fontsize=8)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "_engine":
        engine_child(sys.argv[2], sys.argv[3])
    else:
        main()
