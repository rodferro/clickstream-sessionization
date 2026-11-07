"""Benchmark the pipeline end to end, one subprocess per stage and day, on a fresh data root.

    python scripts/benchmark.py --months 2019-10,2019-11

Measures wall time and peak resident memory (ru_maxrss of each child process, via wait4):
  1. default one-month run: `clickstream run --months <first month>` in a single process
  2. day by day over all months: ingest per month, then silver / sessions / gold per day
     (gold without marts), then marts once
  3. sessionization engines on the same inputs, per day: DuckDB vs Polars (compute only)

Repeats everything --runs times (default 3) and reports median [min-max] across runs.
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
    ap.add_argument("--runs", type=int, default=3, help="repeat the whole benchmark N times")
    args = ap.parse_args()
    months = args.months.split(",")
    env = {
        **os.environ,
        "CLICKSTREAM_DATA": str(args.root),
        "CLICKSTREAM_REPORTS": str(args.root / "reports"),
    }
    rows: list[dict] = []
    conditions: list[dict] = []

    for run_no in range(1, args.runs + 1):

        def record(scope: str, stage: str, secs: float, rss: float, **extra) -> None:
            rows.append(
                {
                    "run": run_no,  # noqa: B023 - recorded within this iteration
                    "scope": scope,
                    "stage": stage,
                    "seconds": round(secs, 3),
                    "peak_rss_mb": round(rss, 1),
                    **extra,
                }
            )
            label = extra.get("day", extra.get("month", ""))
            print(f"run {run_no} {scope:<8} {stage:<10} {label:<12} {secs:7.2f}s {rss:8.0f} MB", flush=True)  # noqa: B023

        conditions.append({"run": run_no, **run_conditions()})
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

    result = {
        "machine": machine(),
        "months": months,
        "runs": args.runs,
        "conditions": conditions,
        "measurements": rows,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "benchmark.json").write_text(json.dumps(result, indent=1) + "\n")
    (args.out / "benchmark.md").write_text(summary_markdown(result))
    plot(result, args.out / "figures" / "benchmark_memory.png")
    print((args.out / "benchmark.md").read_text())


def run_conditions() -> dict:
    """What else the machine was doing when a run started: power source and load average."""
    power = None
    if sys.platform == "darwin":
        out = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True).stdout
        power = "AC" if "AC Power" in out else ("battery" if "Battery Power" in out else None)
    return {
        "power": power,
        "load_avg_1m": round(os.getloadavg()[0], 2),
        "started": time.strftime("%Y-%m-%d %H:%M"),
    }


def _spread(values: list[float], fmt: str = ".1f") -> str:
    """median [min-max] across runs."""
    if len(values) == 1:
        return format(values[0], fmt)
    return f"{statistics.median(values):{fmt}} [{min(values):{fmt}}-{max(values):{fmt}}]"


def summary_markdown(result: dict) -> str:
    rows = result["measurements"]
    mc = result["machine"]
    runs = sorted({r["run"] for r in rows})

    def per_run(pred, agg_time=sum, agg_mem=max) -> tuple[list[float], list[float]]:
        t, p = [], []
        for n in runs:
            sel = [r for r in rows if r["run"] == n and pred(r)]
            if sel:
                t.append(agg_time([r["seconds"] for r in sel]))
                p.append(agg_mem([r["peak_rss_mb"] for r in sel]))
        return t, p

    lines = [
        "# Benchmark",
        "",
        f"Generated by `scripts/benchmark.py --runs {len(runs)}`. Machine: {mc['cpu']}, {mc['cpus']} cores, "
        f"{mc['ram_gb']} GB RAM, {mc['platform']}; Python {mc['python']}, DuckDB {mc['duckdb']}, "
        f"Polars {mc['polars']}, DuckDB memory limit {mc['duckdb_memory_limit']}. Peak memory is the peak RSS "
        "of each child process. Values are **median [min-max] across runs**.",
        "",
        "Conditions at the start of each run: "
        + "; ".join(
            f"run {c['run']} {c['started']}, power {c['power']}, 1-min load average {c['load_avg_1m']}"
            for c in result.get("conditions", [])
        )
        + ".",
        "",
        "## Default run (one month, single process)",
        "",
        "| Command | Time (s) | Peak memory (MB) |",
        "|---|---:|---:|",
    ]
    for m in result["months"][:1]:
        t, p = per_run(lambda r, m=m: r["scope"] == "default" and r["month"] == m)
        if t:
            lines.append(f"| `clickstream run --months {m}` | {_spread(t)} | {_spread(p, '.0f')} |")
    lines += [
        "",
        "## Day by day, per stage and month",
        "",
        "Total time = sum over the month's days in one run; max peak = highest per-day peak in one run.",
        "",
        "| Month | Stage | Days | Total time (s) | Median time per day (s) | Max peak (MB) |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for m in result["months"]:
        for stage in ("ingest", "silver", "sessions", "gold"):

            def pred(r, m=m, stage=stage):
                return r["scope"] == "daily" and r["stage"] == stage and r.get("month") == m

            t, p = per_run(pred)
            med, _ = per_run(pred, agg_time=statistics.median)
            if t:
                days = len([r for r in rows if r["run"] == runs[0] and pred(r)])
                lines.append(
                    f"| {m} | {stage} | {days} | {_spread(t)} | {_spread(med, '.2f')} | {_spread(p, '.0f')} |"
                )
    for stage in ("marts", "check"):
        t, p = per_run(lambda r, stage=stage: r["scope"] == "daily" and r["stage"] == stage)
        if t:
            lines.append(f"| all | {stage} | | {_spread(t)} | | {_spread(p, '.0f')} |")
    lines += [
        "",
        "## Sessionization engines (per day, compute only, same inputs)",
        "",
        "| Month | Engine | Days | Total time (s) | Median time per day (s) | Max peak (MB) |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for m in result["months"]:
        for e in ("duckdb", "polars"):

            def pred(r, m=m, e=e):
                return r["scope"] == "engine" and r["stage"] == e and r.get("month") == m

            t, p = per_run(pred)
            med, _ = per_run(pred, agg_time=statistics.median)
            if t:
                days = len([r for r in rows if r["run"] == runs[0] and pred(r)])
                lines.append(
                    f"| {m} | {e} | {days} | {_spread(t)} | {_spread(med, '.2f')} | {_spread(p, '.0f')} |"
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
        vals: dict[str, list[float]] = {}
        for r in rows:
            if r["scope"] == scope and r["stage"] == stage:
                vals.setdefault(r["day"], []).append(r["peak_rss_mb"])
        label = f"{stage} stage" if scope == "daily" else f"{stage} engine only"
        ys = [statistics.median(vals[d]) if d in vals else None for d in days]
        ax.plot(range(len(days)), ys, color=PALETTE[i], lw=2, label=label)
    ticks = [i for i, d in enumerate(days) if d.endswith("-01") or d.endswith("-15")]
    ax.set_xticks(ticks, [days[i] for i in ticks])
    ax.set_ylabel("Peak memory per process (MB), median of runs")
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
