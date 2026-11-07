"""Quality rules, failure handling and gold calculations on small hand-made inputs."""

from __future__ import annotations

import ast
import json
import sys
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from clickstream import bronze, gold, sessions, silver
from clickstream.io import connect, partition_dir
from clickstream.quality import DataQualityError
from clickstream.sessionize import ENGINES
from tests.conftest import quiet, run_all, write_csv

D1 = date(2019, 10, 1)


def read(path_glob: Path) -> list[dict]:
    return pl.read_parquet(path_glob, hive_partitioning=False).to_dicts()


def test_bad_timestamp_fails_ingest(paths, tmp_path):
    csv = tmp_path / "bad.csv"
    csv.write_text(
        "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n"
        "2019/10/01 00:00:00,view,1,2,,x,1.0,3,s\n"
    )
    with pytest.raises(pl.exceptions.InvalidOperationError):
        bronze.ingest(csv, paths, log=quiet)


def test_wrong_header_fails_ingest(paths, tmp_path):
    csv = tmp_path / "bad.csv"
    csv.write_text("time,type\n2019-10-01 00:00:00 UTC,view\n")
    with pytest.raises(DataQualityError):
        bronze.ingest(csv, paths, log=quiet)


def test_duplicate_counts_by_type_and_purchase_revenue(paths, tmp_path):
    ev = [
        ("2019-10-01 10:00:00", "view", 1),
        ("2019-10-01 10:01:00", "cart", 1),
        ("2019-10-01 10:01:00", "cart", 1),
        ("2019-10-01 10:01:00", "cart", 1),
        ("2019-10-01 10:02:00", "purchase", 1, 1, 99.5),
        ("2019-10-01 10:02:00", "purchase", 1, 1, 99.5),
    ]
    bronze.ingest(write_csv(tmp_path / "d.csv", ev), paths, log=quiet)
    silver.build(paths, log=quiet)
    m = json.loads((paths.dq / "silver" / "2019-10-01.json").read_text())["metrics"]
    assert m["duplicates_removed_by_event_type"] == {"cart": 2, "purchase": 1}
    assert m["purchase_duplicate_revenue_removed"] == 99.5
    assert m["silver_rows"] == 3


def test_failed_check_does_not_publish(paths, tmp_path, monkeypatch):
    ev = [("2019-10-01 10:00:00", "view", 1), ("2019-10-01 10:05:00", "view", 2)]
    bronze.ingest(write_csv(tmp_path / "d.csv", ev), paths, log=quiet)
    silver.build(paths, log=quiet)

    real = ENGINES["duckdb"]

    def lossy(events, carry, end, gap):  # drops one assignment -> must be caught
        a, c = real(events, carry, end, gap)
        return a.slice(0, a.num_rows - 1), c

    monkeypatch.setitem(ENGINES, "duckdb", lossy)
    with pytest.raises(DataQualityError, match="every_event_has_one_session"):
        sessions.build(paths, log=quiet)
    assert not partition_dir(paths.sessionized, D1).exists()
    report = json.loads((paths.dq / "sessions" / "2019-10-01.json").read_text())
    assert report["status"] == "fail"


def test_gold_session_values(paths, tmp_path):
    ev = [
        ("2019-10-01 10:00:00", "view", 1, 11, 5.0, "acme", "electronics.audio"),
        ("2019-10-01 10:10:00", "view", 1, 12, 300.0, "zeta", "appliances.kitchen"),
        ("2019-10-01 10:11:00", "cart", 1, 12, 300.0, "zeta", "appliances.kitchen"),
        ("2019-10-01 10:20:00", "purchase", 1, 12, 300.0, "zeta", "appliances.kitchen"),
        ("2019-10-01 12:00:00", "view", 2, 11, 5.0, "acme", "electronics.audio"),
    ]
    run_all(paths, write_csv(tmp_path / "g.csv", ev))
    s = {r["user_id"]: r for r in read(paths.gold_table("sessions") / "*" / "*.parquet")}
    assert s[1]["duration_s"] == 1200 and s[1]["n_events"] == 4
    assert (s[1]["n_views"], s[1]["n_carts"], s[1]["n_purchases"]) == (2, 1, 1)
    assert float(s[1]["revenue"]) == 300.0
    assert (s[1]["entry_category_top"], s[1]["entry_brand"], s[1]["entry_price_band"]) == (
        "electronics",
        "acme",
        "0-10",
    )
    assert s[2]["duration_s"] == 0 and not s[2]["reached_purchase"]

    m = {r["population"]: r for r in read(paths.gold_table("daily_metrics") / "*" / "*.parquet")}
    assert m["all"]["sessions"] == 2 and m["all"]["sessions_with_purchase"] == 1
    assert m["all"]["single_event_share"] == 0.5

    conv = {r["category_top"]: r for r in read(paths.gold_table("marts") / "conversion_by_category.parquet")}
    assert conv["appliances"]["purchase_rate"] == 1.0
    assert conv["electronics"]["sessions"] == 2 and conv["electronics"]["sessions_purchased"] == 0


def test_price_bands():
    con = connect()
    got = [
        con.execute(f"SELECT {gold.price_band_sql(str(p))}").fetchone()[0]
        for p in (0, 9.99, 10, 999, 1000, 2500)
    ]
    assert got == ["0-10", "0-10", "10-50", "500-1000", "1000+", "1000+"]


def test_reference_engine_uses_only_standard_library():
    src = Path(sys.modules["clickstream.sessionize.reference"].__file__).read_text()
    imported = set()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert imported <= set(sys.stdlib_module_names) | {"__future__"}, imported


def test_engine_outputs_have_common_schema():
    from clickstream.sessionize import ASSIGNMENT_SCHEMA, TS, empty_carry, run_engine
    from tests.conftest import ts

    ev = pa.table(
        {
            "user_id": pa.array([1], pa.int64()),
            "event_time": pa.array([ts("2019-10-01 01:00:00")], TS),
            "source_file": ["f"],
            "source_row": pa.array([0], pa.int64()),
        }
    )
    for name in ENGINES:
        a, _ = run_engine(name, ev, empty_carry(), ts("2019-10-02 00:00:00"), 1800)
        assert a.schema == ASSIGNMENT_SCHEMA


def test_sustained_activity_bot_rule(paths, tmp_path, monkeypatch):
    from clickstream import config

    monkeypatch.setattr(config, "BOT_SUSTAINED_MIN_ACTIVE_DAYS", 3)
    monkeypatch.setattr(config, "BOT_SUSTAINED_MIN_EVENTS_PER_ACTIVE_DAY", 4)
    ev = []
    for day in (1, 2, 3):
        for k in range(5):  # 5 events/day on 3 days, never buys -> flagged
            ev.append((f"2019-10-0{day} 10:0{k}:00", "view", 7, 70, 20.0, "botco", "toys.robot"))
        for k in range(5):  # same activity but buys once -> not flagged
            ev.append((f"2019-10-0{day} 11:0{k}:00", "view", 8, 80, 20.0, "acme", "electronics.audio"))
        ev.append(
            (f"2019-10-0{day} 12:00:00", "view", 9, 80, 20.0, "acme", "electronics.audio")
        )  # too few events
    ev.append(("2019-10-03 11:30:00", "purchase", 8, 80, 20.0, "acme", "electronics.audio"))
    run_all(paths, write_csv(tmp_path / "b.csv", ev))
    marts = paths.gold_table("marts")
    flagged = read(marts / "bot_users_sustained.parquet")
    assert [(r["month"], r["user_id"], r["active_days"], r["events"]) for r in flagged] == [
        ("2019-10", 7, 3, 15)
    ]
    conv = {r["category_top"]: r for r in read(marts / "conversion_by_category.parquet")}
    assert "toys" not in conv  # all of user 7's sessions removed
    assert conv["electronics"]["sessions"] == 6 and conv["electronics"]["sessions_purchased"] == 1
    dur = read(marts / "duration_by_segment.parquet")
    cats = {r["segment"]: r["sessions"] for r in dur if r["segment_type"] == "category_top"}
    assert cats == {"electronics": 6}
