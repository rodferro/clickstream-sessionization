"""End-to-end tests on the committed synthetic sample (data/sample/events_sample.csv)."""

from __future__ import annotations

import json
import sys
from datetime import date

import polars as pl
import pytest

from clickstream import bronze, check, compare, gold, sessions, silver
from clickstream.io import connect, list_partitions, sql_str
from tests.conftest import SAMPLE, quiet, run_all, tree_hash, write_csv

sys.path.insert(0, str(SAMPLE.parents[2] / "scripts"))
import make_sample as ms  # noqa: E402

D1, D2, D3, D4 = (date(2019, 10, d) for d in (1, 2, 3, 4))
OUTPUTS = ("silver", "gold", "state/carry")


@pytest.fixture
def built(paths):
    run_all(paths, SAMPLE)
    return paths


def gold_sessions(paths, user_id: int) -> list[dict]:
    df = pl.read_parquet(paths.gold_table("sessions") / "*" / "*.parquet", hive_partitioning=False)
    return df.filter(pl.col("user_id") == user_id).sort("session_start").to_dicts()


def test_sample_is_reproducible(tmp_path):
    out = tmp_path / "s.csv"
    out.write_text(ms.HEADER + "\n" + "\n".join(ms.build()) + "\n")
    assert out.read_bytes() == SAMPLE.read_bytes(), "run scripts/make_sample.py and commit the result"


def test_layers_reconcile_and_checks_pass(built):
    report = check.run_checks(built, log=quiet)
    assert not report.failed_errors
    assert list_partitions(built.sessionized) == [D1, D2, D3, D4]


def test_exact_threshold_gaps(built):
    s = gold_sessions(built, ms.U_THRESHOLD)
    assert [x["n_events"] for x in s] == [2, 1]
    assert s[0]["duration_s"] == 1800


def test_session_across_midnight(built):
    (s,) = gold_sessions(built, ms.U_MIDNIGHT)
    assert s["crosses_midnight"] and s["session_date"] == D1 and s["n_events"] == 2


def test_session_across_three_day_partitions(built):
    (s,) = gold_sessions(built, ms.U_CHAIN)
    assert s["n_event_days"] == 3 and s["session_date"] == D1
    assert s["n_events"] == 82  # every 20 min from 10-01 22:00 to 10-03 01:00 inclusive


def test_duplicates_removed_and_counted(built):
    (s,) = gold_sessions(built, ms.U_DUPES)
    assert (s["n_carts"], s["n_purchases"]) == (1, 1)
    m = json.loads((built.dq / "silver" / "2019-10-01.json").read_text())["metrics"]
    assert m["duplicates_removed_by_event_type"]["cart"] >= 1
    assert m["purchase_duplicates_removed"] == 1
    assert m["purchase_duplicate_revenue_removed"] > 0


def test_single_event_and_ties(built):
    (single,) = gold_sessions(built, ms.U_SINGLE)
    assert single["n_events"] == 1 and single["duration_s"] == 0
    (ties,) = gold_sessions(built, ms.U_TIES)
    assert ties["n_events"] == 3 and ties["duration_s"] == 0


def test_out_of_order_rows_in_file(built):
    s = gold_sessions(built, ms.U_OUT_OF_ORDER)
    assert [x["n_events"] for x in s] == [2, 1]  # 16:00, 16:10 | 16:50


def test_bot_flag_and_purchase_without_cart(built):
    (bot,) = gold_sessions(built, ms.U_BOT)
    assert bot["suspect_bot"]
    (nc,) = gold_sessions(built, ms.U_NO_CART)
    assert nc["reached_purchase"] and not nc["reached_cart"]


def test_quarantine_reasons(built):
    con = connect()
    reasons = {
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT quarantine_reason FROM read_parquet({sql_str(str(built.quarantine / '*' / '*.parquet'))}, hive_partitioning=false)"
        ).fetchall()
    }
    assert reasons == {"invalid_event_type", "negative_price", "null_user_id"}


def test_three_engines_agree_on_sample(built):
    result = compare.compare(built, list_partitions(built.silver), ["duckdb", "polars", "python"], log=quiet)
    assert result["agree"], result
    assert result["rows_compared"] == 4228


def test_rerun_without_changes_is_a_noop(built):
    before = tree_hash(built.root, *OUTPUTS)
    assert bronze.ingest(SAMPLE, built, log=quiet) == []
    assert silver.build(built, log=quiet) == []
    assert sessions.build(built, log=quiet) == []
    assert gold.build(built, log=quiet) == []
    assert tree_hash(built.root, *OUTPUTS) == before


def test_forced_rerun_is_byte_identical(built):
    before = tree_hash(built.root, *OUTPUTS)
    silver.build(built, force=True, log=quiet)
    sessions.build(built, force=True, log=quiet)
    gold.build(built, force=True, log=quiet)
    assert tree_hash(built.root, *OUTPUTS) == before


@pytest.mark.parametrize("engine", ["duckdb", "polars", "python"])
def test_incremental_equals_full_run(paths, tmp_path, engine):
    from clickstream.config import Paths

    full = Paths(root=tmp_path / "full", reports=tmp_path / "r")
    run_all(full, SAMPLE, engine="duckdb")

    bronze.ingest(SAMPLE, paths, log=quiet)
    silver.build(paths, [D1, D2], log=quiet)
    sessions.build(paths, [D1, D2], engine=engine, log=quiet)
    assert gold.build(paths, log=quiet) == [D1, D2]
    silver.build(paths, log=quiet)
    assert sessions.build(paths, engine=engine, log=quiet) == [D3, D4]  # old days not reprocessed
    rebuilt = gold.build(paths, log=quiet)
    # D1 is rebuilt because the chain user's session (started D1) continues into D3.
    assert D1 in rebuilt and set(rebuilt) == set(gold.affected_dates(paths, [D3, D4]))
    assert tree_hash(paths.root, *OUTPUTS) == tree_hash(full.root, *OUTPUTS)


def test_late_data_reprocesses_only_affected_days(built, tmp_path):
    before = tree_hash(built.root, "silver/sessionized_events", "gold")
    late = write_csv(tmp_path / "late.csv", [("2019-10-02 23:59:00", "view", 123456789)])
    assert bronze.ingest(late, built, log=quiet) == [D2]
    assert silver.build(built, log=quiet) == [D2]
    # D3 is re-run because carry(D2) changed; D4 is skipped because carry(D3) did not.
    assert sessions.build(built, log=quiet) == [D2, D3]
    after = tree_hash(built.root, "silver/sessionized_events", "gold")
    d1 = "silver/sessionized_events/event_date=2019-10-01/part-0.parquet"
    assert after[d1] == before[d1]
    # Gold D1 is re-checked because sessions starting on D1 (the 3-day chain) read D2's
    # partition; its content doesn't change. D3/D4 gold only hold sessions starting on those days.
    assert gold.build(built, log=quiet) == [D1, D2]
    after = tree_hash(built.root, "gold")
    for k in after:
        if "session_date=2019-10-01" in k or "session_date=2019-10-03" in k or "session_date=2019-10-04" in k:
            assert after[k] == before[k], k
    assert not check.run_checks(built, log=quiet).failed_errors
