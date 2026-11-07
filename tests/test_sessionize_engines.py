"""Unit tests of the sessionizers on hand-made events. Every test runs against all three engines."""

from __future__ import annotations

import random
from datetime import timedelta

import pyarrow as pa
import pytest

from clickstream.sessionize import ENGINES, TS, empty_carry, run_engine
from tests.conftest import ts

GAP = 1800
ENGINE_NAMES = sorted(ENGINES)


def events(rows: list[tuple[int, str]], file: str = "f.csv") -> pa.Table:
    """rows: (user_id, 'YYYY-MM-DD HH:MM:SS'); source_row = position in the list."""
    return pa.table(
        {
            "user_id": pa.array([u for u, _ in rows], pa.int64()),
            "event_time": pa.array([ts(t) for _, t in rows], TS),
            "source_file": pa.array([file] * len(rows), pa.string()),
            "source_row": pa.array(range(len(rows)), pa.int64()),
        }
    )


def day_end(day: str):
    return ts(day + " 00:00:00") + timedelta(days=1)


def sessions_of(assignments: pa.Table) -> list[str]:
    """session_id per source_row, in source_row order."""
    d = dict(zip(assignments["source_row"].to_pylist(), assignments["session_id"].to_pylist(), strict=True))
    return [d[i] for i in sorted(d)]


@pytest.mark.parametrize("engine", ENGINE_NAMES)
def test_exact_threshold_gap_stays_in_session(engine):
    ev = events([(1, "2019-10-01 10:00:00"), (1, "2019-10-01 10:30:00"), (1, "2019-10-01 11:00:01")])
    a, _ = run_engine(engine, ev, empty_carry(), day_end("2019-10-01"), GAP)
    s = sessions_of(a)
    assert s[0] == s[1], "a gap of exactly 30:00 must not start a new session"
    assert s[2] != s[1], "a gap of 30:01 must start a new session"
    assert s[0] == f"1-{int(ts('2019-10-01 10:00:00').timestamp())}"


@pytest.mark.parametrize("engine", ENGINE_NAMES)
def test_single_event_session(engine):
    a, carry = run_engine(
        engine, events([(7, "2019-10-01 12:00:00")]), empty_carry(), day_end("2019-10-01"), GAP
    )
    assert a.num_rows == 1
    assert a["session_start"][0].as_py() == ts("2019-10-01 12:00:00")
    assert carry.num_rows == 0


@pytest.mark.parametrize("engine", ENGINE_NAMES)
def test_out_of_order_input_gives_same_sessions(engine):
    rows = [
        (1, "2019-10-01 10:00:00"),
        (1, "2019-10-01 10:10:00"),
        (1, "2019-10-01 11:00:00"),
        (2, "2019-10-01 10:05:00"),
    ]
    a_sorted, _ = run_engine(engine, events(rows), empty_carry(), day_end("2019-10-01"), GAP)
    shuffled = [rows[2], rows[0], rows[3], rows[1]]
    a_shuf, _ = run_engine(engine, events(shuffled), empty_carry(), day_end("2019-10-01"), GAP)
    by_time_sorted = dict(zip([r[1] + str(r[0]) for r in rows], sessions_of(a_sorted), strict=True))
    by_time_shuf = dict(zip([r[1] + str(r[0]) for r in shuffled], sessions_of(a_shuf), strict=True))
    assert by_time_sorted == by_time_shuf
    assert len(set(sessions_of(a_sorted))) == 3  # user 1: two sessions, user 2: one


@pytest.mark.parametrize("engine", ENGINE_NAMES)
def test_same_second_ties_share_a_session(engine):
    ev = events([(3, "2019-10-01 08:00:00")] * 3 + [(3, "2019-10-01 08:00:01")])
    a, _ = run_engine(engine, ev, empty_carry(), day_end("2019-10-01"), GAP)
    assert len(set(sessions_of(a))) == 1


@pytest.mark.parametrize("engine", ENGINE_NAMES)
def test_session_continues_across_midnight_via_carry(engine):
    a1, c1 = run_engine(
        engine, events([(5, "2019-10-01 23:50:00")]), empty_carry(), day_end("2019-10-01"), GAP
    )
    assert c1.num_rows == 1
    a2, c2 = run_engine(
        engine,
        events([(5, "2019-10-02 00:10:00"), (5, "2019-10-02 00:45:00")]),
        c1,
        day_end("2019-10-02"),
        GAP,
    )
    s1, s2 = sessions_of(a1), sessions_of(a2)
    assert s2[0] == s1[0], "20 min after 23:50 continues yesterday's session"
    assert a2["session_start"][0].as_py() == ts("2019-10-01 23:50:00")
    assert s2[1] != s2[0], "35 min later starts a new session"
    assert c2.num_rows == 0


@pytest.mark.parametrize("engine", ENGINE_NAMES)
def test_carry_boundary_is_inclusive(engine):
    # last event at 23:30:00 can still be continued by an event at 00:00:00 (gap exactly 30 min)
    _, c = run_engine(
        engine,
        events([(1, "2019-10-01 23:30:00"), (2, "2019-10-01 23:29:59")]),
        empty_carry(),
        day_end("2019-10-01"),
        GAP,
    )
    assert c["user_id"].to_pylist() == [1]
    a, _ = run_engine(engine, events([(1, "2019-10-02 00:00:00")]), c, day_end("2019-10-02"), GAP)
    assert sessions_of(a)[0] == c["session_id"][0].as_py()


@pytest.mark.parametrize("engine", ENGINE_NAMES)
def test_carried_session_not_continued_after_gap(engine):
    _, c = run_engine(engine, events([(1, "2019-10-01 23:40:00")]), empty_carry(), day_end("2019-10-01"), GAP)
    a, _ = run_engine(engine, events([(1, "2019-10-02 00:10:01")]), c, day_end("2019-10-02"), GAP)
    assert sessions_of(a)[0] != c["session_id"][0].as_py()
    assert a["session_start"][0].as_py() == ts("2019-10-02 00:10:01")


@pytest.mark.parametrize("engine", ENGINE_NAMES)
def test_one_session_spanning_three_day_partitions(engine):
    carry = empty_carry()
    ids = set()
    t = ts("2019-10-01 22:00:00")
    for day in ("2019-10-01", "2019-10-02", "2019-10-03"):
        rows = []
        while t < day_end(day) and t <= ts("2019-10-03 01:00:00"):
            rows.append((9, t.strftime("%Y-%m-%d %H:%M:%S")))
            t += timedelta(minutes=20)
        a, carry = run_engine(engine, events(rows), carry, day_end(day), GAP)
        ids.update(sessions_of(a))
    assert ids == {f"9-{int(ts('2019-10-01 22:00:00').timestamp())}"}


def _random_days(seed: int, n_users: int = 400, n_events: int = 20000):
    r = random.Random(seed)
    start = ts("2019-10-01 00:00:00")
    rows = []
    for i in range(n_events):
        user = r.randrange(n_users)
        # cluster events so gaps around the threshold are common
        offset = r.choice([r.randrange(3 * 86400), (user * 997) % (3 * 86400) + r.randrange(-3600, 3600)])
        offset = min(max(offset, 0), 3 * 86400 - 1)
        rows.append((user, start + timedelta(seconds=offset), i))
    for i in range(0, 300):  # exact-threshold gaps and midnight neighbours
        u = n_users + i
        base = start + timedelta(days=i % 3, hours=23, minutes=r.choice([29, 30, 31, 45]))
        rows += [
            (u, base, 10**6 + 2 * i),
            (u, base + timedelta(seconds=r.choice([1799, 1800, 1801])), 10**6 + 2 * i + 1),
        ]
    by_day = {}
    for u, t, k in rows:
        by_day.setdefault(t.date(), []).append((u, t, k))
    return by_day


def test_engines_agree_on_generated_events():
    by_day = _random_days(seed=7)
    carries = {e: empty_carry() for e in ENGINE_NAMES}
    for day in sorted(by_day):
        rows = by_day[day]
        ev = pa.table(
            {
                "user_id": pa.array([u for u, _, _ in rows], pa.int64()),
                "event_time": pa.array([t for _, t, _ in rows], TS),
                "source_file": pa.array(["gen.csv"] * len(rows), pa.string()),
                "source_row": pa.array([k for _, _, k in rows], pa.int64()),
            }
        )
        end = day_end(day.isoformat())
        outs = {e: run_engine(e, ev, carries[e], end, GAP) for e in ENGINE_NAMES}
        base_a, base_c = outs["duckdb"]
        for e in ENGINE_NAMES:
            assert outs[e][0].equals(base_a), f"{e} assignments differ on {day}"
            assert outs[e][1].equals(base_c), f"{e} carry differs on {day}"
            carries[e] = outs[e][1]
        assert base_c.num_rows > 0 or day == max(by_day)
