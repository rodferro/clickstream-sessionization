from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from clickstream import bronze, gold, sessions, silver
from clickstream.config import Paths

REPO = Path(__file__).resolve().parents[1]
SAMPLE = REPO / "data" / "sample" / "events_sample.csv"
HEADER = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session"


def quiet(*_a, **_k):
    pass


@pytest.fixture
def paths(tmp_path) -> Paths:
    return Paths(root=tmp_path / "data", reports=tmp_path / "reports")


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


def write_csv(path: Path, events: list[tuple]) -> Path:
    """events: (time 'YYYY-MM-DD HH:MM:SS', type, user_id[, product_id, price, brand, category_code, user_session])"""
    lines = [HEADER]
    for e in events:
        t, etype, user = e[0], e[1], e[2]
        product = e[3] if len(e) > 3 else 1
        price = e[4] if len(e) > 4 else 10.0
        brand = e[5] if len(e) > 5 else "acme"
        code = e[6] if len(e) > 6 else "electronics.smartphone"
        us = e[7] if len(e) > 7 else f"s-{user}"
        lines.append(f"{t} UTC,{etype},{product},42,{code},{brand},{price:.2f},{user},{us}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    return path


def run_all(paths: Paths, csv: Path, engine: str = "duckdb"):
    bronze.ingest(csv, paths, log=quiet)
    silver.build(paths, log=quiet)
    sessions.build(paths, engine=engine, log=quiet)
    gold.build(paths, log=quiet)


def tree_hash(root: Path, *subdirs: str) -> dict[str, str]:
    out = {}
    for sub in subdirs:
        for p in sorted((root / sub).rglob("*.parquet")):
            out[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out
