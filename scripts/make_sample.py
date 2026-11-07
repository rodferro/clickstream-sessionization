"""Generate the committed synthetic sample (data/sample/events_sample.csv).

Not real data: random users plus hand-placed edge cases, in the exact raw CSV format of the
REES46 files, so tests and CI can run the whole pipeline without Kaggle. Deterministic (seeded);
tests check that regenerating gives a byte-identical file.

    python scripts/make_sample.py [--out data/sample/events_sample.csv]
"""

from __future__ import annotations

import argparse
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

SEED = 20191001
START = datetime(2019, 10, 1, tzinfo=UTC)
DAYS = 3
N_USERS = 300
HEADER = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session"

CATEGORIES = [
    (2053013555631882655, "electronics.smartphone"),
    (2053013554415534427, "electronics.video.tv"),
    (2053013563810775923, "appliances.kitchen.washer"),
    (2053013552326770905, "appliances.environment.water_heater"),
    (2053013553559896355, "computers.notebook"),
    (2053013566100866035, "apparel.shoes"),
    (2103807459595387724, ""),  # accessories without a category_code (like ~32% of real rows)
]
BRANDS = ["samsung", "apple", "xiaomi", "huawei", "lg", "sony", "bosch", "acer", "nike", ""]

# Fixed user ids for hand-placed edge cases (see tests/test_pipeline_sample.py)
U_THRESHOLD = 900000001  # gaps of exactly 1800 s and 1801 s
U_MIDNIGHT = 900000002  # session crossing midnight 10-01 -> 10-02
U_CHAIN = 900000003  # one session spanning three day partitions (event every 20 min)
U_DUPES = 900000004  # exact duplicate cart and purchase rows
U_SINGLE = 900000005  # a single-event session
U_TIES = 900000006  # several events in the same second
U_BOT = 900000007  # 40 events in one minute
U_OUT_OF_ORDER = 900000008  # rows written out of time order in the file
U_NO_CART = 900000009  # purchase without a cart event


class Gen:
    def __init__(self, seed: int):
        self.r = random.Random(seed)
        self.rows: list[tuple[datetime, int, str]] = []  # (time, insertion seq, csv line)
        self.products = {}

    def uuid(self) -> str:
        return str(uuid.UUID(int=self.r.getrandbits(128), version=4))

    def product(self) -> tuple[int, int, str, str, float]:
        pid = self.r.randint(1000000, 1000400)
        if pid not in self.products:
            cid, code = self.r.choice(CATEGORIES)
            brand = self.r.choice(BRANDS)
            price = round(
                self.r.choice([self.r.uniform(1, 60), self.r.uniform(60, 600), self.r.uniform(600, 2500)]), 2
            )
            self.products[pid] = (pid, cid, code, brand, price)
        return self.products[pid]

    def emit(
        self, t: datetime, etype: str, user: int, session: str | None, product=None, price=None, raw_user=None
    ):
        pid, cid, code, brand, p = product or self.product()
        price = p if price is None else price
        user_s = str(user) if raw_user is None else raw_user
        line = f"{t:%Y-%m-%d %H:%M:%S} UTC,{etype},{pid},{cid},{code},{brand},{price:.2f},{user_s},{session or ''}"
        self.rows.append((t, len(self.rows), line))
        return line


def build() -> list[str]:
    g = Gen(SEED)
    r = g.r
    # --- random users: visits made of bursts of events, realistic-ish gaps ---
    for i in range(N_USERS):
        user = 500000000 + i
        t = START + timedelta(seconds=r.randint(0, DAYS * 86400 - 1))
        for _visit in range(r.choice([1, 1, 1, 2, 2, 3, 4])):
            sess = g.uuid()
            for _ in range(r.choice([1, 1, 2, 3, 5, 8, 13, 21])):
                if t >= START + timedelta(days=DAYS):
                    break
                prod = g.product()
                g.emit(t, "view", user, sess, prod)
                if r.random() < 0.08:
                    t += timedelta(seconds=r.randint(1, 120))
                    g.emit(t, "cart", user, sess, prod)
                    if r.random() < 0.5:
                        t += timedelta(seconds=r.randint(5, 300))
                        g.emit(t, "purchase", user, sess, prod)
                # gaps: mostly seconds, sometimes minutes, rarely > 30 min within the "same" visit
                t += timedelta(
                    seconds=int(
                        r.choice([r.expovariate(1 / 40), r.expovariate(1 / 300), r.uniform(1700, 1900)])
                        if r.random() < 0.97
                        else r.uniform(1800, 4000)
                    )
                )
            t += timedelta(seconds=r.randint(2000, 40000))

    # --- hand-placed edge cases ---
    s = g.uuid()
    t0 = START + timedelta(hours=10)
    g.emit(t0, "view", U_THRESHOLD, s)
    g.emit(t0 + timedelta(seconds=1800), "view", U_THRESHOLD, s)  # exactly 30 min: same session
    g.emit(t0 + timedelta(seconds=3601), "view", U_THRESHOLD, s)  # 30 min 1 s later: new session

    s = g.uuid()
    g.emit(datetime(2019, 10, 1, 23, 50, tzinfo=UTC), "view", U_MIDNIGHT, s)
    g.emit(datetime(2019, 10, 2, 0, 10, tzinfo=UTC), "view", U_MIDNIGHT, s)  # 20 min later, next day

    s = g.uuid()
    t = datetime(2019, 10, 1, 22, 0, tzinfo=UTC)
    while t <= datetime(2019, 10, 3, 1, 0, tzinfo=UTC):  # 1 session, 3 day partitions
        g.emit(t, "view", U_CHAIN, s)
        t += timedelta(minutes=20)

    s = g.uuid()
    t = START + timedelta(hours=14)
    prod = g.product()
    g.emit(t, "view", U_DUPES, s, prod)
    line = g.emit(t + timedelta(seconds=30), "cart", U_DUPES, s, prod)
    g.rows.append((t + timedelta(seconds=30), len(g.rows), line))  # exact duplicate cart
    line = g.emit(t + timedelta(seconds=90), "purchase", U_DUPES, s, prod)
    g.rows.append((t + timedelta(seconds=90), len(g.rows), line))  # exact duplicate purchase

    g.emit(START + timedelta(days=1, hours=5), "view", U_SINGLE, g.uuid())

    s = g.uuid()
    t = START + timedelta(days=1, hours=8)
    for _ in range(3):
        g.emit(t, "view", U_TIES, s)

    s = g.uuid()
    t = START + timedelta(days=2, hours=3)
    for k in range(40):
        g.emit(t + timedelta(seconds=k), "view", U_BOT, s)

    s = g.uuid()
    t = START + timedelta(days=2, hours=12)
    g.emit(t, "view", U_NO_CART, s)
    g.emit(t + timedelta(seconds=60), "purchase", U_NO_CART, s)

    # quarantine material
    g.emit(START + timedelta(hours=6), "wishlist", 500000001, g.uuid())
    g.emit(START + timedelta(hours=7), "view", 500000002, g.uuid(), price=-5.0)
    g.emit(START + timedelta(hours=8), "view", 0, g.uuid(), raw_user="")
    # null user_session, zero price
    g.emit(START + timedelta(hours=9), "view", 500000003, None)
    g.emit(START + timedelta(hours=9, minutes=1), "view", 500000003, None, price=0.0)

    rows = sorted(g.rows)
    lines = [ln for _, _, ln in rows]

    # out of order in the file: this user's 3 events are written newest first
    s = g.uuid()
    t = START + timedelta(days=1, hours=16)
    ooo = [
        f"{t + timedelta(minutes=m):%Y-%m-%d %H:%M:%S} UTC,view,1000001,{CATEGORIES[0][0]},{CATEGORIES[0][1]},samsung,100.00,{U_OUT_OF_ORDER},{s}"
        for m in (0, 10, 50)
    ]
    lines.extend(reversed(ooo))  # appended at the very end of the file, after later timestamps
    return lines


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path("data/sample/events_sample.csv"))
    args = ap.parse_args()
    lines = build()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(HEADER + "\n" + "\n".join(lines) + "\n")
    print(f"wrote {len(lines)} events to {args.out}")


if __name__ == "__main__":
    main()
