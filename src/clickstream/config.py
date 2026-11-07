"""Central configuration: paths, the session definition, quality and bot thresholds.

Everything that changes results lives here, so README numbers can be traced to one place.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

# --- Session definition --------------------------------------------------------------------
# A new session starts when the gap to the user's previous event is strictly greater than this.
# A gap of exactly SESSION_GAP_MINUTES stays in the same session.
SESSION_GAP_MINUTES = 30

# --- Source data ---------------------------------------------------------------------------
KAGGLE_DATASET = "mkechinov/ecommerce-behavior-data-from-multi-category-store"
KAGGLE_URL = "https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store"
REES46_URL = "https://rees46.com/"
# Month key -> file name inside the Kaggle dataset. Only these two months are on Kaggle.
MONTH_FILES = {"2019-10": "2019-Oct.csv", "2019-11": "2019-Nov.csv"}

SOURCE_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]
EVENT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S UTC"
# remove_from_cart is documented by the dataset but absent from Oct/Nov 2019; still accepted.
EVENT_TYPES = ("view", "cart", "remove_from_cart", "purchase")

# --- Suspected bots ------------------------------------------------------------------------
# Flagged per user and UTC day; sessions touching a flagged user-day get suspect_bot = true.
# Flagged rows are kept, never dropped. Segment/conversion marts exclude them.
BOT_MAX_EVENTS_PER_MINUTE = 30  # >= this many events in one calendar minute
BOT_MAX_EVENTS_PER_DAY = 1000  # >= this many events in one UTC day

# --- Gold segments -------------------------------------------------------------------------
# Upper-exclusive price band edges for the entry product's price.
PRICE_BAND_EDGES = (10, 50, 100, 250, 500, 1000)
# Brands with fewer sessions than this are left out of brand marts (too noisy to rank).
MIN_SESSIONS_PER_BRAND = 1000

# --- Data-quality warning thresholds -------------------------------------------------------
WARN_QUARANTINE_RATE = 0.001
WARN_DUPLICATE_RATE = 0.01
WARN_DAILY_VOLUME_DEVIATION = 0.5  # vs trailing median of up to 7 previous days

# --- Engine resources ----------------------------------------------------------------------
DUCKDB_MEMORY_LIMIT = os.environ.get("CLICKSTREAM_DUCKDB_MEMORY", "2GB")
# Spill directory for DuckDB operators that exceed the memory limit.
DUCKDB_TEMP_DIR = os.environ.get(
    "CLICKSTREAM_DUCKDB_TMP", os.path.join(tempfile.gettempdir(), "clickstream-duckdb")
)


@dataclass(frozen=True)
class Paths:
    """Filesystem layout under a single data root (default ./data, override with CLICKSTREAM_DATA)."""

    root: Path
    reports: Path = field(default=Path("reports"))

    @classmethod
    def default(cls) -> Paths:
        return cls(
            root=Path(os.environ.get("CLICKSTREAM_DATA", "data")),
            reports=Path(os.environ.get("CLICKSTREAM_REPORTS", "reports")),
        )

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def bronze(self) -> Path:
        return self.root / "bronze" / "events"

    @property
    def silver(self) -> Path:
        return self.root / "silver" / "events"

    @property
    def quarantine(self) -> Path:
        return self.root / "silver" / "quarantine"

    @property
    def bot_flags(self) -> Path:
        return self.root / "silver" / "user_day_flags"

    @property
    def sessionized(self) -> Path:
        return self.root / "silver" / "sessionized_events"

    @property
    def carry(self) -> Path:
        return self.root / "state" / "carry"

    @property
    def manifest(self) -> Path:
        return self.root / "state" / "manifest.json"

    @property
    def dq(self) -> Path:
        return self.root / "dq"

    @property
    def gold(self) -> Path:
        return self.root / "gold"

    def gold_table(self, name: str) -> Path:
        return self.gold / name


def gap_seconds(minutes: int = SESSION_GAP_MINUTES) -> int:
    if not 0 < minutes < 24 * 60:
        # Carry-over between days assumes a session can't skip a whole day.
        raise ValueError("session gap must be between 0 and 24h")
    return minutes * 60
