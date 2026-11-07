"""Data-quality checks.

Each stage builds a partition in a temp directory, runs its checks against that temp output, and
writes a JSON report to data/dq/<stage>/<partition>.json (always, pass or fail).

- severity "error": the stage raises DataQualityError, the temp output is discarded and the
  previously published partition (if any) is left as it was. The CLI exits with code 2.
- severity "warn": recorded in the report and printed, the partition is published.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


class DataQualityError(RuntimeError):
    pass


@dataclass
class Check:
    name: str
    severity: str  # "error" | "warn"
    passed: bool
    value: Any = None
    detail: str = ""


@dataclass
class DQReport:
    stage: str
    key: str
    checks: list[Check] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def error(self, name: str, passed: bool, value: Any = None, detail: str = "") -> None:
        self.checks.append(Check(name, "error", bool(passed), _plain(value), detail))

    def warn(self, name: str, passed: bool, value: Any = None, detail: str = "") -> None:
        self.checks.append(Check(name, "warn", bool(passed), _plain(value), detail))

    def metric(self, name: str, value: Any) -> None:
        self.metrics[name] = _plain(value)

    @property
    def failed_errors(self) -> list[Check]:
        return [c for c in self.checks if c.severity == "error" and not c.passed]

    @property
    def failed_warnings(self) -> list[Check]:
        return [c for c in self.checks if c.severity == "warn" and not c.passed]

    def write(self, dq_root: Path) -> Path:
        out = dq_root / self.stage / f"{self.key}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "stage": self.stage,
            "key": self.key,
            "status": "fail" if self.failed_errors else ("warn" if self.failed_warnings else "pass"),
            "checks": [asdict(c) for c in self.checks],
            "metrics": self.metrics,
        }
        out.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str))
        return out

    def finish(self, dq_root: Path, log=print) -> None:
        """Write the report, print warnings, raise on any failed error-level check."""
        self.write(dq_root)
        for c in self.failed_warnings:
            log(f"  WARN [{self.stage} {self.key}] {c.name}: {c.value} {c.detail}".rstrip())
        if self.failed_errors:
            msgs = "; ".join(f"{c.name}={c.value} {c.detail}".strip() for c in self.failed_errors)
            raise DataQualityError(f"[{self.stage} {self.key}] failed: {msgs}")


def _plain(v: Any) -> Any:
    """Make DuckDB/NumPy scalars JSON friendly."""
    if isinstance(v, dict):
        return {str(k): _plain(x) for k, x in v.items()}
    if isinstance(v, list | tuple):
        return [_plain(x) for x in v]
    if hasattr(v, "item") and not isinstance(v, str | bytes):
        try:
            return v.item()
        except (ValueError, AttributeError):
            pass
    return v


def load_reports(dq_root: Path, stage: str | None = None) -> list[dict]:
    pattern = f"{stage}/*.json" if stage else "*/*.json"
    return [json.loads(p.read_text()) for p in sorted(dq_root.glob(pattern))]
