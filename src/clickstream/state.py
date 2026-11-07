"""Run manifest: remembers input/output fingerprints per stage and partition.

A stage skips a partition when its input fingerprint is unchanged and the output still matches
the recorded output fingerprint. Anything else (new data, edited input, deleted output, changed
upstream) causes a rebuild. Because each day's sessionization takes the previous day's carry-over
state as an input, a change on day D cascades to D+1, D+2, ... automatically.
"""

from __future__ import annotations

import json
from pathlib import Path


class Manifest:
    def __init__(self, path: Path):
        self.path = path
        self.data: dict[str, dict[str, dict]] = {}
        if path.exists():
            self.data = json.loads(path.read_text())

    def get(self, stage: str, key: str) -> dict | None:
        return self.data.get(stage, {}).get(key)

    def is_current(self, stage: str, key: str, input_fp: str, output_fp: str | None) -> bool:
        rec = self.get(stage, key)
        return (
            rec is not None
            and output_fp is not None
            and rec.get("input") == input_fp
            and rec.get("output") == output_fp
        )

    def record(self, stage: str, key: str, input_fp: str, output_fp: str | None, **extra) -> None:
        self.data.setdefault(stage, {})[key] = {"input": input_fp, "output": output_fp, **extra}
        self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=1, sort_keys=True))
        tmp.replace(self.path)
