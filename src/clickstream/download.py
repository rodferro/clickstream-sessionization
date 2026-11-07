"""Download raw monthly CSVs.

The pipeline only depends on the `DataSource` protocol: "put the CSV for month M in directory D
and return its path". Kaggle is the only implementation. Another source (e.g. the REES46 mirror
that hosts Dec 2019 - Apr 2020) would be a second class with the same method.

Credentials are read by the Kaggle client from ~/.kaggle (kaggle.json or the newer
access_token/credentials.json) or KAGGLE_USERNAME/KAGGLE_KEY. Nothing is stored in this repo.
"""

from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Protocol

from clickstream.config import KAGGLE_DATASET, MONTH_FILES


class DataSource(Protocol):
    def fetch(self, month: str, dest: Path) -> Path: ...


class KaggleSource:
    def __init__(self, dataset: str = KAGGLE_DATASET):
        self.dataset = dataset

    def fetch(self, month: str, dest: Path) -> Path:
        name = month_file(month)
        dest.mkdir(parents=True, exist_ok=True)
        csv = dest / name
        if csv.exists():
            return csv
        zipped = dest / f"{name}.zip"
        if not zipped.exists():
            # Use the CLI entry point of the installed kaggle package (pip install '.[download]').
            cmd = [sys.executable, "-m", "kaggle.cli", "datasets", "download", self.dataset]
            cmd += ["-f", name, "-p", str(dest)]
            subprocess.run(cmd, check=True)
        with zipfile.ZipFile(zipped) as zf:
            members = zf.namelist()
            if members != [name]:
                raise RuntimeError(f"unexpected archive contents: {members}")
            zf.extract(name, dest)
        return csv


def month_file(month: str) -> str:
    try:
        return MONTH_FILES[month]
    except KeyError:
        raise SystemExit(f"unknown month {month!r}; available on Kaggle: {sorted(MONTH_FILES)}") from None


def download(month: str, dest: Path, source: DataSource | None = None) -> Path:
    return (source or KaggleSource()).fetch(month, dest)
