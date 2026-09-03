"""Readers for the landing tree.

Three things this module guarantees, all of them learned from the data rather than assumed:

* **D4** — every CSV is CRLF-terminated and may carry a UTF-8 BOM. Files are opened with
  ``encoding='utf-8-sig'`` and ``newline=''``. A naive read leaves ``\\r`` welded to the last
  column of every row.
* **Read-only.** Files are opened ``'r'``/``'rb'`` and never anything else. The landing tree is
  the source of truth and nothing here may write to it (spec §2).
* **Dot-directories are not data.** Jupyter leaves ``.ipynb_checkpoints/`` inside the landing
  tree. Discovery skips any path with a dot-prefixed component, so checkpoint copies never
  enter the pipeline as phantom batches.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

CSV_ENCODING = "utf-8-sig"

CATALOG_DATASETS = ("workstations", "concession_items")
DIMENSION_DATASETS = ("dim_date", "dim_time")
BATCH_CSV_DATASETS = (
    "members",
    "rental_transactions",
    "concession_purchases",
    "concession_order_items",
    "member_points_ledger",
)
BATCH_JSON_DATASETS = ("workstation_events",)


class SourceLayoutError(FileNotFoundError):
    """The landing tree is not shaped the way the pipeline requires."""


def _is_hidden(path: Path, root: Path) -> bool:
    """True when any component below ``root`` is dot-prefixed."""
    return any(part.startswith(".") for part in path.relative_to(root).parts)


@dataclass(frozen=True, slots=True)
class SourceFile:
    """One discovered source file and the identity the manifest keys on."""

    path: Path
    dataset: str
    source_type: str  # csv | json
    batch_date: date | None = None
    hour: int | None = None

    @property
    def name(self) -> str:
        return self.path.name


def discover(landing: Path) -> list[SourceFile]:
    """Enumerate every source file in deterministic order.

    Deterministic matters: the manifest, the Bronze layout and the reconciliation counts all
    key off this ordering, so two runs must see the same files in the same sequence.
    """
    landing = Path(landing)
    if not landing.is_dir():
        raise SourceLayoutError(f"raw landing directory not found: {landing}")

    found: list[SourceFile] = []

    for dataset in CATALOG_DATASETS:
        path = landing / "catalog" / f"{dataset}.csv"
        if not path.is_file():
            raise SourceLayoutError(f"missing catalog file: {path}")
        found.append(SourceFile(path=path, dataset=dataset, source_type="csv"))

    for dataset in DIMENSION_DATASETS:
        path = landing / "dimensions" / f"{dataset}.csv"
        if not path.is_file():
            raise SourceLayoutError(f"missing dimension file: {path}")
        found.append(SourceFile(path=path, dataset=dataset, source_type="csv"))

    batches_root = landing / "legacy_batches"
    if not batches_root.is_dir():
        raise SourceLayoutError(f"missing legacy_batches directory: {batches_root}")
    for batch_dir in sorted(d for d in batches_root.iterdir() if d.is_dir()):
        if batch_dir.name.startswith("."):
            continue
        batch_date = date.fromisoformat(batch_dir.name)
        for dataset in BATCH_CSV_DATASETS:
            path = batch_dir / f"{dataset}.csv"
            if not path.is_file():
                raise SourceLayoutError(f"missing {dataset}.csv in batch {batch_dir.name}")
            found.append(
                SourceFile(path=path, dataset=dataset, source_type="csv", batch_date=batch_date)
            )
        for dataset in BATCH_JSON_DATASETS:
            path = batch_dir / f"{dataset}.json"
            if not path.is_file():
                raise SourceLayoutError(f"missing {dataset}.json in batch {batch_dir.name}")
            found.append(
                SourceFile(path=path, dataset=dataset, source_type="json", batch_date=batch_date)
            )

    telemetry_root = landing / "telemetry"
    if telemetry_root.is_dir():
        for day_dir in sorted(d for d in telemetry_root.iterdir() if d.is_dir()):
            if day_dir.name.startswith("."):
                continue
            batch_date = date.fromisoformat(day_dir.name)
            for path in sorted(day_dir.glob("*.json")):
                if _is_hidden(path, landing):
                    continue
                found.append(
                    SourceFile(
                        path=path,
                        dataset="telemetry",
                        source_type="json",
                        batch_date=batch_date,
                        hour=int(path.stem),
                    )
                )
    return found


def read_csv_rows(path: Path) -> Iterator[dict[str, str]]:
    """Stream a CSV as dicts, handling the BOM and CRLF (D4)."""
    with path.open("r", encoding=CSV_ENCODING, newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            return
        yield from reader


def read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding=CSV_ENCODING, newline="") as fh:
        header = next(csv.reader(fh), [])
    return [column.strip() for column in header]


def read_json_array(path: Path) -> list[dict[str, Any]]:
    """Read a JSON-array file.

    Telemetry and event files are 1-3 MB each, so loading one whole file is fine; what must
    never happen is holding many of them at once. Callers iterate file by file (§6.3).
    """
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, list):
        raise ValueError(f"{path} does not contain a JSON array")
    return payload


def count_records(source: SourceFile) -> int:
    """Record count for the manifest, without holding the whole file for CSVs."""
    if source.source_type == "csv":
        with source.path.open("r", encoding=CSV_ENCODING, newline="") as fh:
            return max(sum(1 for _ in fh) - 1, 0)  # minus header
    return len(read_json_array(source.path))
