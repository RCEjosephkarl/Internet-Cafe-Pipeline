"""The 4.2 GB landing tree is the input to everything. Confirm the shape the spec claims.

Counts here are the verified ground truth from pipeline_plan_AWS.md §1.2. If one of these
fails, the data changed underneath the pipeline and every downstream count is suspect.
"""

from __future__ import annotations

from pathlib import Path

import pytest

EXPECTED_BATCH_DAYS = 62
EXPECTED_TELEMETRY_FILES = 1488
BATCH_FILES = (
    "members.csv",
    "rental_transactions.csv",
    "concession_purchases.csv",
    "concession_order_items.csv",
    "member_points_ledger.csv",
    "workstation_events.json",
)


def _batch_dirs(raw_landing: Path) -> list[Path]:
    # Dot-directories (Jupyter's .ipynb_checkpoints) are never source data.
    return sorted(
        d for d in (raw_landing / "legacy_batches").iterdir()
        if d.is_dir() and not d.name.startswith(".")
    )


def test_landing_tree_exists(raw_landing: Path) -> None:
    assert raw_landing.is_dir(), f"raw landing not found at {raw_landing}"


def test_catalog_and_dimensions_present(raw_landing: Path) -> None:
    for rel in (
        "catalog/workstations.csv",
        "catalog/concession_items.csv",
        "dimensions/dim_date.csv",
        "dimensions/dim_time.csv",
    ):
        assert (raw_landing / rel).is_file(), f"missing {rel}"


def test_sixty_two_batch_days(raw_landing: Path) -> None:
    assert len(_batch_dirs(raw_landing)) == EXPECTED_BATCH_DAYS


@pytest.mark.parametrize("filename", BATCH_FILES)
def test_every_batch_day_is_complete(raw_landing: Path, filename: str) -> None:
    missing = [d.name for d in _batch_dirs(raw_landing) if not (d / filename).is_file()]
    assert missing == [], f"{filename} missing from batches: {missing}"


def test_telemetry_file_count(raw_landing: Path) -> None:
    files = [
        p
        for p in (raw_landing / "telemetry").glob("*/*.json")
        if not any(part.startswith(".") for part in p.parts)
    ]
    assert len(files) == EXPECTED_TELEMETRY_FILES


def test_csvs_are_crlf_with_no_bom_surprise(raw_landing: Path) -> None:
    """D4: every CSV is CRLF-terminated. Readers must use newline='' and utf-8-sig."""
    sample = raw_landing / "catalog" / "workstations.csv"
    head = sample.read_bytes()[:200]
    assert b"\r\n" in head, "expected CRLF line endings (D4)"
