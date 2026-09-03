"""Every operational snapshot written into Silver must have a consumer, or be declared.

The F7 fix made the RDS export write a full snapshot. It did not check that anything *read*
one. That is the gap the DynamoDB export fell through: it wrote
``workstation_events_operational`` hourly, overwrote it with its delta every time, and nothing
noticed for its entire life -- because no query in Gold referenced it, so no count moved.

A dataset nobody reads cannot be observed to be wrong. This test makes the set of unread
snapshots explicit: a new one must either be consumed by Gold or be added to
UNCONSUMED_SNAPSHOTS with a reason. Both are fine; being neither is not.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aimternet.pipeline.curate import export_dynamodb, export_rds

GOLD = Path("src/aimternet/pipeline/curate/gold.py")


#: Snapshots Gold does not read today, and why. These are a known, bounded gap -- rows the POS
#: creates land in RDS, are exported to Silver, and stop there, because the corresponding
#: facts are still built from the Bronze-derived datasets. Closing it changes the row count of
#: fact_rental and fact_concession_sale, which are pinned as literals in reconcile.py and in
#: the Redshift expectations, so it is a deliberate piece of work rather than a side effect.
UNCONSUMED_SNAPSHOTS = {
    "workstations_operational": "dim_workstation is built from Bronze; the catalog is static",
    "concession_items_operational": "dim_concession_item is built from Bronze; static catalog",
    "rental_transactions_operational": (
        "fact_rental reads silver/rental_transactions (Bronze). API-created rentals therefore "
        "do not reach the warehouse -- a known gap, not an accident"
    ),
    "concession_purchases_operational": (
        "fact_concession_sale reads silver/concession_purchases (Bronze). Same gap"
    ),
}


def _gold_source() -> str:
    return GOLD.read_text(encoding="utf-8")


def _all_snapshots() -> set[str]:
    """Every dataset the two exports write into Silver."""
    return {f"{table}_operational" for table in export_rds.EXPORTS} | {export_dynamodb.DATASET}


@pytest.mark.parametrize("dataset", sorted(_all_snapshots()))
def test_each_snapshot_is_read_by_gold_or_declared_unread(dataset: str) -> None:
    read_by_gold = dataset in _gold_source()
    declared_unread = dataset in UNCONSUMED_SNAPSHOTS

    assert read_by_gold != declared_unread, (
        f"{dataset} is {'both read and declared unread' if read_by_gold else 'neither'}. "
        "A snapshot Gold never reads cannot be checked by row count, which is how the "
        "DynamoDB export discarded events hourly without anything failing. Either read it in "
        "gold.py or add it to UNCONSUMED_SNAPSHOTS with the reason."
    )


def test_the_events_snapshot_is_read_by_gold() -> None:
    """The one this change closed. Pinned separately so it cannot quietly regress."""
    assert export_dynamodb.DATASET not in UNCONSUMED_SNAPSHOTS
    assert export_dynamodb.DATASET in _gold_source()


def test_every_unconsumed_snapshot_carries_a_reason() -> None:
    for dataset, reason in UNCONSUMED_SNAPSHOTS.items():
        assert reason.strip(), f"{dataset} is declared unread with no reason"
        assert dataset in _all_snapshots(), f"{dataset} is not written by any export"
