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


#: Snapshots Gold does not read, and why. Both are deliberate, and both are about the *extra*
#: column the snapshot carries rather than about the catalog itself: the POS cannot add a
#: workstation or invent a SKU, so the 175 rows and the 10 rows are static either way.
UNCONSUMED_SNAPSHOTS = {
    "workstations_operational": (
        "dim_workstation is built from Bronze deliberately. The column the snapshot adds is "
        "`status`, which is live occupancy -- not a dimension attribute. Folding it into a "
        "Type-1 dimension would make every historical join answer 'where is this PC right "
        "now', and /workstations/status already serves that live from RDS"
    ),
    "concession_items_operational": (
        "dim_concession_item is built from Bronze deliberately. The column the snapshot adds "
        "is `stock_quantity`, which every sale decrements; a Type-1 overwrite of a "
        "fast-changing measure into a dimension rewrites history on every build"
    ),
}

#: The snapshots that carry POS transactions. Each one was unread for the POC's whole life,
#: which is exactly why the gap survived: a snapshot nobody reads cannot be observed to be
#: wrong. Pinned positively, not merely omitted from the dict above, so that deleting the
#: read from gold.py fails here instead of quietly reopening the gap.
CONSUMED_SNAPSHOTS = (
    "members_operational",
    "rental_transactions_operational",
    "concession_purchases_operational",
    "concession_order_items_operational",
    "member_points_ledger_operational",
    "workstation_events_operational",
)


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


@pytest.mark.parametrize("dataset", CONSUMED_SNAPSHOTS)
def test_the_transactional_snapshots_are_read_by_gold(dataset: str) -> None:
    """The gap this closed. Pinned positively so it cannot quietly reopen.

    The XOR above is satisfied by *either* reading a snapshot or declaring it unread, so on
    its own it would let someone stop reading one and add a reason instead. These are the
    snapshots carrying money the cafe actually took; none of them may go back to being
    declared away.
    """
    assert dataset not in UNCONSUMED_SNAPSHOTS
    assert dataset in _gold_source()


def test_every_consumed_snapshot_is_actually_written() -> None:
    """A pin on a snapshot no export writes would pass for the wrong reason."""
    assert set(CONSUMED_SNAPSHOTS) <= _all_snapshots()


def test_every_unconsumed_snapshot_carries_a_reason() -> None:
    for dataset, reason in UNCONSUMED_SNAPSHOTS.items():
        assert reason.strip(), f"{dataset} is declared unread with no reason"
        assert dataset in _all_snapshots(), f"{dataset} is not written by any export"
