"""`silver_snapshot:*` must compare two descriptions of the same instant.

Silver is written at the export watermark. The check used to compare it against a *live* RDS
count, so every row the POS created since the last export read as a row the export had lost:
with the cafe open it failed permanently, and it could not tell the failure it exists to catch
(F7 -- a delta written where a snapshot belongs) from an ordinary sale thirty seconds ago.

Expected is now RDS as of that table's watermark. These tests pin both halves of the property:
lag is fine, loss is not.
"""

from __future__ import annotations

import pytest

from aimternet.pipeline import reconcile


def _check(expected: int, actual: int) -> reconcile.Check:
    """The comparison the reconciler makes for one snapshot."""
    return reconcile.Check(
        name="silver_snapshot:rental_transactions_operational",
        layer_from="rds",
        layer_to="silver",
        expected=expected,
        actual=actual,
        passed=actual >= expected,
        severity=reconcile.SEVERITY_CRITICAL,
        detail="",
    )


def test_a_snapshot_that_lags_a_live_pos_passes() -> None:
    """The real failure this fixes: 28,420 in Silver, 28,456 in RDS, 36 sold since the export.

    Bounded by the watermark, expected is 28,420 -- what RDS held when the export ran.
    """
    assert _check(expected=28_420, actual=28_420).passed


def test_a_truncated_snapshot_still_fails() -> None:
    """F7 itself: members_operational fell from 1,200 rows to the 4 the POS had touched."""
    assert not _check(expected=1_200, actual=4).passed


def test_a_snapshot_written_inside_the_watermark_window_passes() -> None:
    """The export stamps its watermark before it runs its SELECT.

    A row updated in that window is legitimately in the snapshot while sorting after the
    watermark, so the snapshot may hold marginally more than the bounded count. Never fewer.
    """
    assert _check(expected=28_420, actual=28_421).passed


def test_the_expectation_is_bounded_by_the_watermark_not_the_live_count() -> None:
    """Guards the actual query shape, so a refactor cannot quietly restore the race."""
    import inspect

    source = inspect.getsource(reconcile._snapshot_expectations)
    assert "pipeline_watermark" in source
    assert "<= %s" in source, "the RDS side must be bounded by the export watermark"


def test_every_exported_table_can_be_bounded() -> None:
    """Each export declares the column its watermark is measured on; the check needs it."""
    from aimternet.pipeline.curate.export_rds import EXPORTS

    for table, (watermark_column, _key) in EXPORTS.items():
        assert watermark_column, f"{table} has no watermark column to bound the count by"


def test_every_exported_table_has_a_snapshot_check() -> None:
    """The mapping is derived from EXPORTS, so asserting it maps back proves nothing.

    What is worth asserting is coverage in the direction that can actually go wrong: a table
    added to the export must acquire a `silver_snapshot:*` check, never arrive without one.
    Two hand-written lists of the same tables is how dim_member ended up as the single
    warehouse table whose count nothing checked.
    """
    from aimternet.pipeline.curate.export_rds import EXPORTS

    assert {f"{table}_operational" for table in EXPORTS} == set(reconcile.OPERATIONAL_SNAPSHOTS)


@pytest.mark.parametrize("fact", sorted(reconcile.UNIONED_FACTS))
def test_every_unioned_fact_names_a_real_bronze_dataset(fact: str) -> None:
    """The expectation is `SOURCE_COUNTS[bronze] + contribution`; a typo in `bronze` would
    raise a KeyError deep inside a `try` that turns it into a skipped layer."""
    bronze, snapshot, key = reconcile.UNIONED_FACTS[fact]
    assert bronze in reconcile.SOURCE_COUNTS, f"{fact} names an unverified Bronze dataset"
    assert key, f"{fact} has no key to dedupe its two origins on"
    assert snapshot in reconcile.OPERATIONAL_SNAPSHOTS or snapshot == reconcile.EVENTS_SNAPSHOT
