"""The DynamoDB -> S3 export must leave a snapshot in Silver, never a delta.

This is F7 in a sibling module. ``export_rds`` was fixed to merge its delta onto the previous
snapshot; ``export_dynamodb`` -- which imports that module's watermark helpers -- was not. It
wrote the delta straight over ``workstation_events_operational`` on an hourly schedule, so the
snapshot held the last hour's events and nothing older, on every run, forever. Gold read the
Bronze events instead, so no count moved and nothing failed.

These tests drive the shared merge against local Parquet, so they need neither DynamoDB nor S3.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from aimternet.pipeline.curate import export_dynamodb, export_rds
from aimternet.pipeline.curate.engine import merge_onto_snapshot


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture
def destination(tmp_path: Path) -> str:
    return str(tmp_path / export_dynamodb.DATASET)


def _snapshot(destination: str, con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Write a previous snapshot where the merge will look for one."""
    target = Path(destination)
    target.mkdir(parents=True, exist_ok=True)
    con.register("seed", pd.DataFrame(rows))
    con.execute(f"COPY (SELECT * FROM seed) TO '{target / 'part-0.parquet'}' (FORMAT PARQUET)")
    con.unregister("seed")


def _merge(
    con: duckdb.DuckDBPyConnection, destination: str, delta: list[dict], key: str = "event_id"
) -> pd.DataFrame:
    con.register("events_frame", pd.DataFrame(delta))
    sql = merge_onto_snapshot(con, delta="events_frame", destination=destination, key=key)
    result = con.execute(sql).fetchdf()
    con.unregister("events_frame")
    return result.sort_values(key).reset_index(drop=True)


def test_the_export_declares_the_key_it_merges_on() -> None:
    assert export_dynamodb.KEY == "event_id"
    assert export_dynamodb.DATASET == "workstation_events_operational"


def test_an_hour_of_events_does_not_replace_the_previous_hours(con, destination) -> None:
    """The bug itself: run two must not discard what run one exported."""
    _snapshot(
        con=con,
        destination=destination,
        rows=[
            {"event_id": "EVT-API-1", "event_type": "SESSION_START"},
            {"event_id": "EVT-API-2", "event_type": "SESSION_END"},
            {"event_id": "EVT-API-3", "event_type": "SESSION_START"},
        ],
    )

    merged = _merge(con, destination, [{"event_id": "EVT-API-4", "event_type": "SESSION_END"}])

    # Three prior events survive alongside the new one. Before the fix this was one row.
    assert list(merged["event_id"]) == ["EVT-API-1", "EVT-API-2", "EVT-API-3", "EVT-API-4"]


def test_a_replayed_delta_adds_no_duplicates(con, destination) -> None:
    _snapshot(
        con=con,
        destination=destination,
        rows=[
            {"event_id": "EVT-API-1", "event_type": "SESSION_START"},
            {"event_id": "EVT-API-2", "event_type": "SESSION_END"},
        ],
    )
    delta = [{"event_id": "EVT-API-2", "event_type": "SESSION_END"}]

    merged = _merge(con, destination, delta)

    assert len(merged) == 2
    assert len(merged["event_id"].unique()) == 2


def test_a_resent_event_is_the_new_version_not_the_old(con, destination) -> None:
    _snapshot(
        con=con,
        destination=destination,
        rows=[{"event_id": "EVT-API-1", "notes": "before"}],
    )

    merged = _merge(con, destination, [{"event_id": "EVT-API-1", "notes": "after"}])

    assert len(merged) == 1
    assert merged.iloc[0]["notes"] == "after"


def test_the_first_ever_run_writes_the_delta_whole(con, destination) -> None:
    """No previous snapshot: the delta *is* the dataset, and there is nothing to merge onto."""
    con.register("events_frame", pd.DataFrame([{"event_id": "EVT-API-1"}]))
    sql = merge_onto_snapshot(
        con, delta="events_frame", destination=destination, key="event_id"
    )
    assert sql.strip() == "SELECT * FROM events_frame"
    con.unregister("events_frame")


def test_a_null_key_in_the_delta_does_not_collapse_the_snapshot(con, destination) -> None:
    """The NOT IN regression.

    `previous.key NOT IN (SELECT key FROM delta)` evaluates to NULL for every row as soon as
    one delta key is NULL, so nothing survives from the previous snapshot -- the merge becomes
    the truncate it exists to prevent. The anti-join fails safe: unmatched rows are retained.
    """
    _snapshot(
        con=con,
        destination=destination,
        rows=[
            {"event_id": "EVT-API-1", "event_type": "SESSION_START"},
            {"event_id": "EVT-API-2", "event_type": "SESSION_END"},
        ],
    )

    merged = _merge(
        con,
        destination,
        [
            {"event_id": None, "event_type": "MALFORMED"},
            {"event_id": "EVT-API-3", "event_type": "SESSION_START"},
        ],
    )

    survivors = set(merged["event_id"].dropna())
    assert {"EVT-API-1", "EVT-API-2"} <= survivors, (
        "a single NULL key in the delta wiped the previous snapshot"
    )


def test_an_unreadable_snapshot_raises_rather_than_truncating(con, destination) -> None:
    """A corrupt snapshot must not be mistaken for an absent one.

    Treating "cannot read it" as "there isn't one" would rebuild the dataset from the delta,
    which is F7 again by a different route.
    """
    target = Path(destination)
    target.mkdir(parents=True, exist_ok=True)
    (target / "part-0.parquet").write_text("this is not parquet")

    con.register("events_frame", pd.DataFrame([{"event_id": "EVT-API-1"}]))
    with pytest.raises(duckdb.Error):
        merge_onto_snapshot(con, delta="events_frame", destination=destination, key="event_id")
    con.unregister("events_frame")


def test_both_exports_use_the_same_merge() -> None:
    """One implementation. Two would be how F7 reached a sibling module in the first place."""
    assert export_rds.merge_onto_snapshot is merge_onto_snapshot
    assert export_dynamodb.merge_onto_snapshot is merge_onto_snapshot
