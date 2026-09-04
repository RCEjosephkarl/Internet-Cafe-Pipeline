"""fact_workstation_event must draw on both event origins, not just Bronze.

Gold read `silver/workstation_events` -- the Bronze-derived dataset -- and nothing else, so
every SESSION_START and SESSION_END the POS emitted after the bootstrap stopped at Silver.
That is also *why* the DynamoDB export could overwrite its snapshot hourly without anything
noticing: no query referenced it, so no count moved.

These drive the real SQL against local Parquet -- no S3, no DynamoDB.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from aimternet.pipeline.curate import gold

BRONZE_EVENTS = [
    {
        "event_id": "EVT-000001",
        "workstation_id": "WS-001",
        "event_type": "SESSION_START",
        "event_timestamp_utc": "2026-07-01T09:00:00+00:00",
        "session_id": "SESS-1",
        "member_id": "M-1001",
        "duration_allocated_hours": "2.00",
        "client_os_version": "CafeOS 3.1",
        "source_file": "workstation_events_2026-07-01.json",
    },
    {
        "event_id": "EVT-000002",
        "workstation_id": "WS-002",
        "event_type": "SESSION_END",
        "event_timestamp_utc": "2026-07-01T11:00:00+00:00",
        "session_id": "SESS-1",
        "member_id": "M-1001",
        "duration_allocated_hours": None,
        "client_os_version": "CafeOS 3.1",
        "source_file": "workstation_events_2026-07-01.json",
    },
]


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def _local_layer(tmp_path, monkeypatch):
    """Point every layer_uri at tmp_path so nothing touches S3."""
    monkeypatch.setattr(gold, "layer_uri", lambda layer, dataset: str(tmp_path / dataset))


def _write(tmp_path: Path, con: duckdb.DuckDBPyConnection, dataset: str, rows: list[dict]):
    target = tmp_path / dataset
    target.mkdir(parents=True, exist_ok=True)
    con.register("seed", pd.DataFrame(rows))
    con.execute(f"COPY (SELECT * FROM seed) TO '{target / 'part-0.parquet'}' (FORMAT PARQUET)")
    con.unregister("seed")


def _events(tmp_path: Path, con: duckdb.DuckDBPyConnection, operational: list[dict] | None):
    _write(tmp_path, con, "workstation_events", BRONZE_EVENTS)
    if operational is not None:
        _write(tmp_path, con, "workstation_events_operational", operational)
    sql, count = gold.event_source(con)
    frame = con.execute(sql).fetchdf().sort_values("event_id").reset_index(drop=True)
    return frame, count


def test_bronze_only_when_the_export_has_never_run(tmp_path, con) -> None:
    """A fresh bucket must build, not fail."""
    frame, count = _events(tmp_path, con, operational=None)

    assert count == 0
    assert list(frame["event_id"]) == ["EVT-000001", "EVT-000002"]


def test_api_events_reach_gold(tmp_path, con) -> None:
    """The gap this closed: POS sessions used to stop at Silver."""
    frame, count = _events(
        tmp_path,
        con,
        operational=[
            {
                "event_id": "EVT-API-20260903120000000001",
                "workstation_id": "WS-003",
                "event_type": "SESSION_START",
                "event_timestamp_utc": "2026-09-03T12:00:00+00:00",
                "session_id": "SESS-API-1",
                "member_id": "M-1500",
                "duration_allocated_hours": "1.50",
                "client_os_version": "CafeOS 3.1",
                # Attributes DynamoDB carries that Gold does not want:
                "PK": "WS#WS-003",
                "SK": "EVT#2026-09-03T12:00:00+00:00#EVT-API-20260903120000000001",
                "GSI2PK": "TYPE#SESSION_START",
            }
        ],
    )

    assert count == 1
    assert len(frame) == 3
    api = frame[frame["event_id"] == "EVT-API-20260903120000000001"].iloc[0]
    assert api["workstation_id"] == "WS-003"
    assert api["source_file"] == "dynamodb:api"   # lineage still says where it came from
    assert str(api["duration_allocated_hours"]) == "1.50"


def test_a_snapshot_missing_optional_columns_still_builds(tmp_path, con) -> None:
    """An hour in which nobody started a session has no session_id column at all.

    DynamoDB items only carry the attributes they have, so the exported Parquet's column set
    varies by hour. The zero-row template fills the gaps with NULL rather than failing.
    """
    frame, count = _events(
        tmp_path,
        con,
        operational=[
            {
                "event_id": "EVT-API-20260903130000000001",
                "workstation_id": "WS-004",
                "event_type": "HARDWARE_ALERT",
                "event_timestamp_utc": "2026-09-03T13:00:00+00:00",
                "client_os_version": "CafeOS 3.1",
            }
        ],
    )

    assert count == 1
    api = frame[frame["event_id"] == "EVT-API-20260903130000000001"].iloc[0]
    assert api["session_id"] is None
    assert api["member_id"] is None
    assert api["duration_allocated_hours"] is None


def test_an_event_present_in_both_origins_is_counted_once(tmp_path, con) -> None:
    """The id spaces do not overlap today. Asserted rather than assumed."""
    frame, _ = _events(
        tmp_path,
        con,
        operational=[
            {
                "event_id": "EVT-000001",     # deliberately collides with a Bronze event
                "workstation_id": "WS-001",
                "event_type": "SESSION_START",
                "event_timestamp_utc": "2026-07-01T09:00:00+00:00",
                "client_os_version": "CafeOS 3.1",
            }
        ],
    )

    assert list(frame["event_id"]) == ["EVT-000001", "EVT-000002"]
    assert len(frame) == 2


def test_an_empty_snapshot_contributes_nothing(tmp_path, con) -> None:
    """The export writes a zero-row snapshot the first time it runs before any POS activity."""
    target = tmp_path / "workstation_events_operational"
    target.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""COPY (SELECT NULL::VARCHAR AS event_id, NULL::VARCHAR AS workstation_id
                  WHERE false)
            TO '{target / "part-0.parquet"}' (FORMAT PARQUET)"""
    )
    _write(tmp_path, con, "workstation_events", BRONZE_EVENTS)

    sql, count = gold.event_source(con)
    frame = con.execute(sql).fetchdf()

    assert count == 0
    assert len(frame) == 2
