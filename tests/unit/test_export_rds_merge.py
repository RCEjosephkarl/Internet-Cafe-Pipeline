"""The RDS -> S3 export must leave a snapshot in Silver, never a delta.

Gold reads ``<table>_operational`` as the current state of the operational store. An
incremental run that wrote only the rows it moved would therefore not be "incremental" at
all — it would be a truncate. That is what happened on the first scheduled run after the
bootstrap: ``members_operational`` went from 1,200 rows to the 4 the POS had touched, and
``dim_member`` collapsed from 1,200 members to 8 SCD2 versions with nothing failing.

These tests drive `_merge_sql` against local Parquet, so they need neither RDS nor S3.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from aimternet.pipeline.curate import export_rds


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


def _snapshot(tmp_path: Path, con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Write a previous snapshot where `_merge_sql` will look for one."""
    target = tmp_path / "members_operational"
    target.mkdir(parents=True, exist_ok=True)
    con.register("seed", pd.DataFrame(rows))
    con.execute(f"COPY (SELECT * FROM seed) TO '{target / 'part-0.parquet'}' (FORMAT PARQUET)")
    con.unregister("seed")


def _merge(con: duckdb.DuckDBPyConnection, delta: list[dict]) -> pd.DataFrame:
    con.register("export_frame", pd.DataFrame(delta))
    result = con.execute(export_rds._merge_sql("members", "member_id", con)).fetchdf()
    con.unregister("export_frame")
    return result.sort_values("member_id").reset_index(drop=True)


@pytest.fixture(autouse=True)
def _local_layer(tmp_path, monkeypatch):
    monkeypatch.setattr(
        export_rds, "layer_uri", lambda layer, dataset: str(tmp_path / dataset)
    )


def test_every_export_table_declares_a_watermark_and_a_key() -> None:
    for table, value in export_rds.EXPORTS.items():
        watermark, key = value
        assert watermark and key, table


def test_delta_is_merged_onto_the_previous_snapshot(tmp_path, con) -> None:
    _snapshot(
        tmp_path,
        con,
        [
            {"member_id": "M-1001", "current_tier": "BRONZE", "current_points_balance": 0},
            {"member_id": "M-1002", "current_tier": "SILVER", "current_points_balance": 10},
            {"member_id": "M-1003", "current_tier": "GOLD", "current_points_balance": 20},
        ],
    )

    merged = _merge(
        con,
        [{"member_id": "M-1002", "current_tier": "GOLD", "current_points_balance": 999}],
    )

    # The two untouched members survive: this is the assertion the bug would have failed.
    assert list(merged["member_id"]) == ["M-1001", "M-1002", "M-1003"]
    # ...and the changed one is the new version, not the old.
    updated = merged[merged["member_id"] == "M-1002"].iloc[0]
    assert updated["current_tier"] == "GOLD"
    assert updated["current_points_balance"] == 999


def test_a_new_member_is_added_rather_than_replacing_everyone(tmp_path, con) -> None:
    _snapshot(tmp_path, con, [{"member_id": "M-1001", "current_tier": "BRONZE"}])

    merged = _merge(con, [{"member_id": "M-9999", "current_tier": "SILVER"}])

    assert list(merged["member_id"]) == ["M-1001", "M-9999"]


def test_no_duplicate_rows_when_the_same_delta_is_replayed(tmp_path, con) -> None:
    _snapshot(
        tmp_path,
        con,
        [
            {"member_id": "M-1001", "current_tier": "BRONZE"},
            {"member_id": "M-1002", "current_tier": "SILVER"},
        ],
    )
    delta = [{"member_id": "M-1002", "current_tier": "GOLD"}]

    once = _merge(con, delta)
    assert len(once) == 2
    assert len(once["member_id"].unique()) == 2


def test_first_ever_run_writes_the_delta_whole(tmp_path, con) -> None:
    """No previous snapshot: the delta *is* the table, and there is nothing to merge onto."""
    con.register("export_frame", pd.DataFrame([{"member_id": "M-1001"}]))
    assert export_rds._merge_sql("members", "member_id", con).strip() == (
        "SELECT * FROM export_frame"
    )
    con.unregister("export_frame")
