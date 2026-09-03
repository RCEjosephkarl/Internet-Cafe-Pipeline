"""Validation engine tests.

The unit tests use small synthetic fixtures so a broken rule is obvious. The full-corpus run
lives in the integration suite -- it takes minutes and reads 6.3M records.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from aimternet.pipeline.validation.findings import (
    Finding,
    Rule,
    Severity,
    ValidationResult,
)


@pytest.fixture
def tiny_landing(tmp_path: Path) -> Path:
    """A miniature landing tree: two workstations, one member, one rental."""
    root = tmp_path / "raw-landing"
    (root / "catalog").mkdir(parents=True)
    (root / "dimensions").mkdir(parents=True)
    batch = root / "legacy_batches" / "2026-07-01"
    batch.mkdir(parents=True)

    def csv(path: Path, header: str, rows: list[str]) -> None:
        path.write_bytes(("\r\n".join([header, *rows]) + "\r\n").encode("utf-8"))

    csv(
        root / "catalog" / "workstations.csv",
        "workstation_id,zone_classification,base_hourly_rate,ip_address,mac_address,commissioned_date",
        [
            "PC-001,Standard Zone,50.00,10.10.1.1,00:1A:2B:3C:01:01,2025-11-15",
            "PC-101,VIP Esports Zone,80.00,10.10.1.2,00:1A:2B:3C:01:02,2025-11-15",
        ],
    )
    csv(
        root / "catalog" / "concession_items.csv",
        "item_sku,item_name,category,unit_cost_price,unit_retail_price,stock_quantity",
        ["SKU-BEV-01,Iced Tea,Beverage,28.00,65.00,250"],
    )
    csv(
        root / "dimensions" / "dim_date.csv",
        "date_id,calendar_date,day_of_week,day_of_month,month_label,month_index,"
        "quarter_index,year_index,weekend_flag,holiday_ph_flag",
        ["20260701,2026-07-01,Wednesday,1,July,7,3,2026,False,False"],
    )
    csv(
        root / "dimensions" / "dim_time.csv",
        "time_id,hour_24,minute_val,day_part_label",
        ["0,0,0,Graveyard", "1430,14,30,Afternoon"],
    )
    csv(
        batch / "members.csv",
        "member_id,first_name,last_name,email,phone_number,current_tier,"
        "current_points_balance,lifetime_spend_amount,registered_at",
        ["M-1841,Ana,Cruz,ana@example.com,+63 900 000 0000,Standard,0,0.00,"
         "2026-07-01T09:00:00+08:00"],
    )
    csv(
        batch / "rental_transactions.csv",
        "rental_id,member_id,workstation_id,session_start,session_end,duration_hours,"
        "base_hourly_rate,member_tier_applied,tier_discount_pct,final_hourly_rate,"
        "gross_rental_amount,points_redeemed,points_credit_value,net_amount_paid,"
        "points_accrued,payment_method",
        ["SESS-20260701-0001-0001,M-1841,PC-001,2026-07-01T10:00:00+08:00,"
         "2026-07-01T12:00:00+08:00,2.00,50.00,Standard,0.00,50.00,100.00,0,0.00,100.00,10,Cash"],
    )
    csv(
        batch / "concession_purchases.csv",
        "purchase_id,member_id,rental_id,total_amount,points_accrued,payment_method,purchased_at",
        ["ORD-20260701-0001-0001,M-1841,SESS-20260701-0001-0001,65.00,6,GCash,"
         "2026-07-01T10:30:00+08:00"],
    )
    csv(
        batch / "concession_order_items.csv",
        "order_item_id,purchase_id,item_sku,quantity,unit_price,total_price",
        ["ITEM-1,ORD-20260701-0001-0001,SKU-BEV-01,1,65.00,65.00"],
    )
    csv(
        batch / "member_points_ledger.csv",
        "ledger_id,member_id,source_reference_id,transaction_type,points_delta,"
        "resulting_balance,created_at",
        ["L-1,M-1841,SESS-20260701-0001-0001,RENTAL_ACCRUAL,10,10,2026-07-01T12:00:00+08:00",
         "L-2,M-1841,ORD-20260701-0001-0001,CONCESSION_ACCRUAL,6,16,2026-07-01T10:30:00+08:00"],
    )
    (batch / "workstation_events.json").write_text(
        json.dumps(
            [
                {
                    "event_id": "EVT-1",
                    "workstation_id": "PC-001",
                    "event_timestamp": "2026-07-01T10:00:00+08:00",
                    "event_type": "SESSION_START",
                    "session_id": "SESS-20260701-0001-0001",
                    "member_id": "M-1841",
                    "duration_allocated_hours": 2.0,
                    "client_os_version": "Win11-Pro-AIM-Build104",
                    "notes": "Normal check-in via POS Terminal",
                }
            ]
        )
    )
    return root


def _run(landing: Path) -> ValidationResult:
    from aimternet.pipeline.validation.engine import Validator

    return Validator(landing).run()


def test_a_clean_tree_passes(tiny_landing: Path) -> None:
    result = _run(tiny_landing)
    assert result.passed, result.counts_by_rule()
    assert result.records_read["rental_transactions"] == 1
    assert result.records_accepted["rental_transactions"] == 1


def test_orphan_member_is_reported_not_fatal(tiny_landing: Path) -> None:
    """D2 in miniature: a rental referencing a member nobody defined."""
    path = tiny_landing / "legacy_batches" / "2026-07-01" / "rental_transactions.csv"
    path.write_bytes(path.read_bytes().replace(b"M-1841", b"M-1001"))

    result = _run(tiny_landing)
    orphans = [f for f in result.findings if f.rule is Rule.FK_ORPHAN and f.dataset == "members"]
    assert [f.record_key for f in orphans] == ["M-1001"]
    assert all(f.severity is Severity.WARNING for f in orphans)
    assert any("referenced by transactions" in note for note in result.notes)


def test_duplicate_primary_key_is_rejected(tiny_landing: Path) -> None:
    path = tiny_landing / "legacy_batches" / "2026-07-01" / "member_points_ledger.csv"
    rows = path.read_bytes().decode().rstrip().split("\r\n")
    path.write_bytes(("\r\n".join([*rows, rows[1]]) + "\r\n").encode())

    result = _run(tiny_landing)
    dupes = [f for f in result.findings if f.rule is Rule.PK_DUPLICATE]
    assert len(dupes) == 1
    assert dupes[0].record_key == "L-1"
    assert not result.passed


def test_unparseable_row_is_quarantined_not_crashed(tiny_landing: Path) -> None:
    path = tiny_landing / "legacy_batches" / "2026-07-01" / "members.csv"
    path.write_bytes(path.read_bytes().replace(b"Standard,0,0.00", b"Platinum,0,0.00"))

    result = _run(tiny_landing)
    invalid = [f for f in result.findings if f.rule is Rule.SCHEMA_INVALID]
    assert len(invalid) == 1
    assert "current_tier" in invalid[0].detail
    assert result.records_accepted["members"] == 0


def test_missing_column_is_detected(tiny_landing: Path) -> None:
    path = tiny_landing / "catalog" / "concession_items.csv"
    text = path.read_bytes().decode()
    path.write_bytes(text.replace("stock_quantity", "stock_qty").encode())

    result = _run(tiny_landing)
    assert any(f.rule is Rule.MISSING_COLUMN for f in result.findings)


def test_pricing_mismatch_is_caught(tiny_landing: Path) -> None:
    """A rental whose arithmetic disagrees with §5 must be reported, not accepted."""
    path = tiny_landing / "legacy_batches" / "2026-07-01" / "rental_transactions.csv"
    path.write_bytes(
        path.read_bytes().replace(b",100.00,0,0.00,100.00,10,", b",100.00,0,0.00,99.00,10,")
    )

    result = _run(tiny_landing)
    mismatches = [f for f in result.findings if f.rule is Rule.PRICING_MISMATCH]
    assert mismatches, "a wrong net_amount_paid should be caught"
    assert "net_amount_paid" in mismatches[0].detail


def test_order_total_mismatch_is_caught(tiny_landing: Path) -> None:
    path = tiny_landing / "legacy_batches" / "2026-07-01" / "concession_order_items.csv"
    path.write_bytes(path.read_bytes().replace(b"1,65.00,65.00", b"2,65.00,130.00"))

    result = _run(tiny_landing)
    assert any(f.rule is Rule.ORDER_TOTAL_MISMATCH for f in result.findings)


def test_overlapping_rentals_on_one_workstation_are_caught(tiny_landing: Path) -> None:
    path = tiny_landing / "legacy_batches" / "2026-07-01" / "rental_transactions.csv"
    rows = path.read_bytes().decode().rstrip().split("\r\n")
    overlapping = rows[1].replace("SESS-20260701-0001-0001", "SESS-20260701-0001-0002").replace(
        "2026-07-01T10:00:00+08:00", "2026-07-01T11:00:00+08:00"
    )
    path.write_bytes(("\r\n".join([*rows, overlapping]) + "\r\n").encode())

    result = _run(tiny_landing)
    assert any(f.rule is Rule.OVERLAPPING_RENTAL for f in result.findings)


def test_session_ending_before_it_starts_is_rejected(tiny_landing: Path) -> None:
    path = tiny_landing / "legacy_batches" / "2026-07-01" / "rental_transactions.csv"
    path.write_bytes(
        path.read_bytes().replace(
            b"2026-07-01T12:00:00+08:00,2.00", b"2026-07-01T09:00:00+08:00,2.00"
        )
    )
    result = _run(tiny_landing)
    assert any(f.rule is Rule.SCHEMA_INVALID for f in result.findings)


def test_quarantine_writes_only_errors(tiny_landing: Path, tmp_path: Path) -> None:
    from aimternet.pipeline.validation.quarantine import write_local

    result = ValidationResult(run_id="qtest")
    result.add(
        Finding(rule=Rule.SCHEMA_INVALID, severity=Severity.ERROR, dataset="members",
                detail="bad", record={"member_id": "M-9999"})
    )
    result.add(
        Finding(rule=Rule.FK_ORPHAN, severity=Severity.WARNING, dataset="members", detail="d2")
    )
    written = write_local(result, tmp_path)
    lines = written["members"].read_text().strip().splitlines()
    assert len(lines) == 1, "warnings must not be quarantined; the record still loads"
    assert json.loads(lines[0])["rule"] == "SCHEMA_INVALID"
    assert json.loads(written["_summary"].read_text())["counts_by_rule"]
