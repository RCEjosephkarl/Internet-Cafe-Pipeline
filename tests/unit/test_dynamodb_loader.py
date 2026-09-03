"""DynamoDB loader tests against moto, so they need no AWS account.

The item-shaping tests are the important ones: the key design in docs/dynamodb.md is only
real if the loader actually produces those keys.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")


EVENT = {
    "event_id": "EVT-20260701-6494-00000",
    "workstation_id": "PC-139",
    "event_timestamp": "2026-07-01T00:30:00+08:00",
    "event_type": "SESSION_START",
    "session_id": "SESS-20260701-6038-0015",
    "member_id": "M-1115",
    "duration_allocated_hours": Decimal("2.0"),
    "client_os_version": "Win11-Pro-AIM-Build104",
    "notes": "Normal check-in via POS Terminal",
}

ALERT = {
    "event_id": "EVT-20260701-6494-00001",
    "workstation_id": "PC-002",
    "event_timestamp": "2026-07-01T01:00:00+08:00",
    "event_type": "HARDWARE_ALERT",
    "client_os_version": "Win11-Pro-AIM-Build104",
}

TELEMETRY = {
    "workstation_id": "PC-001",
    "timestamp": "2026-07-21T01:00:00+08:00",
    "zone": "Standard Zone",
    "status": "IDLE",
    "active_session_id": None,
    "active_member_id": None,
    "hardware_metrics": {"cpu_load_pct": Decimal("4.0"), "cpu_temp_c": 35},
    "network_diagnostics": {"latency_ping_ms": 7},
    "peripherals_connected": {"keyboard": True},
    "expires_at": 1785171600,
}


# --------------------------------------------------------------------------- shaping


def test_event_key_design_matches_the_documented_scheme() -> None:
    from aimternet.pipeline.loaders.dynamodb import event_item

    item = event_item(EVENT)
    assert item["PK"] == "WS#PC-139"
    # +08:00 becomes UTC, so 00:30 on the 1st is 16:30 on 2026-06-30.
    assert item["SK"] == "EVT#2026-06-30T16:30:00+00:00#EVT-20260701-6494-00000"
    assert item["GSI1PK"] == "SESSION#SESS-20260701-6038-0015"
    assert item["GSI2PK"] == "TYPE#SESSION_START"
    assert item["event_timestamp_source"] == "2026-07-01T00:30:00+08:00"


def test_sort_keys_order_chronologically_as_strings() -> None:
    """The whole design leans on ISO-8601 UTC sorting lexicographically."""
    from aimternet.pipeline.loaders.dynamodb import event_item

    early = event_item({**EVENT, "event_timestamp": "2026-07-01T00:30:00+08:00"})
    late = event_item({**EVENT, "event_timestamp": "2026-07-01T23:30:00+08:00"})
    assert early["SK"] < late["SK"]


def test_event_without_a_session_gets_no_session_index_entry() -> None:
    from aimternet.pipeline.loaders.dynamodb import event_item

    item = event_item(ALERT)
    assert "GSI1PK" not in item, "a null GSI key would make the item unindexable"
    assert "session_id" not in item
    assert item["GSI2PK"] == "TYPE#HARDWARE_ALERT"


def test_telemetry_keeps_nested_maps_and_the_source_ttl() -> None:
    from aimternet.pipeline.loaders.dynamodb import telemetry_item

    item = telemetry_item(TELEMETRY)
    assert item["PK"] == "WS#PC-001"
    assert item["SK"] == "TS#2026-07-20T17:00:00+00:00"
    assert item["hardware_metrics"]["cpu_load_pct"] == Decimal("4.0")
    assert item["expires_at"] == 1785171600, "TTL must be the source value, unmodified"
    assert "active_session_id" not in item, "null attributes are omitted, not stored as NULL"


def test_ttl_shift_rebases_expiry_without_touching_the_default() -> None:
    """F2: the shift exists so a live-expiry demo is possible; it is off unless asked for."""
    from aimternet.pipeline.loaders.dynamodb import telemetry_item

    assert telemetry_item(TELEMETRY, shift_days=0)["expires_at"] == 1785171600
    shifted = telemetry_item(TELEMETRY, shift_days=30)["expires_at"]
    assert shifted == 1785171600 + 30 * 86400


def test_no_float_reaches_dynamodb() -> None:
    """DynamoDB rejects floats outright, and float is wrong for money and measurements."""
    from aimternet.pipeline.loaders.dynamodb import event_item, telemetry_item

    def assert_no_floats(value: object, path: str = "") -> None:
        if isinstance(value, float):
            raise AssertionError(f"float found at {path}")
        if isinstance(value, dict):
            for key, inner in value.items():
                assert_no_floats(inner, f"{path}.{key}")

    assert_no_floats(event_item(EVENT))
    assert_no_floats(telemetry_item(TELEMETRY))


# --------------------------------------------------------------------------- against moto


@pytest.fixture
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(key, value)
    os.environ.pop("AWS_PROFILE", None)


def test_tables_are_created_with_on_demand_billing_and_both_indexes(
    aws_credentials: None,
) -> None:
    from moto import mock_aws

    with mock_aws():
        from aimternet.config.settings import settings
        from aimternet.pipeline.loaders.dynamodb import ensure_tables

        statuses = ensure_tables(wait=False)
        assert all("created" in v or "exists" in v for k, v in statuses.items() if ":ttl" not in k)

        client = boto3.client("dynamodb", region_name="us-east-1")
        events = client.describe_table(TableName=settings().ddb_events_table)["Table"]
        assert events["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
        assert {i["IndexName"] for i in events["GlobalSecondaryIndexes"]} == {"GSI1", "GSI2"}


def test_creating_tables_twice_is_safe(aws_credentials: None) -> None:
    from moto import mock_aws

    with mock_aws():
        from aimternet.pipeline.loaders.dynamodb import ensure_tables

        ensure_tables(wait=False)
        second = ensure_tables(wait=False)
        assert all("exists" in v for k, v in second.items() if ":ttl" not in k)
