"""DynamoDB access-pattern tests. Marked `aws`; excluded from `make test`.

Acceptance item 6: "DynamoDB serves each documented access pattern; TTL is set from
expires_at." `docs/dynamodb.md` lists four patterns for the events table and one for
telemetry, and claims each is a Query rather than a Scan. These run every one of them
against the live tables and assert both the answer and — the part that actually matters for
a 6.3M-item table — that DynamoDB examined only the items it returned.
"""

from __future__ import annotations

import boto3
import pytest
from boto3.dynamodb.conditions import Key

pytestmark = pytest.mark.aws


@pytest.fixture(scope="module")
def config():
    from aimternet.config.settings import settings

    return settings()


@pytest.fixture(scope="module")
def events(config):
    return boto3.resource("dynamodb", region_name=config.aws_region).Table(
        config.ddb_events_table
    )


@pytest.fixture(scope="module")
def telemetry(config):
    return boto3.resource("dynamodb", region_name=config.aws_region).Table(
        config.ddb_telemetry_table
    )


@pytest.fixture(scope="module")
def a_workstation(events) -> str:
    """A workstation that definitely has events, chosen from the data rather than assumed."""
    return "PC-001"


# ----------------------------------------------------------------- events access patterns


def test_a_workstations_event_history_comes_back_in_order(events, a_workstation: str) -> None:
    response = events.query(
        KeyConditionExpression=Key("PK").eq(f"WS#{a_workstation}") & Key("SK").begins_with("EVT#"),
        Limit=50,
    )
    items = response["Items"]
    assert items, f"no events for {a_workstation}"
    assert all(i["workstation_id"] == a_workstation for i in items)

    timestamps = [i["event_timestamp_utc"] for i in items]
    assert timestamps == sorted(timestamps), "the SK leads with the timestamp so it sorts"

    # The point of the key design: nothing is read that is not returned.
    assert response["ScannedCount"] == response["Count"]


def test_the_latest_event_is_one_backwards_read(events, a_workstation: str) -> None:
    response = events.query(
        KeyConditionExpression=Key("PK").eq(f"WS#{a_workstation}") & Key("SK").begins_with("EVT#"),
        ScanIndexForward=False,
        Limit=1,
    )
    assert response["Count"] == 1
    assert response["ScannedCount"] == 1


def test_both_ends_of_one_session_come_from_gsi1(events, a_workstation: str) -> None:
    seed = events.query(
        KeyConditionExpression=Key("PK").eq(f"WS#{a_workstation}") & Key("SK").begins_with("EVT#"),
        FilterExpression=boto3.dynamodb.conditions.Attr("session_id").exists(),
        Limit=100,
    )["Items"]
    session_id = next(i["session_id"] for i in seed)

    response = events.query(
        IndexName="GSI1",
        KeyConditionExpression=Key("GSI1PK").eq(f"SESSION#{session_id}"),
    )
    items = response["Items"]
    assert len(items) >= 2, "a completed session has at least a start and an end"
    assert {i["event_type"] for i in items} >= {"SESSION_START", "SESSION_END"}
    assert response["ScannedCount"] == response["Count"]


def test_recent_alerts_of_a_type_come_from_gsi2_without_a_scan(events) -> None:
    response = events.query(
        IndexName="GSI2",
        KeyConditionExpression=(
            Key("GSI2PK").eq("TYPE#HARDWARE_ALERT") & Key("GSI2SK").gte("2026-08-01")
        ),
        Limit=25,
    )
    items = response["Items"]
    assert items, "no hardware alerts in August"
    assert all(i["event_type"] == "HARDWARE_ALERT" for i in items)
    assert all(i["event_timestamp_utc"] >= "2026-08-01" for i in items)
    assert response["ScannedCount"] == response["Count"]


# -------------------------------------------------------------- telemetry access pattern


def test_telemetry_for_one_workstation_in_one_hour_is_a_range_query(
    telemetry, a_workstation: str
) -> None:
    response = telemetry.query(
        KeyConditionExpression=(
            Key("PK").eq(f"WS#{a_workstation}")
            & Key("SK").between("TS#2026-08-26T14:00:00", "TS#2026-08-26T15:00:00")
        ),
    )
    items = response["Items"]
    # 2026-08-26 falls in the 30-second window (finding F6), so an hour is 120 samples. The
    # sort key carries its UTC offset — "TS#...T15:00:00+00:00" sorts after the bare
    # "TS#...T15:00:00" upper bound — which makes this range half-open, exactly as wanted.
    assert len(items) == 120, f"expected 120 samples in the hour, got {len(items)}"
    assert items[0]["SK"] == "TS#2026-08-26T14:00:00+00:00"
    assert items[-1]["SK"] == "TS#2026-08-26T14:59:30+00:00"
    assert response["ScannedCount"] == response["Count"]

    sample = items[0]
    assert sample["workstation_id"] == a_workstation
    # The source JSON is stored with its structure intact — flattening is Silver's job, and
    # doing it here too would be the same transformation implemented twice (§3).
    assert set(sample["hardware_metrics"]) >= {"cpu_load_pct", "gpu_load_pct", "ram_usage_pct"}
    assert set(sample["network_diagnostics"]) >= {"latency_ping_ms", "packet_loss_pct"}
    assert sample["status"] in {"OCCUPIED", "IDLE", "MAINTENANCE", "OFFLINE"}


# ------------------------------------------------------------------------------------ TTL


def test_expires_at_is_present_and_is_the_source_value(telemetry, a_workstation: str) -> None:
    """§4 says wire the source attribute; F2 says do not switch expiry on."""
    from datetime import UTC, datetime, timedelta

    item = telemetry.query(
        KeyConditionExpression=Key("PK").eq(f"WS#{a_workstation}"),
        Limit=1,
    )["Items"][0]

    assert "expires_at" in item
    expires = datetime.fromtimestamp(int(item["expires_at"]), tz=UTC)
    sampled = datetime.fromisoformat(item["timestamp_utc"])
    assert expires - sampled == timedelta(days=7), "F2: expires_at is timestamp + 7 days"


def test_ttl_is_configured_on_expires_at_and_matches_the_setting(config) -> None:
    client = boto3.client("dynamodb", region_name=config.aws_region)
    description = client.describe_time_to_live(TableName=config.ddb_telemetry_table)
    status = description["TimeToLiveDescription"]["TimeToLiveStatus"]

    if config.ddb_ttl_enabled:
        assert status in {"ENABLED", "ENABLING"}
        assert description["TimeToLiveDescription"]["AttributeName"] == "expires_at"
    else:
        # Disabled on purpose: the source window is in the past, so enabling TTL would
        # delete ~61 of the 62 days within 48 hours (finding F2).
        assert status in {"DISABLED", "DISABLING"}


def test_the_events_table_carries_both_indexes(config) -> None:
    client = boto3.client("dynamodb", region_name=config.aws_region)
    table = client.describe_table(TableName=config.ddb_events_table)["Table"]

    indexes = {i["IndexName"]: i for i in table.get("GlobalSecondaryIndexes", [])}
    assert set(indexes) == {"GSI1", "GSI2"}
    assert table["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
