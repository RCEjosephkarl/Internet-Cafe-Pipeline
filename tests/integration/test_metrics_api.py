"""Shape and cross-check tests for the metrics endpoints behind the Streamlit dashboard.

Modeled on ``test_api.py``: ``TestClient(app)``, no running server needed. Most of these
endpoints touch RDS and/or Redshift and/or DynamoDB directly (they are read replicas of
``routers/metrics.py``'s own queries, not mocks), so they carry the same markers
``make test`` already excludes by default.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

pytestmark = pytest.mark.rds


@pytest.fixture(scope="module")
def client() -> Iterator:
    from fastapi.testclient import TestClient

    from aimternet.api.main import app

    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


# --------------------------------------------------------------------------- data-window


@pytest.mark.redshift
def test_data_window_anchors_to_the_warehouses_own_max_date(client) -> None:
    body = client.get("/v1/metrics/data-window").json()
    assert body["source"] == "redshift"
    assert body["warehouse_max_date_utc"] is not None

    warehouse_max = datetime.fromisoformat(body["warehouse_max_date_utc"])
    server_time = datetime.fromisoformat(body["server_time_utc"])
    # The bootstrap batch is historical: the warehouse's max date must be well before the
    # real server clock, proving downstream windows can't accidentally be wall-clock ones.
    assert warehouse_max < server_time - timedelta(days=1)


# --------------------------------------------------------------------------- live (RDS)


def test_summary_reports_a_consistent_occupancy_percentage(client) -> None:
    body = client.get("/v1/metrics/summary").json()
    total = body["available"] + body["occupied"]
    assert total <= body["total_workstations"]
    if body["total_workstations"]:
        expected = round(100 * body["occupied"] / body["total_workstations"], 1)
        assert body["occupancy_pct"] == expected


def test_members_overview_tier_counts_sum_to_a_plausible_total(client) -> None:
    response = client.get("/v1/metrics/members/overview", params={"days": 30})
    assert response.status_code == 200
    body = response.json()
    tier_total = sum(t["members"] for t in body["by_tier"])
    assert tier_total > 0
    assert body["active_members"] <= tier_total
    assert body["backfilled_members"] <= tier_total


# --------------------------------------------------------------------------- DynamoDB


@pytest.mark.aws
def test_telemetry_fleet_health_flags_alerts_against_the_same_thresholds(client) -> None:
    from aimternet.api.metrics_scoring import CPU_TEMP_ALERT_C, GPU_TEMP_ALERT_C

    body = client.get("/v1/metrics/telemetry/fleet-health").json()
    assert body["source"] == "dynamodb"
    fleet = body["fleet"]
    assert fleet["sampled_workstations"] > 0
    assert 0 <= fleet["thermal_alert_count"] <= fleet["sampled_workstations"]

    workstations = body["workstations"]
    manual_thermal_alerts = sum(
        1
        for w in workstations
        if Decimal(w["cpu_temp_c"]) > Decimal(str(CPU_TEMP_ALERT_C))
        or Decimal(w["gpu_temp_c"]) > Decimal(str(GPU_TEMP_ALERT_C))
    )
    assert manual_thermal_alerts == fleet["thermal_alert_count"]


# --------------------------------------------------------------------------- Redshift


@pytest.mark.aws
def test_fleet_health_reports_every_peripheral_for_every_sampled_workstation(client) -> None:
    """Per-peripheral counts must partition the sample, not overlap or lose machines."""
    from aimternet.api.routers.metrics import PERIPHERALS

    body = client.get("/v1/metrics/telemetry/fleet-health").json()
    if body["source"] != "dynamodb" or not body["workstations"]:
        pytest.skip("no telemetry available")
    sampled = body["fleet"]["sampled_workstations"]

    reported = {row["peripheral"]: row for row in body["fleet"]["peripherals"]}
    assert set(reported) == set(PERIPHERALS)
    for name, row in reported.items():
        assert row["connected"] + row["disconnected"] == sampled, name
        assert row["disconnected"] == sum(
            1 for w in body["workstations"] if not w[f"{name}_connected"]
        )

    # `missing_peripherals` is what the dashboard tables and the alert count both read.
    assert body["fleet"]["peripheral_alert_count"] == sum(
        1 for w in body["workstations"] if w["missing_peripherals"]
    )


@pytest.mark.redshift
def test_utilization_heatmap_window_matches_data_window(client) -> None:
    """The cross-cutting `window` field must come from the table's own MAX(date), which is
    the same date `data-window` reports -- never a fresh wall-clock computation."""
    data_window = client.get("/v1/metrics/data-window").json()
    heatmap = client.get("/v1/metrics/utilization/heatmap", params={"days": 7}).json()
    assert heatmap["source"] == "redshift"
    # utilization_date and session_start_utc can differ by a day at the edges of the batch;
    # what matters is that this is NOT today's real date (2026-09-03 at authoring time).
    heatmap_end = datetime.fromisoformat(heatmap["window"]["end_date"])
    assert heatmap_end.date() <= datetime.fromisoformat(
        data_window["warehouse_max_date_utc"]
    ).date() + timedelta(days=1)
    assert heatmap_end.year == 2026
    assert heatmap_end.month in (7, 8)


@pytest.mark.redshift
def test_revenue_trend_totals_equal_the_sum_of_its_parts(client) -> None:
    body = client.get("/v1/metrics/revenue/trend", params={"days": 62}).json()
    assert body["source"] == "redshift"
    for row in body["daily"]:
        expected_total = Decimal(str(row["rental_revenue"])) + Decimal(
            str(row["concession_revenue"])
        )
        assert Decimal(row["total_revenue"]) == expected_total


@pytest.mark.redshift
def test_revenue_trend_concession_matches_the_sale_fact_alone(client) -> None:
    """The cross-check the internal-consistency test above cannot make.

    ``payment_mix`` reads ``fact_concession_sale`` directly; ``daily`` builds its concession
    column through a CTE. Two independent paths over the same facts must agree. They did not:
    the daily CTE used to join the line-item fact and then sum the *sale* total, counting each
    purchase once per line and inflating concession revenue 1.669x (finding F10). Nothing
    noticed, because every other assertion here was internal to the row.
    """
    body = client.get("/v1/metrics/revenue/trend", params={"days": 365}).json()
    assert body["source"] == "redshift"
    daily_rental = sum(Decimal(str(r["rental_revenue"])) for r in body["daily"])
    daily_concession = sum(Decimal(str(r["concession_revenue"])) for r in body["daily"])
    mix_total = sum(Decimal(str(m["amount"])) for m in body["payment_mix"])
    assert daily_rental + daily_concession == mix_total


# ------------------------------------------------------------------------------- events


@pytest.mark.redshift
def test_event_summary_totals_agree_across_its_three_groupings(client) -> None:
    """``daily``, ``by_type`` and ``alerts`` slice one table three ways over one window."""
    body = client.get("/v1/metrics/events/summary", params={"days": 365}).json()
    assert body["source"] == "redshift"
    assert body["by_type"], "fact_workstation_event is loaded; the summary must see it"
    assert sum(r["events"] for r in body["daily"]) == sum(r["events"] for r in body["by_type"])

    alert_types = {"HARDWARE_ALERT", "PERIPHERAL_ALERT"}
    from_by_type = sum(r["events"] for r in body["by_type"] if r["event_type"] in alert_types)
    # `alerts` is capped at the 25 worst workstations, so it can only ever be a subset.
    assert sum(r["alerts"] for r in body["alerts"]) <= from_by_type
    for row in body["alerts"]:
        assert row["hardware_alerts"] + row["peripheral_alerts"] == row["alerts"]


@pytest.mark.aws
def test_recent_events_come_back_newest_first_and_typed(client) -> None:
    body = client.get("/v1/metrics/events/recent", params={"limit": 5}).json()
    assert body["source"] == "dynamodb"
    stamps = [e["timestamp_utc"] for e in body["events"]]
    assert stamps == sorted(stamps, reverse=True)

    from aimternet.config.business_rules import rules

    assert {e["event_type"] for e in body["events"]} <= rules().event_types


# ------------------------------------------------------------------- per-member summary


def test_member_summary_totals_match_the_rows_it_returns(client) -> None:
    """The per-member card is the one place a reader compares a total against its own list."""
    leaderboard = client.get(
        "/v1/metrics/members/leaderboard", params={"days": 365, "limit": 1}
    ).json()
    if leaderboard.get("source") != "redshift" or not leaderboard["members"]:
        pytest.skip("no warehouse member to drill into")
    member_id = leaderboard["members"][0]["member_id"]

    body = client.get(f"/v1/metrics/members/{member_id}/summary").json()
    assert body["source"] == "rds"
    assert body["found"] is True
    assert body["member"]["member_id"] == member_id

    totals = body["totals"]
    assert Decimal(totals["total_spend"]) == Decimal(totals["rental_spend"]) + Decimal(
        totals["concession_spend"]
    )
    from aimternet.config.business_rules import rules

    balance = int(body["member"]["current_points_balance"])
    assert totals["redeemable_units"] == balance // rules().redemption_unit_points
    # The lists are the 25 most recent, so they are a window on the totals, not equal to
    # them -- but every row in them must belong to this member's own history.
    assert len(body["rentals"]) <= 25
    assert len(body["purchases"]) <= 25
    assert all(r["points_delta"] != 0 for r in body["points_ledger"])


def test_member_summary_reports_an_unknown_card_without_raising(client) -> None:
    """A front-desk typo must read as "no such member", not a 500."""
    body = client.get("/v1/metrics/members/M-0000/summary").json()
    assert body["found"] is False
    assert body["member_id"] == "M-0000"


def test_a_static_members_route_is_not_shadowed_by_the_member_id_route(client) -> None:
    """`/members/{member_id}/summary` is declared last on purpose; prove it stayed there."""
    body = client.get("/v1/metrics/members/overview", params={"days": 7}).json()
    assert "by_tier" in body, "the path-parameter route swallowed /members/overview"


@pytest.mark.redshift
def test_points_history_liability_matches_the_shared_business_rules(client) -> None:
    from aimternet.config.business_rules import rules

    body = client.get("/v1/metrics/points/history", params={"days": 62}).json()
    assert body["source"] == "redshift"
    outstanding = body["outstanding"]
    if outstanding > 0:
        expected = (
            Decimal(outstanding) / rules().redemption_unit_points * rules().redemption_unit_value
        ).quantize(Decimal("0.01"))
        assert Decimal(body["outstanding_liability"]) == expected


@pytest.mark.redshift
def test_members_leaderboard_engagement_scores_are_bounded_and_sorted(client) -> None:
    body = client.get(
        "/v1/metrics/members/leaderboard", params={"days": 62, "limit": 10}
    ).json()
    assert body["source"] == "redshift"
    scores = [Decimal(m["engagement_score"]) for m in body["members"]]
    assert all(Decimal("0") <= s <= Decimal("100") for s in scores)
    assert scores == sorted(scores, reverse=True)


@pytest.mark.redshift
def test_tier_migration_current_distribution_covers_every_current_member(client) -> None:
    from aimternet.config.settings import settings
    from aimternet.db import redshift

    body = client.get("/v1/metrics/members/tier-migration", params={"days": 62}).json()
    assert body["source"] == "redshift"
    reported_total = sum(row["members"] for row in body["current_distribution"])

    schema = settings().redshift_schema
    actual = redshift.fetch_all(
        f"SELECT count(*) AS n FROM {schema}.dim_member WHERE is_current"
    )[0]["n"]
    assert reported_total == actual

    for rate in body["conversion_rates"]:
        assert 0.0 <= rate["conversion_rate_pct"] <= 100.0


@pytest.mark.redshift
def test_workstations_health_score_is_bounded_0_to_100(client) -> None:
    body = client.get("/v1/metrics/workstations/health-score", params={"days": 62}).json()
    assert body["source"] == "redshift"
    for w in body["workstations"]:
        assert 0.0 <= w["health_score"] <= 100.0
    if body["workstations"]:
        assert body["fleet_avg_health_score"] is not None


@pytest.mark.redshift
def test_efficiency_revenue_per_hour_covers_every_zone(client) -> None:
    body = client.get("/v1/metrics/efficiency/revenue-per-hour", params={"days": 62}).json()
    assert body["source"] == "redshift"
    zones = {z["zone"] for z in body["zones"]}
    assert zones <= {"Standard Zone", "VIP Esports Zone", "Streamer Pods"}
    assert zones  # at least one zone had rental activity in the bootstrap batch
