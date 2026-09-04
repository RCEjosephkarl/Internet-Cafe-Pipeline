"""Pure-function tests for the Data Science section's scoring math.

No database: `metrics_scoring.py` is deliberately free of RDS/Redshift/DynamoDB imports so
these formulas can be pinned without a live warehouse.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from aimternet.api.metrics_scoring import (
    engagement_score,
    tier_conversion_rates,
    workstation_health_score,
)

# --------------------------------------------------------------------------- health score


def test_a_workstation_with_nothing_to_flag_scores_100() -> None:
    score = workstation_health_score(
        max_cpu_temp_c=60.0, max_gpu_temp_c=65.0,
        avg_latency_ping_ms=20.0, max_packet_loss_pct=0.1,
    )
    assert score == 100.0


def test_crossing_the_cpu_alert_threshold_lowers_the_score() -> None:
    baseline = workstation_health_score(
        max_cpu_temp_c=79.0, max_gpu_temp_c=65.0,
        avg_latency_ping_ms=20.0, max_packet_loss_pct=0.1,
    )
    hot = workstation_health_score(
        max_cpu_temp_c=90.0, max_gpu_temp_c=65.0,
        avg_latency_ping_ms=20.0, max_packet_loss_pct=0.1,
    )
    assert hot < baseline
    assert baseline == 100.0


def test_the_score_never_goes_below_zero_no_matter_how_bad() -> None:
    score = workstation_health_score(
        max_cpu_temp_c=200.0, max_gpu_temp_c=200.0,
        avg_latency_ping_ms=999.0, max_packet_loss_pct=100.0,
    )
    assert score == 0.0


def test_cpu_heat_is_penalized_more_than_network_latency() -> None:
    """The docstring claims CPU is weighted heaviest -- pin that ordering."""
    cpu_hot = workstation_health_score(
        max_cpu_temp_c=100.0, max_gpu_temp_c=65.0,
        avg_latency_ping_ms=20.0, max_packet_loss_pct=0.1,
    )
    network_bad = workstation_health_score(
        max_cpu_temp_c=60.0, max_gpu_temp_c=65.0,
        avg_latency_ping_ms=100.0, max_packet_loss_pct=0.1,
    )
    assert cpu_hot < network_bad


# --------------------------------------------------------------------------- engagement score


def test_the_top_spender_scores_100_when_also_top_in_every_other_factor() -> None:
    score = engagement_score(
        net_revenue=Decimal("5000"), hours=Decimal("40"), points_earned=500,
        max_net_revenue=Decimal("5000"), max_hours=Decimal("40"), max_points_earned=500,
    )
    assert score == Decimal("100.0")


def test_a_member_with_nothing_scores_zero() -> None:
    score = engagement_score(
        net_revenue=Decimal("0"), hours=Decimal("0"), points_earned=0,
        max_net_revenue=Decimal("5000"), max_hours=Decimal("40"), max_points_earned=500,
    )
    assert score == Decimal("0.0")


def test_revenue_is_weighted_more_heavily_than_points() -> None:
    """Spend-only vs points-only at the same relative share: spend should score higher."""
    spend_only = engagement_score(
        net_revenue=Decimal("5000"), hours=Decimal("0"), points_earned=0,
        max_net_revenue=Decimal("5000"), max_hours=Decimal("40"), max_points_earned=500,
    )
    points_only = engagement_score(
        net_revenue=Decimal("0"), hours=Decimal("0"), points_earned=500,
        max_net_revenue=Decimal("5000"), max_hours=Decimal("40"), max_points_earned=500,
    )
    assert spend_only > points_only


def test_a_zero_maximum_does_not_raise_a_division_error() -> None:
    """An empty leaderboard call (max == 0) must not blow up with ZeroDivisionError."""
    score = engagement_score(
        net_revenue=Decimal("0"), hours=Decimal("0"), points_earned=0,
        max_net_revenue=Decimal("0"), max_hours=Decimal("0"), max_points_earned=0,
    )
    assert score == Decimal("0.0")


# --------------------------------------------------------------------------- tier conversion


def test_conversion_rate_divides_by_members_ever_at_the_source_tier() -> None:
    transitions = [{"from_tier": "Standard", "to_tier": "Silver", "members": 25}]
    rates = tier_conversion_rates(transitions, {"Standard": 100, "Silver": 25})
    assert rates == [
        {
            "from_tier": "Standard", "to_tier": "Silver", "members": 25,
            "is_upgrade": True, "conversion_rate_pct": 25.0,
        }
    ]


def test_a_downgrade_is_flagged_as_not_an_upgrade() -> None:
    transitions = [{"from_tier": "Gold", "to_tier": "Standard", "members": 2}]
    rates = tier_conversion_rates(transitions, {"Gold": 10})
    assert rates[0]["is_upgrade"] is False


def test_an_unknown_denominator_yields_a_zero_rate_instead_of_raising() -> None:
    transitions = [{"from_tier": "Platinum", "to_tier": "Gold", "members": 1}]
    rates = tier_conversion_rates(transitions, {})
    assert rates[0]["conversion_rate_pct"] == 0.0


@pytest.mark.parametrize("members", [0, 1, 840])
def test_conversion_rate_is_always_between_0_and_100(members: int) -> None:
    transitions = [{"from_tier": "Standard", "to_tier": "Silver", "members": members}]
    rates = tier_conversion_rates(transitions, {"Standard": max(members, 1)})
    assert 0.0 <= rates[0]["conversion_rate_pct"] <= 100.0
