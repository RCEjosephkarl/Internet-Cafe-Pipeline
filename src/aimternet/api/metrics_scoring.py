"""Pure scoring functions behind the Data Science dashboard section.

Nothing here touches a database. Each function takes plain values or rows already fetched
by ``routers/metrics.py`` and returns a derived number — descriptive math (weighted sums,
normalized ratios, penalty scores), never a trained model. Kept separate from the router so
the formulas can be unit tested without RDS, Redshift or DynamoDB.

The thresholds and weights below are dashboard heuristics for ranking and alerting, not
business rules: spec §3 reserves "business rule" for pricing/points/tiers, which is why none
of this lives in ``business_rules.yaml`` or imports from ``business_rules.py``.
"""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
from typing import Any

# ---------------------------------------------------------------- workstation health

CPU_TEMP_ALERT_C = 80.0
GPU_TEMP_ALERT_C = 85.0
LATENCY_ALERT_MS = 80.0
PACKET_LOSS_ALERT_PCT = 2.0


def workstation_health_score(
    *,
    max_cpu_temp_c: float,
    max_gpu_temp_c: float,
    avg_latency_ping_ms: float,
    max_packet_loss_pct: float,
) -> float:
    """A 0-100 composite: 100 is a workstation with nothing to flag.

    Each factor subtracts a penalty once it crosses its own alert threshold, scaled by how
    far over the threshold it is and capped so one bad factor cannot carry the whole score
    negative. Weighted CPU heaviest (a hot CPU is the most common real failure precursor in
    the telemetry), network diagnostics lightest (noisiest, least actionable signal).
    """
    score = 100.0
    score -= min(40.0, max(0.0, max_cpu_temp_c - CPU_TEMP_ALERT_C) * 2)
    score -= min(30.0, max(0.0, max_gpu_temp_c - GPU_TEMP_ALERT_C) * 2)
    score -= min(20.0, max(0.0, avg_latency_ping_ms - LATENCY_ALERT_MS) * 0.5)
    score -= min(10.0, max(0.0, max_packet_loss_pct - PACKET_LOSS_ALERT_PCT) * 5)
    return round(max(0.0, score), 1)


# ---------------------------------------------------------------- member engagement

ENGAGEMENT_WEIGHT_REVENUE = Decimal("0.5")
ENGAGEMENT_WEIGHT_HOURS = Decimal("0.3")
ENGAGEMENT_WEIGHT_POINTS = Decimal("0.2")


def engagement_score(
    *,
    net_revenue: Decimal,
    hours: Decimal,
    points_earned: int,
    max_net_revenue: Decimal,
    max_hours: Decimal,
    max_points_earned: int,
) -> Decimal:
    """A 0-100 composite ranking members within the set they were scored alongside.

    Each factor is normalized against the maximum seen in that same leaderboard call (so
    the score is relative to the current member population and window, not an absolute
    scale), then combined with fixed weights favoring spend over time-on-floor over points.
    """

    def normalized(value: Decimal, maximum: Decimal) -> Decimal:
        return value / maximum if maximum > 0 else Decimal("0")

    composite = (
        normalized(net_revenue, max_net_revenue) * ENGAGEMENT_WEIGHT_REVENUE
        + normalized(hours, max_hours) * ENGAGEMENT_WEIGHT_HOURS
        + normalized(Decimal(points_earned), Decimal(max_points_earned)) * ENGAGEMENT_WEIGHT_POINTS
    ) * 100
    return composite.quantize(Decimal("0.1"))


# ---------------------------------------------------------------- tier migration

_TIER_ORDER = {"Standard": 0, "Silver": 1, "Gold": 2}


def tier_conversion_rates(
    transitions: list[dict[str, Any]], members_ever_at_tier: dict[str, int]
) -> list[dict[str, Any]]:
    """Turn raw ``(from_tier, to_tier, members)`` transition counts into rates.

    ``transitions`` is the output of grouping Redshift's
    ``LAG(tier) OVER (PARTITION BY member_id ORDER BY valid_from_utc)`` in
    ``dim_member`` — one row per distinct transition pair with a count. The denominator,
    ``members_ever_at_tier``, is every member who was ever observed at the source tier
    (``COUNT(DISTINCT member_id) ... GROUP BY tier``), so a rate answers "of everyone who was
    ever at Standard, what fraction moved to Silver" — not "of today's Standard members".
    """
    rates = []
    for row in transitions:
        from_tier, to_tier, count = row["from_tier"], row["to_tier"], int(row["members"])
        denominator = members_ever_at_tier.get(from_tier, 0)
        rate_pct = round(100 * count / denominator, 1) if denominator else 0.0
        rates.append(
            {
                "from_tier": from_tier,
                "to_tier": to_tier,
                "members": count,
                "is_upgrade": _TIER_ORDER.get(to_tier, 0) > _TIER_ORDER.get(from_tier, 0),
                "conversion_rate_pct": rate_pct,
            }
        )
    return rates


def transition_counter(transitions: list[dict[str, Any]]) -> Counter[tuple[str, str]]:
    """Small helper kept for tests: the raw (from, to) -> count mapping."""
    return Counter(
        {(row["from_tier"], row["to_tier"]): int(row["members"]) for row in transitions}
    )
