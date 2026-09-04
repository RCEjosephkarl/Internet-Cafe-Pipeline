"""Response models for the metrics API, reusing ``schemas.ApiModel``/``MoneyModel``.

Set as ``response_model=`` only on routes with no degrade path — a route that can return
``{"source": "unavailable", ...}`` on a Redshift/DynamoDB outage keeps returning a plain
``dict[str, Any]`` instead, the same way the pre-existing ``/utilization/hourly`` and
``/revenue/by-zone`` routes do, because a strict model would reject that shape.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from aimternet.api.schemas import ApiModel, MoneyModel


class TierCount(MoneyModel):
    tier: str
    members: int
    avg_lifetime_spend: Decimal


class MembersOverview(MoneyModel):
    days: int
    new_members: int
    active_members: int
    backfilled_members: int
    by_tier: list[TierCount]
    generated_at_utc: datetime


class DataWindow(ApiModel):
    server_time_utc: str
    source: str
    warehouse_min_date_utc: str | None
    warehouse_max_date_utc: str | None
    error: str | None = None
