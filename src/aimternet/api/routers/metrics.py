"""Read-only metrics for the dashboard (spec §7.2).

Three sources, each used for what it is good at:

* **RDS** — live and today's numbers. §7.2 suggests Redshift and DynamoDB, but Redshift only
  sees a POS transaction after the next DAG run, and a dashboard that cannot show a check-in
  that just happened is not much of an operations dashboard. Selectable via
  ``AIMTERNET_METRICS_LIVE_SOURCE``; recorded as a deviation in poc_policy.
* **Redshift** — historical aggregates, where the dimensional model earns its keep.
* **DynamoDB** — per-workstation status and recent telemetry, which is exactly the shape it
  was designed for: a bounded set of point queries, never a scan.

Everything here is a SELECT. There is no endpoint that accepts SQL.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Query

from aimternet.config.settings import settings
from aimternet.db.session import fetch_all

log = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/metrics", tags=["metrics"])

# Redshift round trips are slow enough to matter on an auto-refreshing dashboard, and the
# warehouse only changes when a DAG runs, so a short cache costs nothing in freshness.
_CACHE: dict[str, tuple[float, Any]] = {}
_CACHE_TTL_SECONDS = 60


def _cached(key: str, producer: Any) -> Any:
    now = time.time()
    hit = _CACHE.get(key)
    if hit and now - hit[0] < _CACHE_TTL_SECONDS:
        return hit[1]
    value = producer()
    _CACHE[key] = (now, value)
    return value


@router.get("/summary")
def summary() -> dict[str, Any]:
    """The headline numbers, in one round trip for the dashboard's main row."""
    rows = fetch_all(
        """
        SELECT
          (SELECT count(*) FROM rental_transactions WHERE session_end_utc IS NULL)
              AS active_rentals,
          (SELECT count(*) FROM workstations WHERE status = 'AVAILABLE') AS available,
          (SELECT count(*) FROM workstations WHERE status = 'OCCUPIED')  AS occupied,
          (SELECT count(*) FROM workstations)                            AS total_workstations,
          (SELECT count(*) FROM members)                                 AS members,
          (SELECT count(*) FROM members WHERE is_backfilled)             AS members_backfilled
        """
    )
    data = dict(rows[0])
    total = data["total_workstations"] or 1
    data["occupancy_pct"] = round(100 * data["occupied"] / total, 1)
    data["generated_at_utc"] = datetime.now(UTC).isoformat()
    return data


@router.get("/revenue/today")
def revenue_today() -> dict[str, Any]:
    """Revenue since midnight UTC, split rental vs concession.

    Read from RDS so a sale made seconds ago is visible; the warehouse would lag it by a
    DAG run.
    """
    rental = fetch_all(
        """SELECT COALESCE(sum(net_amount_paid), 0) AS amount, count(*) AS transactions
           FROM rental_transactions
           WHERE session_end_utc >= date_trunc('day', now() AT TIME ZONE 'UTC')"""
    )[0]
    concession = fetch_all(
        """SELECT COALESCE(sum(total_amount), 0) AS amount, count(*) AS transactions
           FROM concession_purchases
           WHERE purchased_at_utc >= date_trunc('day', now() AT TIME ZONE 'UTC')"""
    )[0]
    total = Decimal(rental["amount"]) + Decimal(concession["amount"])
    return {
        "currency": "PHP",
        "rental": {"amount": str(rental["amount"]), "transactions": rental["transactions"]},
        "concession": {
            "amount": str(concession["amount"]),
            "transactions": concession["transactions"],
        },
        "total": str(total),
        "source": settings().metrics_live_source,
    }


@router.get("/points")
def points() -> dict[str, Any]:
    """Points issued and redeemed, all time and today."""
    rows = fetch_all(
        """
        SELECT
          COALESCE(sum(points_delta) FILTER (WHERE points_delta > 0), 0) AS issued,
          COALESCE(-sum(points_delta) FILTER (WHERE points_delta < 0), 0) AS redeemed,
          COALESCE(sum(points_delta) FILTER (
              WHERE points_delta > 0
                AND created_at_utc >= date_trunc('day', now() AT TIME ZONE 'UTC')), 0)
              AS issued_today,
          COALESCE(-sum(points_delta) FILTER (
              WHERE points_delta < 0
                AND created_at_utc >= date_trunc('day', now() AT TIME ZONE 'UTC')), 0)
              AS redeemed_today
        FROM member_points_ledger
        """
    )
    return dict(rows[0])


@router.get("/workstations/status")
def workstation_status() -> dict[str, Any]:
    """Per-zone counts plus the individual workstations, for the floor plan."""
    by_zone = fetch_all(
        """SELECT zone_classification, status, count(*) AS workstations
           FROM workstations GROUP BY 1, 2 ORDER BY 1, 2"""
    )
    detail = fetch_all(
        """SELECT w.workstation_id, w.zone_classification, w.status,
                  r.rental_id, r.member_id, r.session_start_utc
           FROM workstations w
           LEFT JOIN rental_transactions r
             ON r.workstation_id = w.workstation_id AND r.session_end_utc IS NULL
           ORDER BY w.workstation_id"""
    )
    return {"by_zone": by_zone, "workstations": detail}


@router.get("/rentals/active")
def active_rentals() -> list[dict[str, Any]]:
    return fetch_all(
        """SELECT r.rental_id, r.member_id, m.first_name, m.last_name, m.current_tier,
                  r.workstation_id, w.zone_classification, r.session_start_utc,
                  r.duration_hours, r.final_hourly_rate
           FROM rental_transactions r
           JOIN members m USING (member_id)
           JOIN workstations w USING (workstation_id)
           WHERE r.session_end_utc IS NULL
           ORDER BY r.session_start_utc"""
    )


@router.get("/utilization/hourly")
def utilization_hourly(
    days: int = Query(default=7, ge=1, le=62, description="Trailing days to include"),
) -> dict[str, Any]:
    """Hourly utilisation from the warehouse aggregate.

    This is the query the Gold aggregate exists for: 260,400 pre-aggregated rows instead of
    6.3M raw telemetry readings.
    """

    def load() -> dict[str, Any]:
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            rows = redshift.fetch_all(
                f"""
                SELECT hour_utc,
                       CAST(AVG(utilization_pct) AS DECIMAL(5,2)) AS avg_utilization_pct,
                       SUM(readings_occupied) AS readings_occupied,
                       SUM(readings)          AS readings
                FROM {schema}.agg_workstation_utilization_hourly
                WHERE utilization_date >= (
                    SELECT MAX(utilization_date) - {days}
                    FROM {schema}.agg_workstation_utilization_hourly
                )
                GROUP BY hour_utc ORDER BY hour_utc
                """
            )
            return {"source": "redshift", "days": days, "hours": rows}
        except Exception as exc:
            log.warning("utilisation unavailable from Redshift: %s", exc)
            return {"source": "unavailable", "days": days, "hours": [], "error": str(exc)[:200]}

    return _cached(f"utilization:{days}", load)


@router.get("/revenue/by-zone")
def revenue_by_zone(
    days: int = Query(default=30, ge=1, le=62),
) -> dict[str, Any]:
    """Historical revenue by zone, from the warehouse."""

    def load() -> dict[str, Any]:
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            rows = redshift.fetch_all(
                f"""
                SELECT zone_classification,
                       COUNT(*)                  AS rentals,
                       SUM(net_amount_paid)      AS net_revenue,
                       CAST(AVG(duration_hours) AS DECIMAL(6,2)) AS avg_hours
                FROM {schema}.fact_rental
                WHERE session_start_utc >= (
                    SELECT MAX(session_start_utc) - INTERVAL '{days} days'
                    FROM {schema}.fact_rental
                )
                GROUP BY 1 ORDER BY net_revenue DESC
                """
            )
            return {"source": "redshift", "days": days, "zones": rows}
        except Exception as exc:
            log.warning("zone revenue unavailable from Redshift: %s", exc)
            return {"source": "unavailable", "days": days, "zones": [], "error": str(exc)[:200]}

    return _cached(f"revenue_by_zone:{days}", load)


@router.get("/telemetry/recent")
def telemetry_recent(
    limit: int = Query(default=12, ge=1, le=175, description="How many workstations to sample"),
) -> dict[str, Any]:
    """The latest telemetry reading per workstation, straight from DynamoDB.

    One ``Limit=1`` reverse query per workstation — the access pattern the table was designed
    for. Never a scan: a scan reads all 6.3M items and gets slower every day.
    """

    def load() -> dict[str, Any]:
        import boto3
        from boto3.dynamodb.conditions import Key

        cfg = settings()
        table = boto3.resource("dynamodb", region_name=cfg.aws_region).Table(
            cfg.ddb_telemetry_table
        )
        readings = []
        try:
            for number in range(1, limit + 1):
                workstation = f"PC-{number:03d}"
                response = table.query(
                    KeyConditionExpression=Key("PK").eq(f"WS#{workstation}"),
                    ScanIndexForward=False,
                    Limit=1,
                )
                if response["Items"]:
                    item = response["Items"][0]
                    readings.append(
                        {
                            "workstation_id": item["workstation_id"],
                            "timestamp_utc": item["timestamp_utc"],
                            "zone": item["zone"],
                            "status": item["status"],
                            "cpu_load_pct": str(item["hardware_metrics"].get("cpu_load_pct")),
                            "cpu_temp_c": str(item["hardware_metrics"].get("cpu_temp_c")),
                            "latency_ping_ms": str(
                                item["network_diagnostics"].get("latency_ping_ms")
                            ),
                        }
                    )
            return {"source": "dynamodb", "count": len(readings), "readings": readings}
        except Exception as exc:
            log.warning("telemetry unavailable from DynamoDB: %s", exc)
            return {"source": "unavailable", "count": 0, "readings": [], "error": str(exc)[:200]}

    return _cached(f"telemetry:{limit}", load)
