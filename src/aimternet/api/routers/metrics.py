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
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Query

from aimternet.api.metrics_schemas import MembersOverview, TierCount
from aimternet.config.business_rules import rules
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


def _redshift_window(
    schema: str, table: str, date_col: str, days: int
) -> tuple[datetime, datetime]:
    """The (start, end) of a trailing window anchored to the table's own max date.

    Never wall-clock ``now()`` — the warehouse holds a fixed 2026-07-01..2026-08-31 batch, so
    "last 7 days" has to mean the last 7 days *of that data*, not of the real calendar.
    Raises when the table is empty, which the caller's existing degrade-on-exception wrapper
    already turns into ``{"source": "unavailable", ...}``.
    """
    from aimternet.db import redshift

    row = redshift.fetch_all(f"SELECT MAX({date_col}) AS max_d FROM {schema}.{table}")[0]
    end = row["max_d"]
    if end is None:
        raise ValueError(f"{schema}.{table} has no rows to anchor a window to")
    return end - timedelta(days=days), end


def _window_dict(start: datetime, end: datetime, days: int) -> dict[str, Any]:
    return {"start_date": start.isoformat(), "end_date": end.isoformat(), "days": days}


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
            start, end = _redshift_window(
                schema, "agg_workstation_utilization_hourly", "utilization_date", days
            )
            rows = redshift.fetch_all(
                f"""
                SELECT hour_utc,
                       CAST(AVG(utilization_pct) AS DECIMAL(5,2)) AS avg_utilization_pct,
                       SUM(readings_occupied) AS readings_occupied,
                       SUM(readings)          AS readings
                FROM {schema}.agg_workstation_utilization_hourly
                WHERE utilization_date >= %s
                GROUP BY hour_utc ORDER BY hour_utc
                """,
                (start,),
            )
            return {
                "source": "redshift", "days": days, "hours": rows,
                "window": _window_dict(start, end, days),
            }
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
            start, end = _redshift_window(schema, "fact_rental", "session_start_utc", days)
            rows = redshift.fetch_all(
                f"""
                SELECT zone_classification,
                       COUNT(*)                  AS rentals,
                       SUM(net_amount_paid)      AS net_revenue,
                       CAST(AVG(duration_hours) AS DECIMAL(6,2)) AS avg_hours
                FROM {schema}.fact_rental
                WHERE session_start_utc >= %s
                GROUP BY 1 ORDER BY net_revenue DESC
                """,
                (start,),
            )
            return {
                "source": "redshift", "days": days, "zones": rows,
                "window": _window_dict(start, end, days),
            }
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


# ============================================================================================
# The endpoints below back the Streamlit dashboard's three sections (PC Telemetry, Descriptive
# Analytics, Data Science). See CLAUDE.md invariant #9 / findings F7-F8: fact_rental and
# fact_concession_sale are built from Bronze-derived Silver, not the RDS incremental exports,
# so every Redshift-sourced number here reflects only the 62-day bootstrap batch, not POS
# activity since bootstrap. Each such response carries "source": "redshift" and a `window`
# anchored to the warehouse's own max date -- Streamlit is expected to badge these as a
# historical snapshot rather than implying they are live.
# ============================================================================================


@router.get("/data-window")
def data_window() -> dict[str, Any]:
    """Server time plus the warehouse's actual date range.

    The one call every dashboard session makes at startup to resolve "Last 7 days" etc.
    against the warehouse's own max date instead of wall-clock time -- see the module note
    above on why: the data stops at 2026-08-31 regardless of what day it is when this runs.
    """
    server_time = fetch_all("SELECT now() AS server_time_utc")[0]["server_time_utc"]

    def load_warehouse_bounds() -> dict[str, Any]:
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            row = redshift.fetch_all(
                f"SELECT MIN(session_start_utc) AS min_d, MAX(session_start_utc) AS max_d "
                f"FROM {schema}.fact_rental"
            )[0]
            return {
                "source": "redshift",
                "warehouse_min_date_utc": row["min_d"].isoformat() if row["min_d"] else None,
                "warehouse_max_date_utc": row["max_d"].isoformat() if row["max_d"] else None,
            }
        except Exception as exc:
            log.warning("data window unavailable from Redshift: %s", exc)
            return {
                "source": "unavailable", "error": str(exc)[:200],
                "warehouse_min_date_utc": None, "warehouse_max_date_utc": None,
            }

    warehouse = _cached("data_window:warehouse", load_warehouse_bounds)
    return {"server_time_utc": server_time.isoformat(), **warehouse}


@router.get("/telemetry/fleet-health")
def telemetry_fleet_health() -> dict[str, Any]:
    """Fleet-wide telemetry health, sampled the same way ``/telemetry/recent`` does.

    One ``Limit=1`` reverse query per workstation across all of them (never a scan), then
    aggregated server-side: fleet averages, alert counts against the heuristics in
    ``metrics_scoring``, and a stale-reading count relative to the fleet's own latest reading
    (never wall-clock, for the same reason ``data_window`` isn't).
    """

    def load() -> dict[str, Any]:
        import boto3
        from boto3.dynamodb.conditions import Key

        from aimternet.api.metrics_scoring import (
            CPU_TEMP_ALERT_C,
            GPU_TEMP_ALERT_C,
            LATENCY_ALERT_MS,
            PACKET_LOSS_ALERT_PCT,
        )

        cfg = settings()
        table = boto3.resource("dynamodb", region_name=cfg.aws_region).Table(
            cfg.ddb_telemetry_table
        )
        readings: list[dict[str, Any]] = []
        try:
            for number in range(1, rules().total_workstations + 1):
                workstation = f"PC-{number:03d}"
                response = table.query(
                    KeyConditionExpression=Key("PK").eq(f"WS#{workstation}"),
                    ScanIndexForward=False,
                    Limit=1,
                )
                if response["Items"]:
                    item = response["Items"][0]
                    hw = item["hardware_metrics"]
                    net = item["network_diagnostics"]
                    readings.append(
                        {
                            "workstation_id": item["workstation_id"],
                            "zone": item["zone"],
                            "status": item["status"],
                            "timestamp_utc": item["timestamp_utc"],
                            "cpu_load_pct": Decimal(str(hw.get("cpu_load_pct", 0))),
                            "cpu_temp_c": Decimal(str(hw.get("cpu_temp_c", 0))),
                            "gpu_load_pct": Decimal(str(hw.get("gpu_load_pct", 0))),
                            "gpu_temp_c": Decimal(str(hw.get("gpu_temp_c", 0))),
                            "latency_ping_ms": Decimal(str(net.get("latency_ping_ms", 0))),
                            "packet_loss_pct": Decimal(str(net.get("packet_loss_pct", 0))),
                        }
                    )
        except Exception as exc:
            log.warning("fleet health unavailable from DynamoDB: %s", exc)
            return {
                "source": "unavailable", "error": str(exc)[:200],
                "fleet": {}, "workstations": [],
            }

        if not readings:
            return {"source": "dynamodb", "fleet": {}, "workstations": []}

        n = len(readings)
        fleet_max_ts = max(datetime.fromisoformat(r["timestamp_utc"]) for r in readings)
        stale_threshold = timedelta(seconds=rules().telemetry_tick_seconds * 3)

        for r in readings:
            reading_ts = datetime.fromisoformat(r["timestamp_utc"])
            r["is_stale"] = (fleet_max_ts - reading_ts) > stale_threshold
            r["has_thermal_alert"] = (
                r["cpu_temp_c"] > Decimal(str(CPU_TEMP_ALERT_C))
                or r["gpu_temp_c"] > Decimal(str(GPU_TEMP_ALERT_C))
            )
            r["has_network_alert"] = (
                r["latency_ping_ms"] > Decimal(str(LATENCY_ALERT_MS))
                or r["packet_loss_pct"] > Decimal(str(PACKET_LOSS_ALERT_PCT))
            )

        fleet = {
            "sampled_workstations": n,
            "avg_cpu_load_pct": str(round(sum(r["cpu_load_pct"] for r in readings) / n, 1)),
            "avg_cpu_temp_c": str(round(sum(r["cpu_temp_c"] for r in readings) / n, 1)),
            "avg_gpu_load_pct": str(round(sum(r["gpu_load_pct"] for r in readings) / n, 1)),
            "avg_gpu_temp_c": str(round(sum(r["gpu_temp_c"] for r in readings) / n, 1)),
            "thermal_alert_count": sum(1 for r in readings if r["has_thermal_alert"]),
            "network_alert_count": sum(1 for r in readings if r["has_network_alert"]),
            "stale_reading_count": sum(1 for r in readings if r["is_stale"]),
            "fleet_latest_reading_utc": fleet_max_ts.isoformat(),
        }
        return {"source": "dynamodb", "fleet": fleet, "workstations": readings}

    return _cached("telemetry_fleet_health", load)


@router.get("/utilization/heatmap")
def utilization_heatmap(
    days: int = Query(default=7, ge=1, le=62, description="Trailing days to include"),
) -> dict[str, Any]:
    """Workstation x hour-of-day utilisation matrix.

    Distinct from ``/utilization/hourly``, which folds every workstation into one line per
    hour -- this keeps ``workstation_id`` so a heatmap can show per-PC patterns.
    """

    def load() -> dict[str, Any]:
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            start, end = _redshift_window(
                schema, "agg_workstation_utilization_hourly", "utilization_date", days
            )
            rows = redshift.fetch_all(
                f"""
                SELECT workstation_id, zone, hour_utc,
                       CAST(AVG(utilization_pct) AS DECIMAL(5,2)) AS avg_utilization_pct
                FROM {schema}.agg_workstation_utilization_hourly
                WHERE utilization_date >= %s
                GROUP BY 1, 2, 3
                ORDER BY 1, 3
                """,
                (start,),
            )
            return {
                "source": "redshift", "days": days, "cells": rows,
                "window": _window_dict(start, end, days),
            }
        except Exception as exc:
            log.warning("utilisation heatmap unavailable from Redshift: %s", exc)
            return {"source": "unavailable", "days": days, "cells": [], "error": str(exc)[:200]}

    return _cached(f"utilization_heatmap:{days}", load)


@router.get("/revenue/trend")
def revenue_trend(
    days: int = Query(default=30, ge=1, le=62, description="Trailing days to include"),
) -> dict[str, Any]:
    """Daily rental vs concession revenue, gross profit, and payment-method mix.

    Rentals have no COGS, so their whole net amount is profit; concessions carry
    ``line_margin`` per item from ``dim_concession_item``'s cost/retail spread.
    """

    def load() -> dict[str, Any]:
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            start, end = _redshift_window(schema, "fact_rental", "session_start_utc", days)
            daily = redshift.fetch_all(
                f"""
                WITH rental_daily AS (
                    SELECT date_trunc('day', session_start_utc) AS day,
                           SUM(net_amount_paid) AS rental_revenue,
                           SUM(net_amount_paid) AS rental_profit,
                           COUNT(*) AS rental_transactions
                    FROM {schema}.fact_rental
                    WHERE session_start_utc >= %s
                    GROUP BY 1
                ),
                concession_daily AS (
                    SELECT date_trunc('day', s.purchased_at_utc) AS day,
                           SUM(s.total_amount) AS concession_revenue,
                           SUM(li.line_margin) AS concession_margin,
                           COUNT(DISTINCT s.purchase_id) AS concession_transactions
                    FROM {schema}.fact_concession_sale s
                    JOIN {schema}.fact_concession_line_item li ON li.purchase_id = s.purchase_id
                    WHERE s.purchased_at_utc >= %s
                    GROUP BY 1
                )
                SELECT COALESCE(r.day, c.day)               AS day,
                       COALESCE(r.rental_revenue, 0)         AS rental_revenue,
                       COALESCE(c.concession_revenue, 0)     AS concession_revenue,
                       COALESCE(r.rental_profit, 0)
                           + COALESCE(c.concession_margin, 0) AS gross_profit,
                       COALESCE(r.rental_transactions, 0)
                           + COALESCE(c.concession_transactions, 0) AS transactions
                FROM rental_daily r
                FULL OUTER JOIN concession_daily c ON c.day = r.day
                ORDER BY 1
                """,
                (start, start),
            )
            for row in daily:
                total = Decimal(row["rental_revenue"] or 0) + Decimal(
                    row["concession_revenue"] or 0
                )
                row["total_revenue"] = str(total)
                row["avg_transaction_value"] = str(
                    (total / row["transactions"]).quantize(Decimal("0.01"))
                    if row["transactions"]
                    else Decimal("0.00")
                )

            payment_mix = redshift.fetch_all(
                f"""
                SELECT payment_method, SUM(amount) AS amount, COUNT(*) AS transactions FROM (
                    SELECT payment_method, net_amount_paid AS amount
                    FROM {schema}.fact_rental WHERE session_start_utc >= %s
                    UNION ALL
                    SELECT payment_method, total_amount AS amount
                    FROM {schema}.fact_concession_sale WHERE purchased_at_utc >= %s
                ) combined
                GROUP BY 1 ORDER BY 2 DESC
                """,
                (start, start),
            )
            return {
                "source": "redshift", "days": days, "daily": daily, "payment_mix": payment_mix,
                "window": _window_dict(start, end, days),
            }
        except Exception as exc:
            log.warning("revenue trend unavailable from Redshift: %s", exc)
            return {
                "source": "unavailable", "days": days, "daily": [], "payment_mix": [],
                "error": str(exc)[:200],
            }

    return _cached(f"revenue_trend:{days}", load)


@router.get("/points/history")
def points_history(
    days: int = Query(default=30, ge=1, le=62, description="Trailing days to include"),
) -> dict[str, Any]:
    """Daily points issued/redeemed and the outstanding redemption liability.

    Liability reuses ``business_rules.rules()`` for the unit/value conversion (spec §3: one
    pricing implementation) rather than re-hardcoding the 100-points-per-PHP-50 rule here.
    """

    def load() -> dict[str, Any]:
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            start, end = _redshift_window(schema, "fact_points_activity", "created_at_utc", days)
            daily = redshift.fetch_all(
                f"""
                SELECT date_trunc('day', created_at_utc) AS day, transaction_type,
                       SUM(CASE WHEN points_delta > 0 THEN points_delta ELSE 0 END) AS issued,
                       SUM(CASE WHEN points_delta < 0 THEN -points_delta ELSE 0 END) AS redeemed
                FROM {schema}.fact_points_activity
                WHERE created_at_utc >= %s
                GROUP BY 1, 2 ORDER BY 1, 2
                """,
                (start,),
            )
            totals = redshift.fetch_all(
                f"""
                SELECT SUM(CASE WHEN points_delta > 0 THEN points_delta ELSE 0 END) AS issued,
                       SUM(CASE WHEN points_delta < 0 THEN -points_delta ELSE 0 END) AS redeemed
                FROM {schema}.fact_points_activity
                WHERE created_at_utc >= %s
                """,
                (start,),
            )[0]
            issued = int(totals["issued"] or 0)
            redeemed = int(totals["redeemed"] or 0)
            outstanding = issued - redeemed
            biz = rules()
            liability = (
                (Decimal(outstanding) / biz.redemption_unit_points * biz.redemption_unit_value)
                .quantize(Decimal("0.01"))
                if outstanding > 0
                else Decimal("0.00")
            )
            return {
                "source": "redshift", "days": days, "daily": daily,
                "issued": issued, "redeemed": redeemed, "outstanding": outstanding,
                "outstanding_liability": str(liability),
                "redemption_rate_pct": round(100 * redeemed / issued, 1) if issued else 0.0,
                "window": _window_dict(start, end, days),
            }
        except Exception as exc:
            log.warning("points history unavailable from Redshift: %s", exc)
            return {"source": "unavailable", "days": days, "daily": [], "error": str(exc)[:200]}

    return _cached(f"points_history:{days}", load)


@router.get("/members/overview", response_model=MembersOverview)
def members_overview(
    days: int = Query(default=30, ge=1, le=365, description="Window for 'new members'"),
) -> MembersOverview:
    """Tier distribution, new registrations, and average spend by tier -- off the live
    ``members`` table, so unlike most Descriptive Analytics cards this one is not
    bootstrap-only: it reflects every registration up to this second.
    """
    by_tier = fetch_all(
        """
        SELECT current_tier AS tier, count(*) AS members,
               COALESCE(avg(lifetime_spend_amount), 0) AS avg_lifetime_spend
        FROM members GROUP BY 1 ORDER BY 1
        """
    )
    totals = fetch_all(
        """
        SELECT
          count(*) FILTER (WHERE registered_at_utc >= now() - make_interval(days => %s))
              AS new_members,
          count(*) FILTER (WHERE is_active)     AS active_members,
          count(*) FILTER (WHERE is_backfilled) AS backfilled_members
        FROM members
        """,
        (days,),
    )[0]
    return MembersOverview(
        days=days,
        new_members=totals["new_members"],
        active_members=totals["active_members"],
        backfilled_members=totals["backfilled_members"],
        by_tier=[TierCount(**row) for row in by_tier],
        generated_at_utc=datetime.now(UTC),
    )


@router.get("/members/leaderboard")
def members_leaderboard(
    days: int = Query(default=30, ge=1, le=62, description="Trailing days to include"),
    limit: int = Query(default=25, ge=1, le=200),
) -> dict[str, Any]:
    """Per-member spend, hours and points, ranked by a composite ``engagement_score``.

    The score itself is pure descriptive math in ``metrics_scoring.engagement_score`` --
    normalized, weighted spend/hours/points, no model.
    """

    def load() -> dict[str, Any]:
        from aimternet.api.metrics_scoring import engagement_score
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            start, end = _redshift_window(schema, "fact_rental", "session_start_utc", days)
            rows = redshift.fetch_all(
                f"""
                WITH rental_agg AS (
                    SELECT member_id, SUM(net_amount_paid) AS net_revenue,
                           SUM(duration_hours) AS hours, COUNT(*) AS rentals
                    FROM {schema}.fact_rental WHERE session_start_utc >= %s GROUP BY 1
                ),
                concession_agg AS (
                    SELECT member_id, SUM(total_amount) AS net_revenue
                    FROM {schema}.fact_concession_sale WHERE purchased_at_utc >= %s GROUP BY 1
                ),
                points_agg AS (
                    SELECT member_id,
                           SUM(CASE WHEN points_delta > 0 THEN points_delta ELSE 0 END)
                               AS points_earned,
                           SUM(CASE WHEN points_delta < 0 THEN -points_delta ELSE 0 END)
                               AS points_redeemed
                    FROM {schema}.fact_points_activity WHERE created_at_utc >= %s GROUP BY 1
                )
                SELECT d.member_id, d.first_name, d.last_name, d.tier,
                       COALESCE(r.net_revenue, 0) + COALESCE(c.net_revenue, 0) AS net_revenue,
                       COALESCE(r.hours, 0)   AS hours,
                       COALESCE(r.rentals, 0) AS rentals,
                       COALESCE(p.points_earned, 0)   AS points_earned,
                       COALESCE(p.points_redeemed, 0) AS points_redeemed
                FROM {schema}.dim_member d
                LEFT JOIN rental_agg r     ON r.member_id = d.member_id
                LEFT JOIN concession_agg c ON c.member_id = d.member_id
                LEFT JOIN points_agg p     ON p.member_id = d.member_id
                WHERE d.is_current AND (r.member_id IS NOT NULL OR c.member_id IS NOT NULL)
                ORDER BY net_revenue DESC
                LIMIT %s
                """,
                (start, start, start, limit),
            )
            if rows:
                max_revenue = max(Decimal(r["net_revenue"] or 0) for r in rows)
                max_hours = max(Decimal(r["hours"] or 0) for r in rows)
                max_points = max(int(r["points_earned"] or 0) for r in rows) or 1
                for r in rows:
                    r["engagement_score"] = str(
                        engagement_score(
                            net_revenue=Decimal(r["net_revenue"] or 0),
                            hours=Decimal(r["hours"] or 0),
                            points_earned=int(r["points_earned"] or 0),
                            max_net_revenue=max_revenue or Decimal(1),
                            max_hours=max_hours or Decimal(1),
                            max_points_earned=max_points,
                        )
                    )
                rows.sort(key=lambda r: Decimal(r["engagement_score"]), reverse=True)
            return {
                "source": "redshift", "days": days, "limit": limit, "members": rows,
                "window": _window_dict(start, end, days),
            }
        except Exception as exc:
            log.warning("leaderboard unavailable from Redshift: %s", exc)
            return {
                "source": "unavailable", "days": days, "limit": limit, "members": [],
                "error": str(exc)[:200],
            }

    return _cached(f"leaderboard:{days}:{limit}", load)


@router.get("/members/tier-migration")
def members_tier_migration(
    days: int = Query(
        default=62, ge=1, le=62,
        description="Accepted for interface symmetry with other cards; see docstring",
    ),
) -> dict[str, Any]:
    """Current tier distribution plus every SCD2 transition ever recorded in ``dim_member``.

    ``days`` is not applied as a filter here: transitions come from
    ``LAG(tier) OVER (PARTITION BY member_id ORDER BY valid_from_utc)``, and cutting the
    input rows by date would break that chain (a member's 3rd-recorded tier needs their 1st
    and 2nd to compute a transition, even if only the 3rd falls inside the window). The whole
    SCD2 history is one bootstrap batch anyway, so windowing it would not change the answer.
    """

    def load() -> dict[str, Any]:
        from aimternet.api.metrics_scoring import tier_conversion_rates
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            current_distribution = redshift.fetch_all(
                f"""SELECT tier, COUNT(*) AS members
                    FROM {schema}.dim_member WHERE is_current GROUP BY 1 ORDER BY 1"""
            )
            transitions = redshift.fetch_all(
                f"""
                WITH tier_history AS (
                    SELECT member_id, tier,
                           LAG(tier) OVER (PARTITION BY member_id ORDER BY valid_from_utc)
                               AS previous_tier
                    FROM {schema}.dim_member
                )
                SELECT previous_tier AS from_tier, tier AS to_tier, COUNT(*) AS members
                FROM tier_history
                WHERE previous_tier IS NOT NULL AND previous_tier <> tier
                GROUP BY 1, 2 ORDER BY 1, 2
                """
            )
            ever_at_tier = redshift.fetch_all(
                f"""SELECT tier, COUNT(DISTINCT member_id) AS members
                    FROM {schema}.dim_member GROUP BY 1"""
            )
            ever_at_tier_map = {r["tier"]: int(r["members"]) for r in ever_at_tier}
            return {
                "source": "redshift", "days": days,
                "current_distribution": current_distribution,
                "transitions": transitions,
                "conversion_rates": tier_conversion_rates(transitions, ever_at_tier_map),
            }
        except Exception as exc:
            log.warning("tier migration unavailable from Redshift: %s", exc)
            return {
                "source": "unavailable", "days": days,
                "current_distribution": [], "transitions": [], "conversion_rates": [],
                "error": str(exc)[:200],
            }

    return _cached("tier_migration", load)


@router.get("/workstations/health-score")
def workstations_health_score(
    days: int = Query(default=7, ge=1, le=62, description="Trailing days to include"),
) -> dict[str, Any]:
    """A 0-100 composite health score per workstation from the hourly telemetry aggregate.

    Scoring itself is pure in ``metrics_scoring.workstation_health_score`` -- a penalty
    function against the same alert thresholds ``telemetry/fleet-health`` uses, so a
    workstation that is currently alerting also shows up with a lower recent-history score.
    """

    def load() -> dict[str, Any]:
        from aimternet.api.metrics_scoring import workstation_health_score
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            start, end = _redshift_window(
                schema, "agg_workstation_utilization_hourly", "utilization_date", days
            )
            rows = redshift.fetch_all(
                f"""
                SELECT workstation_id, zone,
                       MAX(max_cpu_temp_c)                          AS max_cpu_temp_c,
                       MAX(max_gpu_temp_c)                          AS max_gpu_temp_c,
                       CAST(AVG(avg_latency_ping_ms) AS DECIMAL(6,1)) AS avg_latency_ping_ms,
                       MAX(max_packet_loss_pct)                     AS max_packet_loss_pct
                FROM {schema}.agg_workstation_utilization_hourly
                WHERE utilization_date >= %s
                GROUP BY 1, 2 ORDER BY 1
                """,
                (start,),
            )
            for r in rows:
                r["health_score"] = workstation_health_score(
                    max_cpu_temp_c=float(r["max_cpu_temp_c"] or 0),
                    max_gpu_temp_c=float(r["max_gpu_temp_c"] or 0),
                    avg_latency_ping_ms=float(r["avg_latency_ping_ms"] or 0),
                    max_packet_loss_pct=float(r["max_packet_loss_pct"] or 0),
                )
            fleet_avg = round(sum(r["health_score"] for r in rows) / len(rows), 1) if rows else None
            rows.sort(key=lambda r: r["health_score"])
            return {
                "source": "redshift", "days": days, "fleet_avg_health_score": fleet_avg,
                "workstations": rows, "window": _window_dict(start, end, days),
            }
        except Exception as exc:
            log.warning("workstation health score unavailable from Redshift: %s", exc)
            return {
                "source": "unavailable", "days": days, "fleet_avg_health_score": None,
                "workstations": [], "error": str(exc)[:200],
            }

    return _cached(f"health_score:{days}", load)


@router.get("/efficiency/revenue-per-hour")
def efficiency_revenue_per_hour(
    days: int = Query(default=30, ge=1, le=62, description="Trailing days to include"),
) -> dict[str, Any]:
    """Net rental revenue per estimated occupied hour, by zone.

    Occupied hours are estimated from ``readings_occupied`` (a count of telemetry ticks
    where the workstation was occupied) times the tick interval from
    ``business_rules.rules().telemetry_tick_seconds`` -- the real fact about how often a
    reading arrives, reused rather than re-guessed here.
    """

    def load() -> dict[str, Any]:
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            start, end = _redshift_window(schema, "fact_rental", "session_start_utc", days)
            revenue = redshift.fetch_all(
                f"""
                SELECT zone_classification AS zone, SUM(net_amount_paid) AS net_revenue
                FROM {schema}.fact_rental
                WHERE session_start_utc >= %s
                GROUP BY 1
                """,
                (start,),
            )
            occupied = redshift.fetch_all(
                f"""
                SELECT zone, SUM(readings_occupied) AS occupied_readings
                FROM {schema}.agg_workstation_utilization_hourly
                WHERE utilization_date >= %s
                GROUP BY 1
                """,
                (start,),
            )
            tick_seconds = rules().telemetry_tick_seconds
            occupied_hours = {
                r["zone"]: (int(r["occupied_readings"] or 0) * tick_seconds / 3600)
                for r in occupied
            }
            zones = []
            for r in revenue:
                zone = r["zone"]
                hours = occupied_hours.get(zone, 0.0)
                net_revenue = Decimal(r["net_revenue"] or 0)
                rev_per_hour = (
                    (net_revenue / Decimal(str(hours))).quantize(Decimal("0.01"))
                    if hours
                    else Decimal("0.00")
                )
                zones.append(
                    {
                        "zone": zone,
                        "net_revenue": str(net_revenue),
                        "occupied_hours": round(hours, 1),
                        "revenue_per_occupied_hour": str(rev_per_hour),
                    }
                )
            zones.sort(key=lambda z: Decimal(z["revenue_per_occupied_hour"]), reverse=True)
            return {
                "source": "redshift", "days": days, "zones": zones,
                "window": _window_dict(start, end, days),
            }
        except Exception as exc:
            log.warning("revenue-per-hour unavailable from Redshift: %s", exc)
            return {"source": "unavailable", "days": days, "zones": [], "error": str(exc)[:200]}

    return _cached(f"revenue_per_hour:{days}", load)
