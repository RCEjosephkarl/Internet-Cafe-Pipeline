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
#: Ceiling on any trailing-window query. This was 62 -- the width of the bootstrap batch --
#: back when the warehouse was that batch and nothing else. It is not any more: POS rows keep
#: arriving, so the span grows every day the cafe is open, and a 62-day ceiling would silently
#: truncate the window the dashboard asks for rather than refusing it. A year is a ceiling on
#: the query, not a description of the data; the real span is what /data-window reports.
WINDOW_MAX_DAYS = 365

#: The three peripherals every telemetry reading carries (``schemas/source.py``'s
#: ``PeripheralsConnected``). Named here so the fleet-health projection, the fleet counts and
#: the dashboard's per-peripheral sections all iterate one list rather than three hardcodings.
PERIPHERALS = ("keyboard", "mouse", "headset")

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
    days: int = Query(default=7, ge=1, le=WINDOW_MAX_DAYS, description="Trailing days to include"),
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
    days: int = Query(default=30, ge=1, le=WINDOW_MAX_DAYS),
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
# Analytics, Data Science). They read Redshift, and Redshift now sees the POS: Gold unions the
# Bronze-derived Silver datasets with the operational snapshots the RDS and DynamoDB exports
# write, so a rental rung up at the till reaches these numbers on the next pipeline pass.
#
# What remains true is *lag*, not absence. The chain is hourly and staggered -- rds_to_s3 at
# :00, dynamodb_to_s3 at :15, curate at :30, load_redshift at :45 -- so a Redshift-sourced
# card trails RDS by up to about an hour, and the caches below add a further ~2 minutes. Each
# such response carries "source": "redshift" and a `window` anchored to the warehouse's own
# max date rather than wall-clock time, because the batch these rows sit on top of is fixed
# in the past. Streamlit is expected to say which cards are live and which are warehouse.
#
# Two things genuinely do stop at 2026-08-31: agg_workstation_utilization_hourly and anything
# derived from it, because raw telemetry has no POS write path -- the machines emit it, and
# the API never adds a reading.
# ============================================================================================


@router.get("/data-window")
def data_window() -> dict[str, Any]:
    """Server time, the warehouse's actual date range, and where the bootstrap batch ends.

    The one call every dashboard session makes at startup, to resolve "Last 7 days" against
    the warehouse's own max date instead of wall-clock time -- the batch underneath is fixed
    in the past, so a trailing window measured from today would mostly miss it.

    ``bootstrap_max_date_utc`` is reported alongside because the two are no longer the same
    date. The warehouse now holds the 62-day batch *and* everything the POS has done since,
    and the join between them can be a gap of days in which the cafe simply was not used. A
    short window landing entirely inside that gap is empty for an honest reason, and a
    dashboard that cannot tell the reader which reason is showing them a broken chart.

    POS rows are identified by ``source_file = 'rds:api'`` -- the lineage marker Gold stamps
    on rows that never came from a file. ``run_id`` is the build id and says nothing here.
    """
    server_time = fetch_all("SELECT now() AS server_time_utc")[0]["server_time_utc"]

    def load_warehouse_bounds() -> dict[str, Any]:
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            row = redshift.fetch_all(
                f"""SELECT MIN(session_start_utc) AS min_d,
                           MAX(session_start_utc) AS max_d,
                           MAX(CASE WHEN source_file <> 'rds:api' THEN session_start_utc END)
                               AS bootstrap_max_d,
                           -- SUM(CASE ...), not COUNT(*) FILTER: Redshift has no FILTER
                           -- clause. The RDS-backed queries in this module do use it, and
                           -- Postgres is fine with it -- the two dialects are not the same.
                           SUM(CASE WHEN source_file = 'rds:api' THEN 1 ELSE 0 END)
                               AS pos_rows
                    FROM {schema}.fact_rental"""
            )[0]

            def iso(value: object) -> str | None:
                return value.isoformat() if value is not None else None  # type: ignore[attr-defined]

            return {
                "source": "redshift",
                "warehouse_min_date_utc": iso(row["min_d"]),
                "warehouse_max_date_utc": iso(row["max_d"]),
                "bootstrap_max_date_utc": iso(row["bootstrap_max_d"]),
                "pos_rows": int(row["pos_rows"] or 0),
            }
        except Exception as exc:
            log.warning("data window unavailable from Redshift: %s", exc)
            return {
                "source": "unavailable", "error": str(exc)[:200],
                "warehouse_min_date_utc": None, "warehouse_max_date_utc": None,
                "bootstrap_max_date_utc": None, "pos_rows": 0,
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
                    # Stored as a map rather than flattened (docs/dynamodb.md), so it rides
                    # along in the item this query already fetched -- reading it costs no
                    # extra round trip.
                    peripherals = item.get("peripherals_connected") or {}
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
                            **{
                                f"{name}_connected": bool(peripherals.get(name, True))
                                for name in PERIPHERALS
                            },
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
            r["missing_peripherals"] = [
                name for name in PERIPHERALS if not r[f"{name}_connected"]
            ]

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
            # One entry per peripheral rather than a single "peripherals OK" count: a fleet
            # missing 12 headsets and a fleet missing 12 mice are different problems for the
            # person who has to go and fix them.
            "peripherals": [
                {
                    "peripheral": name,
                    "connected": sum(1 for r in readings if r[f"{name}_connected"]),
                    "disconnected": sum(1 for r in readings if not r[f"{name}_connected"]),
                }
                for name in PERIPHERALS
            ],
            "peripheral_alert_count": sum(1 for r in readings if r["missing_peripherals"]),
        }
        return {"source": "dynamodb", "fleet": fleet, "workstations": readings}

    return _cached("telemetry_fleet_health", load)


@router.get("/utilization/heatmap")
def utilization_heatmap(
    days: int = Query(default=7, ge=1, le=WINDOW_MAX_DAYS, description="Trailing days to include"),
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
    days: int = Query(default=30, ge=1, le=WINDOW_MAX_DAYS, description="Trailing days to include"),
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
                -- Sale-level measures come from the sale fact ALONE. Joining the line-item
                -- fact here and summing s.total_amount fans out: a purchase with three lines
                -- is counted three times. That is exactly what this query used to do, and it
                -- inflated concession revenue by 1.669x (finding F10) -- the ratio being, of
                -- course, the mean lines per purchase. Margin is the only measure that
                -- legitimately lives at line grain, so it gets its own CTE and is joined
                -- back by day rather than by row.
                concession_daily AS (
                    SELECT date_trunc('day', purchased_at_utc) AS day,
                           SUM(total_amount) AS concession_revenue,
                           COUNT(*)          AS concession_transactions
                    FROM {schema}.fact_concession_sale
                    WHERE purchased_at_utc >= %s
                    GROUP BY 1
                ),
                margin_daily AS (
                    SELECT date_trunc('day', s.purchased_at_utc) AS day,
                           SUM(li.line_margin) AS concession_margin
                    FROM {schema}.fact_concession_line_item li
                    JOIN {schema}.fact_concession_sale s ON s.purchase_id = li.purchase_id
                    WHERE s.purchased_at_utc >= %s
                    GROUP BY 1
                )
                SELECT COALESCE(r.day, c.day)                AS day,
                       COALESCE(r.rental_revenue, 0)         AS rental_revenue,
                       COALESCE(c.concession_revenue, 0)     AS concession_revenue,
                       COALESCE(r.rental_profit, 0)
                           + COALESCE(m.concession_margin, 0) AS gross_profit,
                       COALESCE(r.rental_transactions, 0)
                           + COALESCE(c.concession_transactions, 0) AS transactions
                FROM rental_daily r
                FULL OUTER JOIN concession_daily c ON c.day = r.day
                LEFT JOIN margin_daily m ON m.day = COALESCE(r.day, c.day)
                ORDER BY 1
                """,
                (start, start, start),
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
    days: int = Query(default=30, ge=1, le=WINDOW_MAX_DAYS, description="Trailing days to include"),
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


@router.get("/events/summary")
def events_summary(
    days: int = Query(default=30, ge=1, le=WINDOW_MAX_DAYS, description="Trailing days to include"),
) -> dict[str, Any]:
    """Workstation event mix and alert ranking from ``fact_workstation_event``.

    That table has been loaded into the warehouse since Gold first built it and, until this
    endpoint, was read by nothing at all -- the same shape of gap as the unread snapshots in
    F8/F9, one layer further along. It carries both origins: the bootstrap batch's events and
    the ``SESSION_START``/``SESSION_END`` the POS emits through DynamoDB.
    """

    def load() -> dict[str, Any]:
        from aimternet.db import redshift

        schema = settings().redshift_schema
        try:
            start, end = _redshift_window(
                schema, "fact_workstation_event", "event_timestamp_utc", days
            )
            daily = redshift.fetch_all(
                f"""
                SELECT date_trunc('day', event_timestamp_utc) AS day,
                       event_type,
                       COUNT(*) AS events
                FROM {schema}.fact_workstation_event
                WHERE event_timestamp_utc >= %s
                GROUP BY 1, 2 ORDER BY 1, 2
                """,
                (start,),
            )
            by_type = redshift.fetch_all(
                f"""
                SELECT event_type, COUNT(*) AS events,
                       COUNT(DISTINCT workstation_id) AS workstations
                FROM {schema}.fact_workstation_event
                WHERE event_timestamp_utc >= %s
                GROUP BY 1 ORDER BY 2 DESC
                """,
                (start,),
            )
            alerts = redshift.fetch_all(
                f"""
                -- SUM(CASE ...), not COUNT(*) FILTER: the aggregate FILTER clause is
                -- Postgres-only and Redshift rejects it outright. The RDS-backed endpoints
                -- above can and do use FILTER; nothing in this file that talks to Redshift
                -- may.
                SELECT workstation_id, zone_classification,
                       SUM(CASE WHEN event_type = 'HARDWARE_ALERT' THEN 1 ELSE 0 END)
                           AS hardware_alerts,
                       SUM(CASE WHEN event_type = 'PERIPHERAL_ALERT' THEN 1 ELSE 0 END)
                           AS peripheral_alerts,
                       COUNT(*) AS alerts
                FROM {schema}.fact_workstation_event
                WHERE event_timestamp_utc >= %s
                  AND event_type IN ('HARDWARE_ALERT', 'PERIPHERAL_ALERT')
                GROUP BY 1, 2 ORDER BY alerts DESC
                LIMIT 25
                """,
                (start,),
            )
            return {
                "source": "redshift", "days": days, "daily": daily, "by_type": by_type,
                "alerts": alerts, "window": _window_dict(start, end, days),
            }
        except Exception as exc:
            log.warning("event summary unavailable from Redshift: %s", exc)
            return {
                "source": "unavailable", "days": days, "daily": [], "by_type": [],
                "alerts": [], "error": str(exc)[:200],
            }

    return _cached(f"events_summary:{days}", load)


@router.get("/events/recent")
def events_recent(
    limit: int = Query(default=25, ge=1, le=200, description="Events per type to fetch"),
) -> dict[str, Any]:
    """The newest events per type, straight from DynamoDB.

    GSI2 is partitioned by event type and sorted by time, so this is one bounded reverse query
    per type -- the same access pattern ``export_dynamodb`` uses, and never a Scan: the events
    table is far too large to scan for a dashboard card.

    Live, so a check-in rung up on the POS seconds ago is already here, whereas
    ``/events/summary`` waits for the pipeline.
    """

    def load() -> dict[str, Any]:
        import boto3
        from boto3.dynamodb.conditions import Key

        cfg = settings()
        try:
            table = boto3.resource("dynamodb", region_name=cfg.aws_region).Table(
                cfg.ddb_events_table
            )
            events: list[dict[str, Any]] = []
            for event_type in sorted(rules().event_types):
                response = table.query(
                    IndexName="GSI2",
                    KeyConditionExpression=Key("GSI2PK").eq(f"TYPE#{event_type}"),
                    ScanIndexForward=False,
                    Limit=limit,
                )
                for item in response.get("Items", []):
                    events.append(
                        {
                            "event_id": item.get("event_id"),
                            "event_type": item.get("event_type"),
                            "workstation_id": item.get("workstation_id"),
                            "session_id": item.get("session_id"),
                            "member_id": item.get("member_id"),
                            "timestamp_utc": item.get("event_timestamp_utc"),
                            "client_os_version": item.get("client_os_version"),
                        }
                    )
            events.sort(key=lambda e: e["timestamp_utc"] or "", reverse=True)
            return {"source": "dynamodb", "count": len(events), "events": events}
        except Exception as exc:
            log.warning("recent events unavailable from DynamoDB: %s", exc)
            return {
                "source": "unavailable", "count": 0, "events": [], "error": str(exc)[:200],
            }

    return _cached(f"events_recent:{limit}", load)


@router.get("/members/leaderboard")
def members_leaderboard(
    days: int = Query(default=30, ge=1, le=WINDOW_MAX_DAYS, description="Trailing days to include"),
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
        default=62, ge=1, le=WINDOW_MAX_DAYS,
        description="Accepted for interface symmetry with other cards; see docstring",
    ),
) -> dict[str, Any]:
    """Current tier distribution plus every SCD2 transition ever recorded in ``dim_member``.

    ``days`` is not applied as a filter here: transitions come from
    ``LAG(tier) OVER (PARTITION BY member_id ORDER BY valid_from_utc)``, and cutting the
    input rows by date would break that chain (a member's 3rd-recorded tier needs their 1st
    and 2nd to compute a transition, even if only the 3rd falls inside the window).

    That reasoning used to rest on a second one -- the history was a single bootstrap batch,
    so windowing it could not have changed the answer. It is not any more: dim_member derives
    its tier history from both rental origins, so a member the POS promotes opens a genuinely
    new version. The chain argument above is what keeps `days` unapplied; the batch argument
    is gone, and the parameter is now honestly vestigial rather than merely redundant.
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
    days: int = Query(default=7, ge=1, le=WINDOW_MAX_DAYS, description="Trailing days to include"),
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
    days: int = Query(default=30, ge=1, le=WINDOW_MAX_DAYS, description="Trailing days to include"),
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


# --------------------------------------------------------------------------------------
# Declared last on purpose. FastAPI matches routes in declaration order, so a path
# parameter this shape (`/members/{member_id}/summary`) placed above `/members/overview`
# or `/members/leaderboard` would swallow neither -- those have a second static segment --
# but `/members/{member_id}` shapes are exactly how that class of bug starts. Keeping every
# static `/members/*` route above this one removes the question.
# --------------------------------------------------------------------------------------


@router.get("/members/{member_id}/summary")
def member_summary(member_id: str) -> dict[str, Any]:
    """Everything one member has done, read live from RDS.

    RDS rather than the warehouse deliberately: this card is the answer to "the customer in
    front of me says they bought something, where is it", and the warehouse is a pipeline
    pass behind. It is the same reason ``/revenue/today`` reads RDS.
    """
    profile = fetch_all(
        """SELECT member_id, first_name, last_name, current_tier, current_points_balance,
                  lifetime_spend_amount, registered_at_utc, is_backfilled, is_active
           FROM members WHERE member_id = %s""",
        (member_id,),
    )
    if not profile:
        return {"source": "rds", "found": False, "member_id": member_id}

    totals = fetch_all(
        """SELECT
             (SELECT count(*) FROM rental_transactions
               WHERE member_id = %s AND session_end_utc IS NOT NULL)      AS rentals,
             (SELECT COALESCE(sum(net_amount_paid), 0) FROM rental_transactions
               WHERE member_id = %s AND session_end_utc IS NOT NULL)      AS rental_spend,
             (SELECT COALESCE(sum(duration_hours), 0) FROM rental_transactions
               WHERE member_id = %s AND session_end_utc IS NOT NULL)      AS hours,
             (SELECT count(*) FROM concession_purchases WHERE member_id = %s)
                                                                          AS purchases,
             (SELECT COALESCE(sum(total_amount), 0) FROM concession_purchases
               WHERE member_id = %s)                                      AS concession_spend""",
        (member_id,) * 5,
    )[0]

    rentals = fetch_all(
        """SELECT r.rental_id, r.workstation_id, w.zone_classification, r.session_start_utc,
                  r.session_end_utc, r.duration_hours, r.final_hourly_rate,
                  r.net_amount_paid, r.points_accrued, r.points_redeemed, r.payment_method
           FROM rental_transactions r
           LEFT JOIN workstations w USING (workstation_id)
           WHERE r.member_id = %s
           ORDER BY r.session_start_utc DESC LIMIT 25""",
        (member_id,),
    )
    purchases = fetch_all(
        """SELECT p.purchase_id, p.rental_id, p.purchased_at_utc, p.total_amount,
                  p.points_accrued, p.payment_method, count(i.order_item_id) AS lines
           FROM concession_purchases p
           LEFT JOIN concession_order_items i USING (purchase_id)
           WHERE p.member_id = %s
           GROUP BY 1, 2, 3, 4, 5, 6
           ORDER BY p.purchased_at_utc DESC LIMIT 25""",
        (member_id,),
    )
    ledger = fetch_all(
        """SELECT ledger_id, transaction_type, source_reference_id, points_delta,
                  resulting_balance, created_at_utc
           FROM member_points_ledger WHERE member_id = %s
           ORDER BY created_at_utc DESC LIMIT 25""",
        (member_id,),
    )
    biz = rules()
    balance = int(profile[0]["current_points_balance"] or 0)
    return {
        "source": "rds",
        "found": True,
        "member": dict(profile[0]),
        "totals": {
            **dict(totals),
            "total_spend": str(
                Decimal(totals["rental_spend"] or 0) + Decimal(totals["concession_spend"] or 0)
            ),
            # Reuses the shared redemption rule rather than re-hardcoding 100-points-per-PHP-50
            # here, the same way /points/history does.
            "redeemable_units": balance // biz.redemption_unit_points,
            "redeemable_value": str(
                (balance // biz.redemption_unit_points) * biz.redemption_unit_value
            ),
        },
        "rentals": rentals,
        "purchases": purchases,
        "points_ledger": ledger,
        "generated_at_utc": datetime.now(UTC).isoformat(),
    }
