"""Silver — cleaned, typed, deduplicated, UTC-normalised (spec §6.4).

One dataset per source entity, partitioned by date where a date exists. Everything money is
``DECIMAL(12,2)``; everything temporal is ``TIMESTAMPTZ`` in UTC with the original ``+08:00``
kept alongside as ``source_tz_offset``, so the offset survives as lineage rather than being
thrown away at the door (spec §4).

Deduplication is deterministic: ``QUALIFY row_number() OVER (PARTITION BY <pk> ORDER BY
source_file)``. Two identical primary keys resolve the same way on every run, which is what
makes a rerun reproducible rather than merely successful.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from aimternet.config.settings import settings
from aimternet.pipeline.curate.engine import (
    bronze_uri,
    count_parquet,
    duck,
    layer_uri,
    write_parquet,
)


def telemetry_dates(limit: int | None = None) -> list[str]:
    """Telemetry partition dates, from the S3 prefix listing rather than the file contents."""
    from aimternet.io.s3 import S3Client

    cfg = settings()
    prefix = f"{cfg.s3_bronze_prefix}/telemetry/"
    keys = S3Client().list_objects(prefix)
    dates = sorted({k.split("date=", 1)[1].split("/", 1)[0] for k in keys if "date=" in k})
    return dates[:limit] if limit else dates

log = logging.getLogger(__name__)

# CSVs are read all_varchar and cast explicitly. Letting a CSV sniffer choose types for money
# is how a DECIMAL silently becomes a DOUBLE.
_CSV = "header=true, all_varchar=true, filename=true"


@dataclass
class CurateReport:
    layer: str
    rows: dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0

    def summary(self) -> str:
        lines = [f"{self.layer}:", f"  {'dataset':30s} {'rows':>12s}", f"  {'-' * 30} {'-' * 12}"]
        for dataset in sorted(self.rows):
            lines.append(f"  {dataset:30s} {self.rows[dataset]:12,d}")
        lines.append(f"  {'-' * 30} {'-' * 12}")
        lines.append(f"  {'TOTAL':30s} {sum(self.rows.values()):12,d}")
        lines.append(f"  elapsed: {self.duration_seconds:.1f}s")
        return "\n".join(lines)


def _select(dataset: str, columns: str, *, pk: str, source: str | None = None) -> str:
    """Standard Silver shape: explicit casts, lineage, deterministic dedup."""
    return f"""
        SELECT {columns},
               regexp_replace(filename, '^.*/', '') AS source_file,
               CAST(? AS VARCHAR)                   AS run_id,
               now()                                AS ingested_at_utc
        FROM read_csv('{source or bronze_uri(dataset)}', {_CSV})
        QUALIFY row_number() OVER (PARTITION BY {pk} ORDER BY filename) = 1
    """


def build(run_id: str, *, telemetry_days: int | None = None) -> CurateReport:
    """Build every Silver dataset from Bronze."""
    import time

    started = time.time()
    report = CurateReport(layer="silver")

    with duck() as con:
        # ---------------------------------------------------------------- catalog
        write_parquet(
            con,
            _select(
                "workstations",
                """
                workstation_id,
                zone_classification,
                CAST(base_hourly_rate AS DECIMAL(12,2)) AS base_hourly_rate,
                ip_address, mac_address,
                CAST(commissioned_date AS DATE)         AS commissioned_date
                """,
                pk="workstation_id",
                source=bronze_uri("workstations", "*.csv"),
            ),
            layer_uri("silver", "workstations"),
            params=[run_id],
        )

        write_parquet(
            con,
            _select(
                "concession_items",
                """
                item_sku, item_name, category,
                CAST(unit_cost_price   AS DECIMAL(12,2)) AS unit_cost_price,
                CAST(unit_retail_price AS DECIMAL(12,2)) AS unit_retail_price,
                CAST(stock_quantity    AS INTEGER)       AS stock_quantity
                """,
                pk="item_sku",
                source=bronze_uri("concession_items", "*.csv"),
            ),
            layer_uri("silver", "concession_items"),
            params=[run_id],
        )

        # ---------------------------------------------------------------- dimensions
        write_parquet(
            con,
            _select(
                "dim_date",
                """
                CAST(date_id AS INTEGER)        AS date_id,
                CAST(calendar_date AS DATE)     AS calendar_date,
                day_of_week,
                CAST(day_of_month AS SMALLINT)  AS day_of_month,
                month_label,
                CAST(month_index AS SMALLINT)   AS month_index,
                CAST(quarter_index AS SMALLINT) AS quarter_index,
                CAST(year_index AS SMALLINT)    AS year_index,
                CAST(weekend_flag AS BOOLEAN)   AS weekend_flag,
                CAST(holiday_ph_flag AS BOOLEAN) AS holiday_ph_flag
                """,
                pk="date_id",
                source=bronze_uri("dim_date", "*.csv"),
            ),
            layer_uri("silver", "dim_date"),
            params=[run_id],
        )

        write_parquet(
            con,
            _select(
                "dim_time",
                """
                CAST(time_id AS INTEGER)      AS time_id,
                CAST(hour_24 AS SMALLINT)     AS hour_24,
                CAST(minute_val AS SMALLINT)  AS minute_val,
                day_part_label
                """,
                pk="time_id",
                source=bronze_uri("dim_time", "*.csv"),
            ),
            layer_uri("silver", "dim_time"),
            params=[run_id],
        )

        # ---------------------------------------------------------------- members
        write_parquet(
            con,
            _select(
                "members",
                """
                member_id, first_name, last_name, email, phone_number, current_tier,
                CAST(current_points_balance AS INTEGER)   AS current_points_balance,
                CAST(lifetime_spend_amount AS DECIMAL(12,2)) AS lifetime_spend_amount,
                CAST(registered_at AS TIMESTAMPTZ)        AS registered_at_utc,
                '+08:00'                                  AS source_tz_offset,
                'LEGACY_BATCH'                            AS source_system,
                false                                     AS is_backfilled
                """,
                pk="member_id",
            ),
            layer_uri("silver", "members"),
            params=[run_id],
        )

        # ---------------------------------------------------------------- rentals
        write_parquet(
            con,
            f"""
            SELECT rental_id, member_id, workstation_id,
                   CAST(session_start AS TIMESTAMPTZ)         AS session_start_utc,
                   CAST(session_end   AS TIMESTAMPTZ)         AS session_end_utc,
                   CAST(duration_hours AS DECIMAL(6,2))       AS duration_hours,
                   CAST(base_hourly_rate AS DECIMAL(12,2))    AS base_hourly_rate,
                   member_tier_applied,
                   CAST(tier_discount_pct AS DECIMAL(5,4))    AS tier_discount_pct,
                   CAST(final_hourly_rate AS DECIMAL(12,2))   AS final_hourly_rate,
                   CAST(gross_rental_amount AS DECIMAL(12,2)) AS gross_rental_amount,
                   CAST(points_redeemed AS INTEGER)           AS points_redeemed,
                   CAST(points_credit_value AS DECIMAL(12,2)) AS points_credit_value,
                   CAST(net_amount_paid AS DECIMAL(12,2))     AS net_amount_paid,
                   CAST(points_accrued AS INTEGER)            AS points_accrued,
                   payment_method,
                   '+08:00'                                   AS source_tz_offset,
                   regexp_replace(filename, '^.*/', '')       AS source_file,
                   CAST(? AS VARCHAR)                         AS run_id,
                   now()                                      AS ingested_at_utc,
                   CAST(session_start AS DATE)                AS rental_date
            FROM read_csv('{bronze_uri("rental_transactions")}', {_CSV})
            QUALIFY row_number() OVER (PARTITION BY rental_id ORDER BY filename) = 1
            """,
            layer_uri("silver", "rental_transactions"),
            partition_by=("rental_date",),
            params=[run_id],
        )

        # ---------------------------------------------------------------- concessions
        write_parquet(
            con,
            f"""
            SELECT purchase_id, member_id,
                   -- 5,293 walk-in purchases carry '' rather than a rental id.
                   nullif(rental_id, '')                    AS rental_id,
                   CAST(total_amount AS DECIMAL(12,2))      AS total_amount,
                   CAST(points_accrued AS INTEGER)          AS points_accrued,
                   payment_method,
                   CAST(purchased_at AS TIMESTAMPTZ)        AS purchased_at_utc,
                   '+08:00'                                 AS source_tz_offset,
                   regexp_replace(filename, '^.*/', '')     AS source_file,
                   CAST(? AS VARCHAR)                       AS run_id,
                   now()                                    AS ingested_at_utc,
                   CAST(purchased_at AS DATE)               AS purchase_date
            FROM read_csv('{bronze_uri("concession_purchases")}', {_CSV})
            QUALIFY row_number() OVER (PARTITION BY purchase_id ORDER BY filename) = 1
            """,
            layer_uri("silver", "concession_purchases"),
            partition_by=("purchase_date",),
            params=[run_id],
        )

        write_parquet(
            con,
            _select(
                "concession_order_items",
                """
                order_item_id, purchase_id, item_sku,
                CAST(quantity AS INTEGER)          AS quantity,
                CAST(unit_price AS DECIMAL(12,2))  AS unit_price,
                CAST(total_price AS DECIMAL(12,2)) AS total_price
                """,
                pk="order_item_id",
            ),
            layer_uri("silver", "concession_order_items"),
            params=[run_id],
        )

        # ---------------------------------------------------------------- points
        write_parquet(
            con,
            f"""
            SELECT ledger_id, member_id, source_reference_id, transaction_type,
                   CAST(points_delta AS INTEGER)      AS points_delta,
                   -- Carried through, but never used to compute anything: it does not
                   -- reconstruct as a running sum (finding F5). points_delta is the truth.
                   CAST(resulting_balance AS INTEGER) AS resulting_balance_source,
                   CAST(created_at AS TIMESTAMPTZ)    AS created_at_utc,
                   '+08:00'                           AS source_tz_offset,
                   regexp_replace(filename, '^.*/', '') AS source_file,
                   CAST(? AS VARCHAR)                 AS run_id,
                   now()                              AS ingested_at_utc,
                   CAST(created_at AS DATE)           AS ledger_date
            FROM read_csv('{bronze_uri("member_points_ledger")}', {_CSV})
            QUALIFY row_number() OVER (PARTITION BY ledger_id ORDER BY filename) = 1
            """,
            layer_uri("silver", "member_points_ledger"),
            partition_by=("ledger_date",),
            params=[run_id],
        )

        # ---------------------------------------------------------------- events
        write_parquet(
            con,
            f"""
            SELECT event_id, workstation_id,
                   CAST(event_timestamp AS TIMESTAMPTZ)          AS event_timestamp_utc,
                   event_type,
                   nullif(session_id, '')                        AS session_id,
                   nullif(member_id, '')                         AS member_id,
                   CAST(duration_allocated_hours AS DECIMAL(6,2)) AS duration_allocated_hours,
                   client_os_version, notes,
                   '+08:00'                                      AS source_tz_offset,
                   regexp_replace(filename, '^.*/', '')          AS source_file,
                   CAST(? AS VARCHAR)                            AS run_id,
                   now()                                         AS ingested_at_utc,
                   CAST(event_timestamp AS DATE)                 AS event_date
            FROM read_json('{bronze_uri("workstation_events", "*/*.json")}', filename=true)
            QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY filename) = 1
            """,
            layer_uri("silver", "workstation_events"),
            partition_by=("event_date",),
            params=[run_id],
        )

        # ---------------------------------------------------------------- telemetry
        # 6.3M rows, written one day at a time. Days come from the S3 prefix listing rather
        # than from reading the files: scanning 4.1 GB of JSON to learn 62 date strings would
        # be an expensive way to ask a cheap question. Per-day COPYs also mean an interrupted
        # run loses only the day in flight.
        for day in telemetry_dates(limit=telemetry_days):
            write_parquet(
                con,
                f"""
                SELECT workstation_id,
                       CAST(timestamp AS TIMESTAMPTZ)                        AS timestamp_utc,
                       zone, status,
                       nullif(active_session_id, '')                         AS active_session_id,
                       nullif(active_member_id, '')                          AS active_member_id,
                       CAST(hardware_metrics.cpu_load_pct    AS DECIMAL(5,1)) AS cpu_load_pct,
                       CAST(hardware_metrics.cpu_temp_c      AS SMALLINT)     AS cpu_temp_c,
                       CAST(hardware_metrics.ram_usage_pct   AS DECIMAL(5,1)) AS ram_usage_pct,
                       CAST(hardware_metrics.gpu_load_pct    AS DECIMAL(5,1)) AS gpu_load_pct,
                       CAST(hardware_metrics.gpu_temp_c      AS SMALLINT)     AS gpu_temp_c,
                       CAST(hardware_metrics.disk_io_read_mbs AS DECIMAL(8,1))
                                                                              AS disk_io_read_mbs,
                       CAST(hardware_metrics.disk_io_write_mbs AS DECIMAL(8,1))
                                                                              AS disk_io_write_mbs,
                       CAST(network_diagnostics.latency_ping_ms AS SMALLINT)
                                                                              AS latency_ping_ms,
                       CAST(network_diagnostics.packet_loss_pct AS DECIMAL(5,2))
                                                                              AS packet_loss_pct,
                       CAST(network_diagnostics.bandwidth_down_mbps AS DECIMAL(8,1))
                                                            AS bandwidth_down_mbps,
                       peripherals_connected.keyboard                         AS keyboard_connected,
                       peripherals_connected.mouse                            AS mouse_connected,
                       peripherals_connected.headset                          AS headset_connected,
                       CAST(expires_at AS BIGINT)                             AS expires_at,
                       '+08:00'                                               AS source_tz_offset,
                       CAST(? AS VARCHAR)                                     AS run_id,
                       now()                                                  AS ingested_at_utc,
                       DATE '{day}'                                           AS telemetry_date,
                       CAST(strftime(CAST(timestamp AS TIMESTAMPTZ), '%H') AS SMALLINT) AS hour_utc
                FROM read_json('{bronze_uri("telemetry", f"date={day}/*/*.json")}',
                               maximum_object_size=40000000)
                """,
                layer_uri("silver", "telemetry"),
                partition_by=("telemetry_date", "hour_utc"),
                params=[run_id],
            )
            log.info("silver telemetry: %s written", day)

        report.rows = {
            name: count_parquet(con, layer_uri("silver", name))
            for name in (
                "workstations", "concession_items", "dim_date", "dim_time", "members",
                "rental_transactions", "concession_purchases", "concession_order_items",
                "member_points_ledger", "workstation_events", "telemetry",
            )
        }

    report.duration_seconds = time.time() - started
    return report
