"""Gold — the analytics-ready dimensional model (spec §6.4, §6.5).

Conformed dimensions and facts, ready for Redshift to consume without further reshaping.
Two decisions worth stating:

**Raw telemetry never reaches Gold as rows.** 6.3M five-second-to-five-minute readings are
aggregated to ``agg_workstation_utilization_hourly`` -- 175 workstations x 24 hours x 62 days
= 260,400 rows. Loading the raw grain into a warehouse would cost a great deal and answer no
question the hourly grain cannot (§6.5).

**Facts include what the POS did, not only what the source files held.** API-emitted
workstation events reach Gold through ``workstation_events_operational`` -- the DynamoDB
export -- unioned onto the Bronze-derived events. Reading only Bronze would mean the warehouse
never saw a single session the cafe actually ran since the bootstrap.

**dim_member is SCD2 on tier.** The tier a member held is recorded on every rental as
``member_tier_applied``, so their tier history can be reconstructed exactly rather than
guessed: consecutive rentals at the same tier collapse into one version, and a change opens a
new one. Members are sourced from ``members_operational`` -- the RDS export -- so the 840
backfilled D2 members arrive already resolved, with the policy applied exactly once.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from aimternet.pipeline.curate.engine import count_parquet, duck, layer_uri, write_parquet

log = logging.getLogger(__name__)


@dataclass
class GoldReport:
    rows: dict[str, int] = field(default_factory=dict)
    #: Rows contributed by the operational snapshots rather than by Bronze. Reported
    #: separately so a caller can derive what a fact table *should* hold: the counts that
    #: matter are no longer constants, and a check that assumes they are cannot see growth.
    operational_rows: dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0

    def summary(self) -> str:
        lines = ["gold:", f"  {'dataset':38s} {'rows':>12s}", f"  {'-' * 38} {'-' * 12}"]
        for name in sorted(self.rows):
            lines.append(f"  {name:38s} {self.rows[name]:12,d}")
        lines.append(f"  {'-' * 38} {'-' * 12}")
        lines.append(f"  {'TOTAL':38s} {sum(self.rows.values()):12,d}")
        lines.append(f"  elapsed: {self.duration_seconds:.1f}s")
        return "\n".join(lines)


def _silver(dataset: str) -> str:
    return f"read_parquet('{layer_uri('silver', dataset)}/**/*.parquet')"


#: The shape Gold needs from the operational events snapshot. The DynamoDB export writes
#: whatever attributes the items carried, and events carry optional ones -- an hour in which
#: nobody started a session produces a snapshot with no ``session_id`` column at all. A
#: zero-row template unioned BY NAME fills those gaps with NULL instead of failing the build.
_OPERATIONAL_EVENT_COLUMNS = (
    "event_id",
    "workstation_id",
    "event_type",
    "event_timestamp_utc",
    "session_id",
    "member_id",
    "duration_allocated_hours",
    "client_os_version",
)

#: Columns the two event sources are unioned on.
_EVENT_COLUMNS = ", ".join([*_OPERATIONAL_EVENT_COLUMNS, "source_file"])


def _operational_events(con: object) -> str | None:
    """The API-emitted events, projected into the same shape as the Bronze-derived ones.

    ``None`` when the export has never run, so a fresh bucket builds Bronze-only rather than
    failing. Everything in the snapshot is VARCHAR -- DynamoDB attributes are cast to string
    on the way out -- so the types are rebuilt here.
    """
    from aimternet.pipeline.curate.engine import count_parquet

    uri = layer_uri("silver", "workstation_events_operational")
    if not count_parquet(con, uri):  # type: ignore[arg-type]
        return None

    template = ", ".join(f"NULL::VARCHAR AS {column}" for column in _OPERATIONAL_EVENT_COLUMNS)
    return f"""
        SELECT event_id, workstation_id, event_type,
               CAST(event_timestamp_utc AS TIMESTAMPTZ)       AS event_timestamp_utc,
               nullif(session_id, '')                         AS session_id,
               nullif(member_id, '')                          AS member_id,
               CAST(duration_allocated_hours AS DECIMAL(6,2)) AS duration_allocated_hours,
               client_os_version,
               -- Lineage still says where the row came from: these never touched a file.
               'dynamodb:api'                                 AS source_file
        FROM (
            SELECT * FROM read_parquet('{uri}/**/*.parquet')
            UNION ALL BY NAME
            SELECT {template} WHERE false
        )
    """


def event_source(con: object) -> tuple[str, int]:
    """Every workstation event Gold should see, and how many came from the POS.

    Two origins: the Bronze events in Silver, and the API-emitted ones the DynamoDB export
    leaves in ``workstation_events_operational``. Reading only the first meant the warehouse
    never saw a session the cafe actually ran after the bootstrap.

    Deduplicated on ``event_id``. The two id spaces do not overlap today -- source ids versus
    ``EVT-API-*`` -- but relying on that rather than asserting it is how a fact table quietly
    doubles. Extracted from ``build`` so it can be exercised without S3.
    """
    operational = _operational_events(con)
    count = 0
    if operational:
        counted = con.execute(f"SELECT count(*) FROM ({operational})").fetchone()  # type: ignore[attr-defined]
        count = int(counted[0]) if counted else 0

    sql = f"""
        SELECT {_EVENT_COLUMNS}
        FROM (
            SELECT {_EVENT_COLUMNS} FROM {_silver('workstation_events')}
            {f"UNION ALL BY NAME {operational}" if operational else ""}
        )
        QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY source_file) = 1
    """
    return sql, count


def build(run_id: str) -> GoldReport:
    started = time.time()
    report = GoldReport()

    with duck() as con:
        # ---------------------------------------------------------------- dimensions

        write_parquet(
            con,
            f"""
            SELECT workstation_id                         AS workstation_key,
                   workstation_id,
                   zone_classification,
                   base_hourly_rate,
                   ip_address, mac_address, commissioned_date,
                   CAST(? AS VARCHAR) AS run_id, now() AS built_at_utc
            FROM {_silver('workstations')}
            """,
            layer_uri("gold", "dim_workstation"),
            params=[run_id],
        )

        write_parquet(
            con,
            f"""
            SELECT item_sku AS item_key, item_sku, item_name, category,
                   unit_cost_price, unit_retail_price,
                   unit_retail_price - unit_cost_price AS unit_margin,
                   CAST(? AS VARCHAR) AS run_id, now() AS built_at_utc
            FROM {_silver('concession_items')}
            """,
            layer_uri("gold", "dim_concession_item"),
            params=[run_id],
        )

        write_parquet(
            con,
            f"SELECT *, CAST(? AS VARCHAR) AS run_id FROM {_silver('dim_date')}",
            layer_uri("gold", "dim_date"),
            params=[run_id],
        )
        write_parquet(
            con,
            f"SELECT *, CAST(? AS VARCHAR) AS run_id FROM {_silver('dim_time')}",
            layer_uri("gold", "dim_time"),
            params=[run_id],
        )

        # dim_member, SCD2 on tier.
        #
        # Tier history comes from the rentals themselves: member_tier_applied is what the cafe
        # actually charged at that moment. Consecutive rentals at the same tier collapse into
        # one version; a change closes the old version and opens a new one. A member with no
        # rentals still gets exactly one row, valid from their registration.
        write_parquet(
            con,
            f"""
            WITH members AS (
                SELECT member_id, first_name, last_name, email, phone_number,
                       current_tier, current_points_balance, lifetime_spend_amount,
                       registered_at_utc, source_system, is_backfilled
                FROM {_silver('members_operational')}
            ),
            tier_events AS (
                SELECT member_id, member_tier_applied AS tier,
                       session_start_utc AS observed_at, rental_id
                FROM {_silver('rental_transactions')}
            ),
            with_previous AS (
                -- rental_id breaks ties. Ordering on observed_at alone is non-deterministic
                -- when a member has two rentals starting in the same second, so the version
                -- count could differ between two builds of identical data -- which, with a
                -- delete-then-insert merge downstream, is not a cosmetic difference.
                SELECT member_id, tier, observed_at, rental_id,
                       lag(tier) OVER (
                           PARTITION BY member_id ORDER BY observed_at, rental_id
                       ) AS prev_tier
                FROM tier_events
            ),
            changes AS (              -- keep only the rows where the tier actually changed
                SELECT member_id, tier, observed_at, rental_id
                FROM with_previous
                WHERE prev_tier IS DISTINCT FROM tier
            ),
            versioned AS (
                SELECT c.member_id, c.tier, c.observed_at AS valid_from_utc,
                       lead(c.observed_at) OVER (
                           PARTITION BY c.member_id ORDER BY c.observed_at, c.rental_id
                       ) AS valid_to_utc
                FROM changes c
            ),
            scd AS (
                SELECT m.member_id, m.first_name, m.last_name, m.email, m.phone_number,
                       v.tier                       AS tier,
                       v.valid_from_utc,
                       v.valid_to_utc,
                       v.valid_to_utc IS NULL       AS is_current,
                       m.current_points_balance, m.lifetime_spend_amount,
                       m.registered_at_utc, m.source_system, m.is_backfilled
                FROM members m
                JOIN versioned v USING (member_id)

                UNION ALL

                -- Members who never rented: one version, from registration, still current.
                SELECT m.member_id, m.first_name, m.last_name, m.email, m.phone_number,
                       m.current_tier, m.registered_at_utc, NULL, true,
                       m.current_points_balance, m.lifetime_spend_amount,
                       m.registered_at_utc, m.source_system, m.is_backfilled
                FROM members m
                WHERE m.member_id NOT IN (SELECT member_id FROM versioned)
            )
            SELECT row_number() OVER (ORDER BY member_id, valid_from_utc) AS member_key,
                   *, CAST(? AS VARCHAR) AS run_id, now() AS built_at_utc
            FROM scd
            """,
            layer_uri("gold", "dim_member"),
            params=[run_id],
        )

        # ---------------------------------------------------------------- facts

        write_parquet(
            con,
            f"""
            SELECT r.rental_id, r.member_id, r.workstation_id,
                   CAST(strftime(r.session_start_utc, '%Y%m%d') AS INTEGER) AS date_key,
                   CAST(strftime(r.session_start_utc, '%H%M')   AS INTEGER) AS time_key,
                   r.session_start_utc, r.session_end_utc, r.duration_hours,
                   r.base_hourly_rate, r.member_tier_applied, r.tier_discount_pct,
                   r.final_hourly_rate, r.gross_rental_amount, r.points_redeemed,
                   r.points_credit_value, r.net_amount_paid, r.points_accrued,
                   r.payment_method,
                   w.zone_classification,
                   r.source_file, CAST(? AS VARCHAR) AS run_id, now() AS built_at_utc,
                   CAST(r.session_start_utc AS DATE) AS rental_date
            FROM {_silver('rental_transactions')} r
            LEFT JOIN {_silver('workstations')} w USING (workstation_id)
            """,
            layer_uri("gold", "fact_rental"),
            partition_by=("rental_date",),
            params=[run_id],
        )

        write_parquet(
            con,
            f"""
            SELECT p.purchase_id, p.member_id, p.rental_id,
                   CAST(strftime(p.purchased_at_utc, '%Y%m%d') AS INTEGER) AS date_key,
                   CAST(strftime(p.purchased_at_utc, '%H%M')   AS INTEGER) AS time_key,
                   p.purchased_at_utc, p.total_amount, p.points_accrued, p.payment_method,
                   p.rental_id IS NULL AS is_walk_in,
                   p.source_file, CAST(? AS VARCHAR) AS run_id, now() AS built_at_utc,
                   CAST(p.purchased_at_utc AS DATE) AS purchase_date
            FROM {_silver('concession_purchases')} p
            """,
            layer_uri("gold", "fact_concession_sale"),
            partition_by=("purchase_date",),
            params=[run_id],
        )

        write_parquet(
            con,
            f"""
            SELECT i.order_item_id, i.purchase_id, i.item_sku, i.quantity,
                   i.unit_price, i.total_price,
                   i.total_price - (i.quantity * c.unit_cost_price) AS line_margin,
                   p.member_id,
                   CAST(strftime(p.purchased_at_utc, '%Y%m%d') AS INTEGER) AS date_key,
                   CAST(? AS VARCHAR) AS run_id, now() AS built_at_utc
            FROM {_silver('concession_order_items')} i
            LEFT JOIN {_silver('concession_purchases')} p USING (purchase_id)
            LEFT JOIN {_silver('concession_items')} c USING (item_sku)
            """,
            layer_uri("gold", "fact_concession_line_item"),
            params=[run_id],
        )

        write_parquet(
            con,
            f"""
            SELECT l.ledger_id, l.member_id, l.source_reference_id, l.transaction_type,
                   l.points_delta,
                   -- Derived by summation, not read from the source column: resulting_balance
                   -- does not reconstruct in timestamp order (finding F5).
                   CAST(sum(l.points_delta) OVER (
                       PARTITION BY l.member_id ORDER BY l.created_at_utc, l.ledger_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS BIGINT) AS running_balance,
                   l.resulting_balance_source,
                   l.created_at_utc,
                   CAST(strftime(l.created_at_utc, '%Y%m%d') AS INTEGER) AS date_key,
                   l.source_file, CAST(? AS VARCHAR) AS run_id, now() AS built_at_utc,
                   CAST(l.created_at_utc AS DATE) AS ledger_date
            FROM {_silver('member_points_ledger')} l
            """,
            layer_uri("gold", "fact_points_activity"),
            partition_by=("ledger_date",),
            params=[run_id],
        )

        events_source, operational_count = event_source(con)
        report.operational_rows["workstation_events_operational"] = operational_count

        write_parquet(
            con,
            f"""
            SELECT e.event_id, e.workstation_id, e.event_type, e.session_id, e.member_id,
                   e.event_timestamp_utc, e.duration_allocated_hours, e.client_os_version,
                   CAST(strftime(e.event_timestamp_utc, '%Y%m%d') AS INTEGER) AS date_key,
                   w.zone_classification,
                   e.source_file, CAST(? AS VARCHAR) AS run_id, now() AS built_at_utc,
                   CAST(e.event_timestamp_utc AS DATE) AS event_date
            FROM ({events_source}) e
            LEFT JOIN {_silver('workstations')} w USING (workstation_id)
            """,
            layer_uri("gold", "fact_workstation_event"),
            partition_by=("event_date",),
            params=[run_id],
        )

        # ---------------------------------------------------------------- aggregate
        #
        # The whole reason Redshift never sees raw telemetry. Utilisation is the share of
        # readings in the hour where the workstation was OCCUPIED -- a ratio, so it stays
        # correct even though the sampling interval changes partway through the window
        # (finding F6): 12 readings/hour for most days, 120/hour for the last seven.
        write_parquet(
            con,
            f"""
            SELECT t.workstation_id,
                   t.zone,
                   CAST(t.telemetry_date AS DATE)                     AS utilization_date,
                   t.hour_utc                                         AS hour_utc,
                   CAST(strftime(t.telemetry_date, '%Y%m%d') AS INTEGER) AS date_key,
                   count(*)                                           AS readings,
                   count(*) FILTER (WHERE t.status = 'OCCUPIED')      AS readings_occupied,
                   CAST(count(*) FILTER (WHERE t.status = 'OCCUPIED') * 100.0 / count(*)
                        AS DECIMAL(5,2))                              AS utilization_pct,
                   count(DISTINCT t.active_session_id)                AS distinct_sessions,
                   CAST(avg(t.cpu_load_pct)    AS DECIMAL(5,1))       AS avg_cpu_load_pct,
                   CAST(max(t.cpu_temp_c)      AS SMALLINT)           AS max_cpu_temp_c,
                   CAST(avg(t.gpu_load_pct)    AS DECIMAL(5,1))       AS avg_gpu_load_pct,
                   CAST(max(t.gpu_temp_c)      AS SMALLINT)           AS max_gpu_temp_c,
                   CAST(avg(t.latency_ping_ms) AS DECIMAL(6,1))       AS avg_latency_ping_ms,
                   CAST(max(t.packet_loss_pct) AS DECIMAL(5,2))       AS max_packet_loss_pct,
                   CAST(? AS VARCHAR) AS run_id, now() AS built_at_utc
            FROM {_silver('telemetry')} t
            GROUP BY 1, 2, 3, 4, 5
            """,
            layer_uri("gold", "agg_workstation_utilization_hourly"),
            params=[run_id],
        )

        report.rows = {
            name: count_parquet(con, layer_uri("gold", name))
            for name in (
                "dim_member", "dim_workstation", "dim_date", "dim_time",
                "dim_concession_item", "fact_rental", "fact_concession_sale",
                "fact_concession_line_item", "fact_points_activity",
                "fact_workstation_event", "agg_workstation_utilization_hourly",
            )
        }

    report.duration_seconds = time.time() - started
    return report
