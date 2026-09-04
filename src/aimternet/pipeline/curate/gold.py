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


#: The facts whose rows can come from two places: the Bronze-derived Silver dataset, and the
#: RDS export's snapshot of the operational store. Each entry is
#: ``(bronze dataset, snapshot, primary key, projected columns, extra predicate)``.
#:
#: The projection is the whole job. The snapshot is a copy of an RDS table, so it carries the
#: table's columns and the table's types -- not Silver's. Two ways that bites, both observed in
#: the live snapshot rather than imagined: NUMERIC(5,2) comes back as DECIMAL(5,2) where Silver
#: holds DECIMAL(12,2), and an open rental's NULL ``points_accrued`` makes pandas widen the
#: whole column to float64, so an INTEGER arrives as DOUBLE. An implicit UNION would resolve
#: both by promoting Gold's column to whatever the snapshot happened to infer that hour, which
#: is how a warehouse column silently changes type between two builds of the same data. Every
#: column is therefore cast back to the Silver type explicitly.
_OPERATIONAL_FACTS: dict[str, tuple[str, str, str, str, str]] = {
    "rental_transactions": (
        "rental_transactions",
        "rental_transactions_operational",
        "rental_id",
        """
        rental_id, member_id, workstation_id,
        CAST(session_start_utc AS TIMESTAMPTZ)   AS session_start_utc,
        CAST(session_end_utc   AS TIMESTAMPTZ)   AS session_end_utc,
        CAST(duration_hours      AS DECIMAL(6,2))  AS duration_hours,
        CAST(base_hourly_rate    AS DECIMAL(12,2)) AS base_hourly_rate,
        member_tier_applied,
        CAST(tier_discount_pct   AS DECIMAL(5,4))  AS tier_discount_pct,
        CAST(final_hourly_rate   AS DECIMAL(12,2)) AS final_hourly_rate,
        CAST(gross_rental_amount AS DECIMAL(12,2)) AS gross_rental_amount,
        CAST(points_redeemed     AS INTEGER)       AS points_redeemed,
        CAST(points_credit_value AS DECIMAL(12,2)) AS points_credit_value,
        CAST(net_amount_paid     AS DECIMAL(12,2)) AS net_amount_paid,
        CAST(points_accrued      AS INTEGER)       AS points_accrued,
        payment_method
        """,
        # A rental with no session_end_utc is someone still sitting at the machine. Its money
        # columns are NULL by construction -- the API prices a session at check-out -- so
        # letting it into fact_rental would put a NULL-revenue row into every average the
        # warehouse computes. It arrives on the first build after they leave.
        "session_end_utc IS NOT NULL",
    ),
    "concession_purchases": (
        "concession_purchases",
        "concession_purchases_operational",
        "purchase_id",
        """
        purchase_id, member_id,
        -- CAST for the same reason source_file has one: a snapshot of nothing but walk-in
        -- purchases is an all-NULL rental_id, which pandas types as a numeric.
        nullif(CAST(rental_id AS VARCHAR), '') AS rental_id,
        CAST(total_amount  AS DECIMAL(12,2))   AS total_amount,
        CAST(points_accrued AS INTEGER)        AS points_accrued,
        payment_method,
        CAST(purchased_at_utc AS TIMESTAMPTZ)  AS purchased_at_utc
        """,
        "",
    ),
    "concession_order_items": (
        "concession_order_items",
        "concession_order_items_operational",
        "order_item_id",
        """
        order_item_id, purchase_id, item_sku,
        CAST(quantity    AS INTEGER)       AS quantity,
        CAST(unit_price  AS DECIMAL(12,2)) AS unit_price,
        CAST(total_price AS DECIMAL(12,2)) AS total_price
        """,
        "",
    ),
    "member_points_ledger": (
        "member_points_ledger",
        "member_points_ledger_operational",
        "ledger_id",
        """
        ledger_id, member_id, source_reference_id, transaction_type,
        CAST(points_delta AS INTEGER)          AS points_delta,
        -- RDS keeps the column its original name; Silver renamed it to record that it is
        -- carried, not trusted (finding F5). Same value, and Gold reads the Silver name.
        CAST(resulting_balance AS INTEGER)     AS resulting_balance_source,
        CAST(created_at_utc AS TIMESTAMPTZ)    AS created_at_utc
        """,
        "",
    ),
}


def snapshot_predicate(fact: str) -> str:
    """The rule deciding which snapshot rows are eligible for ``fact``, or ``""`` for all.

    Public because reconciliation has to count the same rows Gold writes. It could restate
    the rule instead -- and an expectation that shares code with the thing it checks is
    weaker for it -- but a *different* rule is not independence, it is a guaranteed
    off-by-one: `fact_rental` excludes open rentals, so an expectation counting them would
    fail by exactly the number of people currently sitting at a machine. That is a check
    that cannot tell "someone is playing right now" from "the build dropped rows", which is
    the same failure mode as a frozen literal, only noisier. The counts stay independent --
    reconciliation reads Silver and Gold separately and compares them; only the definition
    of *eligible* is shared.

    Unknown facts return ``""`` rather than raising: the workstation-event snapshot has no
    eligibility rule and no entry here, and a caller iterating every two-origin fact should
    not have to special-case it.
    """
    if fact not in _OPERATIONAL_FACTS:
        return ""
    return _OPERATIONAL_FACTS[fact][4]


def operational_source(con: object, fact: str) -> tuple[str, int]:
    """Every row Gold should see for ``fact``, and how many the POS contributed.

    Two origins, the same shape as ``event_source`` below: the Bronze-derived Silver dataset,
    and the rows the RDS export left in ``<table>_operational``. Reading only the first is why
    a rental rung up at the till reached Silver and stopped -- ``fact_rental`` was built from
    Bronze, which is immutable after the bootstrap, so no POS sale could ever appear in it.

    Unlike the event snapshot, this one *overlaps* Bronze: RDS was loaded from the same source
    files, so all 28,287 bootstrap rentals sit in both. The operational branch is therefore
    filtered with ``NOT EXISTS`` -- the idiom the merge already uses, and never ``NOT IN``,
    which one NULL key would turn into the empty set. That makes two things true rather than
    incidental: Bronze wins any collision (so lineage stays with the file the row came from,
    not with an alphabetical accident of ``ORDER BY source_file``), and the count returned is
    exactly the rows the snapshot *added*, which is what a caller needs to derive what the
    fact table should hold.
    """
    from aimternet.pipeline.curate.engine import count_parquet

    bronze, snapshot, key, projection, predicate = _OPERATIONAL_FACTS[fact]
    columns = _projected_names(projection)
    bronze_sql = f"SELECT {', '.join(columns)} FROM {_silver(bronze)}"

    uri = layer_uri("silver", snapshot)
    if not count_parquet(con, uri):  # type: ignore[arg-type]
        # The export has never run, or ran before the cafe opened. Build Bronze-only rather
        # than failing: a fresh bucket must still produce a warehouse.
        return bronze_sql, 0

    where = [f"NOT EXISTS (SELECT 1 FROM {_silver(bronze)} b WHERE b.{key} = o.{key})"]
    if predicate:
        where.insert(0, predicate)
    operational_sql = f"""
        SELECT {projection.strip()},
               -- Lineage still says where the row came from: these never touched a file.
               -- The CAST is not decoration. `source_file` is NULL on every API-written row,
               -- so a snapshot holding only those is an all-NULL column, and the export
               -- writes Parquet from a pandas frame -- which types an all-NULL column as a
               -- numeric, not a string. Today the RDS-backed snapshots always carry the
               -- bootstrap rows too, so the column is VARCHAR by luck rather than by rule.
               coalesce(CAST(source_file AS VARCHAR), 'rds:api') AS source_file
        FROM read_parquet('{uri}/**/*.parquet') o
        WHERE {' AND '.join(where)}
    """

    counted = con.execute(f"SELECT count(*) FROM ({operational_sql})").fetchone()  # type: ignore[attr-defined]
    contributed = int(counted[0]) if counted else 0
    return f"{bronze_sql} UNION ALL BY NAME {operational_sql}", contributed


def _projected_names(projection: str) -> list[str]:
    """The column names a projection produces, in order, plus ``source_file``.

    The Bronze branch has to select the same columns in the same order as the operational one
    or ``UNION ALL BY NAME`` has nothing to match on. Deriving the list from the projection
    keeps one of them from drifting away from the other.

    Splitting has to respect parentheses: ``DECIMAL(12,2)`` and ``nullif(rental_id, '')`` both
    contain a comma that is not a column separator.
    """
    # Comments go first: a comma inside one ("Same value, and Gold reads...") is no more a
    # column separator than a comma inside DECIMAL(12,2), and it is not inside parentheses.
    without_comments = "\n".join(
        line for line in projection.splitlines() if not line.strip().startswith("--")
    )

    items: list[str] = []
    depth = 0
    current: list[str] = []
    for char in without_comments:
        if char == "," and depth == 0:
            items.append("".join(current))
            current = []
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        current.append(char)
    items.append("".join(current))

    names = []
    for item in items:
        cleaned = " ".join(item.split())
        if not cleaned:
            continue
        names.append(cleaned.rsplit(" AS ", 1)[-1].strip() if " AS " in cleaned else cleaned)
    return [*names, "source_file"]


def event_source(con: object) -> tuple[str, int]:
    """Every workstation event Gold should see, and how many came from the POS.

    Two origins: the Bronze events in Silver, and the API-emitted ones the DynamoDB export
    leaves in ``workstation_events_operational``. Reading only the first meant the warehouse
    never saw a session the cafe actually ran after the bootstrap.

    Deduplicated on ``event_id``. The two id spaces do not overlap today -- source ids versus
    ``EVT-API-*`` -- but relying on that rather than asserting it is how a fact table quietly
    doubles. Extracted from ``build`` so it can be exercised without S3.

    The count returned is the *contribution*: rows the snapshot adds that Bronze does not
    already have. That is the same thing ``operational_source`` reports, so every entry of
    ``operational_rows`` means one thing and a caller can add it to a Bronze count without
    having to know which origins happen to overlap.
    """
    operational = _operational_events(con)
    count = 0
    if operational:
        counted = con.execute(  # type: ignore[attr-defined]
            f"""SELECT count(*) FROM ({operational}) o
                WHERE NOT EXISTS (SELECT 1 FROM {_silver('workstation_events')} b
                                  WHERE b.event_id = o.event_id)"""
        ).fetchone()
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
        # Resolve the two-origin sources once. Each is the Bronze-derived Silver dataset
        # unioned with whatever the POS has written since, and each reports how many rows it
        # contributed -- the number a caller needs to work out what the fact table should
        # hold, now that none of these counts is a constant any more.
        rentals, rental_rows = operational_source(con, "rental_transactions")
        sales, sale_rows = operational_source(con, "concession_purchases")
        lines, line_rows = operational_source(con, "concession_order_items")
        points, point_rows = operational_source(con, "member_points_ledger")
        report.operational_rows.update(
            {
                "rental_transactions_operational": rental_rows,
                "concession_purchases_operational": sale_rows,
                "concession_order_items_operational": line_rows,
                "member_points_ledger_operational": point_rows,
            }
        )

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
                -- Both origins, not Bronze alone: a member the POS promoted to GOLD gets a
                -- new SCD2 version, rather than the warehouse insisting they are still SILVER.
                SELECT member_id, member_tier_applied AS tier,
                       session_start_utc AS observed_at, rental_id
                FROM ({rentals})
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
            FROM ({rentals}) r
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
            FROM ({sales}) p
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
            FROM ({lines}) i
            LEFT JOIN ({sales}) p USING (purchase_id)
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
            FROM ({points}) l
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
