"""Rental check-in and check-out (spec §7.1).

The API is the only write path for the POS, so every business rule that matters is enforced
here rather than in the client. Pricing is not reimplemented: it is imported from
``aimternet.config.business_rules``, the same module the pipeline and the tests use (§3).

Concurrency deserves a word. Check-in does not read-then-write. It takes a row lock on the
workstation with ``SELECT ... FOR UPDATE`` and then relies on two partial unique indexes --
one open rental per workstation, one per member -- so that even if two requests get past the
lock simultaneously, the database refuses the second. The application converts that refusal
into a clean 409 rather than a 500.
"""

from __future__ import annotations

import logging
import random
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg2

from aimternet.api import errors
from aimternet.config.business_rules import BusinessRuleError, rules
from aimternet.db.session import connection

log = logging.getLogger(__name__)

OPEN_RENTAL_INDEXES = ("one_open_rental_per_workstation", "one_open_rental_per_member")


def _new_rental_id(now: datetime) -> str:
    """Match the source id format: SESS-YYYYMMDD-NNNN-NNNN."""
    return f"SESS-{now:%Y%m%d}-{random.randint(0, 9999):04d}-{random.randint(0, 9999):04d}"


# The columns the API returns. Named explicitly rather than SELECT * so that adding a
# lineage column to the table cannot break the response contract -- which is exactly what
# happened the first time this ran.
RENTAL_COLUMNS = """
    r.rental_id, r.member_id, r.workstation_id, r.session_start_utc, r.session_end_utc,
    r.duration_hours, r.base_hourly_rate, r.member_tier_applied, r.tier_discount_pct,
    r.final_hourly_rate, r.gross_rental_amount, r.points_redeemed, r.points_credit_value,
    r.net_amount_paid, r.points_accrued, r.payment_method, w.zone_classification
"""


def _fetch_rental(cur: Any, rental_id: str) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT {RENTAL_COLUMNS}
        FROM rental_transactions r
        JOIN workstations w USING (workstation_id)
        WHERE r.rental_id = %s
        """,
        (rental_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    rental = dict(row)
    rental["is_open"] = rental["session_end_utc"] is None
    return rental


def check_in(
    *, member_id: str, workstation_id: str, duration_hours: Decimal
) -> dict[str, Any]:
    """Start a rental. One transaction: rental row, workstation state, session event."""
    business = rules()
    now = datetime.now(UTC)

    with connection() as conn, conn.cursor(cursor_factory=_dict_cursor()) as cur:
        cur.execute(
            "SELECT member_id, current_tier, current_points_balance, is_active "
            "FROM members WHERE member_id = %s FOR UPDATE",
            (member_id,),
        )
        member = cur.fetchone()
        if member is None:
            raise errors.MemberNotFound(f"no member {member_id}", member_id=member_id)
        if not member["is_active"]:
            raise errors.MemberInactive(
                f"member {member_id} is not active", member_id=member_id
            )

        # Lock the workstation row so two check-ins cannot both read 'AVAILABLE'.
        cur.execute(
            "SELECT workstation_id, zone_classification, base_hourly_rate, status "
            "FROM workstations WHERE workstation_id = %s FOR UPDATE",
            (workstation_id,),
        )
        workstation = cur.fetchone()
        if workstation is None:
            raise errors.WorkstationNotFound(
                f"no workstation {workstation_id}", workstation_id=workstation_id
            )
        if workstation["status"] != "AVAILABLE":
            raise errors.WorkstationUnavailable(
                f"{workstation_id} is {workstation['status'].lower()}",
                workstation_id=workstation_id,
                status=workstation["status"],
            )

        cur.execute(
            "SELECT rental_id FROM rental_transactions "
            "WHERE member_id = %s AND session_end_utc IS NULL",
            (member_id,),
        )
        existing = cur.fetchone()
        if existing:
            raise errors.MemberAlreadyCheckedIn(
                f"member {member_id} already has an open rental",
                member_id=member_id,
                rental_id=existing["rental_id"],
            )

        try:
            priced = business.price_rental(
                workstation_id=workstation_id,
                duration_hours=duration_hours,
                tier_name=member["current_tier"],
            )
        except BusinessRuleError as exc:
            raise errors.PricingRejected(str(exc)) from exc

        rental_id = _new_rental_id(now)
        try:
            cur.execute(
                """
                    INSERT INTO rental_transactions (
                        rental_id, member_id, workstation_id, session_start_utc,
                        member_tier_applied, tier_discount_pct, base_hourly_rate,
                        final_hourly_rate, duration_hours, points_redeemed,
                        points_credit_value, source_tz_offset, run_id, ingested_at_utc
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,0,0,'+00:00','api',now())
                    """,
                (
                    rental_id, member_id, workstation_id, now,
                    member["current_tier"], priced.tier_discount_pct,
                    priced.base_hourly_rate, priced.final_hourly_rate, duration_hours,
                ),
            )
        except psycopg2.errors.UniqueViolation as exc:
            # The partial unique indexes are the real guarantee; this is what a lost
            # race looks like, and it is a conflict, not a server error.
            raise _conflict_from_unique_violation(exc, member_id, workstation_id) from exc

        cur.execute(
            "UPDATE workstations SET status = 'OCCUPIED', updated_at = now() "
            "WHERE workstation_id = %s",
            (workstation_id,),
        )
        rental = _fetch_rental(cur, rental_id)

    _emit_event(
        event_type="SESSION_START",
        workstation_id=workstation_id,
        session_id=rental_id,
        member_id=member_id,
        occurred_at=now,
        duration_allocated_hours=duration_hours,
        notes="Check-in via POS terminal",
    )
    assert rental is not None
    return rental


def check_out(
    *,
    rental_id: str | None = None,
    workstation_id: str | None = None,
    points_to_redeem: int = 0,
    payment_method: str = "Cash",
) -> dict[str, Any]:
    """Close a rental: price the actual duration, apply redemption, free the workstation."""
    business = rules()
    now = datetime.now(UTC)

    with connection() as conn, conn.cursor(cursor_factory=_dict_cursor()) as cur:
        if rental_id:
            cur.execute(
                "SELECT * FROM rental_transactions WHERE rental_id = %s FOR UPDATE",
                (rental_id,),
            )
        elif workstation_id:
            cur.execute(
                "SELECT * FROM rental_transactions "
                "WHERE workstation_id = %s AND session_end_utc IS NULL FOR UPDATE",
                (workstation_id,),
            )
        else:
            raise errors.RentalNotFound("provide either rental_id or workstation_id")

        rental = cur.fetchone()
        if rental is None:
            raise errors.RentalNotFound(
                "no such rental", rental_id=rental_id, workstation_id=workstation_id
            )
        if rental["session_end_utc"] is not None:
            # Closing a closed rental is a conflict the caller can act on, not a crash.
            raise errors.RentalAlreadyClosed(
                f"rental {rental['rental_id']} was already closed at "
                f"{rental['session_end_utc'].isoformat()}",
                rental_id=rental["rental_id"],
            )

        cur.execute(
            "SELECT current_tier, current_points_balance FROM members "
            "WHERE member_id = %s FOR UPDATE",
            (rental["member_id"],),
        )
        member = cur.fetchone()

        billed_hours = _billed_hours(rental["session_start_utc"], now, rental["duration_hours"])
        gross_preview = business.price_rental(
            workstation_id=rental["workstation_id"],
            duration_hours=billed_hours,
            tier_name=rental["member_tier_applied"],
        ).gross_rental_amount

        allowed = business.max_redeemable_points(
            member["current_points_balance"], gross_preview
        )
        if points_to_redeem > allowed:
            raise errors.InsufficientPoints(
                f"cannot redeem {points_to_redeem} points: balance "
                f"{member['current_points_balance']} allows at most {allowed} against a "
                f"bill of {gross_preview}",
                requested=points_to_redeem,
                allowed=allowed,
                balance=member["current_points_balance"],
            )

        try:
            priced = business.price_rental(
                workstation_id=rental["workstation_id"],
                duration_hours=billed_hours,
                tier_name=rental["member_tier_applied"],
                points_redeemed=points_to_redeem,
            )
        except BusinessRuleError as exc:
            raise errors.PricingRejected(str(exc)) from exc

        cur.execute(
            """
                UPDATE rental_transactions SET
                    session_end_utc     = %s,
                    duration_hours      = %s,
                    final_hourly_rate   = %s,
                    gross_rental_amount = %s,
                    points_redeemed     = %s,
                    points_credit_value = %s,
                    net_amount_paid     = %s,
                    points_accrued      = %s,
                    payment_method      = %s,
                    updated_at          = now()
                WHERE rental_id = %s
                """,
            (
                now, billed_hours, priced.final_hourly_rate, priced.gross_rental_amount,
                priced.points_redeemed, priced.points_credit_value, priced.net_amount_paid,
                priced.points_accrued, payment_method, rental["rental_id"],
            ),
        )
        cur.execute(
            "UPDATE workstations SET status = 'AVAILABLE', updated_at = now() "
            "WHERE workstation_id = %s",
            (rental["workstation_id"],),
        )

        if points_to_redeem:
            _write_ledger(
                cur, rental["member_id"], rental["rental_id"], "RENTAL_REDEMPTION",
                -points_to_redeem, now,
            )
        if priced.points_accrued:
            _write_ledger(
                cur, rental["member_id"], rental["rental_id"], "RENTAL_ACCRUAL",
                priced.points_accrued, now,
            )

        new_balance = (
            member["current_points_balance"] - points_to_redeem + priced.points_accrued
        )
        cur.execute(
            """UPDATE members SET
                       current_points_balance = %s,
                       lifetime_spend_amount  = lifetime_spend_amount + %s,
                       updated_at             = now()
                   WHERE member_id = %s""",
            (new_balance, priced.net_amount_paid, rental["member_id"]),
        )
        _promote_if_due(cur, rental["member_id"], now)

        closed = _fetch_rental(cur, rental["rental_id"])

    _emit_event(
        event_type="SESSION_END",
        workstation_id=rental["workstation_id"],
        session_id=rental["rental_id"],
        member_id=rental["member_id"],
        occurred_at=now,
        duration_allocated_hours=billed_hours,
        notes="Check-out via POS terminal",
    )
    assert closed is not None
    return closed


# --------------------------------------------------------------------------- helpers


def _billed_hours(start: datetime, end: datetime, booked: Decimal | None) -> Decimal:
    """Bill the greater of the booked time and the time actually used, to 2 decimals.

    Charging less than the booked duration would let a member reserve a Streamer Pod for
    eight hours and pay for one minute. Charging more than actual use when they overstay
    is the same rule in the other direction.
    """
    elapsed = Decimal((end - start).total_seconds()) / Decimal(3600)
    elapsed = elapsed.quantize(Decimal("0.01"))
    minimum = Decimal("0.01")
    candidate = max(elapsed, booked or minimum)
    return max(candidate, minimum)


def _write_ledger(
    cur: Any, member_id: str, reference_id: str, transaction_type: str,
    delta: int, occurred_at: datetime,
) -> None:
    """Append a points ledger entry.

    ``resulting_balance`` is written as the running sum of this member's deltas, computed in
    SQL rather than read from a cached column -- the source data proved that column
    unreliable (finding F5), and new rows should not inherit the flaw.
    """
    cur.execute(
        """
        INSERT INTO member_points_ledger (
            ledger_id, member_id, source_reference_id, transaction_type,
            points_delta, resulting_balance, created_at_utc, source_tz_offset,
            run_id, ingested_at_utc
        )
        SELECT gen_random_uuid()::text, %s, %s, %s, %s,
               COALESCE((SELECT sum(points_delta) FROM member_points_ledger
                         WHERE member_id = %s), 0) + %s,
               %s, '+00:00', 'api', now()
        """,
        (member_id, reference_id, transaction_type, delta, member_id, delta, occurred_at),
    )


def _promote_if_due(cur: Any, member_id: str, occurred_at: datetime) -> None:
    """Apply tier promotion and its bonus, using the shared rules."""
    business = rules()
    cur.execute(
        "SELECT current_tier, lifetime_spend_amount FROM members WHERE member_id = %s",
        (member_id,),
    )
    row = cur.fetchone()
    new_tier, bonus = business.tier_after_spend(row["current_tier"], row["lifetime_spend_amount"])
    if new_tier == row["current_tier"]:
        return
    cur.execute(
        "UPDATE members SET current_tier = %s, current_points_balance = "
        "current_points_balance + %s, updated_at = now() WHERE member_id = %s",
        (new_tier, bonus, member_id),
    )
    if bonus:
        _write_ledger(cur, member_id, f"TIER:{new_tier}", "TIER_BONUS", bonus, occurred_at)
    log.info("member %s promoted to %s (+%d bonus points)", member_id, new_tier, bonus)


def _conflict_from_unique_violation(
    exc: psycopg2.errors.UniqueViolation, member_id: str, workstation_id: str
) -> errors.ApiProblem:
    message = str(exc)
    if "one_open_rental_per_member" in message:
        return errors.MemberAlreadyCheckedIn(
            f"member {member_id} already has an open rental", member_id=member_id
        )
    if "one_open_rental_per_workstation" in message:
        return errors.WorkstationUnavailable(
            f"{workstation_id} already has an open rental", workstation_id=workstation_id
        )
    return errors.WorkstationUnavailable(f"could not open a rental: {message[:200]}")


def _emit_event(
    *,
    event_type: str,
    workstation_id: str,
    session_id: str,
    member_id: str,
    occurred_at: datetime,
    duration_allocated_hours: Decimal | None = None,
    notes: str = "",
) -> None:
    """Write a workstation event to DynamoDB.

    Deliberately outside the database transaction and deliberately non-fatal: the event
    stream is an observability feed, and losing one must not roll back a paid transaction or
    leave a member unable to check out because DynamoDB is briefly unavailable.
    """
    try:
        import boto3

        from aimternet.config.settings import settings
        from aimternet.pipeline.loaders.dynamodb import event_item

        cfg = settings()
        table = boto3.resource("dynamodb", region_name=cfg.aws_region).Table(cfg.ddb_events_table)
        table.put_item(
            Item=event_item(
                {
                    "event_id": f"EVT-API-{occurred_at:%Y%m%d%H%M%S%f}",
                    "workstation_id": workstation_id,
                    "event_timestamp": occurred_at.isoformat(),
                    "event_type": event_type,
                    "session_id": session_id,
                    "member_id": member_id,
                    "duration_allocated_hours": duration_allocated_hours,
                    "client_os_version": rules().client_os_version,
                    "notes": notes,
                }
            )
        )
    except Exception as exc:
        log.warning("could not emit %s event for %s: %s", event_type, session_id, exc)


def _dict_cursor() -> Any:
    import psycopg2.extras

    return psycopg2.extras.RealDictCursor
