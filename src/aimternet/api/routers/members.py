"""Member lookup (spec §7.1)."""

from __future__ import annotations

from fastapi import APIRouter

from aimternet.api import errors
from aimternet.api.schemas import MemberOut
from aimternet.config.business_rules import rules
from aimternet.db.session import fetch_all

router = APIRouter(prefix="/v1/members", tags=["members"])


@router.get("/{member_id}", response_model=MemberOut)
def get_member(member_id: str) -> MemberOut:
    """One member, with what the front desk actually needs to serve them.

    ``redeemable_units`` is computed from the shared business rules rather than left for the
    POS to work out, so the terminal never has to know that points are spent in hundreds.
    """
    rows = fetch_all(
        """
        SELECT m.member_id, m.first_name, m.last_name, m.email, m.current_tier,
               m.current_points_balance, m.lifetime_spend_amount, m.is_active,
               m.is_backfilled,
               (SELECT r.rental_id FROM rental_transactions r
                 WHERE r.member_id = m.member_id AND r.session_end_utc IS NULL
                 LIMIT 1) AS open_rental_id
        FROM members m WHERE m.member_id = %s
        """,
        (member_id,),
    )
    if not rows:
        raise errors.MemberNotFound(f"no member {member_id}", member_id=member_id)

    row = rows[0]
    unit = rules().redemption_unit_points
    row["redeemable_units"] = row["current_points_balance"] // unit
    return MemberOut(**row)
