"""Rental check-in and check-out endpoints (spec §7.1).

The routers stay thin: validation is Pydantic's, business rules belong to
``aimternet.config.business_rules``, and the transactional work belongs to
``aimternet.api.services.rentals``.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from aimternet.api.deps import idempotency_key
from aimternet.api.schemas import CheckInRequest, CheckOutRequest, RentalOut
from aimternet.api.services import idempotency, rentals

router = APIRouter(prefix="/v1/rentals", tags=["rentals"])


@router.post("/check-in", response_model=RentalOut, status_code=status.HTTP_201_CREATED)
def check_in(
    payload: CheckInRequest,
    key: Annotated[str | None, Depends(idempotency_key)] = None,
) -> RentalOut:
    """Start a rental.

    Rejects a second check-in on an occupied workstation, and a member who already has an
    open rental, with 409 in both cases.
    """
    body = payload.model_dump(mode="json")
    if key:
        fingerprint = idempotency.request_fingerprint(body)
        replayed = idempotency.lookup(key, "rentals/check-in", fingerprint)
        if replayed is not None:
            return RentalOut(**replayed[1])

    rental = rentals.check_in(
        member_id=payload.member_id,
        workstation_id=payload.workstation_id,
        duration_hours=payload.duration_hours,
    )
    result = RentalOut(**rental)
    if key:
        idempotency.remember(
            key, "rentals/check-in", idempotency.request_fingerprint(body),
            status.HTTP_201_CREATED, result.model_dump(mode="json"),
        )
    return result


@router.post("/check-out", response_model=RentalOut)
def check_out(
    payload: CheckOutRequest,
    key: Annotated[str | None, Depends(idempotency_key)] = None,
) -> RentalOut:
    """Close a rental and return the completed transaction.

    Closing an already-closed rental returns 409, not 500.
    """
    body = payload.model_dump(mode="json")
    if key:
        fingerprint = idempotency.request_fingerprint(body)
        replayed = idempotency.lookup(key, "rentals/check-out", fingerprint)
        if replayed is not None:
            return RentalOut(**replayed[1])

    rental = rentals.check_out(
        rental_id=payload.rental_id,
        workstation_id=payload.workstation_id,
        points_to_redeem=payload.points_to_redeem,
        payment_method=payload.payment_method,
    )
    result = RentalOut(**rental)
    if key:
        idempotency.remember(
            key, "rentals/check-out", idempotency.request_fingerprint(body),
            status.HTTP_200_OK, result.model_dump(mode="json"),
        )
    return result


@router.get("/active", response_model=list[RentalOut])
def active_rentals() -> list[RentalOut]:
    """Every open rental — who is on the floor."""
    from aimternet.db.session import fetch_all

    # Same explicit column list the service uses: SELECT * would drag the lineage columns
    # into a response model that forbids them.
    rows = fetch_all(
        f"""SELECT {rentals.RENTAL_COLUMNS}
            FROM rental_transactions r JOIN workstations w USING (workstation_id)
            WHERE r.session_end_utc IS NULL ORDER BY r.session_start_utc"""
    )
    return [RentalOut(**{**row, "is_open": True}) for row in rows]
