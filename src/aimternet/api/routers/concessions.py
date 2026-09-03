"""Concession purchase endpoints (spec §7.1)."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, status

from aimternet.api.deps import idempotency_key
from aimternet.api.schemas import PurchaseOut, PurchaseRequest
from aimternet.api.services import concessions, idempotency

router = APIRouter(prefix="/v1/concessions", tags=["concessions"])


@router.get("/catalog")
def catalog() -> list[dict[str, Any]]:
    """What is for sale, with live stock. The POS displays this; it never sets prices."""
    return concessions.catalog()


@router.post("/purchases", response_model=PurchaseOut, status_code=status.HTTP_201_CREATED)
def create_purchase(
    payload: PurchaseRequest,
    key: Annotated[str | None, Depends(idempotency_key)] = None,
) -> PurchaseOut:
    """Sell concessions.

    The request carries SKUs and quantities only. Prices, totals and points come from the
    catalog and the business rules on the server.
    """
    body = payload.model_dump(mode="json")
    if key:
        fingerprint = idempotency.request_fingerprint(body)
        replayed = idempotency.lookup(key, "concessions/purchases", fingerprint)
        if replayed is not None:
            return PurchaseOut(**replayed[1])

    purchase = concessions.create_purchase(
        member_id=payload.member_id,
        items=[line.model_dump() for line in payload.items],
        rental_id=payload.rental_id,
        payment_method=payload.payment_method,
    )
    result = PurchaseOut(**purchase)
    if key:
        idempotency.remember(
            key, "concessions/purchases", idempotency.request_fingerprint(body),
            status.HTTP_201_CREATED, result.model_dump(mode="json"),
        )
    return result
