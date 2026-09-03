"""Request and response models for the operational API (spec §7.1).

Money is ``Decimal`` in and out. FastAPI serialises it as a JSON number by default, which
would reintroduce float at the edge, so every response model pins
``json_encoders``-equivalent behaviour by declaring the fields as ``Decimal`` and letting
Pydantic v2 emit them as strings.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MoneyModel(ApiModel):
    # Money crosses the wire as a string. A JSON number would be parsed back as a float by
    # most clients, which is exactly the error this codebase spends effort avoiding.
    model_config = ConfigDict(extra="forbid", ser_json_inf_nan="strings")


# --------------------------------------------------------------------------- workstations


class WorkstationOut(MoneyModel):
    workstation_id: str
    zone_classification: str
    base_hourly_rate: Decimal
    status: str


class AvailableWorkstations(ApiModel):
    count: int
    workstations: list[WorkstationOut]


# --------------------------------------------------------------------------- members


class MemberOut(MoneyModel):
    member_id: str
    first_name: str
    last_name: str
    email: str
    current_tier: str
    current_points_balance: int
    lifetime_spend_amount: Decimal
    is_active: bool
    is_backfilled: bool
    redeemable_units: int = Field(
        description="Whole 100-point units the member could spend right now"
    )
    open_rental_id: str | None = None


# --------------------------------------------------------------------------- rentals


class CheckInRequest(ApiModel):
    member_id: str = Field(pattern=r"^M-\d{4}$")
    workstation_id: str = Field(pattern=r"^PC-\d{3}$")
    duration_hours: Decimal = Field(gt=0, le=24, description="Hours the member is booking")


class CheckOutRequest(ApiModel):
    rental_id: str | None = None
    workstation_id: str | None = Field(default=None, pattern=r"^PC-\d{3}$")
    points_to_redeem: int = Field(default=0, ge=0)
    payment_method: str = "Cash"


class RentalOut(MoneyModel):
    rental_id: str
    member_id: str
    workstation_id: str
    zone_classification: str
    session_start_utc: datetime
    session_end_utc: datetime | None
    duration_hours: Decimal | None
    base_hourly_rate: Decimal | None
    member_tier_applied: str
    tier_discount_pct: Decimal
    final_hourly_rate: Decimal | None
    gross_rental_amount: Decimal | None
    points_redeemed: int
    points_credit_value: Decimal
    net_amount_paid: Decimal | None
    points_accrued: int | None
    payment_method: str | None
    is_open: bool


# --------------------------------------------------------------------------- concessions


class PurchaseLine(ApiModel):
    item_sku: str = Field(pattern=r"^SKU-[A-Z]{3}-\d{2}$")
    quantity: int = Field(gt=0, le=100)


class PurchaseRequest(ApiModel):
    member_id: str = Field(pattern=r"^M-\d{4}$")
    rental_id: str | None = None
    payment_method: str = "Cash"
    items: list[PurchaseLine] = Field(min_length=1, max_length=50)


class PurchaseLineOut(MoneyModel):
    order_item_id: str
    item_sku: str
    item_name: str
    quantity: int
    unit_price: Decimal
    total_price: Decimal


class PurchaseOut(MoneyModel):
    purchase_id: str
    member_id: str
    rental_id: str | None
    total_amount: Decimal
    points_accrued: int
    payment_method: str
    purchased_at_utc: datetime
    items: list[PurchaseLineOut]


# --------------------------------------------------------------------------- health


class Health(ApiModel):
    status: str
    database: str
    schema_name: str
    version: str
