"""Pydantic v2 models for the ten source datasets (spec §4).

Field names and order mirror the file headers exactly. These models are the contract:
Stage C validation constructs them, and anything they reject becomes a quarantine record
with the reason attached.

Two parsing rules apply everywhere and come from the data, not from preference:

* CSVs are CRLF-terminated and may carry a UTF-8 BOM (D4) — the readers handle that.
* Every timestamp is ISO-8601 with a ``+08:00`` offset. Models keep the original value and
  expose ``*_utc`` for storage, so the offset survives as lineage (§4).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    model_validator,
)

Money = Annotated[Decimal, Field(max_digits=14, decimal_places=2)]
Rate = Annotated[Decimal, Field(max_digits=6, decimal_places=2)]

ZoneName = Literal["Standard Zone", "VIP Esports Zone", "Streamer Pods"]
TierName = Literal["Standard", "Silver", "Gold"]
PaymentMethod = Literal["Cash", "GCash", "Maya", "Credit Card"]
ConcessionCategory = Literal["Beverage", "Hot Food", "Snacks", "Accessories"]
EventType = Literal["SESSION_START", "SESSION_END", "HARDWARE_ALERT", "PERIPHERAL_ALERT"]
LedgerType = Literal[
    "RENTAL_ACCRUAL", "CONCESSION_ACCRUAL", "RENTAL_REDEMPTION", "TIER_BONUS"
]
TelemetryStatus = Literal["IDLE", "OCCUPIED"]


class SourceModel(BaseModel):
    """Strict base: an unexpected column is an error, not something to shrug off."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)


def _to_utc(value: datetime) -> datetime:
    """Normalize to UTC. A naive timestamp is a data defect, so refuse to guess its zone."""
    if value.tzinfo is None:
        raise ValueError(f"timestamp {value!r} has no timezone offset; expected +08:00")
    return value.astimezone(UTC)


def _offset_label(value: datetime) -> str:
    """'+08:00' — kept as a lineage column so the original zone is never lost."""
    offset = value.utcoffset()
    if offset is None:  # pragma: no cover - _to_utc rejects this first
        return ""
    total = int(offset.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    return f"{sign}{total // 3600:02d}:{(total % 3600) // 60:02d}"


# --------------------------------------------------------------------------- catalog


class Workstation(SourceModel):
    """``catalog/workstations.csv`` — 175 rows, static."""

    workstation_id: str = Field(pattern=r"^PC-\d{3}$")
    zone_classification: ZoneName
    base_hourly_rate: Rate
    ip_address: str
    mac_address: str
    commissioned_date: date

    @computed_field  # type: ignore[prop-decorator]
    @property
    def pc_number(self) -> int:
        return int(self.workstation_id.split("-")[1])


class ConcessionItem(SourceModel):
    """``catalog/concession_items.csv`` — 10 rows, static."""

    item_sku: str = Field(pattern=r"^SKU-[A-Z]{3}-\d{2}$")
    item_name: str
    category: ConcessionCategory
    unit_cost_price: Money
    unit_retail_price: Money
    stock_quantity: int = Field(ge=0)


# --------------------------------------------------------------------------- dimensions


class DimDate(SourceModel):
    """``dimensions/dim_date.csv`` — 365 rows, calendar 2026."""

    date_id: int
    calendar_date: date
    day_of_week: str
    day_of_month: int = Field(ge=1, le=31)
    month_label: str
    month_index: int = Field(ge=1, le=12)
    quarter_index: int = Field(ge=1, le=4)
    year_index: int
    weekend_flag: bool
    holiday_ph_flag: bool


class DimTime(SourceModel):
    """``dimensions/dim_time.csv`` — 1,440 rows, minute grain.

    ``time_id`` is HHMM-encoded (0, 1, ... 59, 100, 101, ... 2359), not a 0-1439 minute
    index. Confirmed against the file: 1,440 rows spanning 0..2359.
    """

    time_id: int = Field(ge=0, le=2359)
    hour_24: int = Field(ge=0, le=23)
    minute_val: int = Field(ge=0, le=59)
    day_part_label: str

    @model_validator(mode="after")
    def _time_id_encodes_the_clock(self) -> DimTime:
        expected = self.hour_24 * 100 + self.minute_val
        if self.time_id != expected:
            raise ValueError(
                f"time_id {self.time_id} does not encode {self.hour_24:02d}:{self.minute_val:02d} "
                f"(expected {expected})"
            )
        return self


# --------------------------------------------------------------------------- transactional


class Member(SourceModel):
    """``legacy_batches/<date>/members.csv`` — new registrations that day only."""

    member_id: str = Field(pattern=r"^M-\d{4}$")
    first_name: str
    last_name: str
    email: str
    phone_number: str
    current_tier: TierName
    current_points_balance: int = Field(ge=0)
    lifetime_spend_amount: Money = Field(ge=0)
    registered_at: datetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def registered_at_utc(self) -> datetime:
        return _to_utc(self.registered_at)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def source_tz_offset(self) -> str:
        return _offset_label(self.registered_at)


class RentalTransaction(SourceModel):
    """``legacy_batches/<date>/rental_transactions.csv``.

    Money fields are validated for internal consistency by the Stage C business-rule check,
    not here: a row that disagrees with the pricing rules is a *finding* to quarantine and
    report, not a parse failure.
    """

    rental_id: str = Field(pattern=r"^SESS-\d{8}-\d{4}-\d{4}$")
    member_id: str = Field(pattern=r"^M-\d{4}$")
    workstation_id: str = Field(pattern=r"^PC-\d{3}$")
    session_start: datetime
    session_end: datetime
    duration_hours: Decimal = Field(gt=0)
    base_hourly_rate: Rate
    member_tier_applied: TierName
    tier_discount_pct: Decimal = Field(ge=0, le=1)
    final_hourly_rate: Rate
    gross_rental_amount: Money = Field(ge=0)
    points_redeemed: int = Field(ge=0)
    points_credit_value: Money = Field(ge=0)
    net_amount_paid: Money = Field(ge=0)
    points_accrued: int = Field(ge=0)
    payment_method: PaymentMethod

    @model_validator(mode="after")
    def _session_ends_after_it_starts(self) -> RentalTransaction:
        if self.session_end < self.session_start:
            raise ValueError(
                f"session_end {self.session_end.isoformat()} precedes "
                f"session_start {self.session_start.isoformat()}"
            )
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def session_start_utc(self) -> datetime:
        return _to_utc(self.session_start)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def session_end_utc(self) -> datetime:
        return _to_utc(self.session_end)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def source_tz_offset(self) -> str:
        return _offset_label(self.session_start)


class ConcessionPurchase(SourceModel):
    """``legacy_batches/<date>/concession_purchases.csv``."""

    purchase_id: str = Field(pattern=r"^ORD-\d{8}-\d{4}-\d{4}$")
    member_id: str = Field(pattern=r"^M-\d{4}$")
    rental_id: str
    total_amount: Money = Field(ge=0)
    points_accrued: int = Field(ge=0)
    payment_method: PaymentMethod
    purchased_at: datetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def purchased_at_utc(self) -> datetime:
        return _to_utc(self.purchased_at)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def source_tz_offset(self) -> str:
        return _offset_label(self.purchased_at)


class ConcessionOrderItem(SourceModel):
    """``legacy_batches/<date>/concession_order_items.csv``."""

    order_item_id: str
    purchase_id: str = Field(pattern=r"^ORD-\d{8}-\d{4}-\d{4}$")
    item_sku: str = Field(pattern=r"^SKU-[A-Z]{3}-\d{2}$")
    quantity: int = Field(gt=0)
    unit_price: Money = Field(ge=0)
    total_price: Money = Field(ge=0)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def line_total_matches(self) -> bool:
        """total_price must equal quantity x unit_price (§4 cross-file invariant)."""
        return self.total_price == self.quantity * self.unit_price


class MemberPointsLedger(SourceModel):
    """``legacy_batches/<date>/member_points_ledger.csv``."""

    ledger_id: str
    member_id: str = Field(pattern=r"^M-\d{4}$")
    source_reference_id: str
    transaction_type: LedgerType
    points_delta: int
    resulting_balance: int
    created_at: datetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def created_at_utc(self) -> datetime:
        return _to_utc(self.created_at)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def source_tz_offset(self) -> str:
        return _offset_label(self.created_at)


# --------------------------------------------------------------------------- JSON datasets


class WorkstationEvent(SourceModel):
    """``legacy_batches/<date>/workstation_events.json``.

    Alert events carry no session or member, so those fields must stay optional (§4).
    """

    event_id: str
    workstation_id: str = Field(pattern=r"^PC-\d{3}$")
    event_timestamp: datetime
    event_type: EventType
    session_id: str | None = None
    member_id: str | None = None
    duration_allocated_hours: Decimal | None = None
    client_os_version: str
    notes: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def event_timestamp_utc(self) -> datetime:
        return _to_utc(self.event_timestamp)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def source_tz_offset(self) -> str:
        return _offset_label(self.event_timestamp)


class HardwareMetrics(SourceModel):
    cpu_load_pct: Decimal
    cpu_temp_c: int
    ram_usage_pct: Decimal
    gpu_load_pct: Decimal
    gpu_temp_c: int
    disk_io_read_mbs: Decimal
    disk_io_write_mbs: Decimal


class NetworkDiagnostics(SourceModel):
    latency_ping_ms: int
    packet_loss_pct: Decimal
    bandwidth_down_mbps: Decimal


class PeripheralsConnected(SourceModel):
    keyboard: bool
    mouse: bool
    headset: bool


class TelemetryRecord(SourceModel):
    """``telemetry/<date>/<HH>.json`` — one record per workstation per 5-minute tick."""

    workstation_id: str = Field(pattern=r"^PC-\d{3}$")
    timestamp: datetime
    zone: ZoneName
    status: TelemetryStatus
    active_session_id: str | None = None
    active_member_id: str | None = None
    hardware_metrics: HardwareMetrics
    network_diagnostics: NetworkDiagnostics
    peripherals_connected: PeripheralsConnected
    expires_at: int
    """Unix epoch seconds, source timestamp + 7 days. Wired to the DynamoDB TTL attribute,
    but TTL enforcement is off by default — see poc_policy F2_TELEMETRY_TTL_DISABLED."""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def timestamp_utc(self) -> datetime:
        return _to_utc(self.timestamp)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def source_tz_offset(self) -> str:
        return _offset_label(self.timestamp)


DATASET_MODELS: dict[str, type[SourceModel]] = {
    "workstations": Workstation,
    "concession_items": ConcessionItem,
    "dim_date": DimDate,
    "dim_time": DimTime,
    "members": Member,
    "rental_transactions": RentalTransaction,
    "concession_purchases": ConcessionPurchase,
    "concession_order_items": ConcessionOrderItem,
    "member_points_ledger": MemberPointsLedger,
    "workstation_events": WorkstationEvent,
    "telemetry": TelemetryRecord,
}
