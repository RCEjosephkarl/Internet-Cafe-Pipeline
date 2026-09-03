"""Business rules of record — the single implementation of AIMternet's money math.

Loaded from ``config/business_rules.yaml``. The operational API, the pipeline
transformations and the tests all import from here. If a pricing number appears anywhere
else in this repo, that is a bug (spec §3: "No business rule may be implemented twice").

Every monetary value is a :class:`~decimal.Decimal`. There are no floats in this module and
none may be introduced: a float rounding error in a points balance is silent and permanent.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Final

import yaml

_RULES_PATH_ENV = "AIMTERNET_BUSINESS_RULES"
_DEFAULT_RULES_PATH: Final = Path(__file__).resolve().parents[3] / "config" / "business_rules.yaml"


class BusinessRuleError(ValueError):
    """Raised when a value violates the rules — an unknown tier, an unpriceable workstation."""


@dataclass(frozen=True, slots=True)
class Zone:
    name: str
    pc_first: int
    pc_last: int
    base_hourly_rate: Decimal

    def contains(self, pc_number: int) -> bool:
        return self.pc_first <= pc_number <= self.pc_last


@dataclass(frozen=True, slots=True)
class Tier:
    name: str
    discount_pct: Decimal
    points_multiplier: Decimal
    upgrade_threshold: Decimal | None
    next_tier: str | None
    upgrade_bonus_points: int | None


@dataclass(frozen=True, slots=True)
class RentalPricing:
    """The complete priced result of a rental. Mirrors the source CSV columns."""

    base_hourly_rate: Decimal
    tier_discount_pct: Decimal
    final_hourly_rate: Decimal
    gross_rental_amount: Decimal
    points_redeemed: int
    points_credit_value: Decimal
    net_amount_paid: Decimal
    points_accrued: int


def _dec(value: Any) -> Decimal:
    """Parse to Decimal via str, so a stray float in YAML cannot smuggle in binary error."""
    return Decimal(str(value))


@functools.lru_cache(maxsize=1)
def _raw_rules() -> dict[str, Any]:
    import os

    path = Path(os.environ.get(_RULES_PATH_ENV, _DEFAULT_RULES_PATH))
    if not path.is_file():
        raise BusinessRuleError(
            f"business rules file not found at {path}. Set {_RULES_PATH_ENV} to override."
        )
    with path.open(encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh)
    if not isinstance(loaded, dict):
        raise BusinessRuleError(f"{path} did not parse to a mapping")
    return loaded


class BusinessRules:
    """Typed accessor over the YAML. Construct via :func:`rules`, which caches."""

    def __init__(self, raw: dict[str, Any]) -> None:
        self._raw = raw
        self.currency: str = raw["currency"]
        self.timezone: str = raw["timezone"]
        self.money_quantum: Decimal = _dec(raw["pricing"]["money_quantum"])

        self.window_start: date = raw["simulation_window"]["start"]
        self.window_end: date = raw["simulation_window"]["end"]

        self.total_workstations: int = int(raw["workstations"]["total"])
        self.zones: tuple[Zone, ...] = tuple(
            Zone(
                name=z["name"],
                pc_first=int(z["pc_range"][0]),
                pc_last=int(z["pc_range"][1]),
                base_hourly_rate=_dec(z["base_hourly_rate"]),
            )
            for z in raw["workstations"]["zones"]
        )
        self.tiers: dict[str, Tier] = {
            t["name"]: Tier(
                name=t["name"],
                discount_pct=_dec(t["discount_pct"]),
                points_multiplier=_dec(t["points_multiplier"]),
                upgrade_threshold=(
                    _dec(t["upgrade_threshold"]) if t["upgrade_threshold"] is not None else None
                ),
                next_tier=t["next_tier"],
                upgrade_bonus_points=t["upgrade_bonus_points"],
            )
            for t in raw["tiers"]
        }

        pts = raw["points"]
        self.accrual_peso_per_point: Decimal = _dec(pts["accrual_peso_per_point"])
        self.redemption_unit_points: int = int(pts["redemption_unit_points"])
        self.redemption_unit_value: Decimal = _dec(pts["redemption_unit_value"])

        self.session_durations: tuple[Decimal, ...] = tuple(
            _dec(d) for d in raw["session_durations_hours"]
        )
        self.payment_methods: frozenset[str] = frozenset(raw["payment_methods"])
        self.client_os_version: str = raw["client_os_version"]
        self.concession_categories: frozenset[str] = frozenset(raw["concession_categories"])
        self.event_types: frozenset[str] = frozenset(raw["event_types"])
        self.telemetry_statuses: frozenset[str] = frozenset(raw["telemetry_statuses"])
        self.ledger_transaction_types: frozenset[str] = frozenset(raw["ledger_transaction_types"])
        self.ph_holidays: dict[str, str] = {str(k): v for k, v in raw["ph_holidays_2026"].items()}
        self.telemetry_tick_seconds: int = int(raw["telemetry"]["tick_seconds"])
        self.telemetry_ttl_days: int = int(raw["telemetry"]["ttl_days"])

    # ---------------------------------------------------------------- money helpers

    def quantize(self, amount: Decimal) -> Decimal:
        """Round to the currency quantum. Half-up, the way a cash register rounds."""
        return amount.quantize(self.money_quantum, rounding=ROUND_HALF_UP)

    # ---------------------------------------------------------------- lookups

    def zone_for_workstation(self, workstation_id: str) -> Zone:
        """``PC-137`` -> the VIP Esports Zone. Raises on an id outside the known ranges."""
        try:
            number = int(workstation_id.split("-", 1)[1])
        except (IndexError, ValueError) as exc:
            raise BusinessRuleError(f"unparseable workstation_id {workstation_id!r}") from exc
        for zone in self.zones:
            if zone.contains(number):
                return zone
        raise BusinessRuleError(f"{workstation_id} falls outside every configured zone")

    def tier(self, name: str) -> Tier:
        try:
            return self.tiers[name]
        except KeyError as exc:
            raise BusinessRuleError(
                f"unknown tier {name!r}; known tiers: {sorted(self.tiers)}"
            ) from exc

    # ---------------------------------------------------------------- pricing

    def max_redeemable_points(self, points_balance: int, gross_rental_amount: Decimal) -> int:
        """Points a member may spend on this rental.

        Capped twice: by what they hold, and by the rental's own value — credit can never
        exceed the bill, so ``net_amount_paid`` can never go negative. Both caps come from
        the synthesizer bytecode, not from guesswork.
        """
        if points_balance < 0:
            raise BusinessRuleError(f"negative points balance: {points_balance}")
        units_by_balance = points_balance // self.redemption_unit_points
        units_by_value = int(gross_rental_amount / self.redemption_unit_value)
        return min(units_by_balance, units_by_value) * self.redemption_unit_points

    def points_accrued(self, net_amount_paid: Decimal, tier_name: str) -> int:
        """1 point per PHP 10 of net spend, times the tier multiplier, floored."""
        multiplier = self.tier(tier_name).points_multiplier
        raw = net_amount_paid / self.accrual_peso_per_point * multiplier
        return int(raw.to_integral_value(rounding="ROUND_FLOOR"))

    def price_rental(
        self,
        *,
        workstation_id: str,
        duration_hours: Decimal,
        tier_name: str,
        points_redeemed: int = 0,
    ) -> RentalPricing:
        """Price a rental end to end. The only rental pricing implementation in the repo."""
        if duration_hours <= 0:
            raise BusinessRuleError(f"duration must be positive, got {duration_hours}")
        if points_redeemed < 0:
            raise BusinessRuleError(f"points_redeemed must not be negative, got {points_redeemed}")
        if points_redeemed % self.redemption_unit_points:
            raise BusinessRuleError(
                f"points must be redeemed in whole units of {self.redemption_unit_points}, "
                f"got {points_redeemed}"
            )

        tier = self.tier(tier_name)
        base = self.zone_for_workstation(workstation_id).base_hourly_rate
        final_hourly = self.quantize(base * (Decimal(1) - tier.discount_pct))
        gross = self.quantize(final_hourly * duration_hours)

        credit = self.quantize(
            Decimal(points_redeemed) / self.redemption_unit_points * self.redemption_unit_value
        )
        if credit > gross:
            raise BusinessRuleError(
                f"redeeming {points_redeemed} points is worth {credit} "
                f"but the rental is only {gross}"
            )
        net = self.quantize(gross - credit)

        return RentalPricing(
            base_hourly_rate=base,
            tier_discount_pct=tier.discount_pct,
            final_hourly_rate=final_hourly,
            gross_rental_amount=gross,
            points_redeemed=points_redeemed,
            points_credit_value=credit,
            net_amount_paid=net,
            points_accrued=self.points_accrued(net, tier_name),
        )

    def tier_after_spend(self, current_tier: str, lifetime_spend: Decimal) -> tuple[str, int]:
        """Promote a member as far as their lifetime spend allows.

        Returns the resulting tier and the total bonus points earned by the promotions.
        Multi-step promotion is possible when a single large spend crosses two thresholds.
        """
        tier = self.tier(current_tier)
        bonus = 0
        while (
            tier.upgrade_threshold is not None
            and tier.next_tier is not None
            and lifetime_spend >= tier.upgrade_threshold
        ):
            bonus += tier.upgrade_bonus_points or 0
            tier = self.tier(tier.next_tier)
        return tier.name, bonus


@functools.lru_cache(maxsize=1)
def rules() -> BusinessRules:
    """The process-wide rules singleton."""
    return BusinessRules(_raw_rules())
