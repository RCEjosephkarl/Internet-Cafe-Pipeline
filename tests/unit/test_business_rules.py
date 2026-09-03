"""Tests for the money math (spec §5, §9 "property tests on the money arithmetic").

The headline test replays every historical rental through the pricing module. That is the
strongest available evidence that the rules recovered from the synthesizer bytecode are the
rules that actually produced this data.
"""

from __future__ import annotations

import csv
import glob
from decimal import Decimal
from pathlib import Path

import pytest

from aimternet.config.business_rules import BusinessRuleError, rules

R = rules()


# --------------------------------------------------------------------------- lookups


@pytest.mark.parametrize(
    ("workstation_id", "zone", "rate"),
    [
        ("PC-001", "Standard Zone", "50.00"),
        ("PC-100", "Standard Zone", "50.00"),
        ("PC-101", "VIP Esports Zone", "80.00"),
        ("PC-150", "VIP Esports Zone", "80.00"),
        ("PC-151", "Streamer Pods", "120.00"),
        ("PC-175", "Streamer Pods", "120.00"),
    ],
)
def test_zone_boundaries(workstation_id: str, zone: str, rate: str) -> None:
    found = R.zone_for_workstation(workstation_id)
    assert found.name == zone
    assert found.base_hourly_rate == Decimal(rate)


def test_workstation_outside_every_zone_is_rejected() -> None:
    with pytest.raises(BusinessRuleError, match="outside every configured zone"):
        R.zone_for_workstation("PC-176")


def test_unknown_tier_is_rejected() -> None:
    with pytest.raises(BusinessRuleError, match="unknown tier"):
        R.tier("Platinum")


# --------------------------------------------------------------------------- pricing


def test_standard_tier_pays_the_base_rate() -> None:
    p = R.price_rental(workstation_id="PC-001", duration_hours=Decimal("2"), tier_name="Standard")
    assert p.final_hourly_rate == Decimal("50.00")
    assert p.gross_rental_amount == Decimal("100.00")
    assert p.net_amount_paid == Decimal("100.00")
    assert p.points_accrued == 10  # 100 / 10 * 1.00


def test_gold_tier_discount_and_multiplier() -> None:
    p = R.price_rental(workstation_id="PC-160", duration_hours=Decimal("3"), tier_name="Gold")
    assert p.final_hourly_rate == Decimal("96.00")  # 120 * 0.8
    assert p.gross_rental_amount == Decimal("288.00")
    assert p.points_accrued == 43  # floor(288 / 10 * 1.5) = floor(43.2)


def test_gross_is_computed_from_the_discounted_rate_not_the_base_rate() -> None:
    """Finding F1. Spec §5 says base x duration; the bytecode and the data say otherwise."""
    p = R.price_rental(workstation_id="PC-001", duration_hours=Decimal("5"), tier_name="Silver")
    assert p.gross_rental_amount == Decimal("225.00")  # 45.00 * 5, discounted
    assert p.gross_rental_amount != Decimal("250.00")  # what §5 as written would give


def test_redemption_converts_at_fifty_pesos_per_hundred_points() -> None:
    p = R.price_rental(
        workstation_id="PC-101", duration_hours=Decimal("3"), tier_name="Gold", points_redeemed=100
    )
    assert p.gross_rental_amount == Decimal("192.00")
    assert p.points_credit_value == Decimal("50.00")
    assert p.net_amount_paid == Decimal("142.00")


def test_redemption_must_be_in_whole_units() -> None:
    with pytest.raises(BusinessRuleError, match="whole units"):
        R.price_rental(
            workstation_id="PC-001",
            duration_hours=Decimal("2"),
            tier_name="Standard",
            points_redeemed=150,
        )


def test_credit_may_not_exceed_the_bill() -> None:
    with pytest.raises(BusinessRuleError, match="worth"):
        R.price_rental(
            workstation_id="PC-001",
            duration_hours=Decimal("1"),
            tier_name="Standard",
            points_redeemed=500,  # PHP 250 of credit against a PHP 50 rental
        )


@pytest.mark.parametrize(
    ("balance", "gross", "expected"),
    [
        (0, Decimal("500.00"), 0),
        (99, Decimal("500.00"), 0),  # below one whole unit
        (250, Decimal("500.00"), 200),  # capped by balance, floored to whole units
        (5000, Decimal("120.00"), 200),  # capped by the value of the rental
        (5000, Decimal("49.00"), 0),  # rental too small to absorb even one unit
    ],
)
def test_redemption_caps(balance: int, gross: Decimal, expected: int) -> None:
    assert R.max_redeemable_points(balance, gross) == expected


def test_net_amount_can_never_go_negative_under_the_cap() -> None:
    """Property: pricing with the maximum redeemable points never overdraws the bill."""
    for pc in ("PC-001", "PC-120", "PC-175"):
        for hours in ("1", "2", "3", "5", "8"):
            for tier in ("Standard", "Silver", "Gold"):
                gross = R.price_rental(
                    workstation_id=pc, duration_hours=Decimal(hours), tier_name=tier
                ).gross_rental_amount
                points = R.max_redeemable_points(100_000, gross)
                priced = R.price_rental(
                    workstation_id=pc,
                    duration_hours=Decimal(hours),
                    tier_name=tier,
                    points_redeemed=points,
                )
                assert priced.net_amount_paid >= 0
                assert priced.points_credit_value <= priced.gross_rental_amount


def test_accrual_floors_rather_than_rounds() -> None:
    assert R.points_accrued(Decimal("99.99"), "Standard") == 9
    assert R.points_accrued(Decimal("288.00"), "Gold") == 43  # 43.2 floored


# --------------------------------------------------------------------------- tiers


def test_tier_promotion_is_threshold_driven() -> None:
    assert R.tier_after_spend("Standard", Decimal("2999.99")) == ("Standard", 0)
    assert R.tier_after_spend("Standard", Decimal("3000.00")) == ("Silver", 50)
    assert R.tier_after_spend("Silver", Decimal("10000.00")) == ("Gold", 100)
    assert R.tier_after_spend("Gold", Decimal("999999.00")) == ("Gold", 0)


def test_a_single_spend_can_cross_two_thresholds() -> None:
    assert R.tier_after_spend("Standard", Decimal("12000.00")) == ("Gold", 150)


# --------------------------------------------------------------------------- the real thing


@pytest.mark.slow
def test_pricing_reproduces_every_historical_rental(raw_landing: Path) -> None:
    """Replay all 28,287 source rentals. Every field of every row must match exactly.

    This is the proof behind finding F1 and the reason the recovered rules are trusted as
    the rule of record. If this ever fails, either the data changed or the rules drifted --
    both are serious, neither should be papered over.
    """
    files = sorted(glob.glob(str(raw_landing / "legacy_batches" / "*" / "rental_transactions.csv")))
    files = [f for f in files if ".ipynb_checkpoints" not in f]
    assert len(files) == 62

    checked = 0
    for path in files:
        with open(path, newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                priced = R.price_rental(
                    workstation_id=row["workstation_id"],
                    duration_hours=Decimal(row["duration_hours"]),
                    tier_name=row["member_tier_applied"],
                    points_redeemed=int(row["points_redeemed"]),
                )
                rental_id = row["rental_id"]
                assert priced.base_hourly_rate == Decimal(row["base_hourly_rate"]), rental_id
                assert priced.tier_discount_pct == Decimal(row["tier_discount_pct"]), rental_id
                assert priced.final_hourly_rate == Decimal(row["final_hourly_rate"]), rental_id
                assert priced.gross_rental_amount == Decimal(row["gross_rental_amount"]), rental_id
                assert priced.points_credit_value == Decimal(row["points_credit_value"]), rental_id
                assert priced.net_amount_paid == Decimal(row["net_amount_paid"]), rental_id
                assert priced.points_accrued == int(row["points_accrued"]), rental_id
                checked += 1
    assert checked == 28_287
