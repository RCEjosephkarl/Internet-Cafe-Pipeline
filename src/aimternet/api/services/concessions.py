"""Concession purchases (spec §7.1).

The rule that matters most here: **prices come from the catalog, never from the client**.
A POS terminal sends SKUs and quantities; the server looks up what each item costs. A client
that could name its own price is not a point-of-sale system, it is a suggestion box.

Purchase, line items, inventory decrement and the points ledger entry all happen in one
transaction. A purchase that took stock but recorded no sale, or a sale that never decremented
stock, would each be worse than an outright failure.
"""

from __future__ import annotations

import logging
import random
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from aimternet.api import errors
from aimternet.config.business_rules import rules
from aimternet.db.session import connection

log = logging.getLogger(__name__)


def _new_purchase_id(now: datetime) -> str:
    """Match the source id format: ORD-YYYYMMDD-NNNN-NNNN."""
    return f"ORD-{now:%Y%m%d}-{random.randint(0, 9999):04d}-{random.randint(0, 9999):04d}"


def create_purchase(
    *,
    member_id: str,
    items: list[dict[str, Any]],
    rental_id: str | None = None,
    payment_method: str = "Cash",
) -> dict[str, Any]:
    """Sell concessions to a member.

    ``items`` is ``[{"item_sku": ..., "quantity": ...}]``. Quantities for a repeated SKU are
    merged before pricing, so ordering the same drink twice cannot slip past the stock check
    as two separate reservations of the same units.
    """
    business = rules()
    now = datetime.now(UTC)

    if payment_method not in business.payment_methods:
        raise errors.PricingRejected(
            f"unknown payment method {payment_method!r}",
            accepted=sorted(business.payment_methods),
        )

    wanted: dict[str, int] = {}
    for line in items:
        wanted[line["item_sku"]] = wanted.get(line["item_sku"], 0) + int(line["quantity"])

    with connection() as conn:
        import psycopg2.extras

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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

            if rental_id is not None:
                cur.execute(
                    "SELECT rental_id, member_id FROM rental_transactions WHERE rental_id = %s",
                    (rental_id,),
                )
                rental = cur.fetchone()
                if rental is None:
                    raise errors.RentalNotFound(
                        f"no rental {rental_id}", rental_id=rental_id
                    )
                if rental["member_id"] != member_id:
                    raise errors.RentalNotFound(
                        f"rental {rental_id} belongs to a different member",
                        rental_id=rental_id,
                    )

            # Lock every requested item in a stable order. Sorting the SKUs means two
            # concurrent purchases take the same locks in the same sequence and cannot
            # deadlock against each other.
            cur.execute(
                "SELECT item_sku, item_name, unit_retail_price, stock_quantity "
                "FROM concession_items WHERE item_sku = ANY(%s) ORDER BY item_sku FOR UPDATE",
                (sorted(wanted),),
            )
            catalog = {row["item_sku"]: row for row in cur.fetchall()}

            missing = sorted(set(wanted) - set(catalog))
            if missing:
                raise errors.ItemNotFound(
                    f"unknown item(s): {', '.join(missing)}", item_skus=missing
                )

            short = [
                {
                    "item_sku": sku,
                    "requested": quantity,
                    "available": catalog[sku]["stock_quantity"],
                }
                for sku, quantity in wanted.items()
                if catalog[sku]["stock_quantity"] < quantity
            ]
            if short:
                raise errors.InsufficientStock(
                    "not enough stock for: "
                    + ", ".join(
                        f"{s['item_sku']} (want {s['requested']}, have {s['available']})"
                        for s in short
                    ),
                    items=short,
                )

            purchase_id = _new_purchase_id(now)
            lines: list[dict[str, Any]] = []
            total = Decimal("0.00")
            for sku in sorted(wanted):
                quantity = wanted[sku]
                # The price is the catalog's, not the caller's.
                unit_price = Decimal(catalog[sku]["unit_retail_price"])
                line_total = business.quantize(unit_price * quantity)
                total += line_total
                lines.append(
                    {
                        "order_item_id": (
                            f"ITEM-{purchase_id.removeprefix('ORD-')}-{len(lines) + 1:02d}"
                        ),
                        "item_sku": sku,
                        "item_name": catalog[sku]["item_name"],
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "total_price": line_total,
                    }
                )
            total = business.quantize(total)
            points = business.points_accrued(total, member["current_tier"])

            cur.execute(
                """
                INSERT INTO concession_purchases (
                    purchase_id, member_id, rental_id, total_amount, points_accrued,
                    payment_method, purchased_at_utc, source_tz_offset, run_id, ingested_at_utc
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,'+00:00','api',now())
                """,
                (purchase_id, member_id, rental_id, total, points, payment_method, now),
            )
            for line in lines:
                cur.execute(
                    """
                    INSERT INTO concession_order_items (
                        order_item_id, purchase_id, item_sku, quantity, unit_price,
                        total_price, run_id, ingested_at_utc
                    ) VALUES (%s,%s,%s,%s,%s,%s,'api',now())
                    """,
                    (
                        line["order_item_id"], purchase_id, line["item_sku"],
                        line["quantity"], line["unit_price"], line["total_price"],
                    ),
                )
                # Decrementing with a guard rather than a bare subtraction: the CHECK
                # constraint would catch a negative anyway, but this makes the intent
                # explicit and keeps the failure inside the transaction.
                cur.execute(
                    "UPDATE concession_items SET stock_quantity = stock_quantity - %s, "
                    "updated_at = now() WHERE item_sku = %s AND stock_quantity >= %s",
                    (line["quantity"], line["item_sku"], line["quantity"]),
                )
                if cur.rowcount != 1:
                    raise errors.InsufficientStock(
                        f"stock for {line['item_sku']} changed during the sale",
                        item_sku=line["item_sku"],
                    )

            if points:
                from aimternet.api.services.rentals import _write_ledger

                _write_ledger(cur, member_id, purchase_id, "CONCESSION_ACCRUAL", points, now)

            cur.execute(
                """UPDATE members SET
                       current_points_balance = current_points_balance + %s,
                       lifetime_spend_amount  = lifetime_spend_amount + %s,
                       updated_at             = now()
                   WHERE member_id = %s""",
                (points, total, member_id),
            )

            from aimternet.api.services.rentals import _promote_if_due

            _promote_if_due(cur, member_id, now)

    return {
        "purchase_id": purchase_id,
        "member_id": member_id,
        "rental_id": rental_id,
        "total_amount": total,
        "points_accrued": points,
        "payment_method": payment_method,
        "purchased_at_utc": now,
        "items": lines,
    }


def catalog() -> list[dict[str, Any]]:
    """The sellable catalog, for the POS to display."""
    from aimternet.db.session import fetch_all

    return fetch_all(
        """SELECT item_sku, item_name, category, unit_retail_price, stock_quantity
           FROM concession_items ORDER BY category, item_sku"""
    )
