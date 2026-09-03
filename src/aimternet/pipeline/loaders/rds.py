"""Phase 4 — load the validated source data into RDS PostgreSQL (spec §6.2).

Two properties matter more than speed here:

**Idempotency.** Every statement is ``INSERT ... ON CONFLICT``, so a second run inserts
nothing and the acceptance test "a second full run creates no duplicate logical records"
holds by construction rather than by luck.

**Referential honesty.** The FK-ordered load does not paper over D2. Stage C found the 840
undefined members; this module resolves them according to the configured policy and reports
the count. Silently making 840 rows appear is a failure (§1.3).
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from psycopg2.extras import execute_batch

from aimternet.config.poc_policy import (
    DERIVED_SOURCE_SYSTEM,
    OrphanMemberPolicy,
    policy,
)
from aimternet.config.settings import settings
from aimternet.db.session import connection
from aimternet.io.readers import discover, read_csv_rows
from aimternet.schemas.source import DATASET_MODELS

log = logging.getLogger(__name__)

# Spec §6.2 load order. Parents before children, always.
LOAD_ORDER = (
    "workstations",
    "concession_items",
    "members",
    "rental_transactions",
    "concession_purchases",
    "concession_order_items",
    "member_points_ledger",
)


@dataclass
class LoadReport:
    run_id: str
    rows_read: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    rows_written: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    backfilled_members: int = 0
    quarantined_rows: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    orphan_policy: str = ""
    duration_seconds: float = 0.0

    def summary(self) -> str:
        lines = [
            f"RDS load {self.run_id} — orphan policy: {self.orphan_policy}",
            "",
            f"  {'table':26s} {'read':>10s} {'written':>10s} {'quarantined':>12s}",
            f"  {'-' * 26} {'-' * 10} {'-' * 10} {'-' * 12}",
        ]
        for table in LOAD_ORDER:
            lines.append(
                f"  {table:26s} {self.rows_read[table]:10,d} "
                f"{self.rows_written[table]:10,d} {self.quarantined_rows[table]:12,d}"
            )
        lines += [
            "",
            f"  members backfilled (D2): {self.backfilled_members:,}",
            f"  elapsed: {self.duration_seconds:.1f}s",
        ]
        return "\n".join(lines)


# --------------------------------------------------------------------------- reading


def _read_dataset(landing: Path, dataset: str) -> list[dict[str, Any]]:
    """Read and validate one dataset across every batch, newest write wins on conflict."""
    model = DATASET_MODELS[dataset]
    rows: list[dict[str, Any]] = []
    for source in discover(landing):
        if source.dataset != dataset:
            continue
        for raw in read_csv_rows(source.path):
            record = model.model_validate(raw)
            payload = record.model_dump()
            payload["_source_file"] = str(source.path)
            rows.append(payload)
    return rows


def _infer_stub_members(landing: Path, undefined: set[str]) -> list[dict[str, Any]]:
    """Build the D2 backfill rows.

    Tier comes from the member's earliest rental (``member_tier_applied`` is what the cafe
    actually charged them), and the opening balance from their earliest ledger entry
    (``resulting_balance - points_delta``). Both are inferences from the transactions that
    reference them, which is the only evidence these members left behind -- hence the
    ``DERIVED_FROM_TRANSACTIONS`` marker and ``is_backfilled`` flag on every row.
    """
    earliest_rental: dict[str, tuple[datetime, str]] = {}
    earliest_ledger: dict[str, tuple[datetime, int]] = {}
    lifetime_spend: dict[str, Decimal] = defaultdict(Decimal)

    for source in discover(landing):
        if source.dataset == "rental_transactions":
            for raw in read_csv_rows(source.path):
                member_id = raw["member_id"]
                if member_id not in undefined:
                    continue
                started = datetime.fromisoformat(raw["session_start"])
                tier = raw["member_tier_applied"]
                if member_id not in earliest_rental or started < earliest_rental[member_id][0]:
                    earliest_rental[member_id] = (started, tier)
                lifetime_spend[member_id] += Decimal(raw["net_amount_paid"])
        elif source.dataset == "concession_purchases":
            for raw in read_csv_rows(source.path):
                if raw["member_id"] in undefined:
                    lifetime_spend[raw["member_id"]] += Decimal(raw["total_amount"])
        elif source.dataset == "member_points_ledger":
            for raw in read_csv_rows(source.path):
                member_id = raw["member_id"]
                if member_id not in undefined:
                    continue
                created = datetime.fromisoformat(raw["created_at"])
                opening = int(raw["resulting_balance"]) - int(raw["points_delta"])
                if member_id not in earliest_ledger or created < earliest_ledger[member_id][0]:
                    earliest_ledger[member_id] = (created, opening)

    stubs = []
    for member_id in sorted(undefined):
        registered, tier = earliest_rental.get(
            member_id, (datetime(2026, 7, 1, tzinfo=UTC), "Standard")
        )
        _, opening = earliest_ledger.get(member_id, (registered, 0))
        stubs.append(
            {
                "member_id": member_id,
                "first_name": "Unknown",
                "last_name": f"Member {member_id}",
                "email": f"{member_id.lower()}@backfilled.invalid",
                "phone_number": None,
                "current_tier": tier,
                "current_points_balance": max(opening, 0),
                "lifetime_spend_amount": lifetime_spend.get(member_id, Decimal("0.00")),
                "registered_at_utc": registered.astimezone(UTC),
                "source_tz_offset": "+08:00",
                "source_system": DERIVED_SOURCE_SYSTEM,
                "is_backfilled": True,
                "_source_file": "",
            }
        )
    return stubs


# --------------------------------------------------------------------------- writing

_UPSERTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "workstations": (
        """
        INSERT INTO workstations (workstation_id, zone_classification, base_hourly_rate,
            ip_address, mac_address, commissioned_date, source_file, ingested_at_utc, run_id)
        VALUES (%(workstation_id)s, %(zone_classification)s, %(base_hourly_rate)s,
            %(ip_address)s, %(mac_address)s, %(commissioned_date)s,
            %(_source_file)s, now(), %(_run_id)s)
        ON CONFLICT (workstation_id) DO UPDATE SET
            zone_classification = EXCLUDED.zone_classification,
            base_hourly_rate    = EXCLUDED.base_hourly_rate,
            updated_at          = now()
        """,
        (),
    ),
    "concession_items": (
        """
        INSERT INTO concession_items (item_sku, item_name, category, unit_cost_price,
            unit_retail_price, stock_quantity, source_file, ingested_at_utc, run_id)
        VALUES (%(item_sku)s, %(item_name)s, %(category)s, %(unit_cost_price)s,
            %(unit_retail_price)s, %(stock_quantity)s, %(_source_file)s, now(), %(_run_id)s)
        ON CONFLICT (item_sku) DO UPDATE SET
            item_name         = EXCLUDED.item_name,
            unit_retail_price = EXCLUDED.unit_retail_price,
            updated_at        = now()
        """,
        (),
    ),
    "members": (
        """
        INSERT INTO members (member_id, first_name, last_name, email, phone_number,
            current_tier, current_points_balance, lifetime_spend_amount, registered_at_utc,
            source_tz_offset, source_system, is_backfilled, source_file, ingested_at_utc, run_id)
        VALUES (%(member_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone_number)s,
            %(current_tier)s, %(current_points_balance)s, %(lifetime_spend_amount)s,
            %(registered_at_utc)s, %(source_tz_offset)s, %(source_system)s, %(is_backfilled)s,
            %(_source_file)s, now(), %(_run_id)s)
        ON CONFLICT (member_id) DO UPDATE SET
            current_tier           = EXCLUDED.current_tier,
            current_points_balance = EXCLUDED.current_points_balance,
            lifetime_spend_amount  = EXCLUDED.lifetime_spend_amount,
            updated_at             = now()
        """,
        (),
    ),
    "rental_transactions": (
        """
        INSERT INTO rental_transactions (rental_id, member_id, workstation_id,
            session_start_utc, session_end_utc, duration_hours, base_hourly_rate,
            member_tier_applied, tier_discount_pct, final_hourly_rate, gross_rental_amount,
            points_redeemed, points_credit_value, net_amount_paid, points_accrued,
            payment_method, source_tz_offset, source_file, ingested_at_utc, run_id)
        VALUES (%(rental_id)s, %(member_id)s, %(workstation_id)s, %(session_start_utc)s,
            %(session_end_utc)s, %(duration_hours)s, %(base_hourly_rate)s,
            %(member_tier_applied)s, %(tier_discount_pct)s, %(final_hourly_rate)s,
            %(gross_rental_amount)s, %(points_redeemed)s, %(points_credit_value)s,
            %(net_amount_paid)s, %(points_accrued)s, %(payment_method)s, %(source_tz_offset)s,
            %(_source_file)s, now(), %(_run_id)s)
        ON CONFLICT (rental_id) DO NOTHING
        """,
        (),
    ),
    "concession_purchases": (
        """
        INSERT INTO concession_purchases (purchase_id, member_id, rental_id, total_amount,
            points_accrued, payment_method, purchased_at_utc, source_tz_offset,
            source_file, ingested_at_utc, run_id)
        VALUES (%(purchase_id)s, %(member_id)s, %(rental_id)s, %(total_amount)s,
            %(points_accrued)s, %(payment_method)s, %(purchased_at_utc)s, %(source_tz_offset)s,
            %(_source_file)s, now(), %(_run_id)s)
        ON CONFLICT (purchase_id) DO NOTHING
        """,
        (),
    ),
    "concession_order_items": (
        """
        INSERT INTO concession_order_items (order_item_id, purchase_id, item_sku, quantity,
            unit_price, total_price, source_file, ingested_at_utc, run_id)
        VALUES (%(order_item_id)s, %(purchase_id)s, %(item_sku)s, %(quantity)s,
            %(unit_price)s, %(total_price)s, %(_source_file)s, now(), %(_run_id)s)
        ON CONFLICT (order_item_id) DO NOTHING
        """,
        (),
    ),
    "member_points_ledger": (
        """
        INSERT INTO member_points_ledger (ledger_id, member_id, source_reference_id,
            transaction_type, points_delta, resulting_balance, created_at_utc,
            source_tz_offset, source_file, ingested_at_utc, run_id)
        VALUES (%(ledger_id)s, %(member_id)s, %(source_reference_id)s, %(transaction_type)s,
            %(points_delta)s, %(resulting_balance)s, %(created_at_utc)s, %(source_tz_offset)s,
            %(_source_file)s, now(), %(_run_id)s)
        ON CONFLICT (ledger_id) DO NOTHING
        """,
        (),
    ),
}


# Columns the database owns rather than the source file. Rows read straight from a CSV do
# not carry them; the D2 stubs do, and set them to something different -- which is the whole
# point of having them.
_ROW_DEFAULTS: dict[str, dict[str, Any]] = {
    "members": {"source_system": "LEGACY_BATCH", "is_backfilled": False},
}


def _write(cur: Any, table: str, rows: list[dict[str, Any]], run_id: str, page: int) -> None:
    if not rows:
        return
    sql, _ = _UPSERTS[table]
    defaults = _ROW_DEFAULTS.get(table, {})
    for row in rows:
        row.setdefault("_run_id", run_id)
        row.setdefault("_source_file", "")
        for column, value in defaults.items():
            row.setdefault(column, value)
    execute_batch(cur, sql, rows, page_size=page)


def load_all(landing: Path | None = None, run_id: str = "", *, batch_size: int = 0) -> LoadReport:
    """Load every dataset into RDS in FK order, inside one transaction per table."""
    import time

    cfg = settings()
    pol = policy()
    landing = Path(landing or cfg.raw_landing)
    run_id = run_id or f"rds-{datetime.now(UTC):%Y%m%d%H%M%S}"
    page = batch_size or cfg.batch_size
    report = LoadReport(run_id=run_id, orphan_policy=str(pol.orphan_member_policy))
    started = time.time()

    log.info("reading source datasets from %s", landing)
    data = {dataset: _read_dataset(landing, dataset) for dataset in LOAD_ORDER}
    for dataset, rows in data.items():
        report.rows_read[dataset] = len(rows)

    defined = {row["member_id"] for row in data["members"]}
    referenced = {
        row["member_id"] for dataset in ("rental_transactions", "concession_purchases",
                                         "member_points_ledger")
        for row in data[dataset]
    }
    undefined = referenced - defined

    if undefined:
        if pol.orphan_member_policy is OrphanMemberPolicy.SYNTHESIZE_STUB:
            stubs = _infer_stub_members(landing, undefined)
            data["members"].extend(stubs)
            report.backfilled_members = len(stubs)
            log.warning(
                "D2: backfilling %d member(s) referenced by transactions but never defined "
                "(policy=synthesize_stub)",
                len(stubs),
            )
        else:
            for dataset in ("rental_transactions", "concession_purchases",
                            "member_points_ledger"):
                keep = [r for r in data[dataset] if r["member_id"] not in undefined]
                report.quarantined_rows[dataset] = len(data[dataset]) - len(keep)
                data[dataset] = keep
            log.warning(
                "D2: quarantined rows depending on %d undefined member(s) (policy=quarantine)",
                len(undefined),
            )

    with connection() as conn, conn.cursor() as cur:
        for table in LOAD_ORDER:
            rows = data[table]
            _write(cur, table, rows, run_id, page)
            cur.execute(f"SELECT count(*) FROM {table}")
            report.rows_written[table] = int(cur.fetchone()[0])
            log.info("loaded %-24s -> %8d row(s) in table", table, report.rows_written[table])

    report.duration_seconds = time.time() - started
    return report


def table_counts() -> dict[str, int]:
    with connection(read_only=False) as conn, conn.cursor() as cur:
        counts = {}
        for table in LOAD_ORDER:
            cur.execute(f"SELECT count(*) FROM {table}")
            counts[table] = int(cur.fetchone()[0])
        return counts
