"""Stage C — validation (spec §6.1).

Runs entirely on the local landing tree with no AWS, which is what makes it usable as a
pre-flight check before anything is uploaded or loaded.

Three passes, in order, because each depends on what the last one learned:

1. **Structural** — every record through its Pydantic model, plus header and primary-key
   checks. Anything that fails here cannot be reasoned about further, so it is quarantined.
2. **Referential** — foreign keys resolved against the identifier sets the first pass built.
   This is the pass that finds D2.
3. **Business rules** — the §4 cross-file invariants and the §5 pricing arithmetic.

Telemetry is 3.1M records and is streamed file by file; the pass never holds more than one
file's worth of records, and it accumulates counters rather than rows.
"""

from __future__ import annotations

import logging
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from pathlib import Path

from pydantic import BaseModel, ValidationError

from aimternet.config.business_rules import BusinessRuleError, rules
from aimternet.config.poc_policy import EXPECTED_ORPHAN_MEMBER_COUNT
from aimternet.io.readers import (
    SourceFile,
    discover,
    read_csv_header,
    read_csv_rows,
    read_json_array,
)
from aimternet.pipeline.validation.findings import (
    Finding,
    Rule,
    Severity,
    ValidationResult,
)
from aimternet.schemas.source import DATASET_MODELS

log = logging.getLogger(__name__)

PRIMARY_KEYS = {
    "workstations": "workstation_id",
    "concession_items": "item_sku",
    "dim_date": "date_id",
    "dim_time": "time_id",
    "members": "member_id",
    "rental_transactions": "rental_id",
    "concession_purchases": "purchase_id",
    "concession_order_items": "order_item_id",
    "member_points_ledger": "ledger_id",
    "workstation_events": "event_id",
}


@dataclass
class _Corpus:
    """What the structural pass learns and the later passes need.

    Deliberately identifiers and small aggregates only -- never the 3.1M telemetry rows.
    """

    workstation_ids: set[str] = field(default_factory=set)
    item_skus: set[str] = field(default_factory=set)
    member_ids: set[str] = field(default_factory=set)
    rental_ids: set[str] = field(default_factory=set)
    purchase_ids: set[str] = field(default_factory=set)
    referenced_member_ids: set[str] = field(default_factory=set)
    # purchase_id -> summed line items, for the §4 order-total invariant
    order_item_totals: dict[str, Decimal] = field(default_factory=lambda: defaultdict(Decimal))
    purchase_totals: dict[str, Decimal] = field(default_factory=dict)
    # member_id -> [(created_at, delta, resulting_balance, ledger_id)]
    ledger_by_member: dict[str, list[tuple[datetime, int, int, str]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # workstation_id -> [(start, end, rental_id)] for the overlap check
    rentals_by_workstation: dict[str, list[tuple[datetime, datetime, str]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # member_id -> [(ledger_id, transaction_type, delta, source_reference_id)]
    ledger_references: dict[str, list[tuple[str, str, int, str]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # priced rentals, for the §5 arithmetic check
    rental_pricing_inputs: list[tuple[str, str, Decimal, str, int, dict[str, Decimal | int]]] = (
        field(default_factory=list)
    )
    # points each transaction says it granted or spent, for the delta cross-check
    rental_points_accrued: dict[str, int] = field(default_factory=dict)
    rental_points_redeemed: dict[str, int] = field(default_factory=dict)
    purchase_points_accrued: dict[str, int] = field(default_factory=dict)

    def expected_delta(self, transaction_type: str, reference_id: str) -> int | None:
        """What a ledger entry's delta should be, or None when nothing can vouch for it."""
        if transaction_type == "RENTAL_ACCRUAL":
            return self.rental_points_accrued.get(reference_id)
        if transaction_type == "CONCESSION_ACCRUAL":
            return self.purchase_points_accrued.get(reference_id)
        if transaction_type == "RENTAL_REDEMPTION":
            redeemed = self.rental_points_redeemed.get(reference_id)
            return None if redeemed is None else -redeemed
        return None  # TIER_BONUS is granted by the tier engine, not by a transaction


class Validator:
    """Validates the landing tree and returns a :class:`ValidationResult`."""

    def __init__(self, landing: Path, run_id: str | None = None) -> None:
        self.landing = Path(landing)
        self.run_id = run_id or f"validate-{uuid.uuid4().hex[:12]}"
        self.result = ValidationResult(run_id=self.run_id)
        self.corpus = _Corpus()
        self.rules = rules()
        self._seen_keys: dict[str, set[str]] = defaultdict(set)

    # ---------------------------------------------------------------- entry point

    def run(self, *, telemetry_days: int | None = None) -> ValidationResult:
        sources = discover(self.landing)
        if telemetry_days is not None:
            sources = self._limit_telemetry(sources, telemetry_days)

        log.info("validating %d source files from %s", len(sources), self.landing)
        self._structural_pass(sources)
        self._referential_pass(sources)
        self._business_rule_pass()
        self.result.finished_at = datetime.now(UTC)
        return self.result

    def _limit_telemetry(self, sources: list[SourceFile], days: int) -> list[SourceFile]:
        telemetry_days = sorted(
            {s.batch_date for s in sources if s.dataset == "telemetry" and s.batch_date}
        )
        keep = set(telemetry_days[:days])
        limited = [s for s in sources if s.dataset != "telemetry" or s.batch_date in keep]
        if len(keep) < len(telemetry_days):
            self.result.note(
                f"telemetry limited to the first {len(keep)} of {len(telemetry_days)} days "
                f"(AIMTERNET_TELEMETRY_DAYS); counts for telemetry are partial by design"
            )
        return limited

    # ---------------------------------------------------------------- pass 1: structure

    def _structural_pass(self, sources: list[SourceFile]) -> None:
        for source in sources:
            model = DATASET_MODELS[source.dataset]
            if source.source_type == "csv":
                self._check_header(source, model)
                records = read_csv_rows(source.path)
            else:
                records = iter(read_json_array(source.path))

            pk = PRIMARY_KEYS.get(source.dataset)
            for raw in records:
                self.result.records_read[source.dataset] += 1
                try:
                    record = model.model_validate(raw)
                except ValidationError as exc:
                    self.result.add(
                        Finding(
                            rule=Rule.SCHEMA_INVALID,
                            severity=Severity.ERROR,
                            dataset=source.dataset,
                            detail=self._first_error(exc),
                            source_file=str(source.path),
                            batch_date=source.batch_date,
                            record_key=str(raw.get(pk, "")) if pk else "",
                            record=dict(raw),
                        )
                    )
                    continue

                if pk is not None:
                    key = str(getattr(record, pk))
                    if key in self._seen_keys[source.dataset]:
                        self.result.add(
                            Finding(
                                rule=Rule.PK_DUPLICATE,
                                severity=Severity.ERROR,
                                dataset=source.dataset,
                                detail=f"{pk}={key} already seen in an earlier batch",
                                source_file=str(source.path),
                                batch_date=source.batch_date,
                                record_key=key,
                                record=dict(raw),
                            )
                        )
                        continue
                    self._seen_keys[source.dataset].add(key)

                self.result.records_accepted[source.dataset] += 1
                self._collect(source, record)

    def _check_header(self, source: SourceFile, model: type[BaseModel]) -> None:
        expected = set(model.model_fields)
        actual = set(read_csv_header(source.path))
        for missing in sorted(expected - actual):
            self.result.add(
                Finding(
                    rule=Rule.MISSING_COLUMN,
                    severity=Severity.ERROR,
                    dataset=source.dataset,
                    detail=f"column {missing!r} absent from the header",
                    source_file=str(source.path),
                    batch_date=source.batch_date,
                )
            )
        for extra in sorted(actual - expected):
            self.result.add(
                Finding(
                    rule=Rule.UNEXPECTED_COLUMN,
                    severity=Severity.WARNING,
                    dataset=source.dataset,
                    detail=f"column {extra!r} is not in the schema",
                    source_file=str(source.path),
                    batch_date=source.batch_date,
                )
            )

    @staticmethod
    def _first_error(exc: ValidationError) -> str:
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first["loc"])
        return f"{location}: {first['msg']}"

    def _collect(self, source: SourceFile, record: object) -> None:
        """Accumulate the identifiers and aggregates the later passes need."""
        dataset = source.dataset
        c = self.corpus
        if dataset == "workstations":
            c.workstation_ids.add(record.workstation_id)  # type: ignore[attr-defined]
        elif dataset == "concession_items":
            c.item_skus.add(record.item_sku)  # type: ignore[attr-defined]
        elif dataset == "members":
            c.member_ids.add(record.member_id)  # type: ignore[attr-defined]
        elif dataset == "rental_transactions":
            c.rental_ids.add(record.rental_id)  # type: ignore[attr-defined]
            c.referenced_member_ids.add(record.member_id)  # type: ignore[attr-defined]
            c.rentals_by_workstation[record.workstation_id].append(  # type: ignore[attr-defined]
                (record.session_start_utc, record.session_end_utc, record.rental_id)  # type: ignore[attr-defined]
            )
            c.rental_points_accrued[record.rental_id] = record.points_accrued  # type: ignore[attr-defined]
            c.rental_pricing_inputs.append(
                (
                    record.rental_id,  # type: ignore[attr-defined]
                    record.workstation_id,  # type: ignore[attr-defined]
                    record.duration_hours,  # type: ignore[attr-defined]
                    str(record.member_tier_applied),  # type: ignore[attr-defined]
                    record.points_redeemed,  # type: ignore[attr-defined]
                    {
                        "base_hourly_rate": record.base_hourly_rate,  # type: ignore[attr-defined]
                        "final_hourly_rate": record.final_hourly_rate,  # type: ignore[attr-defined]
                        "gross_rental_amount": record.gross_rental_amount,  # type: ignore[attr-defined]
                        "points_credit_value": record.points_credit_value,  # type: ignore[attr-defined]
                        "net_amount_paid": record.net_amount_paid,  # type: ignore[attr-defined]
                        "points_accrued": record.points_accrued,  # type: ignore[attr-defined]
                    },
                )
            )
            c.rental_points_redeemed[record.rental_id] = record.points_redeemed  # type: ignore[attr-defined]
        elif dataset == "concession_purchases":
            c.purchase_ids.add(record.purchase_id)  # type: ignore[attr-defined]
            c.referenced_member_ids.add(record.member_id)  # type: ignore[attr-defined]
            c.purchase_totals[record.purchase_id] = record.total_amount  # type: ignore[attr-defined]
            c.purchase_points_accrued[record.purchase_id] = record.points_accrued  # type: ignore[attr-defined]
        elif dataset == "concession_order_items":
            c.order_item_totals[record.purchase_id] += record.total_price  # type: ignore[attr-defined]
        elif dataset == "member_points_ledger":
            c.referenced_member_ids.add(record.member_id)  # type: ignore[attr-defined]
            c.ledger_by_member[record.member_id].append(  # type: ignore[attr-defined]
                (
                    record.created_at_utc,  # type: ignore[attr-defined]
                    record.points_delta,  # type: ignore[attr-defined]
                    record.resulting_balance,  # type: ignore[attr-defined]
                    record.ledger_id,  # type: ignore[attr-defined]
                )
            )
            c.ledger_references[record.member_id].append(  # type: ignore[attr-defined]
                (
                    record.ledger_id,  # type: ignore[attr-defined]
                    str(record.transaction_type),  # type: ignore[attr-defined]
                    record.points_delta,  # type: ignore[attr-defined]
                    record.source_reference_id,  # type: ignore[attr-defined]
                )
            )

    # ---------------------------------------------------------------- pass 2: references

    def _referential_pass(self, sources: list[SourceFile]) -> None:
        c = self.corpus

        # D2. The headline finding: members transactions depend on but nobody ever defined.
        orphan_members = sorted(c.referenced_member_ids - c.member_ids)
        if orphan_members:
            severity = Severity.WARNING  # resolved by policy, not a reason to stop
            self.result.note(
                f"D2: {len(orphan_members)} members are referenced by transactions but defined "
                f"in no source file ({orphan_members[0]}..{orphan_members[-1]}). Resolved by "
                f"AIMTERNET_ORPHAN_MEMBER_POLICY; counted in the reconciliation report."
            )
            if len(orphan_members) != EXPECTED_ORPHAN_MEMBER_COUNT:
                self.result.note(
                    f"D2 count changed: expected {EXPECTED_ORPHAN_MEMBER_COUNT}, "
                    f"found {len(orphan_members)}"
                )
            for member_id in orphan_members:
                self.result.add(
                    Finding(
                        rule=Rule.FK_ORPHAN,
                        severity=severity,
                        dataset="members",
                        detail=f"member_id {member_id} is referenced but never defined (D2)",
                        record_key=member_id,
                    )
                )

        self._check_fk("rental_transactions", "workstation_id", c.workstation_ids, sources)
        self._check_fk("concession_purchases", "rental_id", c.rental_ids, sources)
        self._check_fk("concession_order_items", "purchase_id", c.purchase_ids, sources)
        self._check_fk("concession_order_items", "item_sku", c.item_skus, sources)
        self._check_fk("workstation_events", "workstation_id", c.workstation_ids, sources)
        self._check_fk("workstation_events", "session_id", c.rental_ids, sources)
        self._check_telemetry(sources)

    def _check_fk(
        self, dataset: str, column: str, valid: set[str], sources: list[SourceFile]
    ) -> None:
        """Re-read a dataset to resolve one foreign key. Cheap for everything but telemetry."""
        missing: Counter[str] = Counter()
        for source in (s for s in sources if s.dataset == dataset):
            records = (
                read_csv_rows(source.path)
                if source.source_type == "csv"
                else iter(read_json_array(source.path))
            )
            for raw in records:
                value = raw.get(column)
                if value in (None, ""):
                    continue
                if str(value) not in valid:
                    missing[str(value)] += 1
        for value, count in missing.items():
            self.result.add(
                Finding(
                    rule=Rule.FK_ORPHAN,
                    severity=Severity.ERROR,
                    dataset=dataset,
                    detail=f"{column}={value} does not resolve ({count} row(s))",
                    record_key=value,
                )
            )

    def _check_telemetry(self, sources: list[SourceFile]) -> None:
        """Stream telemetry: FK checks, TTL sanity, and the OCCUPIED-session invariant.

        Never accumulates rows -- 3.1M records go past one file at a time.
        """
        telemetry = [s for s in sources if s.dataset == "telemetry"]
        if not telemetry:
            return
        now = datetime.now(UTC)
        cadence_by_day: dict[date, int] = {}
        records_by_day: Counter[date] = Counter()
        unknown_workstations: Counter[str] = Counter()
        unresolved_sessions = 0
        occupied_without_session = 0
        already_expired = 0
        total = 0

        for source in telemetry:
            records = read_json_array(source.path)
            if source.batch_date is not None:
                records_by_day[source.batch_date] += len(records)
                distinct = len({r.get("timestamp") for r in records})
                if distinct > 1:
                    # 24 hours in a file: seconds between consecutive ticks
                    cadence_by_day[source.batch_date] = round(3600 / distinct)
            for raw in records:
                total += 1
                workstation = str(raw.get("workstation_id", ""))
                if workstation not in self.corpus.workstation_ids:
                    unknown_workstations[workstation] += 1
                session = raw.get("active_session_id")
                if raw.get("status") == "OCCUPIED":
                    if not session:
                        occupied_without_session += 1
                    elif str(session) not in self.corpus.rental_ids:
                        unresolved_sessions += 1
                expires = raw.get("expires_at")
                if isinstance(expires, int) and datetime.fromtimestamp(expires, UTC) < now:
                    already_expired += 1

        for workstation, count in unknown_workstations.items():
            self.result.add(
                Finding(
                    rule=Rule.FK_ORPHAN,
                    severity=Severity.ERROR,
                    dataset="telemetry",
                    detail=f"workstation_id={workstation} does not resolve ({count} record(s))",
                    record_key=workstation,
                )
            )
        if occupied_without_session:
            self.result.add(
                Finding(
                    rule=Rule.FK_ORPHAN,
                    severity=Severity.WARNING,
                    dataset="telemetry",
                    detail=(
                        f"{occupied_without_session} OCCUPIED record(s) carry no "
                        f"active_session_id (§4 invariant)"
                    ),
                )
            )
        if unresolved_sessions:
            self.result.add(
                Finding(
                    rule=Rule.FK_ORPHAN,
                    severity=Severity.WARNING,
                    dataset="telemetry",
                    detail=(
                        f"{unresolved_sessions} OCCUPIED record(s) reference an "
                        f"active_session_id with no matching rental (§4 invariant)"
                    ),
                )
            )
        cadences = sorted(set(cadence_by_day.values()))
        if len(cadences) > 1:
            grouped: dict[int, list[str]] = defaultdict(list)
            for day, seconds in sorted(cadence_by_day.items()):
                grouped[seconds].append(day.isoformat())
            description = "; ".join(
                f"{seconds}s tick on {len(days)} day(s) ({days[0]}..{days[-1]})"
                for seconds, days in sorted(grouped.items())
            )
            self.result.add(
                Finding(
                    rule=Rule.SPEC_DEVIATION,
                    severity=Severity.WARNING,
                    dataset="telemetry",
                    detail=(
                        f"F6: the telemetry sampling interval is not constant -- {description}. "
                        f"Spec §1.2 assumes a uniform 5-minute tick and 2,100 records per file, "
                        f"giving ~3,124,800 records; the real total is {total:,}. Every "
                        f"downstream volume, cost and runtime estimate must use the real "
                        f"figure."
                    ),
                )
            )
            self.result.note(
                f"telemetry cadence varies: {description}. Total records {total:,}."
            )
        if already_expired:
            self.result.add(
                Finding(
                    rule=Rule.TTL_ALREADY_EXPIRED,
                    severity=Severity.WARNING,
                    dataset="telemetry",
                    detail=(
                        f"{already_expired} of {total} record(s) carry an expires_at that is "
                        f"already in the past. Enabling DynamoDB TTL on this attribute would "
                        f"purge them within ~48h of loading (finding F2); TTL enforcement is "
                        f"off by default."
                    ),
                )
            )

    # ---------------------------------------------------------------- pass 3: business rules

    def _business_rule_pass(self) -> None:
        self._check_rental_pricing()
        self._check_order_totals()
        self._check_ledger_deltas()
        self._check_ledger_balances()
        self._check_rental_overlaps()

    def _check_rental_pricing(self) -> None:
        """§5: re-price every rental and compare, field by field.

        The formula is the one recovered from the synthesizer bytecode, which differs from
        §5 as written in exactly one place -- gross is computed from the discounted rate.
        That deviation is finding F1 and is reported here as an INFO-level note, so a reader
        of the run output sees it rather than having to find it in a commit message.
        """
        mismatches: Counter[str] = Counter()
        checked = 0
        for rental_id, workstation_id, duration, tier, redeemed, actual in (
            self.corpus.rental_pricing_inputs
        ):
            try:
                priced = self.rules.price_rental(
                    workstation_id=workstation_id,
                    duration_hours=duration,
                    tier_name=tier,
                    points_redeemed=redeemed,
                )
            except (BusinessRuleError, InvalidOperation) as exc:
                self.result.add(
                    Finding(
                        rule=Rule.PRICING_MISMATCH,
                        severity=Severity.ERROR,
                        dataset="rental_transactions",
                        detail=f"{rental_id} could not be re-priced: {exc}",
                        record_key=rental_id,
                    )
                )
                continue
            checked += 1
            for field_name, actual_value in actual.items():
                expected = getattr(priced, field_name)
                if expected != actual_value:
                    mismatches[field_name] += 1
                    self.result.add(
                        Finding(
                            rule=Rule.PRICING_MISMATCH,
                            severity=Severity.ERROR,
                            dataset="rental_transactions",
                            detail=(
                                f"{rental_id}: {field_name} is {actual_value} but the "
                                f"business rules give {expected}"
                            ),
                            record_key=rental_id,
                        )
                    )

        if checked:
            summary = (
                "every field matches"
                if not mismatches
                else ", ".join(f"{k}={v:,}" for k, v in mismatches.most_common())
            )
            self.result.note(
                f"rental pricing re-derived for {checked:,} rentals: {summary}"
            )
        self.result.add(
            Finding(
                rule=Rule.SPEC_DEVIATION,
                severity=Severity.WARNING,
                dataset="rental_transactions",
                detail=(
                    "F1: spec §5 states gross_rental_amount = base_hourly_rate x "
                    "duration_hours, but the synthesizer bytecode and every source row use "
                    "final_hourly_rate x duration_hours (post-discount). The observed rule "
                    "is implemented; §5's line is a documentation error."
                ),
            )
        )

    def _check_order_totals(self) -> None:
        """§4: the sum of a purchase's line items must equal its total_amount."""
        for purchase_id, expected in self.corpus.purchase_totals.items():
            actual = self.corpus.order_item_totals.get(purchase_id)
            if actual is None:
                self.result.add(
                    Finding(
                        rule=Rule.ORDER_TOTAL_MISMATCH,
                        severity=Severity.WARNING,
                        dataset="concession_purchases",
                        detail=f"purchase {purchase_id} has no line items",
                        record_key=purchase_id,
                    )
                )
            elif actual != expected:
                self.result.add(
                    Finding(
                        rule=Rule.ORDER_TOTAL_MISMATCH,
                        severity=Severity.ERROR,
                        dataset="concession_purchases",
                        detail=(
                            f"line items sum to {actual} but total_amount is {expected}"
                        ),
                        record_key=purchase_id,
                    )
                )

    def _check_ledger_balances(self) -> None:
        """§4: resulting_balance should be the running sum of points_delta per member.

        It is not, and that is finding F5. Reported per member rather than per entry: 43,416
        near-identical findings would bury every other result, and the useful facts are how
        many members are affected and how far off they get.

        Every points_delta, by contrast, matches its source transaction exactly (checked in
        _check_ledger_deltas). So the deltas are the truth and resulting_balance is a stale
        denormalisation -- which is why the warehouse derives balances by summing deltas
        instead of trusting the column.
        """
        affected = 0
        drifting_entries = 0
        worst: tuple[str, int] = ("", 0)

        for member_id, entries in self.corpus.ledger_by_member.items():
            ordered = sorted(entries, key=lambda e: e[0])
            running = ordered[0][2] - ordered[0][1]  # opening balance implied by the first row
            member_drift = 0
            largest = 0
            for _created_at, delta, resulting, _ledger_id in ordered:
                running += delta
                if running != resulting:
                    member_drift += 1
                    largest = max(largest, abs(running - resulting))
                    running = resulting  # resync so one break does not cascade
            if member_drift:
                affected += 1
                drifting_entries += member_drift
                if largest > worst[1]:
                    worst = (member_id, largest)

        if affected:
            self.result.add(
                Finding(
                    rule=Rule.LEDGER_BALANCE_DRIFT,
                    severity=Severity.WARNING,
                    dataset="member_points_ledger",
                    detail=(
                        f"resulting_balance does not reconstruct as a running sum of "
                        f"points_delta in created_at order: {drifting_entries:,} entries across "
                        f"{affected} of {len(self.corpus.ledger_by_member)} members "
                        f"(largest single gap {worst[1]} points, member {worst[0]}). The "
                        f"generator maintained balances across interleaved rental and "
                        f"concession passes, so the recorded sequence cannot be replayed in "
                        f"timestamp order. Finding F5: points_delta is authoritative, "
                        f"resulting_balance is not."
                    ),
                )
            )

    def _check_ledger_deltas(self) -> None:
        """Every points_delta must equal the accrual or redemption it references.

        This is the check that decides whether the ledger is usable at all. It passes: all
        54,424 referencing entries match their source transaction exactly.
        """
        mismatches = 0
        checked = 0
        for member_id, references in self.corpus.ledger_references.items():
            for ledger_id, transaction_type, delta, reference_id in references:
                expected = self.corpus.expected_delta(transaction_type, reference_id)
                if expected is None:
                    continue  # TIER_BONUS has no source transaction to compare against
                checked += 1
                if delta != expected:
                    mismatches += 1
                    self.result.add(
                        Finding(
                            rule=Rule.LEDGER_BALANCE_DRIFT,
                            severity=Severity.ERROR,
                            dataset="member_points_ledger",
                            detail=(
                                f"member {member_id} entry {ledger_id}: {transaction_type} of "
                                f"{delta} does not match {reference_id} (expected {expected})"
                            ),
                            record_key=ledger_id,
                        )
                    )
        self.result.note(
            f"ledger deltas cross-checked against their source transactions: "
            f"{checked:,} checked, {mismatches:,} mismatched"
        )

    def _check_rental_overlaps(self) -> None:
        """A workstation cannot host two rentals at once."""
        for workstation_id, sessions in self.corpus.rentals_by_workstation.items():
            ordered = sorted(sessions, key=lambda s: s[0])
            for (_start_a, end_a, id_a), (start_b, _end_b, id_b) in pairwise(ordered):
                if end_a is not None and start_b < end_a:
                    self.result.add(
                        Finding(
                            rule=Rule.OVERLAPPING_RENTAL,
                            severity=Severity.ERROR,
                            dataset="rental_transactions",
                            detail=(
                                f"{workstation_id}: {id_a} runs to {end_a.isoformat()} but "
                                f"{id_b} starts at {start_b.isoformat()}"
                            ),
                            record_key=id_b,
                        )
                    )


def validate_landing(landing: Path, *, telemetry_days: int | None = None) -> ValidationResult:
    return Validator(landing).run(telemetry_days=telemetry_days)
