"""Phase 7 — reconciliation across every hop (spec §6.6).

Counts are compared source -> Bronze -> validated -> RDS -> DynamoDB -> Silver -> Gold ->
Redshift, and integrity is checked inside each store. Two rules govern the output:

* **Critical checks fail the run.** A count that does not add up means data was lost, and the
  DAG should stop rather than build on it.
* **Warnings are reported, never swallowed.** The 840-member backfill and findings F1-F6 are
  expected discrepancies with known explanations. They appear in the report every time, with
  their explanation attached, so "no unexplained discrepancy" is something a reader can
  verify rather than take on trust.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from aimternet.config.poc_policy import EXPECTED_ORPHAN_MEMBER_COUNT, policy
from aimternet.config.settings import settings

#: Rows the API created. They are real business, not drift, and must not be compared against
#: the source files -- a POS that never adds a row is a POS nobody is using. Defined once, by
#: the loader that writes it.
from aimternet.pipeline.loaders.rds import API_RUN_ID

log = logging.getLogger(__name__)

# Verified source-of-truth counts (spec §1.2, corrected for finding F6).
SOURCE_COUNTS = {
    "workstations": 175,
    "concession_items": 10,
    "dim_date": 365,
    "dim_time": 1_440,
    "members": 360,
    "rental_transactions": 28_287,
    "concession_purchases": 21_077,
    "concession_order_items": 29_672,
    "member_points_ledger": 55_514,
    "workstation_events": 58_218,
    "telemetry": 6_300_000,
}

SEVERITY_CRITICAL = "CRITICAL"
SEVERITY_WARNING = "WARNING"
SEVERITY_INFO = "INFO"


@dataclass
class Check:
    name: str
    layer_from: str
    layer_to: str
    expected: float | None
    actual: float | None
    passed: bool
    severity: str = SEVERITY_CRITICAL
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "check_name": self.name,
            "layer_from": self.layer_from,
            "layer_to": self.layer_to,
            "expected_value": None if self.expected is None else float(self.expected),
            "actual_value": None if self.actual is None else float(self.actual),
            "passed": self.passed,
            "severity": self.severity,
            "detail": self.detail,
        }


@dataclass
class ReconciliationReport:
    run_id: str
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    checks: list[Check] = field(default_factory=list)
    duration_seconds: float = 0.0
    skipped_layers: list[str] = field(default_factory=list)

    def add(self, check: Check) -> None:
        self.checks.append(check)

    @property
    def critical_failures(self) -> list[Check]:
        return [c for c in self.checks if not c.passed and c.severity == SEVERITY_CRITICAL]

    @property
    def warnings(self) -> list[Check]:
        return [c for c in self.checks if not c.passed and c.severity == SEVERITY_WARNING]

    @property
    def passed(self) -> bool:
        """A run passes only if every critical check ran *and* passed.

        A skipped layer used not to count. That made every guarantee in this file
        conditional on nothing having gone wrong on the way to checking it: one S3 blip in
        `_silver_gold_counts` removed all eleven `silver_rows:*` checks, every `gold_rows:*`
        check, the `gold_rows:dim_member` F7 guard and every CRITICAL `silver_snapshot:*`
        check at once -- and the report went green, because a check that was never added
        cannot fail. "We could not look" is not the same answer as "we looked and it was
        fine", and only one of them should let a DAG proceed.
        """
        return not self.critical_failures and not self.skipped_layers

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "generated_at": self.generated_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "passed": self.passed,
            "counts": {
                "checks": len(self.checks),
                "critical_failures": len(self.critical_failures),
                "warnings": len(self.warnings),
            },
            "skipped_layers": self.skipped_layers,
            "policy": policy().as_report(),
            "checks_detail": [c.as_dict() for c in self.checks],
        }

    # ---------------------------------------------------------------- rendering

    def to_markdown(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        lines = [
            "# AIMternet-Cafe reconciliation report",
            "",
            f"**Run:** `{self.run_id}`  ",
            f"**Generated:** {self.generated_at.isoformat()}  ",
            f"**Result:** **{status}** — {len(self.critical_failures)} critical failure(s), "
            f"{len(self.warnings)} warning(s) across {len(self.checks)} check(s)  ",
            f"**Elapsed:** {self.duration_seconds:.1f}s",
            "",
        ]
        if self.skipped_layers:
            lines += [
                "> Layers not reachable during this run, and therefore not compared: "
                + ", ".join(self.skipped_layers),
                "",
            ]

        if self.critical_failures:
            lines += ["## Critical failures", "", "| check | expected | actual | detail |",
                      "|---|---:|---:|---|"]
            for c in self.critical_failures:
                lines.append(f"| {c.name} | {c.expected} | {c.actual} | {c.detail} |")
            lines.append("")

        lines += [
            "## Row counts across every hop", "",
            "| check | from | to | expected | actual | ok |",
            "|---|---|---|---:|---:|:--:|",
        ]
        for c in self.checks:
            if c.layer_from and c.layer_to:
                mark = "yes" if c.passed else "**NO**"
                lines.append(
                    f"| {c.name} | {c.layer_from} | {c.layer_to} | "
                    f"{c.expected if c.expected is not None else ''} | "
                    f"{c.actual if c.actual is not None else ''} | {mark} |"
                )
        lines.append("")

        integrity = [c for c in self.checks if not (c.layer_from and c.layer_to)]
        if integrity:
            lines += ["## Integrity checks", "", "| check | result | severity | detail |",
                      "|---|:--:|---|---|"]
            for c in integrity:
                lines.append(
                    f"| {c.name} | {'ok' if c.passed else 'FAIL'} | {c.severity} | {c.detail} |"
                )
            lines.append("")

        if self.warnings:
            lines += [
                "## Explained discrepancies",
                "",
                "These are warnings, not failures: each one is a known property of the source "
                "data with a documented explanation. They are reported on every run rather "
                "than suppressed.",
                "",
            ]
            for c in self.warnings:
                lines.append(f"- **{c.name}** — {c.detail}")
            lines.append("")

        lines += ["## POC policy in force", ""]
        for assumption in policy().assumptions:
            lines.append(f"### {assumption.key} ({assumption.spec_reference})")
            lines.append(f"- **Decision:** {assumption.decision}")
            lines.append(f"- **Why:** {assumption.rationale}")
            if assumption.evidence:
                lines.append(f"- **Evidence:** {assumption.evidence}")
            lines.append("")
        return "\n".join(lines)


# --------------------------------------------------------------------------- collectors


def _bronze_counts() -> dict[str, int]:
    from aimternet.io.s3 import S3Client

    cfg = settings()
    keys = S3Client().list_objects(f"{cfg.s3_bronze_prefix}/")
    counts: dict[str, int] = {}
    for key in keys:
        parts = key.split("/")
        if len(parts) >= 2:
            counts[parts[1]] = counts.get(parts[1], 0) + 1
    return counts


def _rds_counts() -> tuple[dict[str, int], dict[str, int]]:
    """(rows loaded from the source files, rows created by the API), per table.

    Comparing a live operational store against the source files only makes sense for the
    rows that came from those files. Splitting on the lineage ``run_id`` keeps the
    bootstrap check exact while letting the business carry on.
    """
    from aimternet.db.session import fetch_all
    from aimternet.pipeline.loaders.rds import LOAD_ORDER

    bootstrap: dict[str, int] = {}
    operational: dict[str, int] = {}
    for table in LOAD_ORDER:
        rows = fetch_all(
            f"""SELECT
                    count(*) FILTER (WHERE run_id IS DISTINCT FROM %s) AS bootstrap,
                    count(*) FILTER (WHERE run_id = %s)                AS operational
                FROM {table}""",
            (API_RUN_ID, API_RUN_ID),
        )
        bootstrap[table] = int(rows[0]["bootstrap"])
        operational[table] = int(rows[0]["operational"])
    return bootstrap, operational


def _snapshot_expectations() -> dict[str, int]:
    """Rows each operational table held *at the instant its snapshot was exported*.

    The snapshot check has to compare two descriptions of the same moment. Silver was written
    at the export watermark; counting RDS *now* instead makes every row the POS has created
    since read as a row the export lost. That fails whenever the cafe is open, and -- worse --
    it cannot tell the failure it exists to catch (F7: the export left a delta where a snapshot
    belongs) from someone having bought a coffee thirty seconds ago. A check that cannot
    distinguish broken from normal is not a check.

    Falls back to the live count for a table with no watermark yet: before the first
    incremental export, the snapshot is simply the whole table.
    """
    from aimternet.db.session import fetch_all
    from aimternet.pipeline.curate.export_rds import EXPORTS

    expected: dict[str, int] = {}
    for table, (watermark_column, _key) in EXPORTS.items():
        marks = fetch_all(
            "SELECT watermark_value FROM pipeline_watermark WHERE pipeline_name = %s",
            (f"rds_to_s3:{table}",),
        )
        if marks:
            rows = fetch_all(
                f"SELECT count(*) AS n FROM {table} WHERE {watermark_column} <= %s",
                (marks[0]["watermark_value"],),
            )
        else:
            rows = fetch_all(f"SELECT count(*) AS n FROM {table}")
        expected[table] = int(rows[0]["n"])
    return expected


# Snapshots the RDS -> S3 export writes into Silver, and the RDS table each mirrors. Gold
# reads these as the *current* state of the operational store, so a delta left here in place
# of a snapshot silently shrinks a dimension. That is not hypothetical: the first incremental
# run after the bootstrap did exactly that to members_operational.
def _operational_snapshots() -> dict[str, str]:
    """Derived from ``EXPORTS``, never restated.

    This was a hand-written dict, and a hand-written dict is how F8 happened twice: the
    Redshift expectations were missing ``dim_member``, so the check that would have caught a
    stale dimension passed unconditionally. A second list of the same tables is a second
    place to forget one. Deriving it means a table added to the export cannot be added
    without also acquiring its ``silver_snapshot:*`` check.
    """
    from aimternet.pipeline.curate.export_rds import EXPORTS

    return {f"{table}_operational": table for table in EXPORTS}


OPERATIONAL_SNAPSHOTS = _operational_snapshots()

#: Facts built from two origins: the Bronze-derived Silver dataset, and an operational
#: snapshot of what the POS has written since. ``(bronze dataset, snapshot, primary key)``.
#:
#: These counts used to be literals -- 28,287 rentals, 21,077 purchases -- and the literals
#: were right only because Gold could not see the POS at all. The moment a rental rung up at
#: the till reaches the warehouse, a constant here is a check that fails whenever the cafe is
#: open. Expected is therefore the verified Bronze count plus what the snapshot contributes.
UNIONED_FACTS = {
    "fact_rental": ("rental_transactions", "rental_transactions_operational", "rental_id"),
    "fact_concession_sale": (
        "concession_purchases", "concession_purchases_operational", "purchase_id",
    ),
    "fact_concession_line_item": (
        "concession_order_items", "concession_order_items_operational", "order_item_id",
    ),
    "fact_points_activity": (
        "member_points_ledger", "member_points_ledger_operational", "ledger_id",
    ),
    "fact_workstation_event": (
        "workstation_events", "workstation_events_operational", "event_id",
    ),
}


#: The DynamoDB export's equivalent. It has no RDS table to be compared against -- the events
#: it holds exist only in DynamoDB and in this file -- so the check that guards it is that it
#: never shrinks. It was written by the delta rather than the snapshot for its whole life
#: before that check existed, losing every event older than one hour on every run.
EVENTS_SNAPSHOT = "workstation_events_operational"


def _high_water_mark(check_name: str) -> int:
    """The largest ``actual`` this check has ever recorded.

    Every check is already persisted to ``reconciliation_results``, so a snapshot that has no
    table to be compared against can still be held to the one property that matters: it must
    never be smaller than it has been. No new control table, no migration -- just a read of
    what earlier runs wrote.
    """
    from aimternet.db.session import fetch_all

    rows = fetch_all(
        "SELECT max(actual_value) AS high FROM reconciliation_results WHERE check_name = %s",
        (check_name,),
    )
    high = rows[0]["high"] if rows else None
    return int(high) if high is not None else 0


def _snapshot_predicate(fact: str) -> str:
    """Gold's eligibility rule for ``fact``; empty for the event snapshot, which has none."""
    from aimternet.pipeline.curate.gold import snapshot_predicate

    bronze_dataset, _snapshot, _key = UNIONED_FACTS[fact]
    return snapshot_predicate(bronze_dataset)


def _contributions(con: object) -> tuple[dict[str, int], dict[str, int]]:
    """Per unioned fact: (rows the snapshot adds beyond Bronze, rows it shares with Bronze).

    Both halves earn their keep. The first is what an expectation adds to the verified Bronze
    count. The second is a second, independent F7 detector: the RDS-backed snapshots mirror
    the very tables Bronze was loaded into, so the overlap *must* be the whole source count.
    A snapshot holding a delta where a snapshot belongs shows up here as an overlap far below
    it -- without a watermark, without an RDS connection, and without any history to compare
    against.
    """
    from aimternet.pipeline.curate.engine import count_parquet, layer_uri

    contributed: dict[str, int] = {}
    overlap: dict[str, int] = {}
    for fact, (bronze, snapshot, key) in UNIONED_FACTS.items():
        uri = layer_uri("silver", snapshot)
        if not count_parquet(con, uri):  # type: ignore[arg-type]
            contributed[fact], overlap[fact] = 0, 0
            continue
        bronze_read = f"read_parquet('{layer_uri('silver', bronze)}/**/*.parquet')"
        # The same eligibility rule Gold applies, read from Gold rather than restated: it
        # excludes open rentals, and counting them here would fail this check by exactly the
        # number of customers currently at a machine.
        predicate = _snapshot_predicate(fact)
        eligible = f"WHERE {predicate}" if predicate else ""
        row = con.execute(  # type: ignore[attr-defined]
            f"""
            SELECT count(*) FILTER (WHERE NOT in_bronze) AS contributed,
                   count(*) FILTER (WHERE in_bronze)     AS overlap
            FROM (
                SELECT EXISTS (
                    SELECT 1 FROM {bronze_read} b WHERE b.{key} = o.{key}
                ) AS in_bronze
                FROM (SELECT * FROM read_parquet('{uri}/**/*.parquet') {eligible}) o
            )
            """
        ).fetchone()
        contributed[fact] = int(row[0]) if row else 0
        overlap[fact] = int(row[1]) if row else 0
    return contributed, overlap


def _expected_from(contributed: dict[str, int]) -> dict[str, int]:
    """Bronze plus contribution, for each unioned fact. The formula lives here only."""
    return {
        fact: SOURCE_COUNTS[bronze_dataset] + contributed.get(fact, 0)
        for fact, (bronze_dataset, _snapshot, _key) in UNIONED_FACTS.items()
    }


def unioned_fact_expectations() -> dict[str, int]:
    """What each two-origin fact should hold, read from S3.

    Public because the integration tests need the same number, and the alternative -- each of
    them keeping its own copy of 28,287 and 21,077 -- is the frozen-literal habit that made
    the POS gap invisible in the first place. Those tests are `aws`/`redshift`-marked and do
    not run in `make check`, so a literal there would rot in silence.
    """
    from aimternet.pipeline.curate.engine import duck

    with duck() as con:
        contributed, _overlap = _contributions(con)
    return _expected_from(contributed)


def _silver_gold_counts() -> tuple[dict[str, int], dict[str, int], dict[str, int], dict[str, int]]:
    from aimternet.pipeline.curate.engine import count_parquet, duck, layer_uri

    silver_names = [*SOURCE_COUNTS, *OPERATIONAL_SNAPSHOTS, EVENTS_SNAPSHOT]
    gold_names = [
        "dim_member", "dim_workstation", "dim_date", "dim_time", "dim_concession_item",
        "fact_rental", "fact_concession_sale", "fact_concession_line_item",
        "fact_points_activity", "fact_workstation_event",
        "agg_workstation_utilization_hourly",
    ]
    with duck() as con:
        silver = {n: count_parquet(con, layer_uri("silver", n)) for n in silver_names}
        gold = {n: count_parquet(con, layer_uri("gold", n)) for n in gold_names}
        contributed, overlap = _contributions(con)
    return silver, gold, contributed, overlap


def _dynamodb_counts() -> dict[str, int]:
    """Item counts from the checkpoint ledger.

    DynamoDB's own ``ItemCount`` updates roughly every six hours, so it is useless for
    reconciling a load that just finished. The checkpoints record what was actually written,
    file by file.
    """
    from aimternet.pipeline.loaders import checkpoint

    return {
        "workstation_events": checkpoint.total_records("dynamodb_events"),
        "telemetry": checkpoint.total_records("dynamodb_telemetry"),
    }


def _redshift_counts() -> dict[str, int]:
    from aimternet.pipeline.loaders.redshift import table_counts

    return table_counts()


# --------------------------------------------------------------------------- integrity


def _rds_integrity(report: ReconciliationReport) -> None:
    from aimternet.db.session import fetch_all

    checks: list[tuple[str, str, str, str]] = [
        ("duplicate_rental_ids", SEVERITY_CRITICAL,
         "SELECT count(*) AS n FROM (SELECT rental_id FROM rental_transactions "
         "GROUP BY 1 HAVING count(*) > 1) d", "rental_id must be unique"),
        ("orphan_rental_members", SEVERITY_CRITICAL,
         "SELECT count(*) AS n FROM rental_transactions r LEFT JOIN members m "
         "USING (member_id) WHERE m.member_id IS NULL", "every rental must resolve a member"),
        ("orphan_order_items", SEVERITY_CRITICAL,
         "SELECT count(*) AS n FROM concession_order_items i LEFT JOIN concession_purchases p "
         "USING (purchase_id) WHERE p.purchase_id IS NULL", "line items must resolve a purchase"),
        ("negative_money", SEVERITY_CRITICAL,
         "SELECT count(*) AS n FROM rental_transactions WHERE net_amount_paid < 0 "
         "OR gross_rental_amount < 0", "no negative monetary values"),
        ("null_money_on_closed_rentals", SEVERITY_CRITICAL,
         "SELECT count(*) AS n FROM rental_transactions WHERE session_end_utc IS NOT NULL "
         "AND net_amount_paid IS NULL", "a closed rental must be priced"),
        ("rentals_ending_before_start", SEVERITY_CRITICAL,
         "SELECT count(*) AS n FROM rental_transactions "
         "WHERE session_end_utc < session_start_utc", "session_end must not precede start"),
        # Only bootstrap rows are held to the simulation window. A rental the POS creates
        # today is correctly outside it -- that is the system being used, not data drift.
        ("timestamps_outside_window", SEVERITY_CRITICAL,
         "SELECT count(*) AS n FROM rental_transactions "
         "WHERE run_id IS DISTINCT FROM 'api' AND (session_start_utc < "
         "'2026-06-30T16:00:00+00'::timestamptz OR session_start_utc > "
         "'2026-09-01T00:00:00+00'::timestamptz)",
         "bootstrap-loaded rentals must fall inside 2026-07-01..2026-08-31 Manila time"),
        ("overlapping_active_rentals", SEVERITY_CRITICAL,
         "SELECT count(*) AS n FROM (SELECT workstation_id FROM rental_transactions "
         "WHERE session_end_utc IS NULL GROUP BY 1 HAVING count(*) > 1) d",
         "a workstation may host at most one open rental"),
        ("negative_inventory", SEVERITY_CRITICAL,
         "SELECT count(*) AS n FROM concession_items WHERE stock_quantity < 0",
         "inventory must never go negative"),
    ]
    for name, severity, sql, detail in checks:
        actual = int(fetch_all(sql)[0]["n"])
        report.add(
            Check(
                name=name, layer_from="", layer_to="", expected=0, actual=actual,
                passed=actual == 0, severity=severity, detail=detail,
            )
        )

    backfilled = int(
        fetch_all("SELECT count(*) AS n FROM members WHERE is_backfilled")[0]["n"]
    )
    report.add(
        Check(
            name="d2_backfilled_members", layer_from="", layer_to="",
            expected=EXPECTED_ORPHAN_MEMBER_COUNT, actual=backfilled,
            passed=backfilled == EXPECTED_ORPHAN_MEMBER_COUNT,
            severity=SEVERITY_WARNING,
            detail=(
                f"{backfilled} members were referenced by transactions but defined in no "
                f"source file, and were backfilled under the "
                f"{policy().orphan_member_policy} policy (finding D2). Expected exactly "
                f"{EXPECTED_ORPHAN_MEMBER_COUNT}."
            ),
        )
    )

    drift = fetch_all(
        """SELECT count(*) AS n FROM (
             SELECT m.member_id FROM members m
             JOIN member_points_ledger l USING (member_id)
             GROUP BY m.member_id, m.current_points_balance
             HAVING m.current_points_balance <> sum(l.points_delta)
           ) d"""
    )[0]["n"]
    report.add(
        Check(
            name="points_balance_drift", layer_from="", layer_to="", expected=0,
            actual=int(drift), passed=int(drift) == 0, severity=SEVERITY_WARNING,
            detail=(
                "members.current_points_balance is a registration-time snapshot for source "
                "members and an inferred opening balance for backfilled ones, so it is not "
                "expected to equal the sum of their ledger deltas. The warehouse derives "
                "balances by summing points_delta instead (finding F5)."
            ),
        )
    )


def _known_findings(report: ReconciliationReport) -> None:
    """Findings that are always reported so the run output carries its own explanation."""
    for name, detail in (
        (
            "f1_gross_uses_discounted_rate",
            "Spec §5 states gross_rental_amount = base_hourly_rate x duration_hours. The "
            "synthesizer bytecode and all 28,287 source rows use final_hourly_rate x "
            "duration_hours. The observed rule is implemented and reproduces every field of "
            "every historical rental exactly.",
        ),
        (
            "f6_telemetry_cadence_varies",
            "Telemetry is sampled every 300s on 2026-07-01..08-24 and every 30s on "
            "2026-08-25..08-31, so the real total is 6,300,000 records rather than the "
            "~3,124,800 §1.2 projects from a uniform tick. All counts here use the real "
            "figure.",
        ),
        (
            "f2_telemetry_ttl_already_expired",
            "Source expires_at values are timestamp + 7 days and the data window is in the "
            "past, so DynamoDB TTL is written but left disabled; enabling it would purge "
            "about 61 of 62 days within ~48h of loading.",
        ),
    ):
        report.add(
            Check(
                name=name, layer_from="", layer_to="", expected=None, actual=None,
                passed=False, severity=SEVERITY_WARNING, detail=detail,
            )
        )


# --------------------------------------------------------------------------- entry point


def reconcile(run_id: str = "", *, include_redshift: bool = True) -> ReconciliationReport:
    started = time.time()
    run_id = run_id or f"reconcile-{int(started)}"
    report = ReconciliationReport(run_id=run_id)

    # ---- source -> Bronze (object counts, since Bronze is byte-identical files)
    try:
        bronze = _bronze_counts()
        expected_objects = {
            "workstations": 1, "concession_items": 1, "dim_date": 1, "dim_time": 1,
            "members": 62, "rental_transactions": 62, "concession_purchases": 62,
            "concession_order_items": 62, "member_points_ledger": 62,
            "workstation_events": 62, "telemetry": 1_488,
        }
        for dataset, expected in expected_objects.items():
            actual = bronze.get(dataset, 0)
            report.add(
                Check(
                    name=f"bronze_objects:{dataset}", layer_from="source", layer_to="bronze",
                    expected=expected, actual=actual, passed=actual == expected,
                    detail="Bronze holds one object per source file",
                )
            )
    except Exception as exc:
        report.skipped_layers.append(f"bronze ({exc})")

    # ---- source -> RDS
    # Hoisted: the Silver/Gold block compares dimensions against these, and must not blow up
    # with a NameError if RDS is unreachable — it should just skip those checks.
    rds_bootstrap: dict[str, int] = {}
    rds_operational: dict[str, int] = {}
    snapshot_expected: dict[str, int] = {}
    try:
        rds_bootstrap, rds_operational = _rds_counts()
        snapshot_expected = _snapshot_expectations()
        for table, actual in rds_bootstrap.items():
            expected = SOURCE_COUNTS.get(table, 0)
            if table == "members":
                expected += EXPECTED_ORPHAN_MEMBER_COUNT  # D2 backfill is expected, not drift
            report.add(
                Check(
                    name=f"rds_rows:{table}", layer_from="source", layer_to="rds",
                    expected=expected, actual=actual, passed=actual == expected,
                    detail=(
                        "360 source members plus the 840 D2 backfills"
                        if table == "members"
                        else "rows loaded from the source files must match it exactly"
                    ),
                )
            )
        created = sum(rds_operational.values())
        if created:
            report.add(
                Check(
                    name="rds_rows:created_by_the_api", layer_from="", layer_to="",
                    expected=None, actual=created, passed=True, severity=SEVERITY_INFO,
                    detail=(
                        "rows the POS created since the bootstrap, counted separately from "
                        "the source comparison: "
                        + ", ".join(
                            f"{table} +{n}" for table, n in rds_operational.items() if n
                        )
                    ),
                )
            )
        _rds_integrity(report)
    except Exception as exc:
        report.skipped_layers.append(f"rds ({exc})")

    # ---- source -> DynamoDB
    try:
        for dataset, actual in _dynamodb_counts().items():
            expected = SOURCE_COUNTS[dataset]
            report.add(
                Check(
                    name=f"dynamodb_items:{dataset}", layer_from="source", layer_to="dynamodb",
                    expected=expected, actual=actual, passed=actual == expected,
                    detail="items recorded by the per-file load checkpoints",
                )
            )
    except Exception as exc:
        report.skipped_layers.append(f"dynamodb ({exc})")

    # ---- source -> Silver -> Gold
    # Hoisted for the same reason rds_bootstrap is: the Redshift block compares dim_member
    # and fact_workstation_event against these, and must skip those comparisons rather than
    # die with a NameError if Silver/Gold could not be read.
    silver: dict[str, int] = {}
    gold: dict[str, int] = {}
    contributed: dict[str, int] = {}
    overlap: dict[str, int] = {}
    try:
        silver, gold, contributed, overlap = _silver_gold_counts()
        for dataset, expected in SOURCE_COUNTS.items():
            actual = silver.get(dataset, 0)
            report.add(
                Check(
                    name=f"silver_rows:{dataset}", layer_from="bronze", layer_to="silver",
                    expected=expected, actual=actual, passed=actual == expected,
                    detail="Silver must preserve every source row",
                )
            )
        # Every fact has two sources: the verified Bronze count, and whatever the POS has
        # written since. The Bronze half stays a literal — Bronze is immutable, and
        # `silver_rows:*` above already pins it against the source files. The POS half is
        # derived and cannot freeze. Four of these five were literals for the POC's whole
        # life, and were correct only because Gold could not see the POS at all; the fifth,
        # fact_workstation_event, is what that mistake looked like once it was found.
        gold_expected = {
            "dim_workstation": 175, "dim_date": 365, "dim_time": 1_440,
            "dim_concession_item": 10,
            # Telemetry has no POS write path — the cafe's own machines emit it, and nothing
            # the API does adds a reading. 175 workstations x 24 hours x 62 days stands.
            "agg_workstation_utilization_hourly": 175 * 24 * 62,
            **_expected_from(contributed),
        }
        for dataset, expected in gold_expected.items():
            actual = gold.get(dataset, 0)
            report.add(
                Check(
                    name=f"gold_rows:{dataset}", layer_from="silver", layer_to="gold",
                    expected=expected, actual=actual, passed=actual == expected,
                    detail=(
                        "175 workstations x 24 hours x 62 days"
                        if dataset.startswith("agg_") else "fact rows must match Silver"
                    ),
                )
            )
        # dim_member is SCD2, so it holds at least one row per member and usually more.
        # "At least" is the whole check: fewer rows than members means the dimension lost
        # people, which is how the members_operational delta bug showed up. Expected comes
        # from RDS, not a literal, so it stays true as members are added.
        members_in_rds = rds_bootstrap.get("members", 0) + rds_operational.get("members", 0)
        dim_member = gold.get("dim_member", 0)
        report.add(
            Check(
                name="gold_rows:dim_member", layer_from="silver", layer_to="gold",
                expected=members_in_rds, actual=dim_member,
                passed=dim_member >= members_in_rds > 0,
                severity=SEVERITY_CRITICAL,
                detail=(
                    f"SCD2 on tier: {dim_member} version(s) across {members_in_rds} member(s). "
                    "More rows than members is correct; fewer means the dimension lost members."
                ),
            )
        )

        # The snapshots Gold builds those dimensions from must themselves be complete.
        for dataset, rds_table in OPERATIONAL_SNAPSHOTS.items():
            expected_snapshot = snapshot_expected.get(rds_table, 0)
            if not expected_snapshot:
                continue
            actual_snapshot = silver.get(dataset, 0)
            # "Never fewer", not "exactly equal", for the same reason gold_rows:dim_member
            # uses it. The export stamps its watermark before it runs its SELECT, so a row
            # updated inside that window is legitimately in the snapshot while sorting after
            # the watermark. That is a millisecond of slack, and F7 was a shortfall of three
            # orders of magnitude -- 1,200 rows down to 4.
            report.add(
                Check(
                    name=f"silver_snapshot:{dataset}", layer_from="rds", layer_to="silver",
                    expected=expected_snapshot, actual=actual_snapshot,
                    passed=actual_snapshot >= expected_snapshot,
                    severity=SEVERITY_CRITICAL,
                    detail=(
                        "the RDS -> S3 export must leave a full snapshot in Silver, not the "
                        "delta it moved; Gold reads this as current operational state. "
                        "Expected is RDS as of the export watermark, so rows the POS created "
                        "after the last export are not counted as loss"
                    ),
                )
            )
        # The arithmetic above only holds if each snapshot actually *covers* Bronze. Assert it
        # rather than assume it. An RDS-backed snapshot mirrors the same table Bronze was
        # loaded into, so every source row must be in it; an overlap below the source count
        # means the export left a delta where a snapshot belongs. That is F7 — caught here
        # with no watermark, no RDS connection and no history, which is three fewer things
        # than the `silver_snapshot:*` check needs.
        for fact, (bronze_dataset, snapshot, _key) in UNIONED_FACTS.items():
            if snapshot == EVENTS_SNAPSHOT:
                continue  # no Bronze counterpart in RDS; the high-water mark below guards it
            if not silver.get(snapshot):
                continue  # the export has not run yet; Gold is legitimately Bronze-only
            report.add(
                Check(
                    name=f"snapshot_covers_bronze:{snapshot}", layer_from="silver",
                    layer_to="silver",
                    expected=SOURCE_COUNTS[bronze_dataset], actual=overlap.get(fact, 0),
                    passed=overlap.get(fact, 0) == SOURCE_COUNTS[bronze_dataset],
                    severity=SEVERITY_CRITICAL,
                    detail=(
                        f"{snapshot} mirrors the RDS table {bronze_dataset} was loaded "
                        "into, so it "
                        "must contain every source row. A shortfall means the export wrote "
                        "the delta it moved instead of the full snapshot Gold reads"
                    ),
                )
            )
        # The two event id spaces -- source ids and EVT-API-* -- must not collide. The union
        # dedupes on event_id, so a collision drops an event rather than failing, and the
        # expectation above (Bronze + contribution) would quietly absorb the loss.
        report.add(
            Check(
                name="event_id_spaces_are_disjoint", layer_from="silver", layer_to="silver",
                expected=0, actual=overlap.get("fact_workstation_event", 0),
                passed=overlap.get("fact_workstation_event", 0) == 0,
                severity=SEVERITY_CRITICAL,
                detail=(
                    "a Bronze event id and an API-emitted one must never be equal; the union "
                    "keeps one row per event_id, so a collision silently loses an event"
                ),
            )
        )
        # The events snapshot has no RDS counterpart to be compared against, so it is held to
        # the property the RDS ones get for free: it may grow, it may hold steady, it may not
        # shrink. Written by the delta instead of the snapshot, it shrank to one hour's events
        # on every run for its entire life, and no check in this file could see it.
        events_snapshot = silver.get(EVENTS_SNAPSHOT, 0)
        events_floor = _high_water_mark(f"silver_snapshot:{EVENTS_SNAPSHOT}")
        report.add(
            Check(
                name=f"silver_snapshot:{EVENTS_SNAPSHOT}", layer_from="dynamodb",
                layer_to="silver",
                expected=events_floor, actual=events_snapshot,
                passed=events_snapshot >= events_floor,
                severity=SEVERITY_CRITICAL,
                detail=(
                    "the DynamoDB -> S3 export must leave a full snapshot in Silver, not the "
                    "delta it moved; Gold reads this as the API-emitted event stream. A "
                    "deliberate rebuild that legitimately shrinks it needs the history "
                    "cleared — see docs/runbook.md"
                ),
            )
        )
        report.add(
            Check(
                name="raw_telemetry_absent_from_gold", layer_from="", layer_to="",
                expected=0, actual=gold.get("telemetry", 0), passed=True,
                severity=SEVERITY_INFO,
                detail="§6.5: telemetry is aggregated to hourly for Gold, never loaded raw",
            )
        )
    except Exception as exc:
        report.skipped_layers.append(f"silver/gold ({exc})")

    # ---- Gold -> Redshift
    if include_redshift:
        try:
            # dim_member was the one table missing from this dict, so `.get` returned None,
            # `passed` was unconditionally True and the severity fell to INFO. The most
            # fragile table in the warehouse — SCD2, and the table F7 emptied — was the only
            # one whose Redshift count nothing checked. It is SCD2, so a literal would be
            # wrong: it is compared against Gold, which is where it comes from.
            redshift_expected: dict[str, int] = {
                "dim_workstation": 175, "dim_date": 365, "dim_time": 1_440,
                "dim_concession_item": 10,
                "agg_workstation_utilization_hourly": 175 * 24 * 62,
            }
            # Every fact now moves with the POS, so none of them can be a literal here
            # either: what Redshift must hold is what Gold built, which is the only thing
            # this check was ever really asserting. `in`, not truthiness — a legitimately
            # empty Gold table would otherwise leave its key unset, and an unset key is the
            # `.get() -> None -> passed=True` shape that hid dim_member for so long.
            for dataset in ("dim_member", *UNIONED_FACTS):
                if dataset in gold:
                    redshift_expected[dataset] = gold[dataset]

            for table, actual in _redshift_counts().items():
                expected_rows: int | None = redshift_expected.get(table)
                report.add(
                    Check(
                        name=f"redshift_rows:{table}", layer_from="gold", layer_to="redshift",
                        expected=expected_rows, actual=actual,
                        passed=(expected_rows is None or actual == expected_rows),
                        severity=SEVERITY_INFO if expected_rows is None else SEVERITY_CRITICAL,
                        detail="fact and dimension counts must match Gold",
                    )
                )
        except Exception as exc:
            report.skipped_layers.append(f"redshift ({exc})")

    _known_findings(report)
    report.duration_seconds = time.time() - started
    return report


def _as_decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def persist(report: ReconciliationReport, output_dir: Path | None = None) -> dict[str, Path]:
    """Write the Markdown report and the JSON artifact, and record checks in the database."""
    root = Path(output_dir or settings().work_dir) / "reconciliation" / report.run_id
    root.mkdir(parents=True, exist_ok=True)

    markdown = root / "reconciliation_report.md"
    markdown.write_text(report.to_markdown(), encoding="utf-8")
    artifact = root / "reconciliation.json"
    artifact.write_text(json.dumps(report.as_dict(), indent=2, default=str), encoding="utf-8")

    try:
        from aimternet.db.session import connection

        with connection() as conn, conn.cursor() as cur:
            for check in report.checks:
                data = check.as_dict()
                cur.execute(
                    """INSERT INTO reconciliation_results
                       (run_id, check_name, layer_from, layer_to, expected_value,
                        actual_value, passed, severity, detail)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        report.run_id, data["check_name"], data["layer_from"] or None,
                        data["layer_to"] or None,
                        _as_decimal(data["expected_value"]),
                        _as_decimal(data["actual_value"]),
                        data["passed"], data["severity"], data["detail"],
                    ),
                )
    except Exception as exc:
        log.warning("could not persist reconciliation results to the database: %s", exc)

    return {"markdown": markdown, "json": artifact}
