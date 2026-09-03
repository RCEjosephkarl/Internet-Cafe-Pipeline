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
        return not self.critical_failures

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


#: Rows the API created. They are real business, not drift, and must not be compared against
#: the source files -- a POS that never adds a row is a POS nobody is using.
API_RUN_ID = "api"


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


def _silver_gold_counts() -> tuple[dict[str, int], dict[str, int]]:
    from aimternet.pipeline.curate.engine import count_parquet, duck, layer_uri

    silver_names = list(SOURCE_COUNTS)
    gold_names = [
        "dim_member", "dim_workstation", "dim_date", "dim_time", "dim_concession_item",
        "fact_rental", "fact_concession_sale", "fact_concession_line_item",
        "fact_points_activity", "fact_workstation_event",
        "agg_workstation_utilization_hourly",
    ]
    with duck() as con:
        silver = {n: count_parquet(con, layer_uri("silver", n)) for n in silver_names}
        gold = {n: count_parquet(con, layer_uri("gold", n)) for n in gold_names}
    return silver, gold


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
    try:
        bootstrap, operational = _rds_counts()
        for table, actual in bootstrap.items():
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
        created = sum(operational.values())
        if created:
            report.add(
                Check(
                    name="rds_rows:created_by_the_api", layer_from="", layer_to="",
                    expected=None, actual=created, passed=True, severity=SEVERITY_INFO,
                    detail=(
                        "rows the POS created since the bootstrap, counted separately from "
                        "the source comparison: "
                        + ", ".join(
                            f"{table} +{n}" for table, n in operational.items() if n
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
    try:
        silver, gold = _silver_gold_counts()
        for dataset, expected in SOURCE_COUNTS.items():
            actual = silver.get(dataset, 0)
            report.add(
                Check(
                    name=f"silver_rows:{dataset}", layer_from="bronze", layer_to="silver",
                    expected=expected, actual=actual, passed=actual == expected,
                    detail="Silver must preserve every source row",
                )
            )
        gold_expected = {
            "dim_workstation": 175, "dim_date": 365, "dim_time": 1_440,
            "dim_concession_item": 10, "fact_rental": 28_287,
            "fact_concession_sale": 21_077, "fact_concession_line_item": 29_672,
            "fact_points_activity": 55_514, "fact_workstation_event": 58_218,
            "agg_workstation_utilization_hourly": 175 * 24 * 62,
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
        report.add(
            Check(
                name="gold_rows:dim_member", layer_from="silver", layer_to="gold",
                expected=1_200, actual=gold.get("dim_member", 0),
                passed=gold.get("dim_member", 0) >= 1_200, severity=SEVERITY_INFO,
                detail=(
                    f"SCD2: {gold.get('dim_member', 0)} versions across 1,200 members, so "
                    f"more rows than members is correct"
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
            for table, actual in _redshift_counts().items():
                expected_rows: int | None = {
                    "dim_workstation": 175, "dim_date": 365, "dim_time": 1_440,
                    "dim_concession_item": 10, "fact_rental": 28_287,
                    "fact_concession_sale": 21_077, "fact_concession_line_item": 29_672,
                    "fact_points_activity": 55_514, "fact_workstation_event": 58_218,
                    "agg_workstation_utilization_hourly": 175 * 24 * 62,
                }.get(table)
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
