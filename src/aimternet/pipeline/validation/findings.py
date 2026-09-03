"""Findings — the machine-readable output of validation (spec §6.1 Stage C).

Nothing is discarded silently. Every record the pipeline refuses produces a Finding that
carries the rule that rejected it, the lineage back to its source file, and the record
itself, so a rejected row can always be explained and replayed.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any


class Severity(StrEnum):
    ERROR = "ERROR"
    """The record is rejected and quarantined."""

    WARNING = "WARNING"
    """Reported and counted, but the record still loads. Never swallowed (§6.6)."""


class Rule(StrEnum):
    """Every reason a record can be rejected or flagged."""

    SCHEMA_INVALID = "SCHEMA_INVALID"
    MISSING_COLUMN = "MISSING_COLUMN"
    UNEXPECTED_COLUMN = "UNEXPECTED_COLUMN"
    PK_DUPLICATE = "PK_DUPLICATE"
    FK_ORPHAN = "FK_ORPHAN"
    PRICING_MISMATCH = "PRICING_MISMATCH"
    LINE_TOTAL_MISMATCH = "LINE_TOTAL_MISMATCH"
    ORDER_TOTAL_MISMATCH = "ORDER_TOTAL_MISMATCH"
    LEDGER_BALANCE_DRIFT = "LEDGER_BALANCE_DRIFT"
    SESSION_ENDS_BEFORE_START = "SESSION_ENDS_BEFORE_START"
    TIMESTAMP_OUT_OF_WINDOW = "TIMESTAMP_OUT_OF_WINDOW"
    OVERLAPPING_RENTAL = "OVERLAPPING_RENTAL"
    NEGATIVE_AMOUNT = "NEGATIVE_AMOUNT"
    UNKNOWN_ENUM_VALUE = "UNKNOWN_ENUM_VALUE"
    ID_DATE_MISMATCH = "ID_DATE_MISMATCH"
    TTL_ALREADY_EXPIRED = "TTL_ALREADY_EXPIRED"
    SPEC_DEVIATION = "SPEC_DEVIATION"


@dataclass(frozen=True, slots=True)
class Finding:
    """One rejected or flagged record."""

    rule: Rule
    severity: Severity
    dataset: str
    detail: str
    source_file: str = ""
    source_checksum: str = ""
    batch_date: date | None = None
    record_key: str = ""
    record: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule": str(self.rule),
            "severity": str(self.severity),
            "dataset": self.dataset,
            "detail": self.detail,
            "source_file": self.source_file,
            "source_checksum": self.source_checksum,
            "batch_date": self.batch_date.isoformat() if self.batch_date else None,
            "record_key": self.record_key,
            "record": self.record,
        }


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"cannot serialise {type(value).__name__}")


@dataclass
class ValidationResult:
    """Everything one validation run learned."""

    run_id: str
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    records_read: Counter[str] = field(default_factory=Counter)
    records_accepted: Counter[str] = field(default_factory=Counter)
    findings: list[Finding] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    # Rules whose full detail is summarised rather than listed row by row. Keeping 840
    # identical FK_ORPHAN findings in memory helps nobody; the count is what matters.
    truncated_rules: Counter[str] = field(default_factory=Counter)

    MAX_FINDINGS_PER_RULE = 500

    def add(self, finding: Finding) -> None:
        key = f"{finding.dataset}:{finding.rule}"
        self.truncated_rules[key] += 1
        if self.truncated_rules[key] <= self.MAX_FINDINGS_PER_RULE:
            self.findings.append(finding)

    def note(self, message: str) -> None:
        self.notes.append(message)

    # ---------------------------------------------------------------- summaries

    @property
    def error_count(self) -> int:
        error_keys = {
            f"{f.dataset}:{f.rule}" for f in self.findings if f.severity is Severity.ERROR
        }
        return sum(
            count for key, count in self.truncated_rules.items() if key in error_keys
        )

    def counts_by_rule(self) -> dict[str, int]:
        """Full counts, including findings that were truncated from the detail list."""
        return dict(sorted(self.truncated_rules.items()))

    def counts_by_severity(self) -> dict[str, int]:
        severity_of: dict[str, Severity] = {}
        for finding in self.findings:
            severity_of[f"{finding.dataset}:{finding.rule}"] = finding.severity
        totals: Counter[str] = Counter()
        for key, count in self.truncated_rules.items():
            totals[str(severity_of.get(key, Severity.ERROR))] += count
        return dict(totals)

    @property
    def passed(self) -> bool:
        """True when nothing at ERROR severity was found."""
        return self.counts_by_severity().get("ERROR", 0) == 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": (
                (self.finished_at - self.started_at).total_seconds() if self.finished_at else None
            ),
            "passed": self.passed,
            "records_read": dict(sorted(self.records_read.items())),
            "records_accepted": dict(sorted(self.records_accepted.items())),
            "counts_by_rule": self.counts_by_rule(),
            "counts_by_severity": self.counts_by_severity(),
            "notes": self.notes,
            "findings_sample": [f.as_dict() for f in self.findings[:2000]],
        }

    def write_json(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.as_dict(), indent=2, default=_json_default))
        return path

    def summary_table(self) -> str:
        """A readable summary for a terminal or a DAG log."""
        lines = [
            f"validation run {self.run_id} — {'PASSED' if self.passed else 'FAILED'}",
            "",
            f"  {'dataset':26s} {'read':>10s} {'accepted':>10s} {'rejected':>10s}",
            f"  {'-' * 26} {'-' * 10} {'-' * 10} {'-' * 10}",
        ]
        for dataset in sorted(self.records_read):
            read = self.records_read[dataset]
            accepted = self.records_accepted[dataset]
            lines.append(f"  {dataset:26s} {read:10,d} {accepted:10,d} {read - accepted:10,d}")
        totals = (sum(self.records_read.values()), sum(self.records_accepted.values()))
        lines.append(f"  {'-' * 26} {'-' * 10} {'-' * 10} {'-' * 10}")
        lines.append(
            f"  {'TOTAL':26s} {totals[0]:10,d} {totals[1]:10,d} {totals[0] - totals[1]:10,d}"
        )

        if self.truncated_rules:
            lines += ["", "  findings by rule:"]
            for key, count in sorted(self.truncated_rules.items(), key=lambda kv: -kv[1]):
                lines.append(f"    {key:52s} {count:10,d}")
        if self.notes:
            lines += ["", "  notes:"]
            lines += [f"    - {note}" for note in self.notes]
        return "\n".join(lines)
