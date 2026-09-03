"""Reconciliation report logic, without needing any of the layers it compares.

The engine's job is to decide what fails a run and what is merely explained. These tests pin
that decision down: a count mismatch is fatal, a documented finding is not, and a report
never silently omits a warning it chose not to fail on (spec §6.6).
"""

from __future__ import annotations

import json
from pathlib import Path

from aimternet.pipeline.reconcile import (
    SEVERITY_CRITICAL,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    Check,
    ReconciliationReport,
)


def _report() -> ReconciliationReport:
    return ReconciliationReport(run_id="test-run")


def _count_check(name: str, expected: int, actual: int, **kwargs) -> Check:
    return Check(
        name=name, layer_from="source", layer_to="rds",
        expected=expected, actual=actual, passed=expected == actual, **kwargs,
    )


def test_a_clean_report_passes() -> None:
    report = _report()
    report.add(_count_check("rds_rows:members", 1200, 1200))
    assert report.passed
    assert report.critical_failures == []


def test_a_count_mismatch_fails_the_run() -> None:
    """Spec §6.6: the DAG fails on critical checks. A lost row is not a warning."""
    report = _report()
    report.add(_count_check("rds_rows:rental_transactions", 28_287, 28_200))
    assert not report.passed
    assert len(report.critical_failures) == 1
    assert "FAILED" in report.to_markdown()


def test_warnings_do_not_fail_the_run_but_are_still_reported() -> None:
    """The D2 backfill and findings F1/F2/F5/F6 are explained, not swallowed."""
    report = _report()
    report.add(
        Check(
            name="d2_backfilled_members", layer_from="", layer_to="",
            expected=840, actual=840, passed=False, severity=SEVERITY_WARNING,
            detail="840 members backfilled under the configured policy",
        )
    )
    assert report.passed, "a warning must not fail the run"
    assert len(report.warnings) == 1

    markdown = report.to_markdown()
    assert "PASSED" in markdown
    assert "Explained discrepancies" in markdown
    assert "backfilled under the configured policy" in markdown


def test_info_checks_never_fail_the_run() -> None:
    report = _report()
    report.add(
        Check(
            name="rds_rows:created_by_the_api", layer_from="", layer_to="",
            expected=None, actual=115, passed=True, severity=SEVERITY_INFO,
            detail="rows the POS created since the bootstrap",
        )
    )
    assert report.passed


def test_a_failure_appears_in_the_critical_section_of_the_report() -> None:
    report = _report()
    report.add(_count_check("silver_rows:telemetry", 6_300_000, 6_299_999))
    markdown = report.to_markdown()
    assert "## Critical failures" in markdown
    assert "silver_rows:telemetry" in markdown


def test_skipped_layers_are_declared_rather_than_passed_over() -> None:
    """A layer that could not be reached must not read as a layer that reconciled."""
    report = _report()
    report.add(_count_check("rds_rows:members", 1200, 1200))
    report.skipped_layers.append("redshift (connection refused)")
    markdown = report.to_markdown()
    assert "not reachable during this run" in markdown
    assert "redshift (connection refused)" in markdown


def test_a_skipped_layer_fails_the_run() -> None:
    """"We could not look" is not "we looked and it was fine".

    Every collector in reconcile() is wrapped in `except Exception: skipped_layers.append(...)`,
    and `passed` used to ignore that list. So one S3 error inside `_silver_gold_counts` removed
    all eleven silver_rows checks, every gold_rows check, the dim_member F7 guard and every
    CRITICAL silver_snapshot check at once -- and the report went green, because a check that
    was never added cannot fail.
    """
    report = _report()
    report.add(_count_check("rds_rows:members", 1200, 1200))
    assert report.passed

    report.skipped_layers.append("silver/gold (connection reset by peer)")
    assert not report.passed


def test_the_report_embeds_the_policy_and_its_evidence() -> None:
    """§11 asks for every assumption. The report carries them so it stands alone."""
    markdown = _report().to_markdown()
    for key in (
        "D2_ORPHAN_MEMBERS",
        "F1_GROSS_RENTAL_AMOUNT",
        "F2_TELEMETRY_TTL_DISABLED",
        "F5_LEDGER_BALANCE_NOT_REPLAYABLE",
        "F6_TELEMETRY_CADENCE_VARIES",
    ):
        assert key in markdown


def test_the_json_artifact_is_machine_readable(tmp_path: Path) -> None:
    report = _report()
    report.add(_count_check("rds_rows:members", 1200, 1199))
    payload = json.loads(json.dumps(report.as_dict(), default=str))
    assert payload["passed"] is False
    assert payload["counts"]["critical_failures"] == 1
    assert payload["policy"]["orphan_member_policy"]
    names = [c["check_name"] for c in payload["checks_detail"]]
    assert "rds_rows:members" in names


def test_severity_defaults_to_critical() -> None:
    """A new check that forgets to declare severity should fail loudly, not quietly pass."""
    check = Check(
        name="x", layer_from="a", layer_to="b", expected=1, actual=2, passed=False
    )
    assert check.severity == SEVERITY_CRITICAL
