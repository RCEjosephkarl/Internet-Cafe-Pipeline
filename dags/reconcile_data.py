"""Reconciliation across every hop (spec §6.6, §6.7).

Fails the run on a critical check. Warnings - the D2 backfill and findings F1, F2, F5, F6 -
are reported with their evidence and do not fail it, because each is a known property of the
source data rather than a defect in the pipeline.
"""

from __future__ import annotations

import pendulum
from _common import DEFAULT_ARGS, TAGS
from airflow.sdk import dag, task


@dag(
    dag_id="reconcile_data",
    description="Compare counts and integrity across every layer, and publish the report",
    schedule="0 */6 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[*TAGS, "quality"],
    max_active_runs=1,
    doc_md=__doc__,
)
def reconcile_data():
    @task
    def run_reconciliation(**context) -> dict:
        from aimternet.pipeline.reconcile import persist, reconcile

        report = reconcile(run_id=context["run_id"])
        written = persist(report)

        summary = {
            "passed": report.passed,
            "checks": len(report.checks),
            "critical_failures": len(report.critical_failures),
            "warnings": len(report.warnings),
            "report": str(written["markdown"]),
        }
        if not report.passed:
            failures = "; ".join(
                f"{c.name}: expected {c.expected}, got {c.actual}"
                for c in report.critical_failures[:10]
            )
            raise RuntimeError(f"reconciliation failed — {failures}")
        return summary

    run_reconciliation()


reconcile_data()
