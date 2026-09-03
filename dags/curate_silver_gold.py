"""S3 Bronze -> Silver -> Gold (spec §6.4, §6.7).

Reads S3, never the EC2 landing directory: after the bootstrap, S3 is the source of record
(§3). Silver first, then Gold, because Gold reads both Silver and the RDS export.
"""

from __future__ import annotations

import pendulum
from _common import DEFAULT_ARGS, TAGS, int_variable
from airflow.sdk import dag, task


@dag(
    dag_id="curate_silver_gold",
    description="Build the Silver and Gold layers from S3 Bronze",
    schedule="30 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[*TAGS, "curate"],
    max_active_runs=1,
    doc_md=__doc__,
)
def curate_silver_gold():
    @task
    def build_silver(**context) -> dict:
        from aimternet.pipeline.curate.silver import build

        run_id = context["run_id"]
        days = int_variable("aimternet_telemetry_days", 62)
        report = build(run_id, telemetry_days=days)
        return {"rows": report.rows, "seconds": round(report.duration_seconds, 1)}

    @task
    def build_gold(**context) -> dict:
        from aimternet.pipeline.curate.gold import build

        report = build(context["run_id"])
        return {
            "rows": report.rows,
            "operational": report.operational_rows,
            "seconds": round(report.duration_seconds, 1),
        }

    @task
    def check_counts(silver: dict, gold: dict) -> str:
        """Fail the run if Gold lost rows Silver had — the point of curating is not to."""
        # Derived, never a constant. fact_workstation_event draws on two sources: the Bronze
        # events in Silver and the API-emitted ones the DynamoDB export leaves alongside them.
        # Comparing it against Bronze alone would fail every run in which the POS was used —
        # and, before it did that, hid the fact that those events never arrived at all.
        operational = gold.get("operational", {})
        expected = {
            "fact_rental": silver["rows"].get("rental_transactions", 0),
            "fact_concession_sale": silver["rows"].get("concession_purchases", 0),
            "fact_points_activity": silver["rows"].get("member_points_ledger", 0),
            "fact_workstation_event": (
                silver["rows"].get("workstation_events", 0)
                + operational.get("workstation_events_operational", 0)
            ),
        }
        mismatched = {
            table: (count, gold["rows"].get(table))
            for table, count in expected.items()
            if gold["rows"].get(table) != count
        }
        if mismatched:
            raise RuntimeError(f"Gold does not reconcile to Silver: {mismatched}")
        return f"reconciled {len(expected)} fact table(s)"

    silver = build_silver()
    gold = build_gold()
    silver >> gold >> check_counts(silver, gold)


curate_silver_gold()
