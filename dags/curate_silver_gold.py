"""S3 Bronze -> Silver -> Gold (spec §6.4, §6.7).

Reads S3, never the EC2 landing directory: after the bootstrap, S3 is the source of record
(§3). Silver first, then Gold, because Gold reads both Silver and the RDS export.
"""

from __future__ import annotations

import pendulum
from _common import DEFAULT_ARGS, GOLD, SILVER_DYNAMODB, SILVER_RDS, TAGS, int_variable
from airflow.sdk import dag, task


@dag(
    dag_id="curate_silver_gold",
    description="Build the Silver and Gold layers from S3 Bronze",
    schedule=[SILVER_RDS, SILVER_DYNAMODB],
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

    @task(outlets=[GOLD])
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
        # Derived, never a constant. Every one of these facts draws on two sources: the
        # Bronze-derived dataset in Silver, and whatever the POS has written since, which the
        # exports leave alongside it. Comparing against Bronze alone fails every run in which
        # the cafe was open — and, before it did that, hid the fact that POS rows never
        # arrived at all. Four of these five read Bronze-only until that gap was closed.
        operational = gold.get("operational", {})

        def expect(bronze: str) -> int:
            """Bronze rows plus what the snapshot *contributes* beyond them.

            The contribution, never the whole snapshot. The RDS-backed snapshots mirror the
            tables Bronze was loaded into, so they are a superset of it: adding one whole
            would count all 28,287 bootstrap rentals twice.
            """
            return silver["rows"].get(bronze, 0) + operational.get(f"{bronze}_operational", 0)

        expected = {
            "fact_rental": expect("rental_transactions"),
            "fact_concession_sale": expect("concession_purchases"),
            "fact_concession_line_item": expect("concession_order_items"),
            "fact_points_activity": expect("member_points_ledger"),
            "fact_workstation_event": expect("workstation_events"),
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
