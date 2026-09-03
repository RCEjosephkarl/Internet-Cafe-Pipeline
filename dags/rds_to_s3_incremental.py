"""RDS -> S3, incrementally (spec §6.7).

Exports the operational state so the warehouse can be built from it. This is also the hop
that keeps the D2 resolution to a single implementation: the 840 backfilled members are
created once by the RDS loader, and Gold reads the export rather than re-deriving them (§3).

Watermarked on ``updated_at``, so a scheduled run moves only what changed.
"""

from __future__ import annotations

import pendulum
from _common import DEFAULT_ARGS, TAGS, variable
from airflow.sdk import dag, task


@dag(
    dag_id="rds_to_s3_incremental",
    description="Incremental export of operational tables from RDS into S3 Silver",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[*TAGS, "ingest"],
    max_active_runs=1,
    doc_md=__doc__,
)
def rds_to_s3_incremental():
    @task
    def export() -> dict:
        from aimternet.pipeline.curate.export_rds import export as run_export

        full = variable("aimternet_rds_export_full", "false").lower() == "true"
        report = run_export(run_id="", full=full)
        return {"rows": report.rows, "incremental": report.incremental}

    export()


rds_to_s3_incremental()
