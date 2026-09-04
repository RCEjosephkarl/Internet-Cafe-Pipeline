"""S3 Gold -> Redshift (spec §6.5, §6.7).

Applies the DDL, then loads every table through a staging table and a delete-then-insert in
one transaction, so rerunning replaces rows rather than duplicating facts.

COPY is used when the cluster can authenticate to S3 and batched INSERT when it cannot. The
loader probes for that at run time and records which path it took, rather than assuming.
"""

from __future__ import annotations

import pendulum
from _common import DEFAULT_ARGS, GOLD, TAGS
from airflow.sdk import dag, task


@dag(
    dag_id="load_redshift",
    description="Load the Gold layer into the Redshift dimensional model",
    schedule=[GOLD],
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[*TAGS, "warehouse"],
    max_active_runs=1,
    doc_md=__doc__,
)
def load_redshift():
    @task
    def apply_ddl() -> int:
        from aimternet.pipeline.loaders.redshift import apply_ddl as run_ddl

        return len(run_ddl())

    @task
    def load(**context) -> dict:
        from aimternet.pipeline.loaders.redshift import load_all

        report = load_all(run_id=context["run_id"])
        return {
            "strategy": report.strategy,
            "rows": report.rows,
            "seconds": round(report.duration_seconds, 1),
            "notes": report.notes,
        }

    apply_ddl() >> load()


load_redshift()
