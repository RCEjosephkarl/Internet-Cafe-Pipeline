"""DynamoDB -> S3, incrementally (spec §6.7).

Events and telemetry already reach S3 Bronze during the bootstrap, and Bronze is immutable.
What this DAG moves is what has arrived *since*: the SESSION_START and SESSION_END events the
operational API emits as the POS is used. Those exist only in DynamoDB until this runs.

Bounded by timestamp rather than scanned. A scan of a 6.3M-item table would get slower every
day and would cost more each time, to find a handful of new rows.
"""

from __future__ import annotations

import pendulum
from _common import DEFAULT_ARGS, SILVER_DYNAMODB, TAGS, int_variable
from airflow.sdk import dag, task


@dag(
    dag_id="dynamodb_to_s3_incremental",
    description="Export API-emitted workstation events from DynamoDB into S3 Silver",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[*TAGS, "ingest"],
    max_active_runs=1,
    doc_md=__doc__,
)
def dynamodb_to_s3_incremental():
    @task(outlets=[SILVER_DYNAMODB])
    def export_events(**context) -> dict:
        from aimternet.pipeline.curate.export_dynamodb import export_events as run_export

        hours = int_variable("aimternet_ddb_export_lookback_hours", 24)
        report = run_export(run_id=context["run_id"], lookback_hours=hours)
        return {"events": report.events, "since": report.since}

    export_events()


dynamodb_to_s3_incremental()
