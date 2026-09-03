"""One-time bootstrap: EC2 landing directory -> S3 Bronze -> RDS + DynamoDB (spec §6.1).

Manual trigger only. This is the DAG that reads the EC2 landing directory; every other DAG
reads S3, because after this runs S3 is the source of record (§3).

Task groups mirror the stages in §6.1 rather than one giant task, so a failure names the
stage it happened in and can be cleared and retried on its own.
"""

from __future__ import annotations

import pendulum
from _common import DEFAULT_ARGS, TAGS, int_variable, variable
from airflow.sdk import dag, task, task_group


@dag(
    dag_id="bootstrap_raw_landing",
    description="One-time load from the EC2 landing directory into S3, RDS and DynamoDB",
    schedule=None,  # manual only (§6.1)
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[*TAGS, "bootstrap", "manual"],
    max_active_runs=1,
    doc_md=__doc__,
)
def bootstrap_raw_landing():
    @task_group(group_id="stage_a_manifest")
    def stage_a():
        @task
        def build_and_register() -> dict:
            """Inventory every source file, checksum it, register the manifest."""
            from aimternet.config.settings import settings
            from aimternet.pipeline.manifest import build_manifest, register, write_to_s3

            days = int_variable("aimternet_telemetry_days", 62)
            entries = build_manifest(settings().raw_landing, telemetry_days=days)
            counts = register(entries)
            uri = write_to_s3(entries, entries[0].run_id if entries else "empty")
            return {"files": len(entries), "manifest_uri": uri, **counts}

        return build_and_register()

    @task_group(group_id="stage_b_bronze")
    def stage_b():
        @task
        def upload() -> dict:
            """Byte-identical copies into S3 Bronze. Resumable; skips what is already there."""
            from aimternet.config.settings import settings
            from aimternet.pipeline.bronze import upload_bronze, verify_bronze
            from aimternet.pipeline.manifest import build_manifest, set_status

            days = int_variable("aimternet_telemetry_days", 62)
            threads = int_variable("aimternet_load_threads", 8)
            entries = build_manifest(settings().raw_landing, telemetry_days=days)

            result = upload_bronze(entries, threads=threads)
            if result.outcome.failed:
                raise RuntimeError(f"{len(result.outcome.failed)} Bronze upload(s) failed")

            set_status([e.checksum for e in entries], "UPLOADED")
            verification = verify_bronze(entries, sample=200)
            if not verification["verified"]:
                raise RuntimeError(f"Bronze verification failed: {verification}")
            return {
                "uploaded": result.outcome.uploaded,
                "skipped": result.outcome.skipped,
                "verified": True,
            }

        return upload()

    @task_group(group_id="stage_c_validate")
    def stage_c():
        @task
        def validate() -> dict:
            """Validate every record, quarantine rejects, publish the results."""
            from aimternet.config.settings import settings
            from aimternet.pipeline.validation.engine import Validator
            from aimternet.pipeline.validation.quarantine import publish

            days = int_variable("aimternet_telemetry_days", 62)
            result = Validator(settings().raw_landing).run(telemetry_days=days)
            published = publish(result)

            if not result.passed:
                raise RuntimeError(
                    f"validation failed: {result.counts_by_severity()} — "
                    f"see {published['local_dir']}"
                )
            return {
                "records": sum(result.records_read.values()),
                "rejected": published["records_rejected"],
                "findings": result.counts_by_rule(),
            }

        return validate()

    @task_group(group_id="stage_d_load")
    def stage_d():
        @task
        def load_rds() -> dict:
            """FK-ordered, idempotent load. Applies the configured D2 policy."""
            from aimternet.pipeline.loaders.rds import load_all

            report = load_all()
            return {
                "rows": dict(report.rows_written),
                "backfilled_members": report.backfilled_members,
                "policy": report.orphan_policy,
            }

        @task
        def load_dynamodb() -> dict:
            """Events and telemetry. Resumes from per-file checkpoints."""
            from aimternet.pipeline.loaders.dynamodb import ensure_tables, load_dataset

            ensure_tables()
            threads = int_variable("aimternet_load_threads", 8)
            days = int_variable("aimternet_telemetry_days", 62)

            written = {}
            for dataset in ("workstation_events", "telemetry"):
                report = load_dataset(
                    dataset,
                    threads=threads,
                    days=days if dataset == "telemetry" else None,
                )
                if report.failures:
                    raise RuntimeError(f"{dataset}: {len(report.failures)} file(s) failed")
                written[dataset] = report.items_written
            return written

        # RDS and DynamoDB are independent stores; nothing is gained by serialising them.
        return [load_rds(), load_dynamodb()]

    @task
    def report(policy: str) -> str:
        return f"bootstrap complete under orphan policy {policy}"

    manifest = stage_a()
    bronze = stage_b()
    validated = stage_c()
    loaded = stage_d()

    manifest >> bronze >> validated >> loaded
    loaded >> report(variable("aimternet_orphan_member_policy", "synthesize_stub"))


bootstrap_raw_landing()
