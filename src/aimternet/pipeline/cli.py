"""Command line entry point for the pipeline.

Airflow DAGs and the Makefile both drive the pipeline through this module, so there is one
way to run each step and no logic that exists only inside a DAG (spec §6.7).

Subcommands are added as their phase lands; each one is a thin wrapper over a function in
``aimternet.pipeline``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from collections.abc import Sequence
from pathlib import Path

from aimternet.config.poc_policy import policy
from aimternet.config.settings import settings

LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=LOG_FORMAT,
        stream=sys.stderr,
    )


def _cmd_migrate(args: argparse.Namespace) -> int:
    from aimternet.db import migrate

    if args.action == "up":
        applied = migrate.upgrade()
        print("applied:", ", ".join(applied) if applied else "nothing pending")
    elif args.action == "down":
        reverted = migrate.downgrade(steps=args.steps)
        print("reverted:", ", ".join(reverted) if reverted else "nothing to revert")
    else:
        for row in migrate.status():
            mark = "x" if row["applied"] else " "
            print(f"  [{mark}] {row['version']}_{row['name']}")
    return 0


def _cmd_config(args: argparse.Namespace) -> int:
    """Show the resolved configuration. Secrets are never printed."""
    cfg = settings()
    shown = {
        "raw_landing": str(cfg.raw_landing),
        "dag_input": str(cfg.effective_dag_input),
        "work_dir": str(cfg.work_dir),
        "quarantine_dir": str(cfg.quarantine_dir),
        "aws_region": cfg.aws_region,
        "s3_bucket": cfg.s3_bucket,
        "pg_host": cfg.pg_host,
        "pg_schema": cfg.pg_schema,
        "redshift_host": cfg.redshift_host,
        "redshift_schema": cfg.redshift_schema,
        "ddb_events_table": cfg.ddb_events_table,
        "ddb_telemetry_table": cfg.ddb_telemetry_table,
        "ddb_ttl_enabled": cfg.ddb_ttl_enabled,
        "orphan_member_policy": cfg.orphan_member_policy,
        "telemetry_days": cfg.telemetry_days,
        "metrics_live_source": cfg.metrics_live_source,
    }
    print(json.dumps(shown, indent=2))
    return 0


def _cmd_assumptions(args: argparse.Namespace) -> int:
    """Every POC policy decision, with its evidence (spec §0.5, §11)."""
    report = policy().as_report()
    if args.json:
        print(json.dumps(report, indent=2))
        return 0
    for assumption in policy().assumptions:
        print(f"\n{assumption.key}  [{assumption.spec_reference}]")
        print(f"  decision : {assumption.decision}")
        print(f"  because  : {assumption.rationale}")
        if assumption.evidence:
            print(f"  evidence : {assumption.evidence}")
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    """Stage C on the local landing tree. Needs no AWS, which is the point."""
    from aimternet.pipeline.validation.engine import Validator
    from aimternet.pipeline.validation.quarantine import publish, write_local

    cfg = settings()
    landing = Path(args.landing) if args.landing else cfg.raw_landing
    validator = Validator(landing, run_id=args.run_id)
    result = validator.run(telemetry_days=args.telemetry_days)
    print(result.summary_table())

    if args.no_publish:
        written = write_local(result)
        print(f"\nquarantine: {written['_summary'].parent}")
    else:
        published = publish(result)
        print(f"\nquarantine     : {published['local_dir']}")
        print(f"records rejected: {published['records_rejected']}")
        uris = published["s3_uris"]
        if isinstance(uris, list):
            for uri in uris:
                print(f"published       : {uri}")

    if args.json_out:
        path = result.write_json(Path(args.json_out))
        print(f"results         : {path}")
    return 0 if result.passed else 1


def _cmd_manifest(args: argparse.Namespace) -> int:
    """Stage A only: inventory and checksum, register nothing unless asked."""
    from aimternet.pipeline.manifest import build_manifest, register, summarise, write_to_s3

    cfg = settings()
    run_id = args.run_id or f"manifest-{uuid.uuid4().hex[:12]}"
    entries = build_manifest(cfg.raw_landing, run_id=run_id, telemetry_days=args.telemetry_days)
    print(summarise(entries))
    if args.register:
        print("\nregistered:", register(entries))
        print("s3 manifest:", write_to_s3(entries, run_id))
    return 0


def _cmd_bronze(args: argparse.Namespace) -> int:
    """Stages A and B: inventory, then copy into S3 Bronze."""
    from aimternet.pipeline.bronze import upload_bronze, verify_bronze
    from aimternet.pipeline.manifest import (
        build_manifest,
        register,
        set_status,
        summarise,
        write_to_s3,
    )

    cfg = settings()
    run_id = args.run_id or f"bronze-{uuid.uuid4().hex[:12]}"
    entries = build_manifest(cfg.raw_landing, run_id=run_id, telemetry_days=args.telemetry_days)
    print(summarise(entries))
    register(entries)

    result = upload_bronze(entries, threads=args.threads)
    result.manifest_uri = write_to_s3(entries, run_id)
    print("\nbronze upload:")
    print(result.summary())

    uploaded = [e.checksum for e in entries if e.bronze_key not in dict(result.outcome.failed)]
    set_status(uploaded, "UPLOADED")

    if args.verify:
        verification = verify_bronze(entries, sample=args.verify_sample)
        print("\nverification:", json.dumps(verification, indent=2)[:1200])
        return 0 if verification["verified"] else 1
    return 1 if result.outcome.failed else 0


def _cmd_load_rds(args: argparse.Namespace) -> int:
    from aimternet.pipeline.loaders.rds import load_all

    print(load_all(run_id=args.run_id or "").summary())
    return 0


def _cmd_load_dynamodb(args: argparse.Namespace) -> int:
    from aimternet.pipeline.loaders.dynamodb import ensure_tables, load_dataset

    print("tables:", json.dumps(ensure_tables(), indent=2))
    for dataset in args.datasets:
        report = load_dataset(
            dataset, run_id=args.run_id or "", threads=args.threads, days=args.days
        )
        print()
        print(report.summary())
    return 0


def _cmd_load_redshift(args: argparse.Namespace) -> int:
    from aimternet.pipeline.loaders.redshift import load_all

    print(load_all(run_id=args.run_id or "", datasets=args.datasets).summary())
    return 0


def _cmd_curate(args: argparse.Namespace) -> int:
    """Silver, then the RDS export, then Gold. Order matters: Gold reads both."""
    from aimternet.pipeline.curate import export_rds, gold, silver

    run_id = args.run_id or f"curate-{uuid.uuid4().hex[:12]}"
    if args.layer in ("silver", "all"):
        print(silver.build(run_id, telemetry_days=args.telemetry_days).summary())
        print()
    if args.layer in ("export", "all"):
        print(export_rds.export(run_id, full=args.full).summary())
        print()
    if args.layer in ("gold", "all"):
        print(gold.build(run_id).summary())
    return 0


def _cmd_reconcile(args: argparse.Namespace) -> int:
    """Compare every hop and write the report. Exits non-zero on a critical failure."""
    from aimternet.pipeline.reconcile import persist, reconcile

    report = reconcile(
        run_id=args.run_id or "", include_redshift=not args.skip_redshift
    )
    written = persist(report)

    print(report.to_markdown() if args.markdown else _reconcile_summary(report))
    print(f"\nreport : {written['markdown']}")
    print(f"json   : {written['json']}")
    return 0 if report.passed else 1


def _reconcile_summary(report: object) -> str:
    lines = [
        f"reconciliation {report.run_id} — "  # type: ignore[attr-defined]
        f"{'PASSED' if report.passed else 'FAILED'}",  # type: ignore[attr-defined]
        "",
        f"  {'check':44s} {'expected':>12s} {'actual':>12s}  ok",
        f"  {'-' * 44} {'-' * 12} {'-' * 12}  --",
    ]
    for check in report.checks:  # type: ignore[attr-defined]
        if check.layer_from and check.layer_to:
            expected = "" if check.expected is None else f"{int(check.expected):,}"
            actual = "" if check.actual is None else f"{int(check.actual):,}"
            lines.append(
                f"  {check.name:44s} {expected:>12s} {actual:>12s}  "
                f"{'ok' if check.passed else 'NO'}"
            )
    lines += ["", "  integrity:"]
    for check in report.checks:  # type: ignore[attr-defined]
        if not (check.layer_from and check.layer_to):
            lines.append(
                f"    [{'ok' if check.passed else check.severity[:4]}] {check.name}"
            )
    if report.skipped_layers:  # type: ignore[attr-defined]
        lines += ["", "  not reachable: " + ", ".join(report.skipped_layers)]  # type: ignore[attr-defined]
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aimternet", description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    migrate_p = sub.add_parser("migrate", help="apply, roll back or inspect DB migrations")
    migrate_p.add_argument("action", choices=("up", "down", "status"), nargs="?", default="status")
    migrate_p.add_argument("--steps", type=int, default=1, help="how many to roll back")
    migrate_p.set_defaults(func=_cmd_migrate)

    config_p = sub.add_parser("config", help="show resolved configuration (no secrets)")
    config_p.set_defaults(func=_cmd_config)

    assumptions_p = sub.add_parser("assumptions", help="show every POC policy decision")
    assumptions_p.add_argument("--json", action="store_true")
    assumptions_p.set_defaults(func=_cmd_assumptions)

    validate_p = sub.add_parser("validate", help="Stage C: validate the landing tree (no AWS)")
    validate_p.add_argument("--telemetry-days", type=int, default=None,
                            help="limit telemetry to the first N days (default: all 62)")
    validate_p.add_argument("--json-out", default=None, help="write results JSON here")
    validate_p.add_argument("--landing", default=None, help="validate a different tree")
    validate_p.add_argument("--run-id", default=None, help="name this run in the artifacts")
    validate_p.add_argument("--no-publish", action="store_true", help="skip the S3 upload")
    validate_p.set_defaults(func=_cmd_validate)

    manifest_p = sub.add_parser("manifest", help="Stage A: inventory and checksum source files")
    manifest_p.add_argument("--telemetry-days", type=int, default=None)
    manifest_p.add_argument("--run-id", default=None)
    manifest_p.add_argument("--register", action="store_true", help="write to the control table")
    manifest_p.set_defaults(func=_cmd_manifest)

    bronze_p = sub.add_parser("bronze", help="Stages A+B: inventory then copy to S3 Bronze")
    bronze_p.add_argument("--telemetry-days", type=int, default=None)
    bronze_p.add_argument("--run-id", default=None)
    bronze_p.add_argument("--threads", type=int, default=None)
    bronze_p.add_argument("--verify", action="store_true", help="check Bronze against the manifest")
    bronze_p.add_argument("--verify-sample", type=int, default=200,
                          help="how many objects to checksum-verify (default 200)")
    bronze_p.set_defaults(func=_cmd_bronze)

    rds_p = sub.add_parser("load-rds", help="load the validated source data into RDS")
    rds_p.add_argument("--run-id", default=None)
    rds_p.set_defaults(func=_cmd_load_rds)

    ddb_p = sub.add_parser("load-dynamodb", help="create tables and load events/telemetry")
    ddb_p.add_argument(
        "--datasets", nargs="+", default=["workstation_events", "telemetry"],
        choices=["workstation_events", "telemetry"],
    )
    ddb_p.add_argument("--days", type=int, default=None, help="limit to the first N days")
    ddb_p.add_argument("--threads", type=int, default=None)
    ddb_p.add_argument("--run-id", default=None)
    ddb_p.set_defaults(func=_cmd_load_dynamodb)

    curate_p = sub.add_parser("curate", help="build Silver and Gold from Bronze")
    curate_p.add_argument("--layer", choices=("silver", "export", "gold", "all"), default="all")
    curate_p.add_argument("--telemetry-days", type=int, default=None)
    curate_p.add_argument("--full", action="store_true", help="full RDS export, not incremental")
    curate_p.add_argument("--run-id", default=None)
    curate_p.set_defaults(func=_cmd_curate)

    redshift_p = sub.add_parser("load-redshift", help="apply the DDL and load Gold into Redshift")
    redshift_p.add_argument("--run-id", default=None)
    redshift_p.add_argument("--datasets", nargs="*", default=None)
    redshift_p.set_defaults(func=_cmd_load_redshift)

    reconcile_p = sub.add_parser("reconcile", help="compare counts across every hop")
    reconcile_p.add_argument("--run-id", default=None)
    reconcile_p.add_argument("--skip-redshift", action="store_true")
    reconcile_p.add_argument("--markdown", action="store_true", help="print the full report")
    reconcile_p.set_defaults(func=_cmd_reconcile)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
