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
from collections.abc import Sequence

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

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
