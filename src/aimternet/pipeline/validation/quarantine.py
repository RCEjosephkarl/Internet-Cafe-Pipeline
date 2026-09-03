"""Quarantine — where rejected records go (spec §6.1 Stage C).

Rejects are written locally as newline-delimited JSON, one file per dataset per run, and
mirrored to S3 when a bucket is configured. Each record keeps the rule that rejected it and
its lineage, so nothing is ever discarded without an explanation attached.

Local first, S3 second, deliberately: the local copy must succeed even when AWS is
unavailable, because losing the evidence is worse than failing to publish it.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from aimternet.config.settings import settings
from aimternet.pipeline.validation.findings import Finding, Severity, ValidationResult

log = logging.getLogger(__name__)


def _default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def write_local(result: ValidationResult, quarantine_dir: Path | None = None) -> dict[str, Path]:
    """Write rejected records to ``<quarantine_dir>/<run_id>/<dataset>.jsonl``.

    Only ERROR findings are quarantined -- a WARNING means the record still loads, and
    copying it here would imply otherwise.
    """
    root = Path(quarantine_dir or settings().quarantine_dir) / result.run_id
    root.mkdir(parents=True, exist_ok=True)

    grouped: dict[str, list[Finding]] = defaultdict(list)
    for finding in result.findings:
        if finding.severity is Severity.ERROR:
            grouped[finding.dataset].append(finding)

    written: dict[str, Path] = {}
    for dataset, findings in grouped.items():
        path = root / f"{dataset}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for finding in findings:
                fh.write(json.dumps(finding.as_dict(), default=_default) + "\n")
        written[dataset] = path
        log.info("quarantined %d %s record(s) -> %s", len(findings), dataset, path)

    summary = root / "_summary.json"
    summary.write_text(
        json.dumps(
            {
                "run_id": result.run_id,
                "written_at": datetime.now(UTC).isoformat(),
                "counts_by_rule": result.counts_by_rule(),
                "counts_by_severity": result.counts_by_severity(),
                "notes": result.notes,
                "files": {k: str(v) for k, v in written.items()},
            },
            indent=2,
            default=_default,
        )
    )
    written["_summary"] = summary
    return written


def upload(written: dict[str, Path], run_id: str) -> list[str]:
    """Mirror the local quarantine to S3. Returns the URIs written."""
    from aimternet.io.s3 import S3Client

    cfg = settings()
    client = S3Client()
    uris = []
    for dataset, path in written.items():
        key = f"{cfg.s3_quarantine_prefix}/run_id={run_id}/{dataset}{path.suffix}"
        uris.append(client.upload_file(path, key))
    return uris
