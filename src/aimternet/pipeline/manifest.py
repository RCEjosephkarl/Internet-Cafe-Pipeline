"""Stage A — inventory, checksum and register every source file (spec §6.1).

The manifest is the pipeline's idempotency key. Every source file is hashed, and a file whose
(path, checksum) pair is already registered as LOADED is skipped rather than reloaded. That is
what lets `make bootstrap` be run twice without duplicating anything, and what lets an
interrupted run resume.

Checksums are SHA-256 over the file bytes, read in chunks -- a 4.1 GB tree cannot be hashed
by slurping files into memory.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from aimternet.config.settings import settings
from aimternet.io.readers import SourceFile, count_records, discover

log = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024
VALID_STATUSES = ("DISCOVERED", "UPLOADED", "VALIDATED", "LOADED", "FAILED", "SKIPPED")


def sha256_file(path: Path, chunk_size: int = CHUNK_SIZE) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """One row of the load manifest (spec §6.1 Stage A)."""

    source_file: str
    source_type: str
    dataset: str
    checksum: str
    file_size: int
    file_mtime_utc: str
    record_count: int
    batch_date: str | None = None
    hour_of_day: int | None = None
    status: str = "DISCOVERED"
    error_count: int = 0
    bronze_uri: str | None = None
    error_detail: str | None = None
    run_id: str = ""

    @property
    def bronze_key(self) -> str:
        """Deterministic Bronze layout (spec §6.1 Stage B).

        Telemetry is partitioned by date and hour because that is how it is queried and how
        it must be loaded; everything else is partitioned by batch date.
        """
        cfg = settings()
        prefix = cfg.s3_bronze_prefix.strip("/")
        name = Path(self.source_file).name
        if self.dataset == "telemetry":
            return f"{prefix}/telemetry/date={self.batch_date}/hour={self.hour_of_day:02d}/{name}"
        if self.batch_date:
            return f"{prefix}/{self.dataset}/batch_date={self.batch_date}/{name}"
        return f"{prefix}/{self.dataset}/{name}"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _first_telemetry_days(sources: list[SourceFile], days: int) -> set[date]:
    """The earliest ``days`` telemetry dates. Telemetry files always carry a batch_date."""
    dated = sorted({s.batch_date for s in sources if s.dataset == "telemetry" and s.batch_date})
    return set(dated[:days])


def build_entry(source: SourceFile, run_id: str) -> ManifestEntry:
    stat = source.path.stat()
    return ManifestEntry(
        source_file=str(source.path),
        source_type=source.source_type,
        dataset=source.dataset,
        checksum=sha256_file(source.path),
        file_size=stat.st_size,
        file_mtime_utc=datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
        record_count=count_records(source),
        batch_date=source.batch_date.isoformat() if source.batch_date else None,
        hour_of_day=source.hour,
        run_id=run_id,
    )


def build_manifest(
    landing: Path,
    run_id: str | None = None,
    *,
    threads: int | None = None,
    telemetry_days: int | None = None,
) -> list[ManifestEntry]:
    """Inventory the landing tree. Reads every byte to hash it, writes nothing."""
    run_id = run_id or f"bootstrap-{uuid.uuid4().hex[:12]}"
    sources = discover(landing)
    if telemetry_days is not None:
        keep = _first_telemetry_days(sources, telemetry_days)
        sources = [s for s in sources if s.dataset != "telemetry" or s.batch_date in keep]

    workers = threads or settings().load_threads
    log.info("hashing %d source file(s) with %d threads", len(sources), workers)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        entries = list(pool.map(lambda s: build_entry(s, run_id), sources))
    log.info(
        "manifest built: %d file(s), %.1f GB, %s record(s)",
        len(entries),
        sum(e.file_size for e in entries) / 1e9,
        f"{sum(e.record_count for e in entries):,}",
    )
    return entries


# --------------------------------------------------------------------------- persistence


def register(entries: list[ManifestEntry]) -> dict[str, int]:
    """Insert or update manifest rows in the control table.

    ``ON CONFLICT (source_file, checksum)`` is what makes a rerun a no-op: the same bytes at
    the same path are recognised, not re-registered.
    """
    from aimternet.db.session import connection

    inserted = updated = 0
    with connection() as conn, conn.cursor() as cur:
        for entry in entries:
            cur.execute(
                """
                INSERT INTO load_manifest (
                    source_file, source_type, dataset, batch_date, hour_of_day,
                    checksum, file_size, file_mtime_utc, status, record_count,
                    error_count, bronze_uri, error_detail, run_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (source_file, checksum) DO UPDATE SET
                    status       = EXCLUDED.status,
                    record_count = EXCLUDED.record_count,
                    error_count  = EXCLUDED.error_count,
                    bronze_uri   = COALESCE(EXCLUDED.bronze_uri, load_manifest.bronze_uri),
                    error_detail = EXCLUDED.error_detail,
                    run_id       = EXCLUDED.run_id
                RETURNING (xmax = 0) AS was_insert
                """,
                (
                    entry.source_file, entry.source_type, entry.dataset, entry.batch_date,
                    entry.hour_of_day, entry.checksum, entry.file_size, entry.file_mtime_utc,
                    entry.status, entry.record_count, entry.error_count, entry.bronze_uri,
                    entry.error_detail, entry.run_id,
                ),
            )
            row = cur.fetchone()
            if row and row[0]:
                inserted += 1
            else:
                updated += 1
    return {"inserted": inserted, "updated": updated}


def already_loaded(checksums: list[str]) -> set[str]:
    """Checksums already registered as LOADED — the set the loader is allowed to skip."""
    if not checksums:
        return set()
    from aimternet.db.session import connection

    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT checksum FROM load_manifest WHERE status = 'LOADED' AND checksum = ANY(%s)",
            (checksums,),
        )
        return {row[0] for row in cur.fetchall()}


def set_status(
    checksums: list[str], status: str, *, error_detail: str | None = None
) -> int:
    if status not in VALID_STATUSES:
        raise ValueError(f"unknown manifest status {status!r}; expected one of {VALID_STATUSES}")
    if not checksums:
        return 0
    from aimternet.db.session import connection

    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE load_manifest SET status = %s, error_detail = %s WHERE checksum = ANY(%s)",
            (status, error_detail, checksums),
        )
        return int(cur.rowcount)


def write_to_s3(entries: list[ManifestEntry], run_id: str) -> str:
    """Persist the manifest to S3 as well, so it survives the database (spec §6.1)."""
    from aimternet.io.s3 import S3Client

    cfg = settings()
    payload = json.dumps(
        {
            "run_id": run_id,
            "generated_at": datetime.now(UTC).isoformat(),
            "file_count": len(entries),
            "total_bytes": sum(e.file_size for e in entries),
            "total_records": sum(e.record_count for e in entries),
            "entries": [e.as_dict() for e in entries],
        },
        indent=2,
    ).encode()
    key = f"{cfg.s3_manifest_prefix}/run_id={run_id}/manifest.json"
    return S3Client().put_bytes(key, payload)


def summarise(entries: list[ManifestEntry]) -> str:
    from collections import Counter

    by_dataset: Counter[str] = Counter()
    bytes_by_dataset: Counter[str] = Counter()
    records_by_dataset: Counter[str] = Counter()
    for entry in entries:
        by_dataset[entry.dataset] += 1
        bytes_by_dataset[entry.dataset] += entry.file_size
        records_by_dataset[entry.dataset] += entry.record_count

    lines = [
        f"  {'dataset':26s} {'files':>7s} {'records':>13s} {'size':>10s}",
        f"  {'-' * 26} {'-' * 7} {'-' * 13} {'-' * 10}",
    ]
    for dataset in sorted(by_dataset):
        size_mb = bytes_by_dataset[dataset] / 1e6
        lines.append(
            f"  {dataset:26s} {by_dataset[dataset]:7,d} "
            f"{records_by_dataset[dataset]:13,d} {size_mb:9,.1f}M"
        )
    lines.append(f"  {'-' * 26} {'-' * 7} {'-' * 13} {'-' * 10}")
    lines.append(
        f"  {'TOTAL':26s} {len(entries):7,d} "
        f"{sum(records_by_dataset.values()):13,d} "
        f"{sum(bytes_by_dataset.values()) / 1e9:9,.2f}G"
    )
    return "\n".join(lines)
