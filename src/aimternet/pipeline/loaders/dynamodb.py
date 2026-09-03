"""Phase 4 — load workstation events and telemetry into DynamoDB (spec §6.3).

Key design and access patterns are documented in ``docs/dynamodb.md``, written first as the
spec requires. This module implements it.

Telemetry is the performance problem in this POC: **6,300,000 items**, not the ~3.1M spec
§1.2 projects, because the sampling interval is not constant (finding F6). A ``put_item`` loop
would take days. The approach:

* ``batch_writer()`` -- 25 items per request instead of one,
* a bounded thread pool over *files*, so each worker owns its own writer and client,
* exponential backoff, and the unprocessed-item retry that ``batch_writer`` performs
  internally (``BatchWriteItem`` can succeed partially and silently return what it skipped),
* streaming: one file's records in memory at a time, never the corpus,
* a checkpoint per file, so an interrupted run resumes.

Numbers are parsed straight to ``Decimal``. DynamoDB rejects floats outright, and float is
the wrong type for a measurement you intend to aggregate later.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from aimternet.config.settings import settings
from aimternet.io.readers import SourceFile, discover
from aimternet.pipeline.loaders import checkpoint

log = logging.getLogger(__name__)

EVENTS_PIPELINE = "dynamodb_events"
TELEMETRY_PIPELINE = "dynamodb_telemetry"


@dataclass
class DynamoLoadReport:
    table: str
    files_total: int = 0
    files_loaded: int = 0
    files_skipped: int = 0
    items_written: int = 0
    failures: list[tuple[str, str]] = field(default_factory=list)
    checkpoint_failures: int = 0
    duration_seconds: float = 0.0

    def summary(self) -> str:
        rate = self.items_written / self.duration_seconds if self.duration_seconds else 0
        lines = [
            f"{self.table}",
            f"  files      : {self.files_loaded:,} loaded, {self.files_skipped:,} skipped "
            f"(already checkpointed), {self.files_total:,} total",
            f"  items      : {self.items_written:,}",
            f"  elapsed    : {self.duration_seconds / 60:.1f} min ({self.duration_seconds:.0f}s)",
            f"  throughput : {rate:,.0f} items/s",
            f"  failures   : {len(self.failures)}",
            f"  uncheckpointed: {self.checkpoint_failures:,}",
        ]
        for name, error in self.failures[:10]:
            lines.append(f"    {name}: {error[:140]}")
        return "\n".join(lines)


# --------------------------------------------------------------------------- clients


class _Resources:
    """One boto3 resource per thread; boto3 clients are not documented as thread-safe."""

    def __init__(self) -> None:
        cfg = settings()
        self._config = Config(
            region_name=cfg.aws_region,
            retries={"max_attempts": 10, "mode": "adaptive"},
            max_pool_connections=max(cfg.load_threads * 4, 32),
        )
        self._local = threading.local()

    @property
    def resource(self) -> Any:
        existing = getattr(self._local, "resource", None)
        if existing is None:
            existing = boto3.resource("dynamodb", config=self._config)
            self._local.resource = existing
        return existing

    def table(self, name: str) -> Any:
        return self.resource.Table(name)


_RESOURCES = _Resources()


# --------------------------------------------------------------------------- table creation


def _table_definitions() -> list[dict[str, Any]]:
    cfg = settings()
    return [
        {
            "TableName": cfg.ddb_events_table,
            "KeySchema": [
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "GSI1PK", "AttributeType": "S"},
                {"AttributeName": "GSI1SK", "AttributeType": "S"},
                {"AttributeName": "GSI2PK", "AttributeType": "S"},
                {"AttributeName": "GSI2SK", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "GSI1",  # by session: both ends of one rental
                    "KeySchema": [
                        {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "GSI2",  # by event type: alert triage without a scan
                    "KeySchema": [
                        {"AttributeName": "GSI2PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI2SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
        },
        {
            "TableName": cfg.ddb_telemetry_table,
            "KeySchema": [
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        },
    ]


def ensure_tables(*, wait: bool = True) -> dict[str, str]:
    """Create both tables if absent. Never deletes or recreates an existing one."""
    cfg = settings()
    client = boto3.client("dynamodb", region_name=cfg.aws_region)
    statuses: dict[str, str] = {}

    for definition in _table_definitions():
        name = definition["TableName"]
        try:
            existing = client.describe_table(TableName=name)["Table"]
            statuses[name] = f"exists ({existing['TableStatus']}, {existing['ItemCount']} items)"
            continue
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        log.info("creating DynamoDB table %s", name)
        client.create_table(**definition)
        statuses[name] = "created"

    if wait:
        waiter = client.get_waiter("table_exists")
        for definition in _table_definitions():
            waiter.wait(TableName=definition["TableName"])

    # TTL is configured on the telemetry table only, and left DISABLED by default: the source
    # expires_at values are already in the past, so enabling it would purge nearly the whole
    # dataset within ~48h of loading (finding F2).
    telemetry = cfg.ddb_telemetry_table
    desired = "ENABLED" if cfg.ddb_ttl_enabled else "DISABLED"
    try:
        current = client.describe_time_to_live(TableName=telemetry)
        status = current["TimeToLiveDescription"]["TimeToLiveStatus"]
        if status != desired and desired == "ENABLED":
            client.update_time_to_live(
                TableName=telemetry,
                TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
            )
            statuses[f"{telemetry}:ttl"] = "enabled on expires_at"
        else:
            statuses[f"{telemetry}:ttl"] = (
                f"{status.lower()} (AIMTERNET_DDB_TTL_ENABLED={cfg.ddb_ttl_enabled})"
            )
    except ClientError as exc:
        statuses[f"{telemetry}:ttl"] = f"could not read TTL: {exc.response['Error']['Code']}"

    return statuses


# --------------------------------------------------------------------------- item shaping


def _shift_ttl(expires_at: Any, shift_days: int) -> Any:
    """Optionally rebase the source TTL into the future for a live-expiry demo."""
    if not shift_days or not isinstance(expires_at, int | Decimal):
        return expires_at
    return int(expires_at) + int(timedelta(days=shift_days).total_seconds())


def event_item(record: dict[str, Any], shift_days: int = 0) -> dict[str, Any]:
    """Shape one workstation event. See docs/dynamodb.md for why the keys look like this."""
    workstation = record["workstation_id"]
    timestamp = record["event_timestamp"]
    utc = datetime.fromisoformat(timestamp).astimezone(UTC).isoformat()
    item: dict[str, Any] = {
        "PK": f"WS#{workstation}",
        "SK": f"EVT#{utc}#{record['event_id']}",
        "event_id": record["event_id"],
        "workstation_id": workstation,
        "event_timestamp_utc": utc,
        "event_timestamp_source": timestamp,
        "event_type": record["event_type"],
        "client_os_version": record.get("client_os_version"),
        "GSI2PK": f"TYPE#{record['event_type']}",
        "GSI2SK": utc,
    }
    if record.get("session_id"):
        item["session_id"] = record["session_id"]
        item["GSI1PK"] = f"SESSION#{record['session_id']}"
        item["GSI1SK"] = utc
    if record.get("member_id"):
        item["member_id"] = record["member_id"]
    if record.get("duration_allocated_hours") is not None:
        item["duration_allocated_hours"] = record["duration_allocated_hours"]
    if record.get("notes"):
        item["notes"] = record["notes"]
    return item


def telemetry_item(record: dict[str, Any], shift_days: int = 0) -> dict[str, Any]:
    """Shape one telemetry reading.

    The nested maps are kept as maps: reads always want the whole reading, and flattening
    would only lengthen attribute names, which are billed on every write.
    """
    workstation = record["workstation_id"]
    timestamp = record["timestamp"]
    utc = datetime.fromisoformat(timestamp).astimezone(UTC).isoformat()
    item: dict[str, Any] = {
        "PK": f"WS#{workstation}",
        "SK": f"TS#{utc}",
        "workstation_id": workstation,
        "timestamp_utc": utc,
        "timestamp_source": timestamp,
        "zone": record["zone"],
        "status": record["status"],
        "hardware_metrics": record["hardware_metrics"],
        "network_diagnostics": record["network_diagnostics"],
        "peripherals_connected": record["peripherals_connected"],
        "expires_at": _shift_ttl(record["expires_at"], shift_days),
    }
    if record.get("active_session_id"):
        item["active_session_id"] = record["active_session_id"]
    if record.get("active_member_id"):
        item["active_member_id"] = record["active_member_id"]
    return item


# --------------------------------------------------------------------------- loading


def _load_file(
    source: SourceFile,
    table_name: str,
    shaper: Any,
    shift_days: int,
) -> int:
    """Write one file's records. Returns how many items were sent."""
    # parse_float=Decimal: DynamoDB rejects floats, and Decimal is the correct type anyway.
    with source.path.open("r", encoding="utf-8") as fh:
        records = json.load(fh, parse_float=Decimal)

    table = _RESOURCES.table(table_name)
    written = 0
    with table.batch_writer(overwrite_by_pkeys=["PK", "SK"]) as batch:
        for record in records:
            batch.put_item(Item=shaper(record, shift_days))
            written += 1
    return written


def load_dataset(
    dataset: str,
    *,
    landing: Path | None = None,
    run_id: str = "",
    threads: int | None = None,
    days: int | None = None,
    resume: bool = True,
) -> DynamoLoadReport:
    """Load ``workstation_events`` or ``telemetry`` into its table."""
    cfg = settings()
    landing = Path(landing or cfg.raw_landing)
    run_id = run_id or f"ddb-{datetime.now(UTC):%Y%m%d%H%M%S}"
    workers = threads or cfg.load_threads

    if dataset == "workstation_events":
        table_name, shaper, pipeline = cfg.ddb_events_table, event_item, EVENTS_PIPELINE
    elif dataset == "telemetry":
        table_name, shaper, pipeline = cfg.ddb_telemetry_table, telemetry_item, TELEMETRY_PIPELINE
    else:
        raise ValueError(f"{dataset!r} does not live in DynamoDB")

    sources = [s for s in discover(landing) if s.dataset == dataset]
    if days is not None:
        keep = sorted({s.batch_date for s in sources if s.batch_date})[:days]
        sources = [s for s in sources if s.batch_date in set(keep)]

    from aimternet.pipeline.manifest import sha256_file

    done = checkpoint.completed(pipeline) if resume else {}
    report = DynamoLoadReport(table=table_name, files_total=len(sources))

    pending: list[tuple[SourceFile, str]] = []
    for source in sources:
        digest = sha256_file(source.path)
        if done.get(str(source.path)) == digest:
            report.files_skipped += 1
            continue
        pending.append((source, digest))

    if not pending:
        log.info("%s: nothing to do, all %d file(s) already loaded", table_name, len(sources))
        return report

    log.info(
        "%s: loading %d file(s) with %d threads (%d already done)",
        table_name, len(pending), workers, report.files_skipped,
    )
    started = time.time()
    progress: Counter[str] = Counter()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_load_file, source, table_name, shaper, cfg.ddb_ttl_shift_days): (
                source,
                digest,
            )
            for source, digest in pending
        }
        for future in as_completed(futures):
            source, digest = futures[future]
            try:
                written = future.result()
            except Exception as exc:
                report.failures.append((source.path.name, str(exc)))
                log.error("failed to load %s: %s", source.path, exc)
                continue
            checkpoint.mark(pipeline, str(source.path), digest, written, run_id)
            report.files_loaded += 1
            report.items_written += written
            progress["files"] += 1
            if progress["files"] % 50 == 0:
                elapsed = time.time() - started
                log.info(
                    "  %d/%d files, %s items, %.0f items/s",
                    progress["files"], len(pending), f"{report.items_written:,}",
                    report.items_written / elapsed if elapsed else 0,
                )

    report.duration_seconds = time.time() - started
    return report


def item_counts() -> dict[str, int]:
    """Approximate item counts. DynamoDB updates these roughly every six hours."""
    cfg = settings()
    client = boto3.client("dynamodb", region_name=cfg.aws_region)
    counts = {}
    for name in (cfg.ddb_events_table, cfg.ddb_telemetry_table):
        try:
            counts[name] = int(client.describe_table(TableName=name)["Table"]["ItemCount"])
        except ClientError:
            counts[name] = -1
    return counts
