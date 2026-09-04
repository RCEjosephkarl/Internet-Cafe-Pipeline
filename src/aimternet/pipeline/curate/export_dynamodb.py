"""DynamoDB -> S3 export (spec §6.7 ``dynamodb_to_s3_incremental``).

Events and telemetry reach S3 Bronze during the bootstrap, and Bronze is immutable. What this
moves is what arrived *since*: the SESSION_START and SESSION_END events the operational API
emits as the POS is used, which exist only in DynamoDB until this runs.

Bounded by timestamp through GSI2, never scanned. A scan of a 6.3M-item table would get
slower and more expensive every day, to find a handful of new rows.

Incremental by watermark, but what it *writes* is always a full snapshot -- the same rule the
RDS export follows, for the same reason (F7). Gold reads
``silver/workstation_events_operational/`` as the current set of API-emitted events, so an
export that left its delta there would discard every event exported before it. This DAG runs
hourly, so that would have meant keeping the last hour and losing the rest, forever.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key

from aimternet.config.business_rules import rules
from aimternet.config.settings import settings
from aimternet.pipeline.curate.engine import (
    count_parquet,
    duck,
    layer_uri,
    merge_onto_snapshot,
    write_parquet,
)
from aimternet.pipeline.curate.export_rds import get_watermark, set_watermark

log = logging.getLogger(__name__)

PIPELINE = "dynamodb_to_s3:workstation_events"
DATASET = "workstation_events_operational"

#: Merge key. Unique across both origins: bootstrap events carry the source ``event_id``,
#: API events carry ``EVT-API-<%Y%m%d%H%M%S%f>`` (api/services/rentals.py).
KEY = "event_id"


@dataclass
class DynamoExportReport:
    events: int = 0           # events moved this run
    snapshot_rows: int = 0    # events in Silver afterwards
    since: str = ""
    destination: str = ""

    def summary(self) -> str:
        # Both numbers, always. A snapshot the same size as the delta is the F7 signature.
        return (
            f"DynamoDB -> S3: {self.events:,} event(s) since {self.since}, "
            f"snapshot now {self.snapshot_rows:,} event(s)"
            + (f" -> {self.destination}" if self.destination else "")
        )


def export_events(run_id: str = "", *, lookback_hours: int = 24) -> DynamoExportReport:
    """Copy events newer than the watermark into Silver."""
    cfg = settings()
    since = get_watermark(PIPELINE) or datetime.now(UTC) - timedelta(hours=lookback_hours)
    now = datetime.now(UTC)
    report = DynamoExportReport(since=since.isoformat())

    table = boto3.resource("dynamodb", region_name=cfg.aws_region).Table(cfg.ddb_events_table)

    items: list[dict] = []
    for event_type in sorted(rules().event_types):
        # GSI2 is partitioned by event type and sorted by time, so this is a bounded range
        # query per type -- four small queries instead of one enormous scan.
        response = table.query(
            IndexName="GSI2",
            KeyConditionExpression=(
                Key("GSI2PK").eq(f"TYPE#{event_type}") & Key("GSI2SK").gte(since.isoformat())
            ),
        )
        items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            response = table.query(
                IndexName="GSI2",
                KeyConditionExpression=(
                    Key("GSI2PK").eq(f"TYPE#{event_type}")
                    & Key("GSI2SK").gte(since.isoformat())
                ),
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response.get("Items", []))

    report.events = len(items)
    destination = layer_uri("silver", DATASET)

    with duck() as con:
        if not items:
            # Nothing arrived, so the snapshot already on S3 is still correct. Leaving it
            # untouched is both cheaper and safer than rewriting it identically.
            report.snapshot_rows = count_parquet(con, destination)
            log.info("dynamodb->s3: nothing new since %s", since)
        else:
            # Attribute values arrive as Decimal and mixed types; casting to string keeps the
            # export faithful without guessing at a schema the warehouse does not read.
            # Nulls are masked back out afterwards: events carry optional attributes
            # (session_id, member_id, notes), and astype(str) would otherwise turn an absent
            # one into the literal string "nan", which UNION ALL BY NAME cannot line up
            # against a real NULL in the previous snapshot.
            frame = pd.DataFrame(items)
            frame = frame.astype(str).where(frame.notna(), None)

            con.register("events_frame", frame)
            merged = merge_onto_snapshot(
                con, delta="events_frame", destination=destination, key=KEY
            )
            # Materialise the merge before overwriting the file it reads from.
            con.execute(f"CREATE OR REPLACE TEMP TABLE events_merged AS {merged}")
            write_parquet(con, "SELECT * FROM events_merged", destination)
            counted = con.execute("SELECT count(*) FROM events_merged").fetchone()
            report.snapshot_rows = int(counted[0]) if counted else 0
            con.execute("DROP TABLE events_merged")
            con.unregister("events_frame")
            report.destination = destination

    set_watermark(PIPELINE, now, run_id or "dynamodb_to_s3")
    log.info("%s", report.summary())
    return report
