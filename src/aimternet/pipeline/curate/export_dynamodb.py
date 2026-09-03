"""DynamoDB -> S3 export (spec §6.7 ``dynamodb_to_s3_incremental``).

Events and telemetry reach S3 Bronze during the bootstrap, and Bronze is immutable. What this
moves is what arrived *since*: the SESSION_START and SESSION_END events the operational API
emits as the POS is used, which exist only in DynamoDB until this runs.

Bounded by timestamp through GSI2, never scanned. A scan of a 6.3M-item table would get
slower and more expensive every day, to find a handful of new rows.
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
from aimternet.pipeline.curate.engine import duck, layer_uri, write_parquet
from aimternet.pipeline.curate.export_rds import get_watermark, set_watermark

log = logging.getLogger(__name__)

PIPELINE = "dynamodb_to_s3:workstation_events"


@dataclass
class DynamoExportReport:
    events: int = 0
    since: str = ""
    destination: str = ""

    def summary(self) -> str:
        return (
            f"DynamoDB -> S3: {self.events:,} event(s) since {self.since}"
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
    if items:
        # Attribute values arrive as Decimal and mixed types; casting to string keeps the
        # export faithful without guessing at a schema the warehouse does not read.
        frame = pd.DataFrame(items).astype(str)
        destination = layer_uri("silver", "workstation_events_operational")
        with duck() as con:
            con.register("events_frame", frame)
            write_parquet(con, "SELECT * FROM events_frame", destination)
            con.unregister("events_frame")
        report.destination = destination

    set_watermark(PIPELINE, now, run_id or "dynamodb_to_s3")
    log.info("%s", report.summary())
    return report
