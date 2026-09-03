# DynamoDB key design

Written before the loader, as spec §6.3 requires. Two tables, both on-demand billing.

## Why DynamoDB holds these two datasets

Workstation events and telemetry are append-only, high-volume, and always read by
*workstation and time range* — never joined, never aggregated at read time. That is the shape
DynamoDB is good at, and it is the shape that would make RDS miserable: 6.3M telemetry rows
would dominate an operational database that otherwise holds ~135k rows.

Everything relational stays in RDS. These tables are **not** a relational store with a
different syntax, and nothing in the operational path scans them.

---

## `aimternet_workstation_events`

58,218 items. Session lifecycle and hardware alerts.

```
PK  WS#<workstation_id>          e.g. WS#PC-139
SK  EVT#<event_timestamp_utc>#<event_id>
```

The sort key leads with the timestamp so a range query returns a workstation's history in
chronological order, and appends `event_id` so two events on the same workstation at the same
instant cannot collide. Timestamps are UTC ISO-8601, which sorts lexicographically — the
property the whole design leans on.

**GSI1 — by session**

```
GSI1PK  SESSION#<session_id>     GSI1SK  <event_timestamp_utc>
```

Answers "show me this rental's start and end". Alert events carry a session too in this
dataset (finding F4), so they appear here as well, which is useful rather than surprising.

**GSI2 — by event type**

```
GSI2PK  TYPE#<event_type>        GSI2SK  <event_timestamp_utc>
```

Answers "all hardware alerts in the last hour" without a scan. Only four distinct types, so
this index is deliberately narrow: it is for alert triage, not analytics. Analytics belongs
in Redshift.

### Access patterns

| Need | Query |
|---|---|
| A workstation's event history | `PK = WS#PC-139`, SK between two `EVT#` bounds |
| Latest event for a workstation | `PK = WS#PC-139`, `ScanIndexForward=False`, `Limit=1` |
| Both ends of one session | GSI1, `GSI1PK = SESSION#<id>` |
| Recent alerts of a type | GSI2, `GSI2PK = TYPE#HARDWARE_ALERT`, SK ≥ cutoff |

---

## `aimternet_workstation_telemetry`

**6,300,000 items** — not the ~3.1M spec §1.2 projects. The sampling interval is not constant
(finding F6): 2026-07-01…08-24 tick every 300s (2,100 records per hourly file) and
2026-08-25…08-31 tick every 30s (21,000 per file). Every capacity and cost number below uses
the real figure.

```
PK  WS#<workstation_id>          e.g. WS#PC-001
SK  TS#<timestamp_utc>
```

One item per workstation per tick. The partition key spreads writes across 175 partitions,
which is what keeps a 6.3M-item load from hot-spotting; the sort key makes "telemetry for
PC-001 between 14:00 and 15:00" a single range query.

**TTL attribute: `expires_at`** — carried from the source verbatim, epoch seconds.

> **TTL is disabled by default.** `expires_at` is `timestamp + 7 days`, and the simulated
> window is already in the past, so enabling TTL would delete about 61 of 62 days within ~48h
> of loading — after paying to write them. This is finding F2. The attribute is still written
> exactly as the source specifies, so §10.6 is satisfiable on demand:
> `AIMTERNET_DDB_TTL_ENABLED=true` turns enforcement on, and
> `AIMTERNET_DDB_TTL_SHIFT_DAYS=N` rebases the values into the future for a live-expiry demo.

### Access patterns

| Need | Query |
|---|---|
| Latest status for a workstation | `PK = WS#PC-001`, `ScanIndexForward=False`, `Limit=1` |
| Telemetry over a time range | `PK = WS#PC-001`, SK between `TS#<from>` and `TS#<to>` |
| Floor status right now | 175 parallel `Limit=1` queries — bounded, no scan |

"Status of every workstation" is 175 point queries rather than a scan. At 175 partitions that
is a few hundred milliseconds and a predictable cost, whereas a scan reads all 6.3M items and
gets slower every day.

---

## Item shape

Telemetry keeps the source's nested structure — `hardware_metrics`, `network_diagnostics`,
`peripherals_connected` stay as maps rather than being flattened into 13 top-level attributes.
Reads always want the whole reading, flattening would only lengthen attribute names, and
attribute names are billed on every write.

Decimals are written as DynamoDB `N`, never as floats. `boto3` is configured so
`cpu_load_pct` round-trips as `Decimal("4.0")`.

---

## Loading 6.3M items

`put_item` in a loop would take days. The loader:

* uses `batch_writer()` (25 items per request) with a bounded thread pool,
* streams file by file, never holding more than one file's records in memory,
* retries unprocessed items with exponential backoff — `BatchWriteItem` succeeds partially
  and silently returns what it did not write,
* checkpoints per file so an interrupted run resumes.

On-demand billing means no capacity planning, and no scale-down to forget afterwards. A new
on-demand table starts at a baseline throughput and scales up under sustained load, so the
first minutes are slower than the steady state.

### Cost

| | Items | Write units | Cost |
|---|---:|---:|---:|
| Events | 58,218 | ~58k | ~$0.07 |
| Telemetry | 6,300,000 | ~6.3M | ~**$7.88** |
| Storage (TTL off) | ~4.4 GB | — | ~$1.10/month |

At $1.25 per million write request units, ~1 KB per item. Storage accrues monthly until the
items are deleted or TTL is enabled.

Spec §6.3's `POC_TELEMETRY_DAYS` is `AIMTERNET_TELEMETRY_DAYS` here. It defaults to 62 — the
full load — because that was explicitly approved; set it to 7 for a cheap partial load.
