# AIMternet-Cafe — Data Engineering POC Build Prompt

**Target agent:** Claude Code, running inside the repo checkout on an EC2 instance
**Repo:** `RCEjosephkarl/AIMternet-Cafe` (branch `main`)
**Runtime:** single EC2 instance, one conda environment, no Docker
**Mode:** upgrade an existing repo in place — do not scaffold a new project beside it

---

## 0. Operating contract

Read this section before touching anything.

1. **Plan first, then build.** Start in plan mode. Produce a written plan mapped to the phases in §9 and wait for my approval before Phase 1 edits.
2. **Work in the repo.** All changes are commits in this checkout. Do not hand me snippets to paste.
3. **One phase, one commit.** Each phase ends with a runnable repo, a passing test run, and a commit whose message names the phase.
4. **Verify before claiming.** Never report a step as working unless you actually executed it and can show the output. If AWS credentials or a resource are missing, say exactly which step could not run and why. "Should work" is not a status.
5. **Never fabricate schema or business rules.** §4 and §5 below are the ground truth, extracted from the actual files in this repo. If something you need is genuinely absent, put it in a clearly marked `poc_policy` config module, log the assumption, and list it in your summary — do not invent a column.
6. **Cost and destruction guards.**
   - Do not run `terraform destroy`, `aws s3 rb`, `aws rds delete-*`, `aws redshift delete-*`, `aws dynamodb delete-table`, or any equivalent.
   - Do not create resources larger than the sizes pinned in §8.
   - Before the first command that provisions billable AWS infrastructure, stop and ask me.
   - Redshift and RDS are the expensive items. Everything upstream of them must be runnable and testable without them.
7. **Maintain `CLAUDE.md`** at the repo root as you go: stack, conda env name, env vars, how to run each piece, invariants a future session must not break.
8. **Keep the git history clean.** No secrets, no `.pyc`, no new large binaries. See §1.3 — the repo currently violates this and you will fix it.

---

## 1. Ground truth — repository state

This has been verified against the current `main`. Confirm it yourself in Phase 1, but do not re-derive it from scratch.

### 1.1 What exists

```text
.gitignore                      # contains only: .venv/  and  Project*.pdf
README.md                       # two lines, title + one-sentence description
run_synthesis.py                # 0 bytes
src/synthesizer/
    __init__.py                 # 0 bytes
    config.py                   # 0 bytes
    generator.py                # 0 bytes
    generators/
        __init__.py  catalog.py  dimensions.py  members.py
        rentals.py   concessions.py  events.py  telemetry.py     # all 0 bytes
    utils/
        __init__.py  writer.py  math_checks.py                   # all 0 bytes
data/raw-landing/               # ~4.4 GB working tree, ~176 MB packed
```

There is **no** application code, no tests, no CI, no IaC, no Airflow, no API, no notebook, no dependency manifest, no `environment.yml`, no `requirements.txt`, no `pyproject.toml`. You are not extending an existing framework choice — you are making the first one. Choose the stack in §2 and record the decision in `CLAUDE.md`.

### 1.2 What the data actually looks like

The layout is **not** flat. Every transactional dataset is partitioned by day under `legacy_batches/`:

```text
data/raw-landing/
├── catalog/
│   ├── workstations.csv              # 175 rows, static
│   └── concession_items.csv          # 10 rows, static
├── dimensions/
│   ├── dim_date.csv                  # 365 rows, calendar year 2026
│   └── dim_time.csv                  # 1440 rows, minute grain
├── legacy_batches/<YYYY-MM-DD>/      # 62 dirs, 2026-07-01 .. 2026-08-31
│   ├── members.csv                   # NEW registrations that day only
│   ├── rental_transactions.csv
│   ├── concession_purchases.csv
│   ├── concession_order_items.csv
│   ├── member_points_ledger.csv
│   └── workstation_events.json       # JSON array
└── telemetry/<YYYY-MM-DD>/<HH>.json  # 1488 files, JSON array, 2100 records each
```

Verified volumes:

| Dataset | Rows | Notes |
|---|---:|---|
| `catalog/workstations.csv` | 175 | 3 zones |
| `catalog/concession_items.csv` | 10 | |
| `dimensions/dim_date.csv` | 365 | |
| `dimensions/dim_time.csv` | 1440 | |
| `members.csv` (all batches) | 360 | new registrations only |
| `rental_transactions.csv` | 28,287 | |
| `concession_purchases.csv` | 21,077 | |
| `concession_order_items.csv` | 29,672 | |
| `member_points_ledger.csv` | 55,514 | |
| `workstation_events.json` | 58,218 | |
| `telemetry/**/*.json` | **~3,124,800** | **~4.4 GB on disk** |

Telemetry is 99% of the data volume and drives every performance decision in this POC. Treat it as the hard case, not an afterthought.

### 1.3 Known defects you must handle

These are real and already confirmed. Do not "discover" them late.

**D1 — The synthesizer source is empty.**
Every `.py` under `src/synthesizer/` is 0 bytes, but `__pycache__/*.cpython-312.pyc` files were committed with real bytecode. The generation logic exists only as compiled artifacts. Consequences:
- There is no readable code to reuse or follow as a convention. Build fresh.
- The business rules in §5 were recovered from that bytecode; they are authoritative for this POC.
- Add `__pycache__/`, `*.pyc`, `.env`, `*.parquet`, `.terraform/` to `.gitignore` and `git rm --cached` the committed bytecode in Phase 1. Do not delete the `.pyc` files from disk until you have finished cross-checking §5 against them.
- Flag this to me in your Phase 1 report. The synthesizer source needs to be recovered or rewritten separately; it is **out of scope** for this build unless I say otherwise.

**D2 — 840 members are referenced but never defined.**
`rental_transactions.csv` references 1,182 distinct `member_id` values. `members.csv` across all 62 batches contains only 360, all in the range `M-1841`+. The opening cohort **`M-1001` … `M-1840` has no source row anywhere in `data/raw-landing/`.** (This is consistent with the recovered config: `TOTAL_MEMBERS = 1200`, `INITIAL_COHORT_RATIO = 0.7` → 840 pre-existing members that were never snapshotted to file.)

This will break a naive FK-ordered load into RDS. It is the single most important validation case in the POC. Required handling:
- The validator must **detect and report** it as an orphan-FK finding, not crash on it.
- Implement a configurable resolution policy in a `poc_policy` module, defaulting to `synthesize_stub`:
  - `synthesize_stub` — insert minimal member rows for the orphan IDs, marked `source_system='DERIVED_FROM_TRANSACTIONS'` and `is_backfilled=true`, with tier and points balance inferred from the earliest ledger entry per member.
  - `quarantine` — reject the dependent rentals/purchases/ledger rows into the quarantine location and load nothing that violates FK.
- Whichever policy runs, the reconciliation report must state the count explicitly. Silently making 840 rows appear is a failure.

**D3 — Repo weight.** The working tree is ~4.4 GB. Confirm the EC2 volume has headroom before Bronze upload, since Parquet output and any local staging add to it. If disk pressure appears, stream rather than stage.

**D4 — CSVs are CRLF-terminated and may carry a UTF-8 BOM.** Parse with `encoding='utf-8-sig'` and explicit `newline=''`. A naive read leaves `\r` on the last column of every row.

---

## 2. Runtime environment

Everything runs in **one conda environment on one EC2 instance**. No Docker, no docker-compose, no MWAA, no Glue.

Deliver in Phase 1:

- `environment.yml` — pinned, named (suggest `aimternet`), Python 3.11.
  Python 3.11 rather than 3.12: it is the safest common denominator for the Airflow constraints file, and nothing here needs 3.12.
- `requirements-airflow.txt` installed via the official Airflow constraints URL for the pinned Airflow and Python version. Do not let pip resolve Airflow's dependency tree freely inside a conda env; it will fight conda's `pyarrow`/`numpy`.
- `Makefile` targets: `env`, `test`, `lint`, `typecheck`, `bootstrap`, `api`, `metrics`, `airflow`, `dashboard`, `reconcile`.
- Airflow runs as `LocalExecutor` against a local Postgres, or `SequentialExecutor` against SQLite if I have not provisioned a metadata DB. Make the choice config-driven and document both.

Path configuration — env vars with these defaults:

```text
AIMTERNET_RAW_LANDING=/opt/aimternet/data/raw-landing     # source of truth, READ-ONLY
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIMTERNET_DAG_INPUT=/opt/airflow/input/raw-landing        # read-only mount/symlink of the above
AIMTERNET_WORK_DIR=/opt/aimternet/work                    # scratch, safe to delete
AIMTERNET_QUARANTINE_DIR=/opt/aimternet/quarantine
```

Rules:
- The DAG reads from `AIMTERNET_DAG_INPUT` only.
- Nothing writes back into the raw landing directory. Ever. Enforce it — open source files read-only and assert it in a test.
- If the repo checkout is not at `/opt/aimternet`, provide a `make link` target that creates the symlinks, rather than hardcoding a developer's home directory.

Local development must work without any of these paths existing: fall back to the repo-relative `data/raw-landing` when the env var is unset, so tests run anywhere.

---

## 3. Target architecture

```text
EC2 raw landing (read-only)
        │
        ▼
     Airflow ──────────────► S3 Bronze (immutable)
                                 │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
              validate/     DynamoDB     quarantine/
              normalize    (events,       rejects
                    │      telemetry)
        ┌───────────┴───────────┐
        ▼                       ▼
  RDS PostgreSQL          S3 Silver → S3 Gold
  (operational)                        │
                                       ▼
                                   Redshift

Jupyter POS ──HTTP──► Operational API ──► RDS + DynamoDB
Dashboard   ──HTTP──► Metrics API      ──► Redshift + DynamoDB
```

Layer responsibilities — preserve these boundaries:

| Layer | Owns |
|---|---|
| EC2 raw landing | one-time bootstrap staging only |
| S3 Bronze | permanent immutable raw |
| S3 Silver/Gold | cleaned, then analytics-ready curated data |
| RDS PostgreSQL | transactional / current operational state |
| DynamoDB | high-volume workstation events + telemetry |
| Redshift | analytical warehouse |
| Airflow | orchestration only — no business logic in DAG files |
| Operational API | the **only** write path for POS |
| POS notebook | HTTP client, nothing else |

> **Deviation (2026-09-03):** §7.4 and the references to "the dashboard" below describe a
> static HTML/JS page served by the operational API at `/dashboard`. That page was replaced
> by a Streamlit app (`streamlit_app/`, `make streamlit`, port 8501) with three sections —
> PC Telemetry, Descriptive Analytics, Data Science. The constraint on the next line is
> unchanged and still enforced (now by `tests/unit/test_streamlit_boundary.py`): the
> dashboard talks to `/v1/metrics/*` over HTTP only, regardless of which UI framework renders
> it. See CLAUDE.md.

Hard prohibitions:
- The POS notebook must not import `psycopg`, `sqlalchemy`, or `boto3`, contain SQL, or hold credentials.
- The dashboard must not query RDS, Redshift, or DynamoDB directly.
- No business rule may be implemented twice. If the notebook and the API both need pricing, the API owns it.
- No arbitrary-SQL endpoint on any API.

After bootstrap, **S3 is the source of record.** Ongoing DAGs must not read the EC2 landing directory.

---

## 4. Source data contract

Exact headers, as they appear in the files. Build your schemas from this.

```text
catalog/workstations.csv
    workstation_id, zone_classification, base_hourly_rate,
    ip_address, mac_address, commissioned_date
    # zone_classification ∈ {Standard Zone, VIP Esports Zone, Streamer Pods}
    # workstation_id format: PC-001 .. PC-175

catalog/concession_items.csv
    item_sku, item_name, category, unit_cost_price,
    unit_retail_price, stock_quantity
    # category ∈ {Beverage, Hot Food, Snacks, Accessories}

dimensions/dim_date.csv
    date_id, calendar_date, day_of_week, day_of_month, month_label,
    month_index, quarter_index, year_index, weekend_flag, holiday_ph_flag

dimensions/dim_time.csv
    time_id, hour_24, minute_val, day_part_label

legacy_batches/<date>/members.csv
    member_id, first_name, last_name, email, phone_number,
    current_tier, current_points_balance, lifetime_spend_amount, registered_at

legacy_batches/<date>/rental_transactions.csv
    rental_id, member_id, workstation_id, session_start, session_end,
    duration_hours, base_hourly_rate, member_tier_applied, tier_discount_pct,
    final_hourly_rate, gross_rental_amount, points_redeemed,
    points_credit_value, net_amount_paid, points_accrued, payment_method

legacy_batches/<date>/concession_purchases.csv
    purchase_id, member_id, rental_id, total_amount,
    points_accrued, payment_method, purchased_at

legacy_batches/<date>/concession_order_items.csv
    order_item_id, purchase_id, item_sku, quantity, unit_price, total_price

legacy_batches/<date>/member_points_ledger.csv
    ledger_id, member_id, source_reference_id, transaction_type,
    points_delta, resulting_balance, created_at
    # transaction_type ∈ {RENTAL_ACCRUAL, CONCESSION_ACCRUAL,
    #                     RENTAL_REDEMPTION, TIER_BONUS}
```

`workstation_events.json` — JSON array:

```json
{
  "event_id": "EVT-20260701-6494-00000",
  "workstation_id": "PC-139",
  "event_timestamp": "2026-07-01T00:30:00+08:00",
  "event_type": "SESSION_START",
  "session_id": "SESS-20260701-6038-0015",
  "member_id": "M-1115",
  "duration_allocated_hours": 2.0,
  "client_os_version": "Win11-Pro-AIM-Build104",
  "notes": "Normal check-in via POS Terminal"
}
```
`event_type` ∈ `{SESSION_START, SESSION_END, HARDWARE_ALERT, PERIPHERAL_ALERT}`. Alert events have `session_id` / `member_id` unset — your schema must allow it.

`telemetry/<date>/<HH>.json` — JSON array, one record per workstation per 5-minute tick (175 × 12 = 2100 per file):

```json
{
  "workstation_id": "PC-001",
  "timestamp": "2026-07-21T01:00:00+08:00",
  "zone": "Standard Zone",
  "status": "IDLE",
  "active_session_id": null,
  "active_member_id": null,
  "hardware_metrics": { "cpu_load_pct": 4.0, "cpu_temp_c": 35, "ram_usage_pct": 26.6,
                        "gpu_load_pct": 2.7, "gpu_temp_c": 36,
                        "disk_io_read_mbs": 0.0, "disk_io_write_mbs": 0.3 },
  "network_diagnostics": { "latency_ping_ms": 7, "packet_loss_pct": 0.0,
                           "bandwidth_down_mbps": 1.4 },
  "peripherals_connected": { "keyboard": true, "mouse": true, "headset": true },
  "expires_at": 1785171600
}
```
`status` ∈ `{IDLE, OCCUPIED}`. `expires_at` is a Unix epoch seconds TTL, set to `timestamp + 7 days` — wire it to the DynamoDB TTL attribute rather than inventing a new one.

Cross-file invariants worth asserting in validation:
- All timestamps are ISO-8601 with `+08:00` (Asia/Manila). Store UTC internally; keep the original offset in a lineage column.
- `rental_transactions` ↔ `workstation_events`: every `SESSION_START` and `SESSION_END` should map to a `rental_id`.
- `telemetry.active_session_id` should resolve to a rental when `status = OCCUPIED`.
- `concession_order_items.total_price` = `quantity × unit_price`; sum of line items per `purchase_id` = `concession_purchases.total_amount`.
- `member_points_ledger.resulting_balance` should be a running sum of `points_delta` per member in `created_at` order.
- `rental_id`, `purchase_id`, `order_item_id`, `ledger_id`, `event_id` are unique across all batches (verified — treat a duplicate as a hard failure, not a warning).

---

## 5. Business rules of record

Recovered from the committed synthesizer bytecode. Put these in one typed configuration module (`config/business_rules.py` or YAML + a typed loader). The API, the transformations, and the tests all read from that single place.

**Workstation zones and base pricing (PHP):**

| Zone | PC range | Base hourly rate |
|---|---|---:|
| Standard Zone | PC-001 – PC-100 | 50.00 |
| VIP Esports Zone | PC-101 – PC-150 | 80.00 |
| Streamer Pods | PC-151 – PC-175 | 120.00 |

**Membership tiers:**

| Tier | Discount | Points multiplier | Upgrade threshold (lifetime spend) | Next tier | Upgrade bonus |
|---|---:|---:|---:|---|---:|
| Standard | 0% | 1.00× | 3,000.00 | Silver | 50 pts |
| Silver | 10% | 1.25× | 10,000.00 | Gold | 100 pts |
| Gold | 20% | 1.50× | — | — | — |

**Points:**
- Accrual: 1 point per ₱10 of net amount paid, multiplied by the tier multiplier, floored. Applies to both rentals and concession purchases.
- Redemption: in units of **100 points**, each worth **₱50.00** credit. Redemption applies to rentals.
- Ledger `resulting_balance` must stay consistent after every accrual, redemption, or tier bonus.

**Rental price calculation (the order matters):**
```
gross_rental_amount = base_hourly_rate × duration_hours
final_hourly_rate   = base_hourly_rate × (1 − tier_discount_pct)
points_credit_value = (points_redeemed / 100) × 50.00
net_amount_paid     = (final_hourly_rate × duration_hours) − points_credit_value
points_accrued      = floor(net_amount_paid / 10 × tier_points_multiplier)
```
Validate this against the source rows during Phase 3 — if the arithmetic in the file disagrees with the formula, that is a data-quality finding to report, not a reason to change the formula.

**Other constants:** payment methods `{Cash, GCash, Maya, Credit Card}`; session durations 1/2/3/5/8 hours; client build `Win11-Pro-AIM-Build104`; PH holidays in window: 2026-08-21 (Ninoy Aquino Day), 2026-08-31 (National Heroes Day). Simulation window: 2026-07-01 → 2026-08-31.

Use `Decimal` for every monetary value, end to end. No floats in money paths, no float columns for money in any schema.

---

## 6. Pipeline requirements

### 6.1 Bootstrap DAG — `bootstrap_raw_landing`

Manual trigger only (`schedule=None`). Task groups, not one giant task.

**Stage A — inventory and manifest.** For every source file: verify existence, capture size and mtime, compute SHA-256, register a manifest row, leave the file untouched. Manifest fields at minimum:

```text
source_file, source_type, batch_date, checksum, file_size,
load_timestamp, status, record_count, error_count, run_id
```

Persist the manifest somewhere durable (a Postgres control table is fine; a JSON/Parquet manifest in S3 is also fine) and make it the idempotency key: a file whose checksum is already registered as `LOADED` is skipped, not reloaded.

**Stage B — S3 Bronze.** Byte-identical copy, deterministic layout:

```text
s3://<bucket>/bronze/<dataset>/batch_date=<YYYY-MM-DD>/<filename>
s3://<bucket>/bronze/telemetry/date=<YYYY-MM-DD>/hour=<HH>/<filename>
```

Bronze objects are never mutated. Enable versioning on the bucket. Use multipart upload with a bounded thread pool; 1,488 telemetry files at ~3 MB each is the workload to design for, and it must be resumable after an interruption.

**Stage C — validation.** Required columns, types, datetime formats, nullability, PK uniqueness, FK resolution, duplicates, numeric ranges, and the business-rule checks from §5. Emit machine-readable results (JSON per run + a summary table). Rejected records go to `AIMTERNET_QUARANTINE_DIR` and to `s3://<bucket>/quarantine/...` with the rejection reason and source lineage attached. Nothing is discarded silently.

**Stage D — normalization.** Deterministic dedup rules, typed intermediate datasets, and lineage columns (`source_file`, `source_checksum`, `ingested_at_utc`, `run_id`) carried forward from here to Gold.

### 6.2 RDS PostgreSQL

Load order:
```
workstations, concession_items  →  members  →  rental_transactions
  →  concession_purchases  →  concession_order_items  →  member_points_ledger
```

Provide real migrations (Alembic, or plain numbered SQL with a `schema_migrations` table — pick one and stay consistent). Implement PKs, FKs, unique constraints, `created_at`/`updated_at` in UTC, and indexes that serve the POS access patterns in §7: available workstations by status, member lookup by ID, active rental by workstation, ledger by member and time.

The historical load must be idempotent — `INSERT ... ON CONFLICT DO NOTHING` / `DO UPDATE`, or staged tables plus a merge. Load order alone must not be used to paper over invalid references; the FK check in Stage C is what catches D2, and the loader honours the configured policy.

Run reconciliation after the load and fail loudly if critical checks fail.

### 6.3 DynamoDB

Two tables. Document key design and access patterns in `docs/dynamodb.md` before writing the loader.

Suggested starting point — justify or revise it:

```text
workstation_events
    PK: WS#<workstation_id>          SK: EVT#<event_timestamp>#<event_id>
    GSI1: SESSION#<session_id>       SK: <event_timestamp>
    GSI2: TYPE#<event_type>          SK: <event_timestamp>

workstation_telemetry
    PK: WS#<workstation_id>          SK: TS#<iso8601_utc>
    TTL attribute: expires_at        (from source, epoch seconds)
```

Access patterns to support: workstation lookup, latest status per workstation, event history for a workstation or session, telemetry over a time range.

Do not model DynamoDB as a relational store. No scans in the operational path.

**Telemetry loading is the performance problem in this POC.** 3.1M items will not load with a naive `put_item` loop. Required:
- `batch_writer()` with a bounded thread pool, exponential backoff, and unprocessed-item retry.
- Stream and parse file-by-file — never hold the full dataset in memory.
- On-demand billing mode, or provisioned capacity with a documented pre-load scale-up and post-load scale-down.
- A `POC_TELEMETRY_DAYS` config (default `7`) so the full 62-day load is opt-in. The full load must be *possible* and *documented with a runtime estimate*; it must not be the default that runs before I have approved the spend.

### 6.4 S3 Silver and Gold

Silver: cleaned, typed, deduplicated, UTC-normalized, one dataset per source entity, partitioned by date. Gold: analytics-ready, conformed, ready for Redshift `COPY`.

Parquet with Snappy compression for both. Partition telemetry and events by `date=`/`hour=`. Write with `pyarrow` in row-group batches, not by materialising a 3M-row DataFrame.

Minimum datasets: members, workstations, rentals, concession purchases, concession order items, points ledger, workstation events, telemetry, plus the two conformed dimensions from `dimensions/`.

### 6.5 Redshift

Dimensional model. Suggested:

```text
dim_member (SCD2 on tier), dim_workstation, dim_date, dim_time, dim_concession_item
fact_rental, fact_concession_sale, fact_concession_line_item,
fact_points_activity, fact_workstation_event
agg_workstation_utilization_hourly   -- pre-aggregated from telemetry
```

Do not load raw 5-minute telemetry into Redshift. Aggregate to hourly utilization per workstation in Gold and load that.

Initial load via `COPY` from S3 using an IAM role (`iam_role` parameter — never inline keys). Upserts via staging table + `MERGE` (or `DELETE`+`INSERT` in one transaction). Rerunning must not duplicate facts. Distribution and sort keys should be chosen deliberately and explained in a comment.

If pause/resume is not available for the chosen Redshift deployment mode, make it a manual, documented step rather than pretending an unsupported API call works.

### 6.6 Reconciliation

Compare counts across every hop: source → Bronze → validated (accepted/rejected) → RDS → DynamoDB (where measurable) → Silver → Gold → Redshift.

Integrity checks: duplicate PKs, orphan FKs, negative or null monetary values, timestamps outside the 2026-07-01 → 2026-08-31 window, points balance drift, rentals with `session_end < session_start`, overlapping active rentals per workstation, inventory going negative.

Produce a single readable report (Markdown or HTML) plus a JSON artifact. The DAG fails on critical checks. Warnings — including the D2 backfill count — are reported, not swallowed.

### 6.7 Ongoing DAGs

After bootstrap, S3 is the source. These DAGs must never read the EC2 landing directory.

```text
bootstrap_raw_landing        # manual, once
rds_to_s3_incremental        # scheduled, watermark on updated_at
dynamodb_to_s3_incremental   # scheduled, export or timestamp-bounded query
curate_silver_gold           # scheduled, downstream of both
load_redshift                # scheduled, downstream of curate
reconcile_data               # scheduled, downstream of load
```

Use explicit checkpoints/watermarks stored in a control table or S3 manifest. Keep DAG files thin — they import and call functions from `src/`; the logic lives in testable modules.

---

## 7. Operational API and clients

### 7.1 Operational API

FastAPI + Pydantic v2 (nothing exists to inherit from). Endpoints:

```http
GET  /v1/workstations/available
GET  /v1/members/{member_id}
POST /v1/rentals/check-in
POST /v1/concessions/purchases
POST /v1/rentals/check-out
GET  /healthz
```

**Check-in:** validate member exists and is active; validate workstation exists and is genuinely available; price from zone base rate; apply tier discount; create the rental row and update workstation state **in one database transaction**; emit the `SESSION_START` event. Reject a second check-in on an occupied workstation, and reject a member who already has an open rental.

**Concession purchase:** validate member (where required) and each SKU; validate price against the catalog rather than trusting the client; check inventory; compute totals; create the purchase and its line items atomically; decrement inventory in the same transaction; write the points ledger entry.

**Check-out:** find the active rental; compute actual duration; compute final price under §5; apply point redemption in 100-point units if requested and affordable; close the rental; free the workstation; emit `SESSION_END`; return the completed transaction. Checking out a rental that is already closed returns a clean 409, not a 500.

Cross-cutting: every write is idempotency-key aware or otherwise safe to retry; concurrency on workstation state uses `SELECT ... FOR UPDATE` or a unique partial index on active rentals — do not rely on read-then-write; all errors return structured problem responses; all requests are logged with a correlation ID.

### 7.2 Metrics API

Read-only, separate router or separate app: active rentals, available/occupied workstations by zone, revenue today split rental vs concession, points issued and redeemed, per-workstation status, recent telemetry summary. Serve from Redshift + DynamoDB. Cache where the query is expensive.

### 7.3 POS notebook

`notebooks/pos_terminal.ipynb`. Front-desk operator's view. It talks to the API over HTTP and nothing else — no `psycopg`, no `boto3`, no SQL, no credentials, no duplicated pricing logic. Demonstrate the full flow: list available workstations → look up member → check in → buy a concession → check out. Plain language, visible outputs, one cell per action.

Add a test that greps the notebook JSON for forbidden imports and fails the build if any appear. The boundary should be enforced, not just requested.

### 7.4 Dashboard

Minimal single-page HTML/JS reading the Metrics API. No direct database access. Auto-refresh is fine; a build toolchain is not needed.

---

## 8. Configuration, security, infrastructure

Never hardcode: AWS account IDs, access keys, DB passwords, Redshift credentials, bucket names, table names, hostnames, or the EC2 paths.

Deliver a typed settings module (`pydantic-settings`) plus `.env.example` with safe placeholders. Fail fast at startup with a clear message when a required variable is missing. Never commit a real `.env`. Prefer the EC2 instance role over static keys; if static keys must be supported, they come from the environment only.

Infrastructure as Terraform under `infra/`, clearly separate from application code, with the smallest usable sizes:

- S3: one bucket, versioning on, public access blocked, lifecycle rule on `quarantine/` and Bronze telemetry
- DynamoDB: on-demand billing, TTL enabled on `expires_at`
- RDS: `db.t3.micro`, `gp3`, single-AZ, not publicly accessible
- Redshift: Serverless with the minimum RPU, or a single `dc2.large` node — flag either as the main cost item
- IAM: least-privilege roles for EC2 and for the Redshift `COPY`

Annotate every billable resource in the Terraform with an estimated monthly cost comment, and put a cost summary in the README. `terraform plan` is yours to run; `apply` needs my approval; `destroy` you never run.

---

## 9. Phases and definition of done

Each phase ends runnable, tested, committed.

| Phase | Work | Done when |
|---|---|---|
| 1 | Inspect repo, confirm §1, fix `.gitignore`, add `environment.yml` / `Makefile` / `CLAUDE.md`, project skeleton, test harness | `make env && make test` passes on an empty suite; D1 reported to me |
| 2 | Typed config, business rules module, Pydantic schemas for all 10 datasets, Postgres migrations | Schemas validate a real sample from every dataset; migrations apply and roll back |
| 3 | Manifest, checksums, validation, quarantine, S3 Bronze upload | Full validation runs locally against all 62 batches with no AWS; D2 detected and reported with the exact count 840 |
| 4 | RDS loader + DynamoDB loader | Rerun produces zero duplicates; telemetry loads a 7-day slice within a documented runtime |
| 5 | Silver + Gold transformations | Parquet written; row counts reconcile against Bronze; telemetry processed streaming, peak RSS documented |
| 6 | Redshift DDL, `COPY`, `MERGE` | Second run adds zero facts; or, if Redshift is unavailable, SQL is written and unit-tested and the gap is stated |
| 7 | Reconciliation engine and report | Report generated end to end; critical failure actually fails the DAG |
| 8 | Operational API | All endpoints tested including the concurrency and double-check-in cases |
| 9 | POS notebook | Full flow executes against a running API; forbidden-import test passes |
| 10 | Metrics API + dashboard | Dashboard renders live numbers via HTTP only |
| 11 | Airflow DAGs, all six | DAGs import cleanly, `airflow dags test` passes for each |
| 12 | End-to-end run, docs, cleanup | §10 checklist verified item by item, with evidence |

Testing throughout: unit tests for validation, transformation, and pricing (fixtures drawn from the real files, kept small); API tests against a test database; `moto` or equivalent for S3/DynamoDB; DAG import tests; property tests on the money arithmetic. `pytest`, `ruff`, and `mypy` on `src/`.

---

## 10. Acceptance checklist

Verify each item by executing it, and report the evidence:

1. Raw files sit in the configured landing path, untouched after the run.
2. `bootstrap_raw_landing` runs to success from the Airflow UI or CLI.
3. Bronze contains byte-identical copies with a matching checksum manifest.
4. Validation results and the quarantine location are populated and readable.
5. RDS holds referentially valid operational data; the 840-member gap is resolved per the configured policy and reported.
6. DynamoDB serves each documented access pattern; TTL is set from `expires_at`.
7. Silver and Gold Parquet exists and reconciles to Bronze.
8. Redshift is queryable and returns sensible revenue and utilization numbers.
9. The reconciliation report shows no *unexplained* discrepancy.
10. The API starts and the POS notebook completes the full flow.
11. Business rules are demonstrably enforced server-side — show a rejected invalid check-in.
12. The notebook contains no database access — show the passing test.
13. The dashboard displays live metrics through the Metrics API.
14. A second full run creates no duplicate logical records anywhere.

---

## 11. Final report

When you finish, give me:

- What was built, by phase
- Files created and modified
- Every assumption made, especially any `poc_policy` decision
- AWS resources that must be created manually, with estimated cost
- Exact commands to run the POC start to finish
- **Anything that could not be executed**, and why — missing credentials, missing resources, or time
- Known limitations and what I should do next

Do not describe the POC as working where you have not run it.