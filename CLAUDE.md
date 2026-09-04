# CLAUDE.md — AIMternet-Cafe POC

Working notes for any future session. Read this before touching anything.

## What this is

A data engineering POC for an internet cafe: 4.2 GB of synthesized 2026-07-01…2026-08-31 operational
data flows from an EC2 landing directory through Airflow into S3 Bronze, is validated and normalized,
loads into RDS PostgreSQL (operational) and DynamoDB (events + telemetry), is curated into S3
Silver/Gold Parquet, and lands in Redshift as a dimensional model. A FastAPI operational API is the only
write path for the Jupyter POS terminal; a metrics API feeds a Streamlit dashboard.

Build spec: `pipeline_plan_AWS.md` (authoritative). `pipeline_plan.md` is an earlier, looser draft.

## Stack

| | |
|---|---|
| Conda env | `data_eng` — **Python 3.12**, at `~/.conda/envs/data_eng` |
| Orchestration | Apache Airflow 3.3.0, `AIRFLOW_HOME=~/airflow`, LocalExecutor + SQLite |
| API | FastAPI + Pydantic v2 + uvicorn |
| Data | **DuckDB** (Parquet + Snappy, Silver/Gold transforms), pandas, `Decimal` for all money |
| AWS | boto3, `redshift_connector`, psycopg2 |
| Quality | pytest, ruff, mypy, moto |

**Deviation from spec §2:** the plan suggests Python 3.11 for Airflow constraint safety. This env is
3.12 and Airflow 3.3.0 is already installed and passing `airflow db check` on it. Rebuilding was judged
pure downside. `environment.yml` is pinned from the working env.

**Deviation from spec §6.4 — DuckDB replaces pyarrow.** pyarrow is *not installed*, deliberately. On
this host (kernel `7.0.0-1010-aws`) Arrow's thread pool aborts at interpreter shutdown with
`terminate called without an active exception` (SIGABRT, exit 134). Measured: **23/30** runs with the
pip wheel (reproduced on 16.1.0 and 25.0.1) and **11/20** with conda-forge pyarrow 15 in a clean probe
env — so it is the host, not the packaging. Imports alone are safe; the crash needs actual Parquet I/O.
Written data was always correct, but the process died on the way out, which would fail Airflow tasks and
pytest runs at random. fastparquet was also rejected: it silently downcasts `Decimal` to `float64`,
which breaks the no-floats-in-money rule. DuckDB writes true `decimal128(12,2)` Parquet, crashed 0/20,
flattens the nested telemetry JSON natively, and converted one day of telemetry (24 files, 50,400 rows)
in 0.2 s at 159 MB peak RSS. If you reintroduce pyarrow, re-run the stress test first.

**pyarrow is back, but only for Streamlit.** The dashboard pivoted to Streamlit (below), whose
`st.dataframe`/`st.table`/native charts hard-require pyarrow to serialize `pandas.DataFrame` via
Arrow IPC — a different code path from Parquet I/O. Re-probed 2026-09-03 on this same host/kernel:
**30/30** clean fresh-process runs round-tripping a DataFrame through `pyarrow.Table.from_pandas` +
Arrow IPC with `pyarrow==25.0.1`. As a methodology sanity check, a fresh attempt to reproduce the
*original* Parquet-write crash with the same wheel/kernel also came back clean (**0/45**) — that
finding did not reproduce this time, for reasons not investigated further (this is a POC; recorded
as-is rather than silently overwritten). DuckDB still owns every Parquet read/write in the
pipeline — pyarrow is not reintroduced there, and a follow-up DuckDB Parquet-write check with
pyarrow now co-installed also came back clean (0/15).

## Invariants — do not break these

1. **`data/raw-landing/` is read-only.** Nothing writes back into it, ever. `tests/unit/test_landing_immutable.py`
   asserts checksums are unchanged after a run. Three stray `.ipynb_checkpoints/` dirs live in there;
   leave them alone — file discovery skips dot-directories.
2. **One pricing implementation.** `src/aimternet/config/business_rules.py` owns it. The API, the
   transforms and the tests all import from there. The POS notebook computes nothing.
3. **The POS notebook is an HTTP client.** No `psycopg2`, `sqlalchemy`, `boto3`, SQL, or credentials in
   `notebooks/pos_terminal.ipynb`. `tests/unit/test_notebook_boundary.py` enforces it. The *lens*
   notebooks legitimately read databases — the test targets `pos_terminal.ipynb` only.
4. **After bootstrap, S3 is the source of record.** Ongoing DAGs must never read the EC2 landing dir.
5. **RDS and Redshift are shared instances.** Stay inside schema `aimternet_oltp` (RDS) and
   `aimternet_olap` (Redshift). Never touch `public`, `simple_oltp`, `bus_ticketing`, `krusty_krab_olap`.
6. **No secrets in git.** `.env`, `*_pw.txt`, `*.pem`, `*.key` are gitignored. Config comes from
   `pydantic-settings` reading the environment.
7. **Never run** `terraform destroy`, `aws s3 rb`, `aws rds delete-*`, `aws redshift delete-*`, or
   `aws dynamodb delete-table`. Terraform in `infra/` manages *only* S3 config and the two DynamoDB
   tables — the pre-existing RDS and Redshift are deliberately out of its reach.
8. **Airflow DAG files stay thin.** They import and call `src/aimternet/`; no business logic in `dags/`.
9. **Every incremental export leaves a snapshot, never a delta — and something must read
   it.** Gold reads `silver/<dataset>_operational/` as the *current* state of its source, so
   an incremental run merges its delta onto the previous snapshot by primary key before
   writing. One implementation: `curate/engine.py::merge_onto_snapshot`, used by both
   `export_rds` and `export_dynamodb`. Use `NOT EXISTS`, never `NOT IN` — one NULL key in the
   delta makes `NOT IN` discard the entire previous snapshot.
   `tests/unit/test_export_rds_merge.py` and `test_export_dynamodb_merge.py` pin the merge;
   reconciliation checks the result (`silver_snapshot:*`, CRITICAL).

   The second half is not optional. This invariant named only the RDS export for one phase,
   and `export_dynamodb` — which imports that module's watermark helpers — wrote its delta
   over `workstation_events_operational` hourly for its whole life. Nothing failed, because
   **no query read the dataset**: a snapshot nobody reads cannot be observed to be wrong.
   `tests/unit/test_operational_snapshots_are_consumed.py` requires every snapshot to be read
   by Gold or declared unread with a reason, and `CONSUMED_SNAPSHOTS` pins the six that carry
   money so they cannot be declared away again. Only two are unread now —
   `workstations_operational` and `concession_items_operational` — and for a stated reason:
   the one column each adds (`status`, `stock_quantity`) is a fast-changing measure that does
   not belong in a Type-1 dimension. See F9.
10. **Terraform configures; it does not own.** `infra/` has no `aws_s3_bucket` resource (the
    bucket is a data source), the two DynamoDB tables carry `prevent_destroy`, and RDS and
    Redshift are absent entirely. `tests/unit/test_infra_terraform.py` enforces all three.

## Data findings that shape the code

- **D1** — every `src/synthesizer/*.py` is 0 bytes; only `__pycache__/*.pyc` held real logic. Those
  `.pyc` files are untracked now but kept on disk as the recovered source of business rules.
  Regenerating the synthesizer is out of scope.
- **D2** — 840 members (`M-1001`…`M-1840`) are referenced by transactions but defined nowhere.
  Verified exactly 840. Policy `AIMTERNET_ORPHAN_MEMBER_POLICY`, default `synthesize_stub`.
- **F1** — `gross_rental_amount` in the source is **post-discount** (`final_hourly_rate × duration`),
  not `base × duration` as spec §5 says. Confirmed from the bytecode and against the data.
  The bytecode + data are the rule of record; §5's line is a documentation error.
- **F2** — telemetry `expires_at` is `timestamp + 7 days`, and the data window is already in the past.
  Enabling DynamoDB TTL on it would purge ~61 of 62 days within ~48h. `AIMTERNET_DDB_TTL_ENABLED`
  defaults to `false`; the attribute is still written verbatim.
- **D4** — every CSV is CRLF-terminated. Read with `encoding='utf-8-sig'` and `newline=''`.
- **F7** — the first scheduled `rds_to_s3_incremental` after a bootstrap used to *replace*
  `silver/members_operational` with the rows it had moved. `members_operational` fell from
  1,200 rows to 4 and `dim_member` from 2,235 SCD2 versions to 8, and nothing failed:
  reconciliation rated the dimension check `INFO`. Fixed in `curate/export_rds.py` (merge on
  the primary key) and the check is now `CRITICAL`, alongside a per-snapshot count check.
- **F8** — F7 again, in the sibling module, plus the three reasons nobody saw it.
  `dynamodb_to_s3_incremental` (hourly) wrote its delta over
  `silver/workstation_events_operational`, so the snapshot held one hour of API-emitted events
  and nothing older. It survived because **nothing read the dataset** (Gold built
  `fact_workstation_event` from Bronze alone), **no check covered it** (`OPERATIONAL_SNAPSHOTS`
  listed only the five RDS tables), and **no test existed**. Fixed in
  `curate/export_dynamodb.py`; Gold now unions both origins; the snapshot is held to a
  never-shrinks high-water mark read from `reconciliation_results`.

  Found alongside it, same class, also fixed: Redshift merged `dim_member` on `member_key`, a
  `row_number()` Gold recomputes every build — a shrink left stale rows behind with the count
  unchanged (rekeyed to `(member_id, valid_from_utc)`, and to `member_id` alone in F9);
  `dim_member` was the one table missing from the Redshift expectations dict, so its count
  check was `INFO` and always passed;
  and `ReconciliationReport.passed` ignored `skipped_layers`, so any layer that threw removed
  all of its `CRITICAL` checks and the run went green. See `docs/runbook.md` for the one-time
  `dim_member` cleanup this requires.

- **F9** — the same class again, and the last of it. `fact_rental` and `fact_concession_sale`
  were built from Bronze-derived Silver, so the 169 rentals and 39 purchases the POS had
  taken reached RDS, reached `silver/*_operational`, and stopped. Two tables were worse off:
  `concession_order_items` and `member_points_ledger` were not in `export_rds.EXPORTS` **at
  all**, so they had no snapshot — and a table with no snapshot is invisible to the test that
  checks every snapshot has a reader. Fixed by exporting both and unioning all four
  operational snapshots into Gold (`curate/gold.py::operational_source`), which uses
  `NOT EXISTS` rather than a `source_file` tie-break: the RDS snapshots are a *superset* of
  Bronze, so all 28,287 bootstrap rentals sit in both origins and an ordering tie-break would
  pick between identical `source_file` values at random on every build.

  Found alongside it, same class, also fixed: every fact count in `reconcile.py` and in the
  integration tests was a frozen literal that was correct only because Gold could not see the
  POS — now `SOURCE_COUNTS[bronze] + contribution`, derived; `dim_member` merged on
  `(member_id, valid_from_utc)`, but Gold *recomputes* `valid_from_utc`, so a member's first
  POS rental redates their version and orphans the old row with `is_current` still true (now
  keyed on `member_id` alone); the export stamped its watermark from the worker's clock while
  filtering on the database's, which for the two append-only tables would skip a row
  permanently (now `SELECT now()` from RDS); and nine metrics endpoints capped `days` at 62,
  which silently truncates a window that no longer stops growing.

- **F10** — the same class one layer further along, and this time in the *serving* code
  rather than the pipeline. `/v1/metrics/revenue/trend` built its daily concession column by
  joining `fact_concession_sale` to `fact_concession_line_item` and then summing the **sale**
  total, so a purchase with three lines was counted three times. Concession revenue on the
  dashboard's headline chart read ₱4,778,800 against an actual ₱2,862,820 — inflated ×1.669,
  which is exactly the mean lines per purchase — and `gross_profit`, `total_revenue` and
  `avg_transaction_value` all inherited it.

  Nothing caught it for the reason everything in F7–F9 went uncaught: **the only test was
  internal to the row.** `test_revenue_trend_totals_equal_the_sum_of_its_parts` asserted
  `total == rental + concession`, which a wrong `concession` satisfies perfectly. The fix is
  the query (sale-grain measures from the sale fact alone, margin in its own CTE joined by
  day) and, more importantly, a test that compares two *independent* paths over the same
  facts: `payment_mix` reads `fact_concession_sale` directly, so
  `sum(daily) == sum(payment_mix)` must hold. It now does, to the peso, and the day the POS
  wrote also matches `/revenue/today`'s RDS figure exactly.

  The rule this leaves: **an aggregate over a join is a fan-out until proven otherwise.**
  Measures at different grains do not belong in one `GROUP BY` — put each at its own grain
  and join the results. And a cross-check is only a check if the two sides can disagree.

## The dashboard

Two Streamlit pages under `streamlit_app/pages/`, and the split is by what a reader is
looking for, not by which store answered:

* **PC Telemetry** — floor status, per-peripheral connectivity, compute, network, the
  utilization heatmap, and workstation events. Live from RDS + DynamoDB except the heatmap
  and the event summary.
* **Business Analytics** — money, membership activity, and a per-member summary. Today's
  figures and anything about a single member come from RDS, so they match the till exactly.

Three things about it are load-bearing:

1. **`streamlit_app/lib/charts.py` owns every chart's appearance.** Pages say what they plot;
   that module says how it looks. Categorical colours are assigned in a **fixed slot order
   and never cycled** — that ordering is what keeps the palette colour-blind-safe, so sorting
   or recycling it silently breaks accessibility. It also caps the slots: past four series the
   answer is a table or a facet, never a generated hue. `show()` passes `theme=None` because
   Streamlit's own plotly theme would repaint the traces and undo all of it.
2. **No dual-axis charts.** Two measures on different scales get two charts. Revenue and
   gross profit are plotted separately for exactly this reason.
3. **The peripheral cards are one instant** — the latest reading per machine, from DynamoDB.
   Gold drops the peripheral columns, so there is no historical per-peripheral series; the
   history lives in `fact_workstation_event` as `PERIPHERAL_ALERT`, which the events section
   reads. In this dataset only the headset ever disconnects (~3% of readings), so an
   all-connected snapshot is the normal case, not a broken card.

## Commands

```bash
make env         # conda env update + editable install
make link        # create the §2 logical paths (/opt/aimternet, /opt/airflow); needs sudo
make check       # lint + typecheck + test
make validate    # Stage C over all 62 batches, no AWS required
make bootstrap   # manifest -> Bronze -> validate -> RDS -> DynamoDB
make curate      # Silver + Gold Parquet
make redshift    # Redshift DDL + COPY/MERGE
make reconcile   # cross-layer reconciliation report
make docs        # regenerate docs/assumptions.md from poc_policy.py
make infra-plan  # terraform init + plan (never apply without asking, never destroy)
make api         # operational + metrics API on :8000
make streamlit   # Streamlit dashboard on :8501 (HTTP-only client of the metrics API)
make airflow     # airflow standalone on :8080
```

## Environment variables

See `.env.example` — every variable, with safe placeholders and a comment. Settings are typed in
`src/aimternet/config/settings.py` and fail fast at startup when a required one is missing. Paths fall
back to repo-relative locations when unset, so the test suite runs anywhere.
