# CLAUDE.md — AIMternet-Cafe POC

Working notes for any future session. Read this before touching anything.

## What this is

A data engineering POC for an internet cafe: 4.2 GB of synthesized 2026-07-01…2026-08-31 operational
data flows from an EC2 landing directory through Airflow into S3 Bronze, is validated and normalized,
loads into RDS PostgreSQL (operational) and DynamoDB (events + telemetry), is curated into S3
Silver/Gold Parquet, and lands in Redshift as a dimensional model. A FastAPI operational API is the only
write path for the Jupyter POS terminal; a metrics API feeds an HTML/JS dashboard.

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
9. **The RDS→S3 export leaves a snapshot, never a delta.** Gold reads
   `silver/<table>_operational/` as the *current* state of the operational store, so an
   incremental run merges its delta onto the previous snapshot by primary key before writing.
   `tests/unit/test_export_rds_merge.py` pins this; reconciliation checks it directly
   (`silver_snapshot:*`).
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
make api         # operational + metrics API on :8000, dashboard at /dashboard
make airflow     # airflow standalone on :8080
```

## Environment variables

See `.env.example` — every variable, with safe placeholders and a comment. Settings are typed in
`src/aimternet/config/settings.py` and fail fast at startup when a required one is missing. Paths fall
back to repo-relative locations when unset, so the test suite runs anywhere.
