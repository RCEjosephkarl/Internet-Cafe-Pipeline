# Runbook

How to run, inspect and repair the AIMternet-Cafe pipeline. Written for the person who
inherits it, not for the person who built it.

Read `CLAUDE.md` first — it holds the invariants. The two that will bite you fastest:
**`data/raw-landing/` is read-only**, and **RDS and Redshift are shared instances** — stay
inside `aimternet_oltp` and `aimternet_olap`.

---

## 1. Standing up a fresh checkout

```bash
cp .env.example .env          # fill in bucket, RDS, Redshift, Airflow credentials
make env                      # conda env update + editable install
make link                     # /opt/aimternet, /opt/airflow, ~/airflow/dags symlinks (sudo)
make check                    # ruff + mypy + 186 unit tests, none of which need AWS
```

`make check` passing on a machine with no AWS credentials is the smoke test. If it fails
there, nothing downstream is worth trying.

## 2. First load, in order

```bash
make migrate                  # apply the four migrations into schema aimternet_oltp
make validate                 # Stage C over all 62 batches, locally, no AWS
make bootstrap                # manifest -> S3 Bronze -> validate -> RDS + DynamoDB
make curate                   # Silver, the RDS export, then Gold
make redshift                 # DDL then load Gold into aimternet_olap
make reconcile                # the report that says whether any of that was true
```

Expect roughly: bootstrap 20 min on a warm bucket (the DynamoDB telemetry load is the long
pole at ~3 h cold), curate 6 min, Redshift 7 min, reconcile 1 min.

Every one of these is **idempotent**. A second run adds no rows anywhere; that is acceptance
item 14 and `make reconcile` checks it.

## 3. Day to day

```bash
make api                      # :8000 — operational API, metrics API, /dashboard
make airflow                  # :8080 — api-server + scheduler
jupyter lab                   # :8888 — the four notebooks
```

| Where | What |
|---|---|
| `http://<host>:8000/dashboard` | Live metrics, 15-second refresh |
| `http://<host>:8000/docs` | OpenAPI for the operational API |
| `http://<host>:8080` | Airflow UI |
| `notebooks/pos_terminal.ipynb` | The till. HTTP only |
| `notebooks/airflow_lens.ipynb` | Trigger, pause, inspect DAGs and Variables |
| `notebooks/db_lens.ipynb` | Read-only SQL over RDS and Redshift |

No port open? Tunnel instead of changing a security group:

```bash
ssh -i jupyter.pem -L 8000:localhost:8000 -L 8080:localhost:8080 ubuntu@<host>
```

## 4. The DAGs

| DAG | Schedule | What it does |
|---|---|---|
| `bootstrap_raw_landing` | manual | The only DAG that reads the EC2 landing directory |
| `rds_to_s3_incremental` | `0 * * * *` | RDS -> Silver snapshots, by `updated_at` watermark |
| `dynamodb_to_s3_incremental` | `15 * * * *` | Recent events -> Silver, via GSI2 per event type |
| `curate_silver_gold` | `30 * * * *` | Bronze -> Silver -> Gold |
| `load_redshift` | `45 * * * *` | Gold -> Redshift, delete-then-insert per table |
| `reconcile_data` | `0 */6 * * *` | Every check; a critical failure fails the run |

They are tuned through **Airflow Variables**, not code:

| Variable | Default | Effect |
|---|---|---|
| `aimternet_telemetry_days` | `62` | Days of telemetry to process |
| `aimternet_load_threads` | `8` | Upload/load concurrency |
| `aimternet_batch_size` | `1000` | Rows per insert batch |
| `aimternet_orphan_member_policy` | `synthesize_stub` | D2 handling; `quarantine` is the alternative |
| `aimternet_ddb_export_lookback_hours` | `24` | Event export window |
| `aimternet_rds_export_full` | `false` | `true` forces a full snapshot instead of a delta |

`notebooks/airflow_lens.ipynb` writes these. A missing Variable never breaks DAG *parsing* —
`dags/_common.py::variable` swallows the lookup and returns the default.

Test one without a scheduler:

```bash
airflow dags test reconcile_data
```

## 5. When something is wrong

### `make reconcile` fails

The report lands in `work/reconciliation/<run_id>/` as Markdown and JSON, and every check is
persisted to `reconciliation_results` in RDS. Read the Markdown: critical failures are listed
first with expected vs. actual, and the known findings (D2, F1, F2, F5, F6) are printed as
explained warnings so you can tell a real regression from a documented one.

### A dimension shrank

Symptom: `gold_rows:dim_member` or `silver_snapshot:*_operational` fails.

Cause: the RDS -> S3 export writes `<table>_operational` into Silver, and Gold reads that as
the *current* state of the operational store. An export that wrote only its delta would
truncate the snapshot. That happened once, on the first incremental run after the bootstrap:
`members_operational` dropped to the 4 rows the POS had touched and `dim_member` fell from
1,200 members to 8 versions. The export now merges its delta onto the previous snapshot by
primary key.

Repair:

```bash
$(PY) -m aimternet.pipeline.cli curate --layer export --full   # rebuild the snapshots
$(PY) -m aimternet.pipeline.cli curate --layer gold            # rebuild the dimensions
make redshift                                                  # push them to the warehouse
make reconcile
```

### The DynamoDB load looks stuck

It is almost certainly not stuck — it is draining a thread pool after an exception. Check
with `py-spy dump --pid <pid>`; a main thread in `Executor.__exit__ -> shutdown(wait=True)`
means a worker raised and the pool is finishing 1,400 queued files invisibly. The loader now
cancels queued futures on `BaseException`, and per-file checkpoint failures are counted
rather than raised, so this shape should not recur. Resume by re-running the same command:
checkpoints make it skip what is already written.

### Redshift `COPY` fails with "Cannot find default IAM role"

Expected. The cluster has no default IAM role and inline credentials are prohibited, so the
loader falls back to batched `INSERT` — it probes at run time and records which path it took.
To switch back to `COPY`: create the role (`infra/iam.tf` has it, behind `manage_iam`),
attach it to the cluster, and set `AIMTERNET_REDSHIFT_COPY_IAM_ROLE`.

### Airflow will not list DAGs

`DeserializationError` on an example DAG means stale serialized DAGs in the metadata
database from a previous Airflow version:

```bash
airflow dags delete <dag_id> --yes
```

### `pandas.to_parquet` raises

Correct. **pyarrow is deliberately not installed** — it SIGABRTs at interpreter shutdown on
this host in ~77% of runs, which would fail Airflow tasks at random. All Parquet I/O goes
through `aimternet.pipeline.curate.engine` (DuckDB). See `CLAUDE.md` for the measurements.

## 6. Infrastructure

```bash
make infra-plan               # terraform init + plan, read-only
```

`apply` needs the owner's approval — the plan turns on the bucket's public-access block,
which is bucket-wide. **Never `terraform destroy`.** The configuration cannot delete the
bucket (it is a data source, not a resource) or the DynamoDB tables (`prevent_destroy`), and
RDS and Redshift are not in it at all. `infra/README.md` explains why and carries the costs.

## 7. Things that are safe, and things that are not

| Safe | Not safe |
|---|---|
| Re-running any `make` target | `terraform destroy`, `aws s3 rb`, `aws rds delete-*` |
| `airflow dags test <id>` | `pytest -m rds` against the live schema — it runs migrations |
| `make curate` / `make redshift` mid-day | Enabling DynamoDB TTL on the shipped data (F2) |
| Editing Airflow Variables | Writing anything into `data/raw-landing/` |

The `pytest -m rds` entry is not hypothetical: a migration rollback test once dropped
`load_checkpoint` out from under a running DynamoDB load. That test now builds and drops its
own scratch schema, but the marker still points at the live database.
