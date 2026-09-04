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
make api                      # :8000 — operational API, metrics API
make streamlit                # :8501 — dashboard (HTTP-only client of the metrics API)
make airflow                  # :8080 — api-server + scheduler
jupyter lab                   # :8888 — the four notebooks
```

| Where | What |
|---|---|
| `http://<host>:8501` | Streamlit dashboard — PC Telemetry, Descriptive Analytics, Data Science |
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
| `rds_to_s3_incremental` | `*/15 * * * *` | RDS -> Silver snapshots, by `updated_at` watermark. Publishes `SILVER_RDS` |
| `dynamodb_to_s3_incremental` | `*/15 * * * *` | API-emitted events -> Silver **snapshot**, via GSI2 per event type. Publishes `SILVER_DYNAMODB` |
| `curate_silver_gold` | on `SILVER_RDS` **and** `SILVER_DYNAMODB` | Bronze -> Silver -> Gold. Publishes `GOLD` |
| `load_redshift` | on `GOLD` | Gold -> Redshift, delete-then-insert per table |
| `reconcile_data` | `0 */6 * * *` | Every check; a critical failure fails the run |

The four pipeline DAGs are **chained on Airflow Assets** (`dags/_common.py`), not staggered on
the clock. They used to run at `:00`, `:15`, `:30` and `:45`, which put a POS sale up to 1h45m
away from the dashboard and — worse — let `load_redshift` fire on a Gold build that
`curate_silver_gold` had not finished writing, because nothing connected them but a guess about
how long each stage takes. Now each stage triggers the next off the data it produced. A list
schedule is AND, so `curate` waits for *both* exports; that is why the two exports publish
different assets rather than sharing one.

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

The same rule binds the DynamoDB export. It wrote its delta over
`workstation_events_operational` on every hourly run, so the snapshot held one hour of
API-emitted events and nothing older. Nothing failed, because Gold read the Bronze-derived
events and never looked at the snapshot at all. Gold now reads both, and
`silver_snapshot:workstation_events_operational` is CRITICAL.

Repair:

```bash
$(PY) -m aimternet.pipeline.cli curate --layer export --full   # rebuild the RDS snapshots
$(PY) -m aimternet.pipeline.cli curate --layer export-ddb      # rebuild the events snapshot
$(PY) -m aimternet.pipeline.cli curate --layer gold            # rebuild the dimensions
make redshift                                                  # push them to the warehouse
make reconcile
```

### `silver_snapshot:*` fails while the POS is in use

It should not any more. `expected` is RDS **as of that table's export watermark**, not a live
count, so rows the POS created since the last `rds_to_s3_incremental` are not counted as loss.
The comparison is "never fewer", because the export stamps its watermark before it runs its
SELECT and a row updated inside that window is legitimately in the snapshot.

If it does fail, the snapshot genuinely holds fewer rows than RDS held when it was written —
that is F7, not lag. Rebuild it:

```bash
$(PY) -m aimternet.pipeline.cli curate --layer export --full
```

Before the fix this compared Silver against a live RDS read, so an open cafe failed it
permanently and it could not distinguish a truncated snapshot from an ordinary sale.

### `silver_snapshot:workstation_events_operational` fails after a deliberate rebuild

That check has no RDS table to compare against, so it is held to a high-water mark: the
snapshot may grow or hold steady, never shrink. The mark is read from `reconciliation_results`,
which means a rebuild that legitimately makes it smaller — a cleared bucket, a re-bootstrapped
DynamoDB table — trips it once and keeps tripping.

Confirm the shrink is intended, then clear the history for that check alone:

```sql
DELETE FROM aimternet_oltp.reconciliation_results
 WHERE check_name = 'silver_snapshot:workstation_events_operational';
```

The next run re-establishes the mark at the new size. Clear nothing else: the point of the
check is that shrinking requires a person to say so.

### `redshift_rows:dim_member` fails, or a member has several `is_current` rows

`dim_member` used to be merged on `member_key`, which Gold recomputes as a `row_number()` on
every build. Delete-then-insert on a key that renumbers itself deletes the keys the new build
happens to occupy and leaves everything above that watermark behind. After a build that
produced fewer SCD2 versions than the last one, Redshift kept the new rows *and* the stale
ones — duplicate `member_id`s, several `is_current = true` rows per member, and a row count
that had not fallen, so no count check could see it.

The merge is now keyed on `(member_id, valid_from_utc)`. That fixes every future load but
does not retract rows an earlier `member_key`-keyed merge already orphaned. Once, after
deploying this change:

```sql
DELETE FROM aimternet_olap.dim_member;   -- one-time; the merge is correct from here
```

```bash
$(PY) -m aimternet.pipeline.cli load-redshift --datasets dim_member
make reconcile        # redshift_rows:dim_member is CRITICAL now, and compares against Gold
```

Check first — if it is already clean, skip it:

```sql
SELECT member_id, count(*) FILTER (WHERE is_current) AS current_versions
  FROM aimternet_olap.dim_member GROUP BY member_id HAVING count(*) FILTER (WHERE is_current) > 1;
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

To switch to `COPY`, in this order:

1. **Create the role.** Set `manage_redshift_copy_role = true` in `infra/terraform.tfvars` and
   `make infra-plan`, then apply. This flag exists separately from `manage_iam` because
   `manage_iam` also creates the EC2 pipeline role, which would duplicate the instance profile
   this shared account already has. Take the ARN from the `redshift_copy_role_arn` output.
2. **Attach it to the cluster by hand**, in the console or with `aws redshift
   modify-cluster-iam-roles`. Terraform deliberately cannot do this: invariant 10 keeps
   Redshift out of its reach, `aws_redshift_cluster_iam_roles` is on the forbidden-resource
   list in `tests/unit/test_infra_terraform.py`, and the IAM user cannot call
   `redshift:DescribeClusters` in any case.
3. **Set `AIMTERNET_REDSHIFT_COPY_IAM_ROLE`** to the ARN. The loader probes on the next run and
   switches strategy on its own — no code change.

Watch the first `COPY` run closely. That path had never executed on this cluster, so it has no
production history: Redshift matches Parquet columns by *position*, and the loader now names
its columns explicitly on both paths for that reason.

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
