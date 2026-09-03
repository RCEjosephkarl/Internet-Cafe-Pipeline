# AIMternet-Cafe

Data engineering POC for an internet cafe: membership, PC rentals, concessions, workstation
telemetry. Raw files on EC2 flow through Airflow into S3 Bronze, are validated and normalized,
load into RDS PostgreSQL (operational) and DynamoDB (events + telemetry), are curated into S3
Silver/Gold Parquet, and land in Redshift as a dimensional model. A FastAPI operational API is
the only write path for the Jupyter POS terminal; a metrics API feeds an HTML/JS dashboard.

```
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

## Data

62 days, 2026-07-01 → 2026-08-31, ~4.2 GB in `data/raw-landing/` (read-only):

| Dataset | Rows |
|---|---:|
| `catalog/workstations.csv` | 175 |
| `catalog/concession_items.csv` | 10 |
| `dimensions/dim_date.csv` / `dim_time.csv` | 365 / 1,440 |
| `members.csv` (all batches) | 360 |
| `rental_transactions.csv` | 28,287 |
| `concession_purchases.csv` | 21,077 |
| `concession_order_items.csv` | 29,672 |
| `member_points_ledger.csv` | 55,514 |
| `workstation_events.json` | 58,218 |
| `telemetry/**/*.json` | ~3,124,800 |

## Quick start

```bash
cp .env.example .env      # then fill in the real values
make env                  # conda env + editable install
make link                 # create /opt/aimternet and /opt/airflow (needs sudo)
make check                # lint + typecheck + test
make validate             # validate all 62 batches locally — no AWS needed
```

See `CLAUDE.md` for the stack, the invariants, and the data findings that shape the code.
Full build specification: `pipeline_plan_AWS.md`.

## Notebooks

| Notebook | What it is |
|---|---|
| `notebooks/pos_terminal.ipynb` | Front-desk POS. HTTP to the API only — no DB access, no credentials. |
| `notebooks/airflow_lens.ipynb` | Observe and control DAGs, runs, tasks, Variables and pools via the Airflow REST API. |
| `notebooks/db_lens.ipynb` | Read-only explorer over RDS (OLTP) and Redshift (OLAP). |
| `notebooks/dashboard_lens.ipynb` | Exercises the metrics API and links to the live dashboard. |

## Cost

New spend is small and one-time: the full DynamoDB telemetry load is ~$3.90 in write units plus
~$0.55/mo storage, and S3 is ~$0.11/mo for 4.2 GB of Bronze. The pre-existing RDS and Redshift
instances are the real monthly cost — Redshift especially, if it runs 24/7. This build does not
create, manage, or destroy either of them.
