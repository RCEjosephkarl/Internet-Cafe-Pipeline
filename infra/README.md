# `infra/` — Terraform for the AIMternet-Cafe POC

Run `terraform plan` from this directory. **Never `terraform destroy`.** `apply` needs the
repository owner's approval.

```bash
cp terraform.tfvars.example terraform.tfvars   # fill in s3_bucket
terraform init
terraform plan
```

## What this manages, and what it refuses to

| Resource | Managed here | Why |
|---|---|---|
| S3 bucket **configuration** — versioning, public access block, default encryption, lifecycle | yes | This is the spec §8 posture, and it is enforceable without owning the data |
| S3 bucket **itself** | **no** | There is no `resource "aws_s3_bucket"` anywhere in this configuration. The bucket holds 4.5 GB of Bronze/Silver/Gold; if Terraform owned it, one `destroy` would take the lake with it. Managing the sub-resources gives Terraform every control it needs and no way to lose the data |
| DynamoDB tables (2) | yes, with `prevent_destroy` | They are wholly ours, and their key design has to stay in step with the loader |
| RDS PostgreSQL | **no** | Pre-existing and **shared** with `simple_oltp` and `bus_ticketing`. This POC lives inside schema `aimternet_oltp` and has no business managing the instance |
| Redshift | **no** | Same: shared with `krusty_krab_olap`; we occupy `aimternet_olap` |
| IAM roles | rendered always, created only when `manage_iam = true` | The account's EC2 instance profile already exists; a second one would be noise |

That table is the whole design argument. The spec asks for RDS and Redshift in Terraform;
this deviates deliberately, and CLAUDE.md invariant 7 records it. A configuration that
*could* delete a shared database is a configuration that eventually does.

## Adopting what already exists

`make bootstrap` creates the DynamoDB tables through boto3 (`ensure_tables`), so the pipeline
runs whether or not Terraform is installed. The `import` blocks in `dynamodb.tf` let
Terraform adopt those tables rather than plan a create that would immediately conflict. Set
`adopt_existing_dynamodb_tables = false` only in a genuinely empty account.

## The one behavioural change in this plan

The live bucket has **all four public-access-block settings off**. Spec §8 requires public
access blocked, so `aws_s3_bucket_public_access_block.lake` turns all four on. Everything
else in the plan either records current state (versioning, encryption) or adds lifecycle
rules scoped to this project's prefixes. Applying it therefore changes bucket-wide access
posture — which is why it needs an explicit approval rather than a quiet `apply`.

## Cost

Measured on 2026-09-03, us-east-1 on-demand pricing.

### Managed by this configuration

| Resource | Size | Monthly |
|---|---|---|
| S3 Standard | 4.51 GB (bronze 4.41 GB, silver 88.3 MB, gold 10.2 MB, quarantine 314 KB, manifests 1.05 MB) | **$0.10** |
| DynamoDB storage | 2.93 GB (telemetry 2.90 GB, events 29.2 MB) | **$0.73** |
| IAM | 2 roles, 1 instance profile | $0.00 |
| | | **$0.83/month** |

Plus one one-off charge already incurred: **~$8.09** of DynamoDB write units for the initial
load — 6,300,000 telemetry items at 1 WRU and 58,311 event items at 3 WRU (two GSIs), at
$1.25 per million. That is double the spec's projection because the spec's record count was
wrong; the sampling interval drops from 300 s to 30 s on 2026-08-25 (finding **F6**).

The Bronze-telemetry lifecycle rule moves 4.2 GB to Standard-IA after 30 days, taking the S3
line from $0.10 to about $0.06.

### NOT managed here — and the real bill

| Resource | Monthly | Note |
|---|---|---|
| RDS `db.t3.micro`, 20 GB gp3, single-AZ | ~$14.70 | $0.017/hr compute + $0.115/GB storage. Shared instance |
| Redshift `dc2.large`, 1 node | **~$180** | $0.25/hr, always on. **The main cost item by an order of magnitude** |
| Redshift Serverless alternative | ~$3.00/hr active, 8 RPU minimum | Cheaper only if the cluster is genuinely idle most of the day |
| EC2 host | varies | Pre-existing; runs Airflow, the APIs and JupyterLab |

If this POC is ever paused, pausing or deleting the Redshift cluster is the only action that
materially changes the bill — and it is the repository owner's action to take, not
Terraform's.
