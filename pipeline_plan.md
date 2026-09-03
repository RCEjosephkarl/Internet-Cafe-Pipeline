# Internet Cafe Branch Management — Data Engineering POC

You are implementing a Proof of Concept (POC) for an internet cafe branch management system data engineering pipeline.

Work directly in the current repository. First inspect the existing repository structure, files, configuration, sample data, and documentation. Reuse existing code and conventions where practical instead of replacing them unnecessarily.

Do not ask me to manually implement intermediate pieces. Make the repository changes yourself.

## 1. POC objective

Build an executable end-to-end POC implementing this architecture:

```text
EC2 local raw landing
        |
        v
     Airflow
        |
        v
   S3 Bronze
        |
        +--------------------+
        |                    |
        v                    v
   validation/            DynamoDB
   normalization
        |
        +-----------> RDS PostgreSQL
        |
        +-----------> S3 Silver/Gold
                              |
                              v
                          Redshift

Jupyter POS
    |
    v
Operational API
    |
    +----> RDS
    |
    +----> DynamoDB

Redshift + DynamoDB
        |
        v
    Metrics API
        |
        v
    Dashboard
```

The POC must demonstrate:

1. One-time bootstrap from EC2-local files.
2. Immutable raw landing in S3 Bronze.
3. Validation, normalization, deduplication and reconciliation.
4. Correct foreign-key-aware loading into RDS PostgreSQL.
5. Loading workstation events and telemetry into DynamoDB.
6. Silver/Gold curated datasets in S3.
7. COPY/MERGE-style analytical loading into Redshift.
8. A live operational API.
9. A Jupyter POS notebook that talks ONLY to the API.
10. Airflow orchestration.
11. Reconciliation and data-quality reporting.
12. Repeatable execution with configuration separated from code.

## 2. Important architectural constraints

Preserve these responsibilities:

* EC2 local folder = initial bootstrap staging only.
* S3 = permanent raw and curated data lake.
* RDS PostgreSQL = transactional/current operational business state.
* DynamoDB = high-volume/current workstation state and telemetry.
* Redshift = analytical warehouse.
* Airflow = orchestration.
* Jupyter POS notebook = client only; it must never connect directly to RDS or DynamoDB.
* API = the only operational write interface used by POS.

The POS notebook must NOT contain database credentials or database connection logic.

The API must enforce:

* workstation availability validation
* member validation
* rental pricing
* tier discounts
* point redemption
* point accrual
* concession pricing
* inventory validation/update
* transactional validation
* prevention of invalid duplicate/overlapping transactions
* appropriate database transactions where atomicity is required

## 3. Airflow paths

Use these exact logical locations:

```text
/opt/airflow/dags/
/opt/aimternet/data/raw-landing/
/opt/airflow/input/raw-landing/
```

The initial bootstrap DAG must use:

```text
/opt/airflow/input/raw-landing/
```

as a read-only input location.

The initial bootstrap source is the EC2 folder:

```text
/opt/aimternet/data/raw-landing/
```

Do not make the DAG dependent on writing back into the source landing directory.

After bootstrap, S3 becomes the source of record for subsequent pipelines.

## 4. Bootstrap processing order

Implement a manual DAG named:

```text
bootstrap_raw_landing
```

The bootstrap workflow must execute approximately as follows:

### Stage A — inspect and register source files

Read:

```text
catalog/workstations.csv
catalog/concession_items.csv
legacy_batches/*/members.csv
rental_transactions.csv
concession_purchases.csv
concession_order_items.csv
member_points_ledger.csv
workstation_events.json
telemetry/YYYY-MM-DD/HH.json
```

For each source file:

* verify existence
* capture file size
* capture modified time
* calculate checksum
* register a load manifest record
* retain the original file unchanged

Create an explicit load manifest containing at least:

```text
source_file
source_type
checksum
file_size
load_timestamp
status
record_count
error_count
```

### Stage B — S3 Bronze

Copy the source files unchanged into an S3 Bronze layout.

Use a deterministic structure similar to:

```text
s3://<bucket>/bronze/<dataset>/<filename>
```

Do not mutate the original Bronze objects.

Prefer checksum/idempotency checks so rerunning the bootstrap does not create duplicate logical loads.

### Stage C — data validation

Validate:

* required columns
* data types
* date/time formats
* nullability
* primary-key uniqueness
* foreign-key references
* duplicate records
* numeric ranges
* business-rule violations

Generate machine-readable validation results.

Do not silently discard bad records.

Separate rejected records into an explicit quarantine/reject location.

### Stage D — normalization/deduplication

Create normalized intermediate datasets.

Implement deterministic deduplication rules.

Preserve lineage back to the source file and record where practical.

## 5. RDS load order

Respect this dependency order:

```text
workstations
concession_items
        |
        v
members
        |
        v
rental_transactions
        |
        v
concession_purchases
        |
        v
concession_order_items
        |
        v
member_points_ledger
```

Create PostgreSQL DDL/migrations for the required tables and constraints.

At minimum implement:

* primary keys
* foreign keys
* uniqueness constraints where appropriate
* timestamps
* indexes supporting POS access patterns
* transaction-safe updates

The historical load must be idempotent or safely rerunnable.

Do not rely on load order alone to hide invalid references.

Provide explicit reconciliation checks after the load.

## 6. DynamoDB

Create DynamoDB table definitions/configuration for:

```text
workstation_events
workstation_telemetry
```

Choose sensible partition/sort keys based on likely access patterns.

The POC must support:

* workstation lookup
* recent workstation status
* event history
* telemetry retrieval
* time-oriented queries

Document the key design and access patterns.

Do not attempt to model DynamoDB as a relational replacement for RDS.

## 7. S3 Silver and Gold

Create curated datasets such as:

```text
silver/
gold/
```

Silver should contain cleaned/standardized data.

Gold should contain analytical-ready datasets suitable for Redshift loading.

At minimum include logical datasets for:

* members
* workstations
* rentals
* concession purchases
* concession order items
* points
* workstation events
* telemetry

Add load timestamps and useful lineage columns.

Prefer Parquet for curated analytical outputs unless the existing repository clearly uses another format.

## 8. Redshift

Create the analytical warehouse schema.

Use a sensible dimensional model for the POC.

At minimum provide dimensions/facts for:

* member
* workstation
* date/time
* concession item
* rental
* concession sales
* points activity
* workstation activity/telemetry where appropriate

Implement an initial load using COPY from S3.

Implement MERGE/upsert logic where required.

The process must be rerunnable without generating duplicate analytical facts.

Do not hard-code AWS credentials.

Make Redshift connection information environment/config driven.

If automatic Redshift pause/resume is not practical for the selected Redshift deployment mode, implement the POC so the pause step is configurable/manual rather than pretending an unsupported operation works.

## 9. Reconciliation

After bootstrap, execute reconciliation checks between layers.

At minimum compare:

* source record counts
* Bronze record counts
* validated/accepted/rejected counts
* RDS record counts
* DynamoDB ingestion counts where measurable
* Silver/Gold record counts
* Redshift fact counts

Also check:

* duplicate primary keys
* orphan foreign keys
* invalid monetary values
* invalid timestamps
* points balance consistency
* rental state consistency
* inventory consistency

Produce a concise reconciliation report.

The bootstrap DAG should fail when critical reconciliation checks fail.

## 10. Operational API

Implement an API, preferably using the framework already used by the repository. If none exists, use FastAPI.

Required endpoints:

```http
GET  /v1/workstations/available
GET  /v1/members/{member_id}
POST /v1/rentals/check-in
POST /v1/concessions/purchases
POST /v1/rentals/check-out
```

The API is the operational boundary.

The API must:

### Check-in

* validate member
* validate workstation
* verify workstation is available
* calculate rental pricing according to configured pricing rules
* apply member tier discount where applicable
* create the rental transaction atomically
* update workstation state
* record appropriate workstation event/state information

### Concession purchase

* validate member where required
* validate item existence
* validate price
* validate inventory
* calculate totals
* apply business rules
* create purchase and line items transactionally
* decrement inventory atomically
* accrue/redeem points according to business rules

### Check-out

* locate active rental
* calculate duration
* calculate final price
* apply pricing and member rules
* apply point redemption where supported
* close the rental
* update workstation availability
* record resulting workstation event
* return the completed transaction

Do not expose arbitrary SQL/database write endpoints.

## 11. POS notebook

Create/update a Jupyter notebook acting as the front-desk POS terminal.

The notebook must communicate exclusively through HTTP to the API.

Demonstrate the following flow:

```text
GET available workstations
GET member
POST check-in
POST concession purchase
POST check-out
```

The notebook must NOT:

* import the RDS driver directly
* import DynamoDB client libraries for operational writes
* contain SQL against the operational database
* contain database credentials

Make the notebook understandable to a front-desk operator.

## 12. Metrics API and dashboard

Implement a simple metrics API.

At minimum provide metrics such as:

* active rentals
* available workstations
* occupied workstations
* sales today
* rental revenue today
* concession revenue today
* points issued/redeemed
* workstation status
* recent workstation telemetry

The dashboard may be minimal for the POC.

Prefer a simple HTML/JS dashboard or existing repository dashboard technology.

The dashboard must obtain data via the Metrics API rather than directly connecting to RDS/DynamoDB.

## 13. Ongoing Airflow ingestion

In addition to the manual bootstrap DAG, create a pattern for ongoing ingestion.

After bootstrap:

```text
RDS -> scheduled Airflow extract -> S3 Bronze
DynamoDB -> scheduled Airflow export -> S3 Bronze
S3 Bronze -> validation/transform -> Silver/Gold
Silver/Gold -> Redshift
```

Create separate DAGs where appropriate, for example:

```text
bootstrap_raw_landing
rds_to_s3_incremental
dynamodb_to_s3_incremental
curate_silver_gold
load_redshift
reconcile_data
```

Do not make the ongoing workflows depend on the EC2 raw landing folder.

Use S3 prefixes/checkpoints/manifests or another simple deterministic incremental strategy.

## 14. Configuration

Do not hard-code:

* AWS account IDs
* AWS access keys
* database passwords
* Redshift credentials
* S3 bucket names
* DynamoDB table names
* hostnames

Use environment variables and/or a typed configuration module.

Provide:

```text
.env.example
```

with safe placeholders.

Never commit real secrets.

## 15. Local POC/testing

The POC must be testable without requiring production AWS resources.

Where practical:

* unit-test transformation/validation logic
* test API business rules
* test SQL/migration definitions
* mock or abstract AWS services
* provide representative fixtures
* test DAG logic independently from actual execution

If the repository already contains tests, extend them rather than replacing them.

## 16. Infrastructure

Inspect the existing repository before adding infrastructure.

If infrastructure-as-code already exists, extend it.

If it does not exist, create a minimal Terraform configuration or equivalent that can define the POC resources, but keep it clearly separated from application code.

Do NOT automatically destroy existing AWS infrastructure.

Do NOT execute destructive AWS commands.

Do NOT create expensive production-sized resources.

The POC should default to the smallest practical resource sizes and clearly identify resources that may incur AWS charges.

## 17. Developer experience

Create a clear README covering:

1. architecture
2. prerequisites
3. environment variables
4. directory structure
5. database setup
6. Airflow setup
7. bootstrap execution
8. API startup
9. POS notebook startup
10. metrics/dashboard startup
11. validation/reconciliation
12. ongoing DAG execution
13. AWS deployment notes
14. cleanup instructions

Also create useful scripts/Make targets where appropriate, for example:

```text
make test
make lint
make bootstrap
make api
make airflow
make dashboard
```

Adapt these to the repository's existing tooling rather than forcing them where they conflict with current conventions.

## 18. Engineering quality

Prefer:

* typed Python
* clear module boundaries
* structured logging
* retry handling for AWS/network operations
* idempotent pipeline steps
* explicit transaction boundaries
* UTC timestamps
* consistent error handling
* schema validation
* configuration-driven behavior
* meaningful tests

Avoid:

* giant monolithic scripts
* hidden state
* hard-coded credentials
* direct database access from notebooks
* silent data loss
* duplicated business logic across API and notebook

## 19. Execution strategy

Follow this implementation order:

### Phase 1

Inspect repository and existing data.

### Phase 2

Implement schemas/models and configuration.

### Phase 3

Implement validation, manifest and S3 Bronze bootstrap.

### Phase 4

Implement RDS and DynamoDB bootstrap loaders.

### Phase 5

Implement Silver/Gold transformations.

### Phase 6

Implement Redshift schema and loading.

### Phase 7

Implement reconciliation.

### Phase 8

Implement operational API.

### Phase 9

Implement POS notebook.

### Phase 10

Implement metrics API/dashboard.

### Phase 11

Implement ongoing Airflow DAGs.

### Phase 12

Run tests and perform an end-to-end POC validation.

After each phase, keep the repository in a runnable state.

## 20. Final acceptance criteria

The implementation is successful when a developer can:

1. Place the supplied raw files into the configured raw landing path.
2. Run the bootstrap DAG.
3. See immutable copies in S3 Bronze.
4. See validation and manifest results.
5. See valid operational data in RDS.
6. See workstation events/telemetry in DynamoDB.
7. See Silver/Gold datasets in S3.
8. Load/query analytical data in Redshift.
9. Receive a reconciliation report with no unexplained discrepancies.
10. Start the API.
11. Open the POS notebook.
12. Perform:

    * workstation availability lookup
    * member lookup
    * rental check-in
    * concession purchase
    * rental check-out
13. Verify that business rules are enforced by the API.
14. Verify that the POS notebook never directly accesses databases.
15. View operational metrics through the Metrics API/dashboard.
16. Run the pipeline a second time without creating duplicate logical records.

## 21. Important Codex behavior

Before changing code:

* inspect the repository thoroughly
* identify the existing stack
* identify where the sample data actually lives
* identify existing schemas/models/APIs/DAGs/tests
* reuse compatible components

When there is ambiguity, choose the simplest implementation consistent with the architecture and document the decision.

Do not fabricate source columns or business rules that are not present in the repository. Where a business rule is missing, isolate it in configuration or a clearly marked POC policy module and document the assumption.

At the end:

* run the available tests
* run lint/type checks if configured
* fix failures
* provide a concise implementation summary
* list created/modified files
* list any assumptions
* list any AWS resources that must be created manually
* list commands required to run the POC
* explicitly identify anything that could not be executed because AWS credentials/resources are unavailable

Do not claim the POC is fully working unless the relevant execution was actually verified.
