# Assumptions and policy decisions

**Generated from `src/aimternet/config/poc_policy.py`. Do not edit by hand —**
**run `make docs`.** `make assumptions` prints the same register to a terminal and
`--json` emits it for a machine.

Every entry here is a place where the source data was silent or contradicted the
spec, and a decision had to be made and recorded rather than buried in a transform
(spec §0.5, §11).

## `D2_ORPHAN_MEMBERS` — spec §1.3 D2, §6.2

**Decision.** 840 members referenced by transactions but never defined are backfilled as stub rows marked source_system='DERIVED_FROM_TRANSACTIONS', is_backfilled=true. Tier comes from the member's earliest rental; opening balance from their earliest ledger row (resulting_balance - points_delta).

**Why.** A naive FK-ordered load would fail outright. The alternative policy, quarantine, would discard a large share of the transactional history.

**Evidence.** members.csv across all 62 batches defines 360 members, all M-1841+. Rentals and the ledger reference M-1001..M-1840, which appear in no source file. Verified count: exactly 840.

## `F1_GROSS_RENTAL_AMOUNT` — spec §5

**Decision.** gross_rental_amount = final_hourly_rate x duration_hours (post-discount), not base_hourly_rate x duration_hours as §5 states.

**Why.** The spec contradicts both the synthesizer bytecode and the data. §5 itself says a disagreement is a data-quality finding, so the finding is reported and the observed rule is implemented.

**Evidence.** rentals.cpython-312.pyc line 215 computes gross from final_hourly_rate. Replaying all 28,287 historical rentals through business_rules.price_rental() reproduces every field of every row exactly; the §5 formula disagrees with ~37% of them (every Silver and Gold rental).

## `F2_TELEMETRY_TTL_DISABLED` — spec §4, §6.3, §10.6

**Decision.** expires_at is written verbatim as the DynamoDB TTL attribute, but TTL enforcement is off by default (AIMTERNET_DDB_TTL_ENABLED=false). AIMTERNET_DDB_TTL_SHIFT_DAYS can rebase it into the future for a live demo.

**Why.** Honours §4's instruction to wire the source attribute rather than invent one, without the POC deleting its own dataset shortly after paying to load it.

**Evidence.** expires_at = timestamp + 7 days and the simulated window is 2026-07-01..08-31, already past. 2026-07-01T00:00+08:00 expires 2026-07-07. 61 of 62 days would be purged within ~48h of loading.

## `F5_LEDGER_BALANCE_NOT_REPLAYABLE` — spec §4, §6.6

**Decision.** points_delta is treated as authoritative and balances are derived by summing it. member_points_ledger.resulting_balance is carried through as a source column but never used to compute anything.

**Why.** One of the two columns has to be trusted and they disagree. The deltas reconcile perfectly against the transactions that caused them; the balances do not reconstruct under any ordering.

**Evidence.** All 54,424 ledger entries that reference a transaction match it exactly (28,157 rental accruals, 21,077 concession accruals, 5,190 redemptions; zero mismatches). Replaying deltas in created_at order reproduces resulting_balance for only 5 of 1,183 members -- the generator maintained balances across interleaved rental and concession passes, so the recorded sequence cannot be replayed in timestamp order.

## `F6_TELEMETRY_CADENCE_VARIES` — spec §1.2, §6.3

**Decision.** Telemetry volume is taken from the data (6,300,000 records), not from spec §1.2's projection of ~3,124,800. All cost, runtime and capacity estimates use the real figure.

**Why.** The spec projects the total from a uniform 5-minute tick. The tick is not uniform, so the projection is low by roughly half -- which would have understated the DynamoDB load by about 3.2M writes.

**Evidence.** 1,320 files hold 2,100 records (300s tick, 2026-07-01..08-24); 168 files hold 21,000 (30s tick, 2026-08-25..08-31). 1,320x2,100 + 168x21,000 = 6,300,000.

## `F4_ALERT_EVENTS_ARE_FULLY_POPULATED` — spec §4

**Decision.** WorkstationEvent keeps session_id and member_id optional even though every alert event in this dataset populates them.

**Why.** The stated contract permits null, tolerating it costs nothing, and a loader that crashes on a null the contract allows is worse than one that accepts a value the contract did not promise.

**Evidence.** Spec §4 says alert events have session_id/member_id unset. All 1,644 alert events across the 62 batches (1,101 HARDWARE_ALERT, 543 PERIPHERAL_ALERT) carry both.

## `DUCKDB_REPLACES_PYARROW` — spec §6.4

**Decision.** Parquet is written and read with DuckDB. pyarrow is not installed.

**Why.** pyarrow aborts at interpreter shutdown on this host, which would fail Airflow tasks and test runs at random. fastparquet silently downcasts Decimal to float64, which breaks the no-floats-in-money rule.

**Evidence.** SIGABRT 'terminate called without an active exception' in 23/30 runs with the pip wheel (16.1.0 and 25.0.1) and 11/20 with conda-forge pyarrow 15 in a clean probe env. DuckDB: 0/20, writes true decimal128(12,2).

## `SHARED_DATABASE_NAMESPACES` — spec §8

**Decision.** All objects live in schema aimternet_oltp (RDS) and aimternet_olap (Redshift). Terraform manages only S3 configuration and the two DynamoDB tables.

**Why.** Both instances are shared with unrelated coursework. Namespacing keeps this build from touching it, and keeping them out of Terraform means no plan can destroy them.

**Evidence.** RDS already holds schemas simple_oltp and public (bus tables); Redshift holds bus_ticketing, krusty_krab_olap and catalog_history.

## `REDSHIFT_LOADS_VIA_INSERT` — spec §6.5

**Decision.** Redshift is loaded with batched INSERT from the Gold Parquet rather than COPY FROM S3. AIMTERNET_REDSHIFT_COPY_IAM_ROLE switches COPY back on if a role is attached later; the loader probes for it at run time rather than assuming.

**Why.** COPY needs an IAM role on the cluster and there is none. Inline access keys are prohibited by §6.5, so INSERT is the only remaining path that does not weaken the credential rules.

**Evidence.** A COPY probe against a deliberately absent key returns 'Cannot find default IAM role on this cluster' (code 30001). The IAM user cannot call redshift:DescribeClusters to inspect or attach one.

## `METRICS_LIVE_SOURCE` — spec §7.2

**Decision.** The metrics API reads today's and live numbers from RDS, historical aggregates from Redshift, and workstation status/telemetry from DynamoDB. Configurable via AIMTERNET_METRICS_LIVE_SOURCE.

**Why.** §7.2 says Redshift + DynamoDB, but Redshift only sees a POS transaction after the next DAG run. A check-in the dashboard cannot show is not a useful dashboard.

## `PYTHON_VERSION` — spec §2

**Decision.** Python 3.12, not the 3.11 §2 suggests.

**Why.** Airflow 3.3.0 was already installed and passing `airflow db check` on 3.12 in this environment. Rebuilding carried risk with no benefit.
