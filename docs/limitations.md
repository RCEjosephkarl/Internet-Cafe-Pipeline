# AIMternet-Cafe: Comprehensive Audit of Limitations and Business Constraints

**Document Version:** 1.0.0  
**Project:** AIMternet Centralized Branch Management & Analytics Platform  
**Target Repository:** `RCEjosephkarl/AIMternet-Cafe`  
**Reference Documents:**
- `Project_Proposal_DEDS.pdf` (Initial Project Proposal)
- `Project_Proposal_DEDS_Game_Plan.pdf` (System Architecture, Ingestion Protocols & Blueprint)
- `pipeline_plan_AWS.md` & `pipeline_plan.md` (Engineering Specifications & Authoritative Guidelines)
- `config/business_rules.yaml` & `src/aimternet/config/poc_policy.py` (Recovered Invariants & Policy Decisions)

---

## Executive Summary

The **AIMternet Centralized Branch Management & Analytics Platform** was originally conceived in `Project_Proposal_DEDS.pdf` as an enterprise-wide, multi-branch information system coordinating operational transactions (OLTP), workstation device telemetry (NoSQL), an Amazon S3 Data Lakehouse, and an analytical data warehouse (OLAP) across multiple café branches.

During design formalization (`Project_Proposal_DEDS_Game_Plan.pdf`) and subsequent codebase implementation (`pipeline_plan_AWS.md`), the scope, business mechanics, and technical architecture were significantly bounded and adapted. Multiple strict business invariants were established to maintain referential integrity and simplify accounting logic. Furthermore, empirical inspection of the 62-day historical dataset (~4.4 GB in `data/raw-landing/`) and compiled generator bytecode revealed profound discrepancies, edge cases, and runtime realities (documented as defects **D1–D4** and findings **F1–F7**).

This document provides an exhaustive, rigorously validated catalog of **35 business constraints, operational limitations, architectural boundaries, and subtle data defects**, sequenced hierarchically from the **most obvious macro-level constraints (starting at #01)** down to the **least obvious edge cases, hidden data discrepancies, and compiler-level anomalies**.

---

## Master Registry: Limitations & Business Constraints

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                                RANKING STRUCTURE                                  │
├───────────────────┬───────────────────────────────────────────────────────────────┤
│ Tier 1: 01 – 10   │ High-Level Scope, Commercial Policies & Business Constraints  │
│ Tier 2: 11 – 16   │ Operational Workflows & Front-Desk Invariants                 │
│ Tier 3: 17 – 23   │ System Architecture, Data Flow & Cloud Boundaries             │
│ Tier 4: 24 – 35   │ Deep Data Quality Anomalies, Deficiencies & Special Cases     │
└───────────────────┴───────────────────────────────────────────────────────────────┘
```

---

### 01. Single Flagship Branch Scope (Reduction from Multi-Branch Enterprise Proposal)
- **Category:** Business Domain & Architectural Scope
- **References:** `Project_Proposal_DEDS.pdf` (§1); `Project_Proposal_DEDS_Game_Plan.pdf` (§1.2); `src/aimternet/db/migrations/0001_initial_schema.up.sql`
- **Description:** The foundational proposal explicitly pitches AIMternet as a *"fictional multi-branch internet café business"* operating *"across several locations"* requiring centralized coordination of inter-branch inventory, roaming member histories, and regional OLAP comparison. In the game plan and implementation, the entire system is strictly scoped to a **single flagship location (Makati Branch, Metro Manila)**.
- **Business & Technical Impact:**
  - The database schema completely omits `branch_id` across operational tables (`workstations`, `rental_transactions`, `concession_purchases`).
  - No capability exists for multi-tenancy, cross-branch session roaming, or inter-branch inventory logistics.
  - Multi-location analytical comparisons envisioned in the proposal are impossible without substantial schema alterations.

---

### 02. Workstation Fleet Fixed Cap (Exactly 175 Units in 3 Inflexible Zones)
- **Category:** Hardware Fleet & Physical Capacity Constraint
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§1.2, §3); `config/business_rules.yaml` (lines 17–28); `src/aimternet/db/migrations/0001_initial_schema.up.sql` (line 34)
- **Description:** The café's physical and logical hardware capacity is hardcoded to exactly **175 numbered workstations (`PC-001` through `PC-175`)**, statically partitioned into three physical hardware tiers:
  1. **Standard Zone (`PC-001` to `PC-100`):** 100 units (Core i5 / Ryzen 5, RTX 3060, 1080p 144Hz) @ **₱50.00/hr**.
  2. **VIP Esports Zone (`PC-101` to `PC-150`):** 50 units (Core i7 / Ryzen 7, RTX 4070, 1440p 240Hz) @ **₱80.00/hr**.
  3. **Streamer / Production Pods (`PC-151` to `PC-175`):** 25 units (Core i9 / Ryzen 9, RTX 4090, Dual 4K, Studio Mic) @ **₱120.00/hr**.
- **Business & Technical Impact:**
  - Rigid database constraint: `CHECK (workstation_id ~ '^PC-[0-9]{3}$')` and strict numeric range lookups in `business_rules.py`.
  - The system cannot dynamically reclassify PCs (e.g., converting Standard units to tournament mode or adjusting hourly rates based on hardware upgrades).
  - Physical floor expansion beyond 175 machines requires manual schema updates and code changes.

---

### 03. 100% Mandatory Membership Constraint (Zero Anonymous Walk-Ins or Guests)
- **Category:** Commercial Policy & Referential Constraint
- **References:** `Project_Proposal_DEDS.pdf` (§1); `Project_Proposal_DEDS_Game_Plan.pdf` (§2.1); `src/aimternet/db/migrations/0001_initial_schema.up.sql` (lines 88, 148)
- **Description:** While the initial proposal stated the café serves *"both walk-in customers and registered members"*, the implemented architecture strictly enforces a **100% Mandatory Membership Rule**. Anonymous walk-in sessions, guest logins, and temporary tokens are completely eliminated.
- **Business & Technical Impact:**
  - Foreign keys referencing `member_id` in `rental_transactions`, `concession_purchases`, and `member_points_ledger` are strictly defined as `NOT NULL REFERENCES members(member_id)`.
  - A customer cannot rent a PC or purchase a bottle of water without full account registration.
  - While Redshift includes an `is_walk_in` column in `fact_concession_sale`, it is an analytical artifact—the operational OLTP engine outright rejects unauthenticated transactions.

---

### 04. Discrete, Inflexible Session Durations (No Arbitrary Rental Times)
- **Category:** Operational Constraint & Billing Mechanics
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§7.2.4); `config/business_rules.yaml` (line 76); `src/aimternet/api/services/rentals.py`
- **Description:** Rental durations are restricted to a closed, discrete set: **[1.0, 2.0, 3.0, 5.0, 8.0] hours**.
- **Business & Technical Impact:**
  - Customers cannot book arbitrary or fractional durations (e.g., 30 minutes, 1.5 hours, 4 hours, or overnight 10-hour passes).
  - Front-desk staff using the POS terminal must select from predefined dropdown intervals.
  - Simplifies scheduling logic but limits commercial revenue capture for quick casual usage.

---

### 05. Static, Inelastic Concession Inventory Catalog (Frozen 10 SKUs, No Soft Deletes)
- **Category:** Retail Inventory & Supply Chain Constraint
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§2.3, §7.2.3); `config/business_rules.yaml` (lines 80); `src/aimternet/db/migrations/0001_initial_schema.up.sql` (lines 37–51)
- **Description:** The concession catalog is frozen at exactly **10 SKUs** across 4 categories:
  - **Beverages (4):** Iced Mountain Tea (`SKU-BEV-01`), Citrus Energy Drink (`SKU-BEV-02`), Mineral Water (`SKU-BEV-03`), Cold Brew Coffee (`SKU-BEV-04`).
  - **Hot Food (2):** Spicy Tonkotsu Ramen (`SKU-SNK-01`), Cheesy Nacho Platter (`SKU-SNK-02`).
  - **Snacks (2):** Truffle Potato Crisps (`SKU-SNK-03`), Salted Caramel Popcorn (`SKU-SNK-04`).
  - **Accessories (2):** Disposable Anti-Bacterial Ear Pads (`SKU-ACC-01`), Braided USB-C Cable (`SKU-ACC-02`).
- **Business & Technical Impact:**
  - Operational and analytical tables completely omit `is_active` or soft-delete flags.
  - The POS dropdown exposes the entire catalog unconditionally.
  - The system cannot model discontinued items, seasonal promotions, variable cost inflation, or combo meals (e.g., PC rental + beverage bundles).

---

### 06. Exclusive Tier-Based Discounting (Elimination of Promos, Vouchers, and Coupons)
- **Category:** Pricing Invariant & Revenue Policy
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§2.2, §3.1); `config/business_rules.yaml` (lines 30–49); `src/aimternet/config/business_rules.py` (lines 208–232)
- **Description:** Standard promotional campaigns, marketing coupon codes, student vouchers, and blanket happy-hour discounts are completely abolished. Discounts apply exclusively to workstation hourly rentals and are deterministically resolved by the customer's membership tier:
  - **Standard Tier:** 0% discount (base rate applies).
  - **Silver Tier:** 10% discount on workstation rentals.
  - **Gold Tier:** 20% discount on workstation rentals.
- **Business & Technical Impact:**
  - Concessions (F&B and accessories) are strictly excluded from tier discounting.
  - Marketing teams cannot run ad-hoc holiday campaigns or partner promotional codes without altering backend business rules.

---

### 07. Strict Non-Negative Inventory Invariant (Zero Backorders or Rain Checks)
- **Category:** Transactional Integrity & Inventory Policy
- **References:** `src/aimternet/db/migrations/0001_initial_schema.up.sql` (line 44); `src/aimternet/api/services/concessions.py` (lines 94–125, 172–185)
- **Description:** Concession inventory enforces `CHECK (stock_quantity >= 0)` at the database level. During purchases, the API acquires row-level locks via `SELECT ... FOR UPDATE` ordered by `item_sku` and verifies available stock.
- **Business & Technical Impact:**
  - If a member requests 2 orders of ramen and only 1 remains, the entire transaction is rejected with an `InsufficientStock` 400 error.
  - Backordering, rain checks, delayed fulfillment, and negative inventory overrides are strictly prohibited.

---

### 08. Rigid Currency & Single-Timezone Boundary (PHP / Asia/Manila)
- **Category:** Financial & Temporal Localization Constraint
- **References:** `config/business_rules.yaml` (lines 10–15); `src/aimternet/config/business_rules.py` (lines 91–96); `CLAUDE.md` (lines 8)
- **Description:** The system is hardcoded to **Philippine Pesos (`PHP`)** quantized to two decimal places (`0.01`). All source timestamps originate in **`Asia/Manila` (`UTC+08:00`)**.
- **Business & Technical Impact:**
  - No multi-currency support (no USD, EUR, or crypto conversion).
  - While internal timestamps are converted and stored as UTC in PostgreSQL and Parquet, the operational logic and diurnal shift segments (Morning, Afternoon, Evening, Graveyard) depend on Manila calendar days.

---

### 09. Closed Payment Method Taxonomy (4 Hardcoded Rails, No Split Tender)
- **Category:** POS Billing Constraint
- **References:** `src/aimternet/db/migrations/0001_initial_schema.up.sql` (lines 104, 152); `config/business_rules.yaml` (line 77)
- **Description:** Payment methods are restricted to a closed check constraint:
  $$\text{payment\_method} \in \{\text{'Cash'}, \text{'GCash'}, \text{'Maya'}, \text{'Credit Card'}\}$$
- **Business & Technical Impact:**
  - No split payments permitted (e.g., paying ₱50 in cash and ₱35 via GCash for an ₱85 bill).
  - No debit cards, GrabPay, ShopeePay, bank transfers, or prepaid store credits supported.

---

### 10. Loyalty Points Accrual Model (Net Spend Only, Floored Arithmetic)
- **Category:** Loyalty Program & Financial Logic
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§4.1); `config/business_rules.yaml` (lines 50–56); `src/aimternet/config/business_rules.py` (lines 183–188)
- **Description:** Points accrue at **1 point per ₱10.00 of net cash paid**, scaled by tier multiplier and strictly floored:
  $$\text{points\_accrued} = \left\lfloor \frac{\text{net\_amount\_paid}}{10.00} \times \text{tier\_points\_multiplier} \right\rfloor$$
  - Standard: Multiplier = $1.00\times$
  - Silver: Multiplier = $1.25\times$
  - Gold: Multiplier = $1.50\times$
- **Business & Technical Impact:**
  - Net amount paid excludes any redeemed point credits.
  - Fractional points are discarded (e.g., ₱49 net spend at $1.0\times$ earns 4 points, not 4.9).
  - Applies to both rental checkouts and concession purchases.

---

### 11. Loyalty Points Redemption Mechanics (Coarse 100-Point Units, Rentals Only)
- **Category:** Loyalty Mechanics & Liability Control
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§4.2); `config/business_rules.yaml` (lines 53–55); `src/aimternet/config/business_rules.py` (lines 170–182, 202–206)
- **Description:** Redemption is governed by three strict invariants:
  1. **Coarse Blocks:** Points must be redeemed in integer multiples of **100 points** (100 points = **₱50.00 credit**). A customer with 95 points cannot redeem anything; a customer with 180 points can only redeem 100 points.
  2. **Rental Exclusive:** Points can **only** offset workstation rental fees. They cannot be redeemed for cash, merchandise, food, or beverages.
  3. **Non-Negative Bill Cap:** Points credit cannot exceed the rental's gross amount:
     $$\text{redeemable\_units} = \min\left(\left\lfloor \frac{\text{points\_balance}}{100} \right\rfloor, \left\lfloor \frac{\text{gross\_rental\_amount}}{50.00} \right\rfloor\right)$$
- **Business & Technical Impact:**
  - Protects the business from cash outflows or concession margins erosion.
  - Creates a "breakage" barrier for casual customers with point balances under 100.

---

### 12. Single Concurrency per Member & Workstation (Anti-Double Booking Invariant)
- **Category:** Transactional Concurrency & Resource Allocation
- **References:** `src/aimternet/db/migrations/0001_initial_schema.up.sql` (lines 139–142); `src/aimternet/api/services/rentals.py` (lines 88–117, 376–389)
- **Description:** The PostgreSQL operational database enforces two partial unique indexes:
  ```sql
  CREATE UNIQUE INDEX one_open_rental_per_workstation
      ON rental_transactions (workstation_id) WHERE session_end_utc IS NULL;
  CREATE UNIQUE INDEX one_open_rental_per_member
      ON rental_transactions (member_id) WHERE session_end_utc IS NULL;
  ```
- **Business & Technical Impact:**
  - A workstation cannot host two overlapping sessions.
  - A registered member cannot rent two workstations concurrently (e.g., paying for a friend, or holding a Standard PC while using a Streamer Pod).
  - Any concurrent check-in attempt returns a clean HTTP 409 Conflict.

---

### 13. Asymmetric Check-Out Billed Hours Calculation ("The House Always Wins")
- **Category:** Billing Invariant & Operational Policy
- **References:** `src/aimternet/api/services/rentals.py` (lines 315–326)
- **Description:** When closing a session, the billed duration is calculated as:
  $$\text{billed\_hours} = \max(\text{elapsed\_hours}, \text{booked\_duration})$$
  rounded up to 2 decimal places (minimum 0.01 hours).
- **Business & Technical Impact:**
  - If a member books a Streamer Pod for 5 hours and leaves after 45 minutes, they are billed for the full 5 hours.
  - If a member books 1 hour and overstays to 2.5 hours, they are billed for 2.5 hours.
  - The business never refunds unused booked time, but penalizes overstays.

---

### 14. Unidirectional Lifetime Spend Tier Promotions (No Demotions or Expirations)
- **Category:** Membership Lifecycle & Retention Policy
- **References:** `config/business_rules.yaml` (lines 30–49); `src/aimternet/config/business_rules.py` (lines 234–250)
- **Description:** Tier progression is strictly unidirectional and determined by cumulative `lifetime_spend_amount`:
  - **Standard $\rightarrow$ Silver:** Threshold = **₱3,000.00**; Bonus = **+50 points**.
  - **Silver $\rightarrow$ Gold:** Threshold = **₱10,000.00**; Bonus = **+100 points**.
- **Business & Technical Impact:**
  - Tiers never expire or reset annually.
  - There is no tier demotion mechanism for inactive or churned members.
  - Upgrading across multiple tiers in a single transaction grants accumulated bonuses.

---

### 15. Client Workstation Hardware & OS Homogeneity
- **Category:** Device Management & Fleet Homogeneity
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§6.2, §8.1); `config/business_rules.yaml` (line 78); `src/aimternet/schemas/source.py`
- **Description:** Every workstation across all zones is assumed to run an identical client OS: **`Win11-Pro-AIM-Build104`**.
- **Business & Technical Impact:**
  - No heterogeneous OS support (no Linux workstations for competitive CS2/Dota, no macOS for creative production).
  - Workstation telemetry and event payloads expect fixed hardware metrics (CPU load, GPU temp, ping, RAM usage) formatted exclusively for Windows client agents.

---

### 16. Closed 62-Day Simulation Horizon (2026-07-01 to 2026-08-31)
- **Category:** Temporal Modeling Boundary
- **References:** `config/business_rules.yaml` (lines 13–15); `src/aimternet/pipeline/reconcile.py` (lines 313–318)
- **Description:** The historical data corpus and calibration window span exactly 62 calendar days from **July 1, 2026 through August 31, 2026**.
- **Business & Technical Impact:**
  - The bootstrap reconciliation checks strictly verify that historical rentals fall inside this 62-day Manila time window.
  - Live POS transactions created today have `run_id = 'api'` and are partitioned separately to prevent triggering historical integrity alerts.

---

### 17. Strict Separation of Concerns & Notebook Client Isolation
- **Category:** Architectural Security & Layer Boundaries
- **References:** `pipeline_plan_AWS.md` (§2, §3, §7.3); `CLAUDE.md` (invariant #3); `tests/unit/test_notebook_boundary.py`
- **Description:** The front-desk POS notebook (`notebooks/pos_terminal.ipynb`) is strictly a presentation-layer HTTP client communicating with the FastAPI service.
- **Business & Technical Impact:**
  - Direct database imports (`psycopg2`, `sqlalchemy`, `boto3`), SQL execution, or database connection strings inside the POS notebook are prohibited and enforced by automated AST parsing tests.
  - The dashboard is similarly prohibited from querying RDS, DynamoDB, or Redshift directly—it must consume the Metrics API.

---

### 18. Dual-Write Asynchronous Decoupling & Eventual Consistency Risk
- **Category:** Distributed Systems & State Synchronization
- **References:** `src/aimternet/api/services/rentals.py` (lines 157–166, 391–432); `pipeline_plan_AWS.md` (§3)
- **Description:** Check-in and check-out perform a dual-write:
  1. Transactional state (rental row, member points, workstation status) is committed to PostgreSQL inside an ACID transaction.
  2. Telemetry event logs (`SESSION_START`, `SESSION_END`) are pushed to DynamoDB asynchronously **outside** the database transaction.
- **Business & Technical Impact:**
  - Designed intentionally: DynamoDB outages or throttling will not roll back a financial transaction or strand a customer at the till.
  - **Limitation:** If DynamoDB is unavailable or the network drops during `_emit_event()`, the event is dropped (logged as a warning). This introduces state drift between operational billing and workstation event history.

---

### 19. Multi-Tenant Shared Cloud Environment (Confined Schemas & Terraform Limits)
- **Category:** Infrastructure Isolation & Governance
- **References:** `CLAUDE.md` (invariants #5, #7, #10); `infra/README.md`; `docs/assumptions.md` (`SHARED_DATABASE_NAMESPACES`)
- **Description:** The PostgreSQL RDS instance and Amazon Redshift cluster are shared with unrelated academic coursework (`simple_oltp`, `bus_ticketing`, `krusty_krab_olap`).
- **Business & Technical Impact:**
  - Terraform in `infra/` is prohibited from managing, creating, or destroying RDS and Redshift instances.
  - All database objects must be isolated inside schemas `aimternet_oltp` (RDS) and `aimternet_olap` (Redshift).
  - Accidental global schema modifications or migrations without explicit search paths could corrupt foreign coursework.

---

### 20. Single-Node EC2 Deployment Limitation (No Cloud-Native Distributed ETL)
- **Category:** Scalability & Compute Infrastructure
- **References:** `pipeline_plan_AWS.md` (§0, §2); `CLAUDE.md` (lines 19–24)
- **Description:** The entire data platform runs on a **single EC2 instance** within a single Conda environment (`data_eng`, Python 3.12).
- **Business & Technical Impact:**
  - No managed orchestration (MWAA), serverless extractors (AWS Glue), or distributed engines (Spark/EMR).
  - Airflow runs locally using `LocalExecutor` + SQLite.
  - Memory spikes during large data curation or telemetry aggregation directly threaten API responsiveness.

---

### 21. Redshift High-Speed Ingestion Deficit (Batched INSERT Fallback instead of S3 COPY)
- **Category:** Data Warehouse Ingestion Bottleneck
- **References:** `src/aimternet/pipeline/loaders/redshift.py` (lines 1–17, 106–150); `docs/assumptions.md` (`REDSHIFT_LOADS_VIA_INSERT`)
- **Description:** Target specifications required loading Redshift from Gold Parquet using the parallel `COPY FROM S3` command. However, the shared Redshift cluster lacks an attached default IAM role (code 30001), and inline AWS credentials are strictly prohibited by security rules.
- **Business & Technical Impact:**
  - Ingestion falls back to executing batched SQL `INSERT` statements via DuckDB and Python.
  - While idempotent (using delete-then-insert staging tables), this is significantly slower than native S3 COPY and will not scale to high-volume multi-year analytical loads.

---

### 22. Warehouse Telemetry Exclusion (Aggregated Hourly Facts Only in OLAP)
- **Category:** Analytical Granularity & Data Modeling
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§6.3); `src/aimternet/pipeline/curate/gold.py` (lines 6–10); `docs/pipeline_schema_diagrams.md`
- **Description:** Raw 5-minute / 30-second telemetry readings (6.3 million records) are completely excluded from the Redshift analytical warehouse.
- **Business & Technical Impact:**
  - Gold curation pre-aggregates telemetry into `agg_workstation_utilization_hourly` (260,400 rows across 175 PCs $\times$ 24 hours $\times$ 62 days).
  - Redshift cannot query real-time hardware telemetry, instantaneous thermal throttling events, or fine-grained per-minute packet loss spikes. Deep diagnostic investigation requires querying raw Parquet in S3 or DynamoDB.

---

### 23. Metrics API Dual-Source Architecture (RDS vs Redshift Latency Split)
- **Category:** Operational BI & Latency Boundary
- **References:** `src/aimternet/api/routers/metrics.py`; `docs/assumptions.md` (`METRICS_LIVE_SOURCE`)
- **Description:** Specification §7.2 dictated that the Metrics API read from Redshift + DynamoDB. However, Redshift only updates on scheduled Airflow DAG intervals (e.g., hourly/daily).
- **Business & Technical Impact:**
  - A transaction executed at the POS would not appear on the dashboard until the next batch run.
  - **Resolution:** The Metrics API was re-architected to query **RDS PostgreSQL** for today's live revenue/active rentals, **DynamoDB** for real-time workstation status/telemetry, and **Redshift** only for historical aggregates.

---

### 24. Defect D2: 840 Orphan Members (70% Ghost Cohort Missing from Source Registrations)
- **Category:** Source Data Anomaly & Referential Integrity Defect
- **References:** `docs/assumptions.md` (`D2_ORPHAN_MEMBERS`); `pipeline_plan_AWS.md` (§1.3); `src/aimternet/config/poc_policy.py` (lines 56–74)
- **Description:** `members.csv` across all 62 daily batches defines only **360 members** (all in range `M-1841` to `M-2200`). However, historical rental transactions and points ledgers reference **1,182 distinct members** (`M-1001` through `M-1840`).
- **Business & Technical Impact:**
  - **840 members (70% of the entire customer base) have no source registration record.**
  - A naive foreign-key load into PostgreSQL fails immediately.
  - **Resolution:** The system implements a mandatory `synthesize_stub` policy in `poc_policy.py`, generating dummy member rows flagged as `source_system='DERIVED_FROM_TRANSACTIONS'` and `is_backfilled=true`, inferring their tier from their earliest rental and opening points from their earliest ledger entry.

---

### 25. Finding F1: Gross Rental Calculation Contradiction (Pre- vs. Post-Discount)
- **Category:** Pricing Formula Inconsistency & Accounting Specification Defect
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§5, §7.2.4); `docs/assumptions.md` (`F1_GROSS_RENTAL_AMOUNT`); `config/business_rules.yaml` (lines 57–65)
- **Description:** Specification §5 formally states:
  $$\text{gross\_rental\_amount} = \text{base\_hourly\_rate} \times \text{duration\_hours}$$
  In reality, the synthesizer bytecode (`rentals.cpython-312.pyc`, line 215) and **100% of the 28,287 source rental rows** compute gross from the *discounted* rate:
  $$\text{gross\_rental\_amount} = \text{final\_hourly\_rate} \times \text{duration\_hours}$$
- **Business & Technical Impact:**
  - The spec formula contradicts ~37% of historical rental rows (every Silver and Gold rental).
  - In this business model, "gross" is actually a post-discount gross. The engine implements the observed bytecode formula to maintain 100% historical parity and flags the specification as erroneous.

---

### 26. Finding F6: Telemetry Cadence Shift & 2x Volume Surge (300s to 30s Tick)
- **Category:** Data Volume Discrepancy & Cloud Billing Impact
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§5.1, §7.1); `docs/assumptions.md` (`F6_TELEMETRY_CADENCE_VARIES`); `docs/dynamodb.md` (lines 63–67)
- **Description:** The system design projected ~3,124,800 telemetry records based on a uniform 5-minute sampling tick (175 PCs $\times$ 12 pings/hr = 2,100 records per hourly file).
- **Business & Technical Impact:**
  - From `2026-07-01` to `2026-08-24`, the files follow the 300s tick (1,320 files $\times$ 2,100 = 2,772,000 records).
  - On `2026-08-25`, the tick silently increased to every **30 seconds** (168 files $\times$ 21,000 = 3,528,000 records).
  - **Actual total:** **6,300,000 records** (~4.4 GB).
  - This doubled DynamoDB write request units (WRU), raising one-off load costs from ~$3.90 to **~$8.09** and doubling ongoing table storage costs.

---

### 27. Finding F5: Points Ledger Resulting Balance Non-Replayability
- **Category:** Ledger Inconsistency & Mathematical Non-Determinism
- **References:** `docs/assumptions.md` (`F5_LEDGER_BALANCE_NOT_REPLAYABLE`); `src/aimternet/pipeline/validation/engine.py` (lines 585–636)
- **Description:** In `member_points_ledger.csv`, summing `points_delta` in `created_at` order fails to reproduce `resulting_balance` for **1,178 out of 1,183 members** (only 5 members match).
- **Business & Technical Impact:**
  - The synthetic data generator ran interleaved rental and concession passes independently, keeping separate balances that drifted out of chronological order.
  - However, all 54,424 transaction deltas match their source rentals and concessions exactly.
  - **Resolution:** The system treats `points_delta` as the authoritative source of truth. The stored `resulting_balance` column is ignored, and downstream facts calculate running balances dynamically.

---

### 28. Finding F2: Telemetry DynamoDB TTL Expiration Paradox (Immediate Purge Risk)
- **Category:** NoSQL Lifecycle Management & Data Loss Risk
- **References:** `Project_Proposal_DEDS_Game_Plan.pdf` (§6.2); `docs/assumptions.md` (`F2_TELEMETRY_TTL_DISABLED`); `docs/dynamodb.md` (lines 77–85)
- **Description:** Telemetry records define `expires_at` as epoch seconds representing `timestamp + 7 days`.
- **Business & Technical Impact:**
  - Because the simulation period (July–August 2026) is already in the past relative to system startup, enabling native DynamoDB TTL would cause AWS to **automatically delete 61 of the 62 days (~98% of all telemetry)** within 48 hours of loading.
  - **Resolution:** `AIMTERNET_DDB_TTL_ENABLED` is set to `false` by default. An optional shift parameter (`AIMTERNET_DDB_TTL_SHIFT_DAYS`) is provided for live TTL demonstrations.

---

### 29. Defect D1: Missing Synthesizer Source Code (Bytecode-Only Black Box)
- **Category:** Codebase Maintainability & Knowledge Preservation Defect
- **References:** `pipeline_plan_AWS.md` (§1.3); `CLAUDE.md` (lines 71–73)
- **Description:** Every Python file in `src/synthesizer/` (`catalog.py`, `rentals.py`, `telemetry.py`, etc.) was committed as an empty, 0-byte stub. The data generation logic existed solely as compiled `.cpython-312.pyc` bytecode inside `__pycache__/`.
- **Business & Technical Impact:**
  - The data generation engine cannot be rerun or modified.
  - All business rules, formulas, and constants had to be extracted and verified through Python bytecode disassembly and statistical replay against the raw data.

---

### 30. Host Kernel PyArrow SIGABRT Incompatibility (DuckDB Architectural Pivot)
- **Category:** Operating System & Runtime Dependency Crash
- **References:** `CLAUDE.md` (lines 30–40); `docs/assumptions.md` (`DUCKDB_REPLACES_PYARROW`)
- **Description:** On this Linux host kernel (`7.0.0-1010-aws`), `pyarrow` causes a fatal process crash (`terminate called without an active exception` — SIGABRT, exit 134) at Python interpreter shutdown during Parquet I/O in **23 out of 30 test runs**.
- **Business & Technical Impact:**
  - Randomly terminates Airflow tasks, CLI pipelines, and Pytest suites.
  - `fastparquet` was evaluated but rejected because it silently downcasts `Decimal` to `float64`, violating the strict financial rule against float arithmetic.
  - **Resolution:** `pyarrow` is completely uninstalled. The pipeline uses **DuckDB** for all Parquet reading, writing, and flattening, preserving `decimal128(12,2)` precision with zero process crashes.

---

### 31. Finding F7: SCD Type 2 Dimension Truncation Vulnerability (Snapshot Merging)
- **Category:** Data Pipeline Regression & Historical Dimension Loss
- **References:** `CLAUDE.md` (lines 83–88); `src/aimternet/pipeline/curate/export_rds.py`; `tests/unit/test_export_rds_merge.py`
- **Description:** In the initial pipeline design, the incremental `rds_to_s3_incremental` DAG exported only the rows modified since the last watermark into `silver/members_operational`.
- **Business & Technical Impact:**
  - Gold curation reads `members_operational` as the *current* state of the operational store. When an incremental run moved only 4 POS-touched rows, `members_operational` dropped from 1,200 rows to 4, causing Redshift's `dim_member` SCD2 table to collapse from 2,235 historical versions to 8.
  - **Resolution:** `export_rds.py` was re-engineered to merge incremental deltas onto the previous Silver snapshot by primary key, preserving historical dimension continuity.

---

### 32. Finding F4: Alert Events Metadata Population Deviation
- **Category:** Schema Contract vs. Data Payload Discrepancy
- **References:** `pipeline_plan_AWS.md` (§4); `docs/assumptions.md` (`F4_ALERT_EVENTS_ARE_FULLY_POPULATED`)
- **Description:** Specification §4 established that alert events (`HARDWARE_ALERT`, `PERIPHERAL_ALERT`) omit session and member contexts (`session_id` and `member_id` set to null).
- **Business & Technical Impact:**
  - In the actual generated data, all **1,644 alert events** across the 62 daily batches carry both a `session_id` and a `member_id`.
  - **Resolution:** `WorkstationEvent` models keep these fields optional, successfully accepting populated alert data without failing on nulls.

---

### 33. Defect D4: CRLF Line Endings & UTF-8 BOM Parsing Vulnerability
- **Category:** File Format & Ingestion Hygiene
- **References:** `pipeline_plan_AWS.md` (§1.3); `src/aimternet/io/readers.py` (lines 20–45)
- **Description:** All source CSV files in `data/raw-landing/` were generated with Windows-style CRLF (`\r\n`) terminators and occasional UTF-8 Byte Order Marks (BOM).
- **Business & Technical Impact:**
  - Naive Python CSV readers leave trailing `\r` characters on the last column and corrupt header names (e.g., `\ufeffmember_id`).
  - Loaders must strictly specify `encoding='utf-8-sig'` and `newline=''`.

---

### 34. Airflow Standalone Execution & Serialization Conflicts
- **Category:** Orchestration Engine Limitations
- **References:** `README.md` (lines 121); `docs/runbook.md` (lines 140–148)
- **Description:** Airflow 3.3.0 is configured as `standalone` using `LocalExecutor` backed by a local SQLite database.
- **Business & Technical Impact:**
  - Stale serialized DAG definitions from prior schema versions cause `DeserializationError` crashes.
  - The Airflow UI warns that Scheduler and Triggerer components are unhealthy unless started via `airflow standalone`.
  - SQLite metadata backend prevents high-concurrency parallel task execution.

---

### 35. Read-Only EC2 Landing Immutability & Bronze Ingestion Overhead
- **Category:** Storage Architecture & Idempotency Invariant
- **References:** `pipeline_plan_AWS.md` (§1.1, §6.1); `CLAUDE.md` (invariant #1); `tests/unit/test_landing_immutable.py`
- **Description:** The ~4.4 GB raw landing directory on EC2 is treated as an immutable, read-only staging bootstrap.
- **Business & Technical Impact:**
  - Airflow DAGs and pipelines are strictly prohibited from writing or altering the source directory.
  - Bootstrap requires uploading 1,488 JSON telemetry files to S3 Bronze via multipart upload with SHA-256 manifest verification, consuming significant network bandwidth and compute cycles before any processing can occur.

---

## Comparative Matrix: Proposal Vision vs. Implemented Reality

| Feature / Domain | Initial Proposal (`Project_Proposal_DEDS.pdf`) | Game Plan & Implementation (`AIMternet-Cafe`) | Primary Constraint / Limitation Reference |
|---|---|---|---|
| **Branch Topology** | Multi-branch network across several city locations | Single flagship location (Makati Branch, Metro Manila) | **Constraint 01** |
| **Workstation Capacity** | Flexible branch-level fleet | Hard cap of 175 units (`PC-001` to `PC-175`) in 3 fixed zones | **Constraint 02** |
| **Customer Registration** | Walk-in casual customers + registered members | 100% Mandatory membership (`member_id NOT NULL`) | **Constraint 03** |
| **Rental Durations** | Continuous / open-ended rental times | Discrete durations: [1.0, 2.0, 3.0, 5.0, 8.0] hours only | **Constraint 04** |
| **Promotions & Vouchers** | Blanket discounts, promo codes, vouchers | Tiers only (Standard 0%, Silver 10%, Gold 20%); no codes | **Constraint 06** |
| **Concession Inventory** | Dynamic multi-category retail inventory | Frozen catalog of 10 SKUs, zero negative stock | **Constraints 05, 07** |
| **Payment Rails** | Generic POS and point-of-sale integration | Closed enum (`Cash`, `GCash`, `Maya`, `Credit Card`) | **Constraint 09** |
| **Loyalty Points** | Earned on activity, redeemable across services | Net spend only (floored), 100-pt rental bundles only | **Constraints 10, 11** |
| **Customer Overbooking** | Unspecified | Strict partial unique indexes prevent double booking | **Constraint 12** |
| **Telemetry Volume** | Regular workstation diagnostic logs | 6.3M items (300s $\rightarrow$ 30s cadence shift; 2x volume) | **Constraint 26** |
| **Warehouse Ingestion** | Scheduled S3 Data Lakehouse to Redshift COPY | Batched SQL INSERT fallback (No Redshift IAM role) | **Constraint 21** |
| **Warehouse Telemetry** | Central Lakehouse OLAP on PC usage | Hourly aggregated facts only (raw telemetry excluded) | **Constraint 22** |
| **Member Cohort Integrity** | Assumed complete registration history | 840 missing member profiles (Defect D2 stub backfill) | **Constraint 24** |
| **Rental Gross Pricing** | Gross = Base Hourly Rate $\times$ Duration | Gross = Final (Discounted) Rate $\times$ Duration | **Constraint 25** |
| **Parquet Engine** | Standard PyArrow / Fastparquet | DuckDB replacement due to Linux host kernel crash | **Constraint 30** |

### 36. Telemetry Has No POS Write Path, So Any Metric Dividing Money by Occupancy Under-Reports
- **Category:** Serving-Layer Boundary & Derived-Metric Validity
- **References:** `src/aimternet/api/routers/metrics.py` (`/v1/metrics/efficiency/revenue-per-hour`); `src/aimternet/pipeline/curate/gold.py` (`agg_workstation_utilization_hourly`); `src/aimternet/api/services/rentals.py` (`_emit_event`)
- **Description:** The POS writes rentals, purchases and points to RDS, and workstation *events* to DynamoDB — but it emits **no telemetry readings**. `agg_workstation_utilization_hourly` is built from Bronze telemetry alone, so it stops dead at the end of the bootstrap batch (2026-08-31) while `fact_rental` keeps growing with every POS check-out.
- **Business & Technical Impact:**
  - Any metric that puts a POS-inclusive numerator over a telemetry-derived denominator is wrong by construction. `/v1/metrics/efficiency/revenue-per-hour` is the one that exists: because `_redshift_window` anchors on `fact_rental`'s own max date, a short window sits entirely after 2026-08-31, finds zero occupied readings, and reports **₱0.00 per occupied hour** for every zone rather than an error.
  - The endpoint is still served and still tested, but **no dashboard page reads it** — the card was removed when Data Science was folded into Business Analytics. It is recorded here rather than silently left as a trap for the next reader.
  - The same boundary caps the utilization heatmap and the per-workstation health score: both are honest about the batch in their captions, because neither mixes in a POS-fed measure.
- **Resolution Path:** either have the POS emit synthetic telemetry on check-in/check-out, or derive occupied hours from `fact_rental.duration_hours` instead of telemetry ticks. The second is cheaper and would make the metric POS-complete; it changes what "occupied" means (booked, not observed), which is why it was not done silently.


---

## Actionable Recommendations for Enterprise Production

If transitioning this Proof of Concept (POC) into a multi-branch, enterprise-grade production environment, the following structural enhancements are recommended:

1. **Multi-Tenancy & Branch Hierarchy:**
   - Introduce a `branches` dimension table and add `branch_id` as a foreign key across all operational and analytical tables.
2. **Dynamic Product Catalog & Soft Deletions:**
   - Add `is_active`, `effective_date`, and `discontinued_date` attributes to `concession_items` to enable menu turnover, happy-hour specials, and seasonal offerings.
3. **Redshift Ingestion Optimization:**
   - Provision an IAM role with read permissions to the S3 bucket, attach it to the Redshift cluster, and re-enable `COPY FROM S3` to eliminate batched `INSERT` latency.
4. **Automated Synthesizer Recovery:**
   - Decompile and re-author the Python data synthesizer (`src/synthesizer/`) into executable, version-controlled source code to enable ongoing testing and stress simulation.
5. **Real-Time Stream Ingestion (Kafka / Kinesis):**
   - Replace batch S3 telemetry drops with an event streaming bus (Amazon Kinesis or Apache Kafka) feeding DynamoDB and an Apache Iceberg / Delta Lake lakehouse for real-time fleet anomaly detection.

---
*Report compiled autonomously following deep codebase inspection, schema verification, and empirical data validation.*
