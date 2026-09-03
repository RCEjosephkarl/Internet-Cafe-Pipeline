-- Operational (OLTP) schema for AIMternet-Cafe.
--
-- Namespaced deliberately: this RDS instance is shared with unrelated coursework
-- (schemas simple_oltp and public). Nothing here touches anything outside ${SCHEMA}.
--
-- Conventions:
--   * money is NUMERIC(12,2) -- never float, never double precision
--   * every timestamp is TIMESTAMPTZ stored in UTC; source_tz_offset preserves the
--     original '+08:00' as lineage (spec §4)
--   * every loaded table carries source_file / source_checksum / ingested_at_utc / run_id
--     so any row can be traced back to the byte range it came from (spec §6.1 Stage D)

CREATE SCHEMA IF NOT EXISTS ${SCHEMA};
SET search_path TO ${SCHEMA};

-- ---------------------------------------------------------------- catalog

CREATE TABLE workstations (
    workstation_id      TEXT PRIMARY KEY,
    zone_classification TEXT NOT NULL,
    base_hourly_rate    NUMERIC(12,2) NOT NULL CHECK (base_hourly_rate >= 0),
    ip_address          TEXT NOT NULL,
    mac_address         TEXT NOT NULL,
    commissioned_date   DATE NOT NULL,
    -- Operational state owned by the API, not by the source files.
    status              TEXT NOT NULL DEFAULT 'AVAILABLE'
                        CHECK (status IN ('AVAILABLE', 'OCCUPIED', 'MAINTENANCE')),
    source_file         TEXT,
    source_checksum     TEXT,
    ingested_at_utc     TIMESTAMPTZ,
    run_id              TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT workstations_id_format CHECK (workstation_id ~ '^PC-[0-9]{3}$')
);

CREATE TABLE concession_items (
    item_sku          TEXT PRIMARY KEY,
    item_name         TEXT NOT NULL,
    category          TEXT NOT NULL
                      CHECK (category IN ('Beverage', 'Hot Food', 'Snacks', 'Accessories')),
    unit_cost_price   NUMERIC(12,2) NOT NULL CHECK (unit_cost_price   >= 0),
    unit_retail_price NUMERIC(12,2) NOT NULL CHECK (unit_retail_price >= 0),
    stock_quantity    INTEGER NOT NULL CHECK (stock_quantity >= 0),
    source_file       TEXT,
    source_checksum   TEXT,
    ingested_at_utc   TIMESTAMPTZ,
    run_id            TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- members

CREATE TABLE members (
    member_id             TEXT PRIMARY KEY,
    first_name            TEXT NOT NULL,
    last_name             TEXT NOT NULL,
    email                 TEXT NOT NULL,
    phone_number          TEXT,
    current_tier          TEXT NOT NULL CHECK (current_tier IN ('Standard', 'Silver', 'Gold')),
    current_points_balance INTEGER NOT NULL DEFAULT 0 CHECK (current_points_balance >= 0),
    lifetime_spend_amount NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (lifetime_spend_amount >= 0),
    registered_at_utc     TIMESTAMPTZ NOT NULL,
    source_tz_offset      TEXT,
    -- D2: 840 members (M-1001..M-1840) are referenced by transactions but defined in no
    -- source file. Under the synthesize_stub policy they are inserted here, flagged, and
    -- counted in the reconciliation report -- never silently conjured.
    source_system         TEXT NOT NULL DEFAULT 'LEGACY_BATCH',
    is_backfilled         BOOLEAN NOT NULL DEFAULT FALSE,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    source_file           TEXT,
    source_checksum       TEXT,
    ingested_at_utc       TIMESTAMPTZ,
    run_id                TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT members_id_format CHECK (member_id ~ '^M-[0-9]{4}$')
);

CREATE INDEX members_tier_idx        ON members (current_tier);
CREATE INDEX members_backfilled_idx  ON members (is_backfilled) WHERE is_backfilled;

-- ---------------------------------------------------------------- rentals

CREATE TABLE rental_transactions (
    rental_id           TEXT PRIMARY KEY,
    member_id           TEXT NOT NULL REFERENCES members (member_id),
    workstation_id      TEXT NOT NULL REFERENCES workstations (workstation_id),
    session_start_utc   TIMESTAMPTZ NOT NULL,
    -- NULL means the rental is still open. The POS creates the row at check-in and
    -- completes it at check-out, so every money column below is nullable until then.
    session_end_utc     TIMESTAMPTZ,
    duration_hours      NUMERIC(6,2) CHECK (duration_hours > 0),
    base_hourly_rate    NUMERIC(12,2) CHECK (base_hourly_rate >= 0),
    member_tier_applied TEXT NOT NULL CHECK (member_tier_applied IN ('Standard','Silver','Gold')),
    tier_discount_pct   NUMERIC(5,4) NOT NULL CHECK (tier_discount_pct BETWEEN 0 AND 1),
    final_hourly_rate   NUMERIC(12,2) CHECK (final_hourly_rate >= 0),
    gross_rental_amount NUMERIC(12,2) CHECK (gross_rental_amount >= 0),
    points_redeemed     INTEGER NOT NULL DEFAULT 0 CHECK (points_redeemed >= 0),
    points_credit_value NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (points_credit_value >= 0),
    net_amount_paid     NUMERIC(12,2) CHECK (net_amount_paid >= 0),
    points_accrued      INTEGER CHECK (points_accrued >= 0),
    payment_method      TEXT CHECK (payment_method IN ('Cash','GCash','Maya','Credit Card')),
    source_tz_offset    TEXT,
    source_file         TEXT,
    source_checksum     TEXT,
    ingested_at_utc     TIMESTAMPTZ,
    run_id              TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT rental_ends_after_it_starts
        CHECK (session_end_utc IS NULL OR session_end_utc >= session_start_utc),
    -- A closed rental must be fully priced. An open one is allowed to be incomplete.
    CONSTRAINT closed_rental_is_priced CHECK (
        session_end_utc IS NULL OR (
            duration_hours      IS NOT NULL AND
            base_hourly_rate    IS NOT NULL AND
            final_hourly_rate   IS NOT NULL AND
            gross_rental_amount IS NOT NULL AND
            net_amount_paid     IS NOT NULL AND
            points_accrued      IS NOT NULL AND
            payment_method      IS NOT NULL
        )
    ),
    CONSTRAINT credit_never_exceeds_gross
        CHECK (gross_rental_amount IS NULL OR points_credit_value <= gross_rental_amount)
);

-- POS access patterns (spec §6.2).
CREATE INDEX rental_member_time_idx   ON rental_transactions (member_id, session_start_utc DESC);
CREATE INDEX rental_workstation_idx   ON rental_transactions (workstation_id, session_start_utc DESC);
CREATE INDEX rental_start_idx         ON rental_transactions (session_start_utc);

-- These two are the real concurrency guarantee behind check-in (spec §7.1). A partial
-- unique index makes a double check-in impossible at the storage layer, so correctness
-- does not depend on the API winning a read-then-write race.
CREATE UNIQUE INDEX one_open_rental_per_workstation
    ON rental_transactions (workstation_id) WHERE session_end_utc IS NULL;
CREATE UNIQUE INDEX one_open_rental_per_member
    ON rental_transactions (member_id) WHERE session_end_utc IS NULL;

-- ---------------------------------------------------------------- concessions

CREATE TABLE concession_purchases (
    purchase_id      TEXT PRIMARY KEY,
    member_id        TEXT NOT NULL REFERENCES members (member_id),
    rental_id        TEXT REFERENCES rental_transactions (rental_id),
    total_amount     NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    points_accrued   INTEGER NOT NULL DEFAULT 0 CHECK (points_accrued >= 0),
    payment_method   TEXT NOT NULL CHECK (payment_method IN ('Cash','GCash','Maya','Credit Card')),
    purchased_at_utc TIMESTAMPTZ NOT NULL,
    source_tz_offset TEXT,
    source_file      TEXT,
    source_checksum  TEXT,
    ingested_at_utc  TIMESTAMPTZ,
    run_id           TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX purchase_member_time_idx ON concession_purchases (member_id, purchased_at_utc DESC);
CREATE INDEX purchase_rental_idx      ON concession_purchases (rental_id);
CREATE INDEX purchase_time_idx        ON concession_purchases (purchased_at_utc);

CREATE TABLE concession_order_items (
    order_item_id   TEXT PRIMARY KEY,
    purchase_id     TEXT NOT NULL REFERENCES concession_purchases (purchase_id) ON DELETE CASCADE,
    item_sku        TEXT NOT NULL REFERENCES concession_items (item_sku),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    total_price     NUMERIC(12,2) NOT NULL CHECK (total_price >= 0),
    source_file     TEXT,
    source_checksum TEXT,
    ingested_at_utc TIMESTAMPTZ,
    run_id          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- §4 cross-file invariant, enforced rather than merely checked downstream.
    CONSTRAINT line_total_is_quantity_times_unit_price
        CHECK (total_price = quantity * unit_price)
);

CREATE INDEX order_item_purchase_idx ON concession_order_items (purchase_id);
CREATE INDEX order_item_sku_idx      ON concession_order_items (item_sku);

-- ---------------------------------------------------------------- points

CREATE TABLE member_points_ledger (
    ledger_id           TEXT PRIMARY KEY,
    member_id           TEXT NOT NULL REFERENCES members (member_id),
    source_reference_id TEXT,
    transaction_type    TEXT NOT NULL CHECK (transaction_type IN (
                            'RENTAL_ACCRUAL', 'CONCESSION_ACCRUAL',
                            'RENTAL_REDEMPTION', 'TIER_BONUS')),
    points_delta        INTEGER NOT NULL,
    resulting_balance   INTEGER NOT NULL,
    created_at_utc      TIMESTAMPTZ NOT NULL,
    source_tz_offset    TEXT,
    source_file         TEXT,
    source_checksum     TEXT,
    ingested_at_utc     TIMESTAMPTZ,
    run_id              TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ledger_member_time_idx ON member_points_ledger (member_id, created_at_utc);
CREATE INDEX ledger_reference_idx   ON member_points_ledger (source_reference_id);
CREATE INDEX ledger_type_idx        ON member_points_ledger (transaction_type);
