-- Redshift analytical model for AIMternet-Cafe (spec §6.5).
--
-- Namespaced into ${SCHEMA}: this cluster is shared with unrelated coursework
-- (bus_ticketing, krusty_krab_olap, catalog_history). Nothing here touches them.
--
-- Distribution and sort keys are chosen deliberately and explained per table. The guiding
-- facts: every dimension here is small enough to replicate (the largest is 2,235 rows), and
-- every fact is queried by time first, so time leads every sort key.

CREATE SCHEMA IF NOT EXISTS ${SCHEMA};
SET search_path TO ${SCHEMA};

-- ---------------------------------------------------------------- dimensions
-- DISTSTYLE ALL on all five: the biggest is 2,235 rows, so replicating them to every node
-- removes the redistribution step from every join for a few hundred KB of storage.

CREATE TABLE IF NOT EXISTS dim_member (
    member_key             BIGINT        NOT NULL,
    member_id              VARCHAR(16)   NOT NULL,
    first_name             VARCHAR(128),
    last_name              VARCHAR(128),
    email                  VARCHAR(256),
    phone_number           VARCHAR(64),
    tier                   VARCHAR(16)   NOT NULL,
    valid_from_utc         TIMESTAMPTZ   NOT NULL,
    valid_to_utc           TIMESTAMPTZ,
    is_current             BOOLEAN       NOT NULL,
    current_points_balance INTEGER,
    lifetime_spend_amount  DECIMAL(12,2),
    registered_at_utc      TIMESTAMPTZ,
    source_system          VARCHAR(64),
    is_backfilled          BOOLEAN       NOT NULL DEFAULT FALSE,
    run_id                 VARCHAR(64),
    built_at_utc           TIMESTAMPTZ,
    PRIMARY KEY (member_key)
)
DISTSTYLE ALL
-- Point lookups are by member_id; SCD2 range scans need valid_from beside it.
SORTKEY (member_id, valid_from_utc);

CREATE TABLE IF NOT EXISTS dim_workstation (
    workstation_key     VARCHAR(16)   NOT NULL,
    workstation_id      VARCHAR(16)   NOT NULL,
    zone_classification VARCHAR(32)   NOT NULL,
    base_hourly_rate    DECIMAL(12,2) NOT NULL,
    ip_address          VARCHAR(64),
    mac_address         VARCHAR(64),
    commissioned_date   DATE,
    run_id              VARCHAR(64),
    built_at_utc        TIMESTAMPTZ,
    PRIMARY KEY (workstation_key)
)
DISTSTYLE ALL
-- Almost every analytical question groups by zone before it filters by workstation.
SORTKEY (zone_classification, workstation_id);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         INTEGER     NOT NULL,
    calendar_date   DATE        NOT NULL,
    day_of_week     VARCHAR(16),
    day_of_month    SMALLINT,
    month_label     VARCHAR(16),
    month_index     SMALLINT,
    quarter_index   SMALLINT,
    year_index      SMALLINT,
    weekend_flag    BOOLEAN,
    holiday_ph_flag BOOLEAN,
    run_id          VARCHAR(64),
    PRIMARY KEY (date_id)
)
DISTSTYLE ALL
SORTKEY (date_id);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id        INTEGER NOT NULL,
    hour_24        SMALLINT,
    minute_val     SMALLINT,
    day_part_label VARCHAR(32),
    run_id         VARCHAR(64),
    PRIMARY KEY (time_id)
)
DISTSTYLE ALL
SORTKEY (time_id);

CREATE TABLE IF NOT EXISTS dim_concession_item (
    item_key          VARCHAR(32)   NOT NULL,
    item_sku          VARCHAR(32)   NOT NULL,
    item_name         VARCHAR(128),
    category          VARCHAR(32),
    unit_cost_price   DECIMAL(12,2),
    unit_retail_price DECIMAL(12,2),
    unit_margin       DECIMAL(12,2),
    run_id            VARCHAR(64),
    built_at_utc      TIMESTAMPTZ,
    PRIMARY KEY (item_key)
)
DISTSTYLE ALL
SORTKEY (category, item_sku);

-- ---------------------------------------------------------------- facts

CREATE TABLE IF NOT EXISTS fact_rental (
    rental_id           VARCHAR(32)   NOT NULL,
    member_id           VARCHAR(16)   NOT NULL,
    workstation_id      VARCHAR(16)   NOT NULL,
    date_key            INTEGER       NOT NULL,
    time_key            INTEGER,
    session_start_utc   TIMESTAMPTZ   NOT NULL,
    session_end_utc     TIMESTAMPTZ,
    duration_hours      DECIMAL(6,2),
    base_hourly_rate    DECIMAL(12,2),
    member_tier_applied VARCHAR(16),
    tier_discount_pct   DECIMAL(5,4),
    final_hourly_rate   DECIMAL(12,2),
    gross_rental_amount DECIMAL(12,2),
    points_redeemed     INTEGER,
    points_credit_value DECIMAL(12,2),
    net_amount_paid     DECIMAL(12,2),
    points_accrued      INTEGER,
    payment_method      VARCHAR(32),
    zone_classification VARCHAR(32),
    source_file         VARCHAR(256),
    run_id              VARCHAR(64),
    built_at_utc        TIMESTAMPTZ,
    PRIMARY KEY (rental_id)
)
-- Co-located with fact_points_activity and fact_concession_sale on member_id, so
-- "everything this member did" joins without redistribution. Time leads the sort key
-- because every query filters a date range first.
DISTKEY (member_id)
SORTKEY (session_start_utc, workstation_id);

CREATE TABLE IF NOT EXISTS fact_concession_sale (
    purchase_id      VARCHAR(32)   NOT NULL,
    member_id        VARCHAR(16)   NOT NULL,
    rental_id        VARCHAR(32),
    date_key         INTEGER       NOT NULL,
    time_key         INTEGER,
    purchased_at_utc TIMESTAMPTZ   NOT NULL,
    total_amount     DECIMAL(12,2),
    points_accrued   INTEGER,
    payment_method   VARCHAR(32),
    is_walk_in       BOOLEAN,
    source_file      VARCHAR(256),
    run_id           VARCHAR(64),
    built_at_utc     TIMESTAMPTZ,
    PRIMARY KEY (purchase_id)
)
DISTKEY (member_id)
SORTKEY (purchased_at_utc);

CREATE TABLE IF NOT EXISTS fact_concession_line_item (
    order_item_id VARCHAR(64)   NOT NULL,
    purchase_id   VARCHAR(32)   NOT NULL,
    item_sku      VARCHAR(32)   NOT NULL,
    quantity      INTEGER,
    unit_price    DECIMAL(12,2),
    total_price   DECIMAL(12,2),
    line_margin   DECIMAL(12,2),
    member_id     VARCHAR(16),
    date_key      INTEGER,
    run_id        VARCHAR(64),
    built_at_utc  TIMESTAMPTZ,
    PRIMARY KEY (order_item_id)
)
-- Line items are almost always joined back to their purchase, so co-locate on purchase_id.
DISTKEY (purchase_id)
SORTKEY (date_key, item_sku);

CREATE TABLE IF NOT EXISTS fact_points_activity (
    ledger_id                VARCHAR(64) NOT NULL,
    member_id                VARCHAR(16) NOT NULL,
    source_reference_id      VARCHAR(64),
    transaction_type         VARCHAR(32) NOT NULL,
    points_delta             INTEGER     NOT NULL,
    running_balance          BIGINT,
    resulting_balance_source INTEGER,
    created_at_utc           TIMESTAMPTZ NOT NULL,
    date_key                 INTEGER,
    source_file              VARCHAR(256),
    run_id                   VARCHAR(64),
    built_at_utc             TIMESTAMPTZ,
    PRIMARY KEY (ledger_id)
)
DISTKEY (member_id)
-- Balance questions are per member over time, so member leads here rather than time.
SORTKEY (member_id, created_at_utc);

CREATE TABLE IF NOT EXISTS fact_workstation_event (
    event_id                 VARCHAR(64) NOT NULL,
    workstation_id           VARCHAR(16) NOT NULL,
    event_type               VARCHAR(32) NOT NULL,
    session_id               VARCHAR(32),
    member_id                VARCHAR(16),
    event_timestamp_utc      TIMESTAMPTZ NOT NULL,
    duration_allocated_hours DECIMAL(6,2),
    client_os_version        VARCHAR(64),
    date_key                 INTEGER,
    zone_classification      VARCHAR(32),
    source_file              VARCHAR(256),
    run_id                   VARCHAR(64),
    built_at_utc             TIMESTAMPTZ,
    PRIMARY KEY (event_id)
)
DISTKEY (workstation_id)
SORTKEY (event_timestamp_utc, event_type);

CREATE TABLE IF NOT EXISTS agg_workstation_utilization_hourly (
    workstation_id      VARCHAR(16)  NOT NULL,
    zone                VARCHAR(32),
    utilization_date    DATE         NOT NULL,
    hour_utc            SMALLINT     NOT NULL,
    date_key            INTEGER,
    readings            INTEGER      NOT NULL,
    readings_occupied   INTEGER      NOT NULL,
    utilization_pct     DECIMAL(5,2),
    distinct_sessions   INTEGER,
    avg_cpu_load_pct    DECIMAL(5,1),
    max_cpu_temp_c      SMALLINT,
    avg_gpu_load_pct    DECIMAL(5,1),
    max_gpu_temp_c      SMALLINT,
    avg_latency_ping_ms DECIMAL(6,1),
    max_packet_loss_pct DECIMAL(5,2),
    run_id              VARCHAR(64),
    built_at_utc        TIMESTAMPTZ,
    PRIMARY KEY (workstation_id, utilization_date, hour_utc)
)
-- 260,400 rows: small, and joined to dim_workstation constantly. Distributing by
-- workstation_id keeps each workstation's history on one slice.
DISTKEY (workstation_id)
SORTKEY (utilization_date, hour_utc);
