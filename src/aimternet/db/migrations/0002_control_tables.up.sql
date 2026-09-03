-- Control plane: the load manifest, watermarks, and API idempotency.
-- These are pipeline state, not business data, but they live beside it so a single
-- backup or a single `DROP SCHEMA` covers everything this POC created.

SET search_path TO ${SCHEMA};

-- Spec §6.1 Stage A. The checksum is the idempotency key: a file already registered as
-- LOADED with the same checksum is skipped, not reloaded.
CREATE TABLE load_manifest (
    manifest_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file     TEXT        NOT NULL,
    source_type     TEXT        NOT NULL,
    dataset         TEXT        NOT NULL,
    batch_date      DATE,
    hour_of_day     SMALLINT,
    checksum        TEXT        NOT NULL,
    file_size       BIGINT      NOT NULL CHECK (file_size >= 0),
    file_mtime_utc  TIMESTAMPTZ,
    load_timestamp  TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT        NOT NULL DEFAULT 'DISCOVERED'
                    CHECK (status IN ('DISCOVERED','UPLOADED','VALIDATED','LOADED','FAILED','SKIPPED')),
    record_count    INTEGER     NOT NULL DEFAULT 0 CHECK (record_count  >= 0),
    error_count     INTEGER     NOT NULL DEFAULT 0 CHECK (error_count   >= 0),
    bronze_uri      TEXT,
    error_detail    TEXT,
    run_id          TEXT        NOT NULL,
    CONSTRAINT manifest_file_checksum_unique UNIQUE (source_file, checksum)
);

CREATE INDEX manifest_status_idx  ON load_manifest (status);
CREATE INDEX manifest_dataset_idx ON load_manifest (dataset, batch_date);
CREATE INDEX manifest_run_idx     ON load_manifest (run_id);

-- Spec §6.7: explicit checkpoints for the ongoing incremental DAGs.
CREATE TABLE pipeline_watermark (
    pipeline_name   TEXT PRIMARY KEY,
    watermark_value TEXT        NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id          TEXT
);

-- Spec §7.1: every write endpoint is safe to retry. A replayed Idempotency-Key returns
-- the original response instead of creating a second rental or charging twice.
CREATE TABLE api_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    endpoint        TEXT        NOT NULL,
    request_hash    TEXT        NOT NULL,
    response_status INTEGER     NOT NULL,
    response_body   JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX api_idempotency_created_idx ON api_idempotency (created_at);

-- Spec §6.1 Stage C: rejected records, with the reason and the lineage attached.
-- Nothing is discarded silently.
CREATE TABLE quarantine_records (
    quarantine_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset         TEXT        NOT NULL,
    source_file     TEXT        NOT NULL,
    source_checksum TEXT,
    batch_date      DATE,
    record_key      TEXT,
    rejection_rule  TEXT        NOT NULL,
    rejection_detail TEXT,
    severity        TEXT        NOT NULL DEFAULT 'ERROR'
                    CHECK (severity IN ('ERROR','WARNING')),
    raw_record      JSONB       NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id          TEXT        NOT NULL
);

CREATE INDEX quarantine_dataset_idx ON quarantine_records (dataset, rejection_rule);
CREATE INDEX quarantine_run_idx     ON quarantine_records (run_id);

-- Spec §6.6: one row per check per run, so reconciliation history is queryable rather
-- than living only in a report file.
CREATE TABLE reconciliation_results (
    result_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id         TEXT        NOT NULL,
    check_name     TEXT        NOT NULL,
    layer_from     TEXT,
    layer_to       TEXT,
    expected_value NUMERIC,
    actual_value   NUMERIC,
    passed         BOOLEAN     NOT NULL,
    severity       TEXT        NOT NULL DEFAULT 'CRITICAL'
                   CHECK (severity IN ('CRITICAL','WARNING','INFO')),
    detail         TEXT,
    checked_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX reconciliation_run_idx ON reconciliation_results (run_id, passed);
