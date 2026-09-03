-- Per-file load checkpoints.
--
-- The manifest's `status` column tracks a file's furthest progress overall; this table
-- records completion per (pipeline, file), which is what makes an interrupted 1,488-file
-- DynamoDB load resumable without re-writing millions of items. Keyed on the checksum too,
-- so a file whose contents changed is reloaded rather than skipped.

SET search_path TO ${SCHEMA};

CREATE TABLE load_checkpoint (
    pipeline       TEXT        NOT NULL,
    source_file    TEXT        NOT NULL,
    checksum       TEXT        NOT NULL,
    records_loaded INTEGER     NOT NULL DEFAULT 0 CHECK (records_loaded >= 0),
    completed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id         TEXT        NOT NULL,
    PRIMARY KEY (pipeline, source_file, checksum)
);

CREATE INDEX load_checkpoint_pipeline_idx ON load_checkpoint (pipeline, completed_at);
