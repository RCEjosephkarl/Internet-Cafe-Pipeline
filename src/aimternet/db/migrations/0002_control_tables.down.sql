SET search_path TO ${SCHEMA};
DROP TABLE IF EXISTS reconciliation_results CASCADE;
DROP TABLE IF EXISTS quarantine_records     CASCADE;
DROP TABLE IF EXISTS api_idempotency        CASCADE;
DROP TABLE IF EXISTS pipeline_watermark     CASCADE;
DROP TABLE IF EXISTS load_manifest          CASCADE;
