-- A genuinely read-only role for notebooks/db_lens.ipynb.
--
-- The notebook is an exploration tool, so the safety comes from the grant, not from the
-- notebook's good intentions: this role has USAGE and SELECT and nothing else, and future
-- tables inherit the same. Combined with `SET default_transaction_read_only = on` in
-- db.session, an accidental UPDATE in a notebook cell fails twice over.

SET search_path TO ${SCHEMA};

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${RO_USER}') THEN
        EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '${RO_USER}', '${RO_PASSWORD}');
    ELSE
        EXECUTE format('ALTER ROLE %I LOGIN PASSWORD %L', '${RO_USER}', '${RO_PASSWORD}');
    END IF;
END
$$;

GRANT CONNECT ON DATABASE ${DATABASE} TO ${RO_USER};
GRANT USAGE ON SCHEMA ${SCHEMA} TO ${RO_USER};
GRANT SELECT ON ALL TABLES IN SCHEMA ${SCHEMA} TO ${RO_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA ${SCHEMA} GRANT SELECT ON TABLES TO ${RO_USER};
