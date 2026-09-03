-- Reverse the grants this migration made *in this schema*.
--
-- The role itself is cluster-scoped and may be in use by another schema on this shared
-- instance, so it is dropped only when nothing depends on it any more. Attempting an
-- unconditional DROP ROLE here would make any rollback in a scratch schema fail, and would
-- be wrong besides: this migration did not create the whole cluster.

ALTER DEFAULT PRIVILEGES IN SCHEMA ${SCHEMA} REVOKE SELECT ON TABLES FROM ${RO_USER};
REVOKE ALL ON ALL TABLES IN SCHEMA ${SCHEMA} FROM ${RO_USER};
REVOKE ALL ON SCHEMA ${SCHEMA} FROM ${RO_USER};

DO $$
BEGIN
    EXECUTE format('REVOKE ALL ON DATABASE %I FROM %I', current_database(), '${RO_USER}');
    EXECUTE format('DROP ROLE %I', '${RO_USER}');
    RAISE NOTICE 'dropped role %', '${RO_USER}';
EXCEPTION
    WHEN dependent_objects_still_exist OR insufficient_privilege THEN
        RAISE NOTICE 'role % is still used elsewhere; grants revoked, role kept', '${RO_USER}';
END
$$;
