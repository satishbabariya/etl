-- 00-app-role.sql — runs once on first container boot.
-- Creates a non-superuser role the catalog connects as in production-like
-- mode. RLS policies bypass for SUPERUSER and for any role with
-- BYPASSRLS, so this role explicitly has neither.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_app') THEN
    CREATE ROLE etl_app LOGIN PASSWORD 'etl_app' NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

-- Grant on the catalog DB. Object-level grants (per-table) come in
-- migration 0006 alongside the RLS policies themselves.
GRANT CONNECT ON DATABASE etl_catalog TO etl_app;
GRANT USAGE ON SCHEMA public TO etl_app;
