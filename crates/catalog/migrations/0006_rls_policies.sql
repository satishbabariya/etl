-- 0006_rls_policies.sql — enable RLS + define a single per-tenant policy
-- on every tenant-scoped table. The policy reads `app.tenant_id` from
-- the session; callers MUST `SET LOCAL app.tenant_id = '<uuid>'` inside
-- a transaction before issuing any DML.
--
-- Admin (NULL app.tenant_id) bypasses for tenant CRUD + migrations.

-- 1. Grant catalog perms to etl_app.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO etl_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_app;

-- 2. Helper: read app.tenant_id, return NULL if unset.
CREATE OR REPLACE FUNCTION app_tenant_id()
RETURNS uuid
LANGUAGE sql
STABLE
AS $$
  SELECT NULLIF(current_setting('app.tenant_id', true), '')::uuid
$$;

-- 3. Per-table RLS for tenant-scoped tables.
DO $$
DECLARE
  tbl text;
  tables text[] := ARRAY[
    'connections',
    'pipelines',
    'runs',
    'workspaces',
    'streams',
    'schemas',
    'stream_state',
    'cdc_slots'
  ];
BEGIN
  FOREACH tbl IN ARRAY tables LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format(
      'DROP POLICY IF EXISTS tenant_isolation ON %I', tbl
    );
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I
         USING (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
         WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)',
      tbl
    );
  END LOOP;
END
$$;

-- 4. tenants table: a tenant sees only its own row; admin (NULL) sees all.
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_self ON tenants;
CREATE POLICY tenant_self ON tenants
  USING (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
