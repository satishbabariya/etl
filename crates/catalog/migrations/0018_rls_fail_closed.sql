-- 0018_rls_fail_closed.sql — make RLS fail closed.
--
-- Until now every policy admitted all rows when `app.tenant_id` was
-- unset (`OR app_tenant_id() IS NULL`), so an etl_app session that forgot
-- to scope itself saw — and could write — every tenant's data. Admin mode
-- is now an explicit opt-in: `SET LOCAL app.admin = 'on'`. With neither
-- setting a session sees no tenant rows.
--
-- Also brings cdc_snapshots (0015), which was created without RLS, under
-- the same policy.

-- 1. Helper: true only when the session explicitly asked for admin mode.
CREATE OR REPLACE FUNCTION app_is_admin()
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
  SELECT coalesce(current_setting('app.admin', true), '') = 'on'
$$;

-- 2. Tenant-scoped tables.
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
    'cdc_slots',
    'cdc_snapshots',
    'secrets',
    'principals',
    'refresh_tokens',
    'revoked_tokens',
    'metering_events'
  ];
BEGIN
  FOREACH tbl IN ARRAY tables LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', tbl);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I
         USING (tenant_id = app_tenant_id() OR app_is_admin())
         WITH CHECK (tenant_id = app_tenant_id() OR app_is_admin())',
      tbl
    );
  END LOOP;
END
$$;

-- 3. Audit tables keep their system chain (tenant_id IS NULL) visible.
DO $$
DECLARE
  tbl text;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['audit_log', 'audit_verified_chain'] LOOP
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', tbl);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I
         USING (tenant_id IS NULL OR tenant_id = app_tenant_id() OR app_is_admin())
         WITH CHECK (tenant_id IS NULL OR tenant_id = app_tenant_id() OR app_is_admin())',
      tbl
    );
  END LOOP;
END
$$;

-- 4. tenants: a tenant sees only its own row; admin sees all.
DROP POLICY IF EXISTS tenant_self ON tenants;
CREATE POLICY tenant_self ON tenants
  USING (tenant_id = app_tenant_id() OR app_is_admin())
  WITH CHECK (tenant_id = app_tenant_id() OR app_is_admin());
