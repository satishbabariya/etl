-- 0009_principals.sql — per-tenant user/principal table for the dev
-- login flow. JWT subject claims map to a row here. Phase II.2.c
-- federates this with OIDC.

CREATE TABLE IF NOT EXISTS principals (
    principal_id   UUID PRIMARY KEY,
    tenant_id      UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    password_hash  TEXT NOT NULL,
    role           TEXT NOT NULL CHECK (role IN ('admin', 'operator', 'viewer')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE INDEX IF NOT EXISTS principals_tenant_id_idx ON principals(tenant_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON principals TO etl_app;
ALTER TABLE principals ENABLE ROW LEVEL SECURITY;
ALTER TABLE principals FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON principals;
CREATE POLICY tenant_isolation ON principals
  USING  (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
