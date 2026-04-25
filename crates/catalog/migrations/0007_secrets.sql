-- 0007_secrets.sql — tenant-scoped secret references.
-- Each row points at a (backend, key) pair holding the plaintext
-- elsewhere. No plaintexts in catalog.

CREATE TABLE IF NOT EXISTS secrets (
    secret_id   UUID PRIMARY KEY,
    tenant_id   UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    backend     TEXT NOT NULL CHECK (backend IN ('env', 'file')),
    key         TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE INDEX IF NOT EXISTS secrets_tenant_id_idx ON secrets(tenant_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON secrets TO etl_app;
ALTER TABLE secrets ENABLE ROW LEVEL SECURITY;
ALTER TABLE secrets FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON secrets;
CREATE POLICY tenant_isolation ON secrets
  USING (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
