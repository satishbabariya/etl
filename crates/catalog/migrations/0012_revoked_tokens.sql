-- 0012_revoked_tokens.sql — explicit jti revocation list.

CREATE TABLE IF NOT EXISTS revoked_tokens (
    jti        UUID PRIMARY KEY,
    tenant_id  UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    exp        TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS revoked_tokens_exp_idx ON revoked_tokens(exp);

GRANT SELECT, INSERT, DELETE ON revoked_tokens TO etl_app;
ALTER TABLE revoked_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE revoked_tokens FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON revoked_tokens;
CREATE POLICY tenant_isolation ON revoked_tokens
  USING  (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
