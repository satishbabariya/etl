-- 0011_refresh_tokens.sql — long-lived refresh tokens, rotate-on-use.

CREATE TABLE IF NOT EXISTS refresh_tokens (
    token_id     UUID PRIMARY KEY,
    tenant_id    UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    principal_id UUID NOT NULL REFERENCES principals(principal_id) ON DELETE CASCADE,
    hash         TEXT NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS refresh_tokens_principal_id_idx ON refresh_tokens(principal_id);
CREATE INDEX IF NOT EXISTS refresh_tokens_expires_at_idx ON refresh_tokens(expires_at);

GRANT SELECT, INSERT, UPDATE, DELETE ON refresh_tokens TO etl_app;
ALTER TABLE refresh_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE refresh_tokens FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON refresh_tokens;
CREATE POLICY tenant_isolation ON refresh_tokens
  USING  (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
