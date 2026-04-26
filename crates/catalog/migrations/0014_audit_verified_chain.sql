-- 0014_audit_verified_chain.sql — checkpoint table so audit retention
-- can prune old rows without breaking verify-chain.

CREATE TABLE IF NOT EXISTS audit_verified_chain (
    tenant_id              UUID NULL,
    last_verified_audit_id BIGINT NOT NULL,
    last_verified_hash     BYTEA NOT NULL,
    last_verified_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Functional unique index — Postgres doesn't allow COALESCE in PRIMARY
-- KEY directly, so we use a unique expression index instead. This
-- ensures one row per tenant (or one row for the system chain).
CREATE UNIQUE INDEX IF NOT EXISTS audit_verified_chain_tenant_uniq
    ON audit_verified_chain
       ((COALESCE(tenant_id, '00000000-0000-0000-0000-000000000000'::UUID)));

GRANT SELECT, INSERT, UPDATE ON audit_verified_chain TO etl_app;
ALTER TABLE audit_verified_chain ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_verified_chain FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON audit_verified_chain;
CREATE POLICY tenant_isolation ON audit_verified_chain
  USING  (tenant_id IS NULL OR tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id IS NULL OR tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
