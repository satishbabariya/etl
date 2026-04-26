-- 0013_audit_log.sql — hash-chained, per-tenant audit log.
-- tenant_id is NULL-able for system-scoped events (e.g. AUTH_LOGIN_FAILED
-- before the principal is identified).

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id      BIGSERIAL PRIMARY KEY,
    tenant_id     UUID NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    principal_id  UUID NULL,
    jti           UUID NULL,
    action        TEXT NOT NULL,
    target        TEXT NULL,
    payload       JSONB NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    prev_hash     BYTEA NOT NULL,
    hash          BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS audit_log_tenant_id_audit_id_idx
    ON audit_log (tenant_id, audit_id DESC);
CREATE INDEX IF NOT EXISTS audit_log_action_idx ON audit_log (action);

GRANT SELECT, INSERT ON audit_log TO etl_app;
GRANT USAGE, SELECT ON SEQUENCE audit_log_audit_id_seq TO etl_app;
ALTER TABLE audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_log FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON audit_log;
CREATE POLICY tenant_isolation ON audit_log
  USING  (tenant_id IS NULL OR tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id IS NULL OR tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
