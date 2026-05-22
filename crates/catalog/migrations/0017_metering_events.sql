-- 0017_metering_events.sql — append-only billing metering events.
-- RFC-17 §"Metering Events". MVP: direct Postgres insert; Kafka transport deferred.

CREATE TABLE IF NOT EXISTS metering_events (
    event_id      UUID          PRIMARY KEY,
    tenant_id     UUID          NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    pipeline_id   UUID          NULL,
    run_id        UUID          NULL,
    metric        TEXT          NOT NULL,
    value         BIGINT        NOT NULL,
    source        TEXT          NOT NULL,
    emitted_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS metering_events_tenant_emitted_idx
    ON metering_events (tenant_id, emitted_at DESC);

CREATE INDEX IF NOT EXISTS metering_events_pipeline_idx
    ON metering_events (pipeline_id)
    WHERE pipeline_id IS NOT NULL;

GRANT SELECT, INSERT ON metering_events TO etl_app;

ALTER TABLE metering_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE metering_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON metering_events;
CREATE POLICY tenant_isolation ON metering_events
    USING  (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
    WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
