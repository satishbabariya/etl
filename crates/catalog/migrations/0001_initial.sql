-- RFC-10 catalog minimal schema. Every row is tenant-scoped.
-- Later phases will add: workspaces, streams, schemas, transformations, secrets_refs, audit.

CREATE TABLE tenants (
    tenant_id    UUID PRIMARY KEY,
    name         TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE connections (
    connection_id  UUID PRIMARY KEY,
    tenant_id      UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    connector_ref  TEXT NOT NULL,           -- e.g. "postgres@0.1.0"
    config         JSONB NOT NULL,          -- non-secret config
    secret_refs    JSONB NOT NULL DEFAULT '{}'::jsonb, -- placeholder for RFC-11
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name)
);
CREATE INDEX connections_tenant_id_idx ON connections(tenant_id);

CREATE TABLE pipelines (
    pipeline_id      UUID PRIMARY KEY,
    tenant_id        UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name             TEXT NOT NULL,
    source_conn_id   UUID NOT NULL REFERENCES connections(connection_id),
    dest_conn_id     UUID REFERENCES connections(connection_id), -- nullable for I.1
    spec             JSONB NOT NULL,        -- YAML DSL body after parse
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name)
);
CREATE INDEX pipelines_tenant_id_idx ON pipelines(tenant_id);

CREATE TABLE runs (
    run_id              UUID PRIMARY KEY,
    tenant_id           UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    pipeline_id         UUID NOT NULL REFERENCES pipelines(pipeline_id),
    status              TEXT NOT NULL CHECK (status IN ('queued','running','completed','failed','cancelled')),
    trigger             TEXT NOT NULL,         -- 'manual', 'schedule', 'signal'
    temporal_workflow_id TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    error               TEXT
);
CREATE INDEX runs_tenant_pipeline_idx ON runs(tenant_id, pipeline_id, started_at DESC);
CREATE INDEX runs_status_idx ON runs(status) WHERE status IN ('queued','running');
