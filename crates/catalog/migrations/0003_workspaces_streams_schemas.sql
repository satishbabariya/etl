-- Phase I.4 catalog elaboration per RFC-10.

CREATE TABLE workspaces (
    workspace_id  UUID PRIMARY KEY,
    tenant_id     UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name)
);
CREATE INDEX workspaces_tenant_id_idx ON workspaces(tenant_id);

-- Backfill: one "default" workspace per existing tenant.
INSERT INTO workspaces (workspace_id, tenant_id, name)
SELECT gen_random_uuid(), tenant_id, 'default' FROM tenants;

-- Denormalize workspace_id onto connections + pipelines.
ALTER TABLE connections ADD COLUMN workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE CASCADE;
UPDATE connections c
SET workspace_id = (
    SELECT workspace_id FROM workspaces w
    WHERE w.tenant_id = c.tenant_id AND w.name = 'default'
);
ALTER TABLE connections ALTER COLUMN workspace_id SET NOT NULL;
CREATE INDEX connections_workspace_id_idx ON connections(workspace_id);

ALTER TABLE pipelines ADD COLUMN workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE CASCADE;
UPDATE pipelines p
SET workspace_id = (
    SELECT workspace_id FROM workspaces w
    WHERE w.tenant_id = p.tenant_id AND w.name = 'default'
);
ALTER TABLE pipelines ALTER COLUMN workspace_id SET NOT NULL;
CREATE INDEX pipelines_workspace_id_idx ON pipelines(workspace_id);

-- Streams.
CREATE TABLE streams (
    stream_id          UUID PRIMARY KEY,
    tenant_id          UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    pipeline_id        UUID NOT NULL REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    name               TEXT NOT NULL,
    sync_mode          TEXT NOT NULL DEFAULT 'incremental'
                         CHECK (sync_mode IN ('full_refresh','incremental','cdc')),
    cursor_config      JSONB NOT NULL,
    pk_config          JSONB NOT NULL DEFAULT '[]'::jsonb,
    destination_table  TEXT,
    current_schema_id  UUID,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, name)
);
CREATE INDEX streams_tenant_id_idx ON streams(tenant_id);

-- Schemas.
CREATE TABLE schemas (
    schema_id                    UUID PRIMARY KEY,
    tenant_id                    UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    stream_id                    UUID NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
    version                      INT NOT NULL,
    parent_schema_id             UUID REFERENCES schemas(schema_id) ON DELETE SET NULL,
    fingerprint                  TEXT NOT NULL,
    arrow_schema_json            JSONB NOT NULL,
    change_summary               JSONB NOT NULL DEFAULT '[]'::jsonb,
    detected_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_to_destination_at    TIMESTAMPTZ,
    UNIQUE (stream_id, version)
);
CREATE INDEX schemas_stream_id_idx ON schemas(stream_id);
CREATE INDEX schemas_fingerprint_idx ON schemas(fingerprint);

ALTER TABLE streams
  ADD CONSTRAINT streams_current_schema_fk
  FOREIGN KEY (current_schema_id) REFERENCES schemas(schema_id) ON DELETE SET NULL;
