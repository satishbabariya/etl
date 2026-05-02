-- 0015_cdc_snapshots.sql — per-pipeline snapshot progress tracking
-- Survives worker crashes and workflow failures so re-runs of the
-- same pipeline pick up where the previous run left off.
CREATE TABLE IF NOT EXISTS cdc_snapshots (
    pipeline_id        UUID PRIMARY KEY
                       REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    tenant_id          UUID        NOT NULL
                       REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    last_pk            BIGINT,
    completed          BOOLEAN     NOT NULL DEFAULT false,
    -- Captured GTID (MySQL) or LSN (Postgres). For Postgres CDC the
    -- consistent_point is also stored in cdc_slots; this column
    -- duplicates it so workflows have a single place to read snapshot
    -- state without joining tables.
    captured_position  TEXT        NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cdc_snapshots_completed_idx
    ON cdc_snapshots(completed) WHERE completed = false;
