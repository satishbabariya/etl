-- 0004_cdc_slots.sql — per-pipeline replication slot tracking
CREATE TABLE IF NOT EXISTS cdc_slots (
    pipeline_id       UUID PRIMARY KEY
                      REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    slot_name         TEXT        NOT NULL UNIQUE,
    publication_name  TEXT        NOT NULL,
    consistent_point  TEXT        NOT NULL,
    confirmed_flush   TEXT,
    state             TEXT        NOT NULL DEFAULT 'active'
                      CHECK (state IN ('active', 'paused', 'released')),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cdc_slots_state_idx ON cdc_slots(state);
