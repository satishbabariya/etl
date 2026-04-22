-- Per-stream cursor state. One row per (pipeline_id, stream_name).
-- Phase I.2 scope. Full Stream/Schema entity lands in Phase I.4 (RFC-10).

CREATE TABLE stream_state (
    pipeline_id    UUID NOT NULL REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    stream_name    TEXT NOT NULL,
    cursor_kind    TEXT NOT NULL CHECK (cursor_kind IN ('int64','timestamptz')),
    cursor_value   TEXT,           -- null = never synced; strings for kind-agnostic storage
    last_run_id    UUID REFERENCES runs(run_id) ON DELETE SET NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, stream_name)
);
