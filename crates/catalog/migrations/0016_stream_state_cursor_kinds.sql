-- Widen stream_state.cursor_kind CHECK to include the CDC variants.
--
-- Phase II.3.e added cursor-kind=gtid + snapshot_pk to the Rust enum and
-- WIT, but the original 0002_stream_state.sql constraint only allowed
-- ('int64','timestamptz'). Phase II.3.f's Postgres CDC SDK port also
-- writes 'lsn' values to this table via SyncActivities::commit_cursor.
-- All three are valid; the constraint must allow them.

ALTER TABLE stream_state DROP CONSTRAINT IF EXISTS stream_state_cursor_kind_check;
ALTER TABLE stream_state ADD CONSTRAINT stream_state_cursor_kind_check
    CHECK (cursor_kind IN ('int64','timestamptz','lsn','gtid','snapshot_pk'));
