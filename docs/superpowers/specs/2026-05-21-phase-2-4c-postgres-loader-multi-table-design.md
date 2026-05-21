# Phase 2.4.c: Postgres Loader — Multi-Table Routing

**Status:** Shipped 2026-05-21 (branch `phase-2-4c-postgres-loader-multi-table`).
**Plan:** `docs/superpowers/plans/2026-05-21-phase-2-4c-postgres-loader-multi-table.md`
**Builds on:** Phase 2.4.b (`docs/superpowers/specs/2026-05-21-phase-2-4b-postgres-loader-cdc-design.md`).
**RFC:** RFC-0009 §"Per-Destination Specifics — Postgres".

## What this adds

`PostgresLoader` now reads `LoadId.stream_name` to pick a per-batch target table inside `spec.schema`. Multi-table CDC pipelines (e.g. `public.users` + `public.orders` from the same Postgres source) land each table's batches in its own destination table, with no spec changes needed.

## Resolution

- `stream_name = ""` ⇒ `spec.table` (single-table behavior — phase 2.4.a/b unchanged).
- `stream_name = "<n>"` ⇒ table `<n>` inside `spec.schema`.

The connector convention is `<src_schema>.<src_table>` (see `docs/superpowers/specs/2026-05-05-phase-2-3g-multi-table-cdc-design.md`), so a source row from `public.users` lands in `<spec.schema>."public.users"` (literal dot in the destination table name). Future work splits this into a `<src_schema>` schema namespace at the destination.

## Validation

`resolve_target_table` rejects target names containing:
- `"` (would break quoted-identifier escaping)
- NUL byte
- Any Unicode `is_control()` character

Dots, hyphens, underscores, mixed case all pass.

## Idempotency

Unchanged from phase 2.4.a. The `_etl_loaded_batches` PK is `(tenant_id, pipeline_id, run_id, stream_name, batch_seq)`, so two streams with the same `batch_seq` in the same run do not collide.

## Limitations (known, deferred)

- **Per-stream `pk_columns` override** — every stream uses `spec.pk_columns`. Mixed-PK multi-table pipelines need a per-stream config (future `tables: HashMap<String, TableConfig>` field on the spec).
- **Destination-schema split** — `public.users` lands as a literal `"public.users"` table; a future variant could route it to `<spec.schema>."users"` (taking the post-dot segment) or `"public"."users"` (separate schema).
- **Schema evolution** still deferred to phase 2.4.d.
- **Cross-stream atomicity** — each batch is atomic; cross-table consistency is a workflow concern (RFC-4).

## Tests

- Unit: 5 new tests in `loaders::postgres::tests` covering `resolve_target_table` (fallback, stream-wins, quote rejection, control-char rejection, both-empty error).
- Integration: 5 new tests in `tests/integration/tests/postgres_loader_multi_table.rs` covering multi-table append, multi-table CDC, idempotency-by-stream, dot-in-name, and quote rejection at runtime.
- Phase 2.4.a/b's 10 integration tests still green (no regression — they use empty `stream_name`, hitting the `spec.table` fallback).

## Follow-ups (priority order)

1. Schema evolution (apply additive `ALTER TABLE` on `c` events; pause on destructive).
2. Per-stream `pk_columns` override / per-stream renaming map.
3. Dead-letter routing for Postgres failures.
4. `COPY FROM STDIN` perf path for snapshot-heavy batches.
5. Secret-ref URLs (RFC-11 wiring on the loader side).
6. Audit-log destination mode option.
7. Destination-schema split for `<src>.<tbl>` stream names.
