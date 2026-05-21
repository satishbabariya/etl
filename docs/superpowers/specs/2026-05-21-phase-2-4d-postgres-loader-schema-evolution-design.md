# Phase 2.4.d: Postgres Loader — Schema Evolution

**Status:** Shipped 2026-05-21 (branch `phase-2-4d-postgres-loader-schema-evolution`).
**Plan:** `docs/superpowers/plans/2026-05-21-phase-2-4d-postgres-loader-schema-evolution.md`
**Builds on:** Phase 2.4.c (`docs/superpowers/specs/2026-05-21-phase-2-4c-postgres-loader-multi-table-design.md`).
**RFC:** RFC-0009 §"Schema Application" + §"Mid-run schema change"; RFC-0010 §"Evolution Policy" (`propagate_additive`).

## What this adds

The Postgres loader now applies additive schema changes when a `_cdc.op = "c"` event appears in a CDC batch. Previously the `"c"` arm was a warn-and-skip no-op. Now it:

1. Queries `information_schema.columns` for the target table inside the current transaction.
2. Diffs the batch's data schema against destination columns via `diff_schema`.
3. Applies `ALTER TABLE ADD COLUMN` or `ALTER TABLE ALTER COLUMN TYPE` for additive changes.
4. Returns a non-retriable `anyhow::Error` for destructive changes — transaction rolls back, destination unchanged.

## Inline diffing — no catalog wiring (yet)

RFC-9 says DDL is "governed by RFC-10's policy but with loader-specific mechanics." This phase implements the mechanics inline. The loader unconditionally acts under `propagate_additive` semantics. Catalog wiring (policy evaluation, `applied_to_destination_at`, schema-version chain) is the next phase when the catalog service is wired into the data plane.

## Evolution fires before the row loop

A `RecordBatch` has one schema for all rows. When the connector emits a `"c"` sentinel, the batch is already the widened schema — the `"c"` row is just the signal. So:

```rust
let has_schema_change = (0..batch.num_rows())
    .any(|r| cdc_op_at(batch, r).map(|op| op == "c").unwrap_or(false));
if has_schema_change {
    apply_schema_evolution(&mut *tx, &spec.schema, target_table, &data_schema).await?;
}
```

Data rows before and after the `"c"` sentinel all bind against the updated destination.

## Additive changes (applied)

| Condition | DDL |
|---|---|
| Column in batch not in destination | `ALTER TABLE ... ADD COLUMN IF NOT EXISTS "<n>" <type>` — always nullable |
| Batch type wider than destination | `ALTER TABLE ... ALTER COLUMN "<n>" TYPE <new> USING "<n>"::<new>` |

## Destructive changes (pause pipeline)

| Variant | Condition | RFC-10 |
|---|---|---|
| `DropColumn` | Destination has column, batch doesn't | `field_removed` |
| `NarrowType` | Batch type narrower (e.g., BIGINT→INTEGER) | `field_type_narrowed` |
| `IncompatibleType` | No widening path exists | `field_type_incompatible` |

Pause = `bail!("destructive schema change detected ... operator action required")`. Temporal retries; all retries fail identically until the operator intervenes. Transaction rollback ensures no partial writes.

## Type relations

| dest (`information_schema`) | batch (`pg_column_type`) | relation |
|---|---|---|
| `smallint` | `integer` / `bigint` | Widening |
| `integer` | `bigint` | Widening |
| `real` | `double precision` | Widening |
| `varchar` / `character varying` / `character` | `text` | Widening |
| `bigint` | `integer` / `smallint` | Narrowing |
| `integer` | `smallint` | Narrowing |
| `double precision` | `real` | Narrowing |
| same | same | Same (no delta) |
| anything else | anything else | Incompatible |

## Key invariants

- **Idempotency**: `ADD COLUMN IF NOT EXISTS` safe on retry. `ALTER COLUMN TYPE` no-ops when already at target type.
- **Atomicity**: DDL + data writes + idempotency log insert all in one `sqlx` transaction. Any failure rolls back everything.
- **No partial writes on destructive error**: `apply_schema_evolution` returns `Err` before any DDL runs.
- **New columns always nullable**: existing rows survive without a `DEFAULT`.

## Tests

- **Unit** (in `crates/worker/src/loaders/postgres.rs::tests`):
  - `DestCol` equality (2)
  - `diff_schema` — add / widen / drop / no-op / narrow (5)
  - `add_column_ddl` — nullable output, forced-nullable (2)
  - `alter_column_type_ddl` — USING cast (1)
  - `schema_delta_is_destructive_classification` (1)
  — 11 new unit tests total; worker total: 42 → 53 (still building per phase).
- **Integration** (in `tests/integration/tests/postgres_loader_schema_evolution.rs`):
  - Additive new column mid-stream
  - Schema-change-only batch
  - Destructive drop column
  - Destructive type narrowing
  — 4 new integration tests; all 15 prior PG loader integration tests still green.

## Limitations (deferred)

- **Column renames** — treated as `DropColumn + AddColumn` → pauses. Operator must reconcile via RFC-10 rename heuristic.
- **PK-type-change guard** — `WidenType` on a PK column is currently applied; should pause for lock-contention safety on large tables.
- **Catalog wiring** — no `applied_to_destination_at` updates, no policy evaluation.
- **`COPY FROM STDIN` fast path** — when added, must call `apply_schema_evolution` before its copy loop.

## Follow-ups (priority order)

1. PK-type-change guard.
2. Catalog wiring (policy evaluation + version chain).
3. `COPY FROM STDIN` perf path.
4. Secret-ref URLs (RFC-11).
5. Dead-letter routing for CDC failures.
6. Audit-log destination mode.
