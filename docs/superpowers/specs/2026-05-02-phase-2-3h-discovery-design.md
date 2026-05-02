# Phase II.3.h — WASM Connector Schema Discovery — Design Spec

> **Status:** Draft 2026-05-02. Approved by agent (user delegated all design calls). Predecessors: II.3.e (`2026-05-02-phase-2-3e-cdc-sdk-design.md`, MySQL SDK lift), II.3.f (`2026-05-02-phase-2-3f-postgres-sdk-design.md`, Postgres SDK lift).

## Goal

Replace the hardcoded `id BIGINT, name TEXT` schema in `examples/mysql-cdc-rs` and `examples/postgres-cdc-rs` with a real schema discovery flow. Both example connectors should handle arbitrary tables by querying `information_schema.columns` (or `pg_attribute` for Postgres) at `discover` time, mapping each column to an Arrow `DataType`, and using that schema dynamically in snapshot SELECT projection, streaming JSON decode, and Arrow IPC encoding.

## Non-goals

- **Multi-table CDC.** Each connector still binds one table. Multi-table is II.3.g and requires workflow/catalog/loader changes.
- **Type-coverage parity with native connectors.** The native MySQL CDC handles BLOB/TIME/JSONB; v1 of the SDK discovery covers the common scalar types. Extensions can land in II.3.h.x patches.
- **Schema evolution at runtime.** If the source table schema changes mid-run, the connector observes the change at the next discover (next pipeline run). Live schema-change handling is II.3.x or later.

---

## Architecture overview

```
guest                                          host
─────                                          ────
discover(conn, source)
  ├─ db.open(url)
  ├─ db.query("SELECT column_name, data_type, is_nullable
  │           FROM information_schema.columns
  │           WHERE table_schema=$1 AND table_name=$2
  │           ORDER BY ordinal_position", [...])
  ├─ db.close
  └─ build Arrow schema from column rows
       (id BIGINT NOT NULL → Field::new("id", Int64, false), …)
       append _cdc.op + _cdc.position metadata fields
       emit Arrow IPC schema bytes

read_batch (snapshot phase)
  ├─ … existing flow …
  ├─ build SELECT "<col1>", "<col2>", … FROM tbl WHERE id > $1 LIMIT N
  │   (column list comes from discover-time schema, cached in source-config? no —
  │    re-discover at the top of read_batch since handles don't survive activations)
  ├─ for each row: parse cells positionally to Arrow values

read_batch (streaming phase)
  ├─ db.subscribe-changes(...)
  ├─ for each ChangeEvent: parse row_json["after"]/["before"] positionally to Arrow values
```

Key architectural decisions:

- **Re-discover schema at the top of every `read_batch` call.** Handles don't survive activations; re-discovery is one extra `db.query` per call. Cheap. Avoids serializing schema state in the cursor.
- **Column-name + ordinal both matter.** Pgoutput v1 returns positional values; MySQL binlog Rows iterator + TableMapEvent gives ordinal columns. Snapshot SELECT also produces positional rows. So Arrow schema construction follows discovery order; values are appended positionally at runtime.
- **Type-mapping module per DB family.** Add `examples/{mysql-cdc-rs,postgres-cdc-rs}/src/discover.rs` with `query_columns(handle, schema, table)` returning `Vec<DiscoveredColumn>` and `to_arrow_field(col)` mapping each to `arrow_schema::Field`.

---

## Type mapping coverage (v1)

### Postgres → Arrow

| Postgres type | Arrow DataType | Nullable |
|---|---|---|
| `bigint`, `int8` | `Int64` | per col |
| `integer`, `int4` | `Int32` | per col |
| `smallint`, `int2` | `Int16` | per col |
| `text`, `varchar`, `character varying`, `name` | `Utf8` | per col |
| `boolean`, `bool` | `Boolean` | per col |
| `real`, `float4` | `Float32` | per col |
| `double precision`, `float8` | `Float64` | per col |
| `timestamp without time zone` | `Timestamp(Microsecond, None)` | per col |
| `timestamp with time zone`, `timestamptz` | `Timestamp(Microsecond, Some("UTC"))` | per col |
| `date` | `Date32` | per col |
| any other type | `Utf8` (fallback — text representation) | per col |

### MySQL → Arrow

| MySQL type | Arrow DataType | Nullable |
|---|---|---|
| `bigint` | `Int64` | per col |
| `int`, `mediumint` | `Int32` | per col |
| `smallint` | `Int16` | per col |
| `tinyint` | `Int8` | per col |
| `varchar`, `text`, `char`, `mediumtext`, `longtext`, `tinytext` | `Utf8` | per col |
| `bit(1)` (and tinyint(1)) | `Boolean` | per col |
| `float` | `Float32` | per col |
| `double` | `Float64` | per col |
| `datetime`, `timestamp` | `Timestamp(Microsecond, None)` | per col |
| `date` | `Date32` | per col |
| any other type | `Utf8` (fallback) | per col |

NOT supported in v1 (fall back to Utf8 with a warning logged): `numeric`/`decimal` (variable-precision), `json`/`jsonb`, `bytea`/`blob` (already in II.3.d.7 native MySQL but not in the SDK guest), `uuid`, `interval`, `array`. Listed for honesty; not blockers for the SDK ergonomics demonstration.

---

## Snapshot SELECT projection

Replace the hardcoded:
```sql
SELECT id, name FROM "schema"."table" WHERE id > $1 ORDER BY id LIMIT N
```

with dynamic projection:
```sql
SELECT "col1", "col2", "col3" FROM "schema"."table"
  WHERE "<pk_col>" > $1 ORDER BY "<pk_col>" LIMIT N
```

The `<pk_col>` is discovered separately via:
```sql
SELECT a.attname
  FROM pg_index i
  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
  WHERE i.indrelid = '<schema>.<table>'::regclass AND i.indisprimary
  LIMIT 1
```
(Postgres) or:
```sql
SELECT column_name
  FROM information_schema.key_column_usage
  WHERE table_schema=? AND table_name=? AND constraint_name='PRIMARY'
  ORDER BY ordinal_position LIMIT 1
```
(MySQL).

If the table has a composite primary key, v1 takes only the first column. Tables with no primary key fail at discover time with `ConnectorError::InvalidConfig("table has no primary key; required for snapshot ordering")`.

For typed projection (so the host gets uniform text values), wrap each column in `CAST AS TEXT` (Postgres) / `CAST AS CHAR` (MySQL) only when the host's existing serialization isn't already text-friendly. **Decision:** Skip the CAST entirely. The host's existing `value_to_string` (MySQL) and `try_get::<Option<String>>` (Postgres) already handle the common scalar types. Worst case for unusual types: the cell comes back as `None` instead of a typed string; the connector's `to_arrow_field` returns `Utf8` so the row builder accepts whatever is there.

---

## Row decode (snapshot)

Each cell from `db.query` arrives as `Option<String>`. The connector parses per discovered column type:

```rust
match (column.arrow_type, cell_text) {
    (DataType::Int64, Some(s)) => append_int64(builder, s.parse()?),
    (DataType::Int32, Some(s)) => append_int32(builder, s.parse()?),
    (DataType::Utf8, Some(s)) => append_utf8(builder, s),
    (DataType::Boolean, Some(s)) => append_bool(builder, s == "t" || s == "1" || s.eq_ignore_ascii_case("true")),
    (DataType::Date32, Some(s)) => append_date32(builder, parse_iso_date(s)?),
    (DataType::Timestamp(...), Some(s)) => append_ts(builder, parse_iso_timestamp(s)?),
    (_, None) => builder.append_null(),
}
```

A small `arrow_io::DynamicBatchBuilder` wraps an enum of `arrow_array::builder::*` to dispatch by type. Same code shape as the native connectors but lives guest-side.

---

## Row decode (streaming)

The host emits `row_json` as `{after: [...positional]}` / `{before: [...]}`. Each cell is a JSON value (string for text, string for pgoutput-v1 numeric, etc.). The connector iterates `arr.iter().zip(schema.fields().iter())` and dispatches by field type, mirroring the snapshot path.

For Postgres: pgoutput v1 returns text values for everything, so cells are always JSON strings (or null). Same parsing as the snapshot path.

For MySQL: the host's `mysql_value_to_json` already coerces numerics to JSON numbers. The connector handles both `Value::String("42")` and `Value::Number(42)` for numeric columns:

```rust
fn cell_to_i64(c: &serde_json::Value) -> Option<i64> {
    match c {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        Value::Null => None,
        _ => None,
    }
}
```

---

## Source config evolution

Old (hardcoded schema):
```json
{ "schema": "test", "table": "items" }
```

New (same shape — schema is auto-discovered, no extra config needed):
```json
{ "schema": "test", "table": "items" }
```

The user-facing config doesn't change. That's the point.

---

## File structure

| Path | Action | Responsibility |
|---|---|---|
| `examples/mysql-cdc-rs/src/discover.rs` | **New** | `query_columns`, `query_pk_column`, `to_arrow_field`, MySQL→Arrow type mapping |
| `examples/mysql-cdc-rs/src/arrow_io.rs` | Modify | Replace static `Row` struct + hardcoded schema with `DynamicBatchBuilder` driven by discovered schema |
| `examples/mysql-cdc-rs/src/lib.rs` | Modify | `discover` calls into new module; `read_batch` re-discovers up front |
| `examples/mysql-cdc-rs/src/snapshot.rs` | Modify | Dynamic SELECT projection; row decode by type |
| `examples/mysql-cdc-rs/src/streaming.rs` | Modify | Row decode by type, column-by-column |
| `examples/postgres-cdc-rs/src/discover.rs` | **New** | Same shape, Postgres-specific SQL + type mapping |
| `examples/postgres-cdc-rs/src/arrow_io.rs` | Modify | Same shape as mysql-cdc-rs version (could share but kept per-connector for now) |
| `examples/postgres-cdc-rs/src/{lib,snapshot,streaming}.rs` | Modify | Mirror MySQL changes |
| `tests/integration/tests/mysql_cdc_wasm_e2e.rs` | Modify | Test now uses 4-column items table to exercise type mapping |
| `tests/integration/tests/postgres_cdc_wasm_e2e.rs` | Modify | Same |
| `README.md` | Modify | Bump "Currently:" line |

Sharing discover/arrow_io between examples is tempting but creates a workspace dependency mess (each example is its own workspace). Duplicate files keeps each example self-contained and viewable by SDK consumers.

---

## Build sequence (eight tasks)

1. **mysql-cdc-rs discover module** — column query + PK query + type mapping + tests.
2. **mysql-cdc-rs arrow_io DynamicBatchBuilder** — replace `Row` with type-driven builder.
3. **mysql-cdc-rs snapshot dynamic projection** — discover at top of read_batch, build dynamic SELECT, decode per type.
4. **mysql-cdc-rs streaming dynamic decode** — decode JSON cells per discovered field type.
5. **postgres-cdc-rs discover module** — Postgres column + PK queries, type mapping.
6. **postgres-cdc-rs arrow_io + snapshot + streaming** — bundled (same shape as mysql; mostly copy-paste with tweaked type mapping).
7. **e2e tests updated** — 4-column tables (`id BIGINT, name TEXT, active BOOL, created TIMESTAMP`) for both connectors.
8. **README + final verification.**

Each task ends with a commit `phase-2-3h-N: <description>` matching the II.3.e/f cadence.

---

## Open concerns

1. **Cursor column hardcoded to PK.** Snapshot orders by primary key. For tables with non-monotonic PKs (e.g. UUID PKs), this loops forever. v1 says "use BIGINT or INT primary keys"; documenting the constraint, not enforcing it. UUID/sortable-PK is a future patch.

2. **Re-discover on every read_batch.** Cheap (~1ms over a warm conn) but does mean a network roundtrip per snapshot chunk. Mitigation: future SDK could expose a host-side schema cache keyed by `(conn_url, schema, table, last_seen_modified)`. Not in v1.

3. **Type-mapping fallback to Utf8.** Tables with `numeric`/`json` columns will load successfully but emit those columns as text strings, not their typed Arrow representation. Behavior matches what the user gets if they `CAST AS TEXT` natively. We log a warning at discover time.

4. **Date/timestamp parsing is locale-sensitive.** `2026-05-02 13:14:15.999999` (MySQL DATETIME) vs `2026-05-02 13:14:15.999999+00` (Postgres TIMESTAMPTZ). The connector parses the format produced by the host's existing `value_to_string` / sqlx text representation; we test both.

5. **`information_schema.columns` access in some Postgres setups requires permissions.** If the role can't read `information_schema`, discover fails with a clear error. Documenting; not solving.

---

## Acceptance criteria

- Both `mysql-cdc-rs` and `postgres-cdc-rs` compile to `wasm32-wasip2` and pass their unit tests (≥6 each — type mapping coverage + PK extraction + dynamic builder).
- `cargo test -p worker --lib` clean.
- e2e tests use a 4-column table and assert the parquet output schema matches the discovered shape (BIGINT, Utf8, Boolean, Timestamp(Microsecond, _) for the data columns plus the existing `_cdc.op`/`_cdc.position`).
- README "Currently:" line updated.
