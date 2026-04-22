# RFC 0003: Data Interchange Format and Type System

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0001 (Platform Vision), RFC 0002 (Core Architecture)

## Summary

This RFC specifies the format in which data moves between every component of the platform: from connector into worker, from worker into staging, from staging into loader, and across the wasm boundary into user transformations. We commit to **Apache Arrow** as the in-memory format, **Arrow IPC** as the on-wire and on-disk format, and we define a **platform type system** that sits above Arrow and mediates between source-native types, destination-native types, and user-visible types.

This is the most load-bearing technical decision in the platform. Getting it right means every component speaks the same language and zero-copy is the default. Getting it wrong means every integration point needs a serialization layer and we leave performance (and dollars) on the floor.

## Motivation

A data platform's internal format determines:

1. **Performance ceiling.** If the format requires row-by-row conversion at every boundary, no amount of Rust or wasm optimization saves you. Columnar, zero-copy-capable formats are the only option for a cost-competitive platform.
2. **Language reach.** User code runs in wasm (many source languages). Connectors may be authored in Python, JS, Go, Rust. The format must have first-class support in every language we want in our ecosystem.
3. **Type fidelity.** Source systems have rich types (Postgres arrays, JSON columns, timezone-aware timestamps, decimals of arbitrary precision, geometry). Destinations have their own type systems. We need a canonical intermediate that preserves enough information to round-trip correctly.
4. **Schema evolution.** Sources add columns, change types, rename fields. The format must make these changes representable and the semantics must be defined.

Arrow is the only format that credibly addresses all four. It is not a controversial choice — Databricks (Photon), Snowflake (internal), DuckDB, Polars, DataFusion, ClickHouse Connect, BigQuery Storage API, Snowflake's Python Connector, and Fivetran's newer connectors all use Arrow internally. The thing that is worth designing carefully is not "should we use Arrow" (yes) but **the type system we layer on top of Arrow** and **the rules for how we map between source types, Arrow types, and destination types**.

## Non-Goals

- This RFC does not specify the wire protocol for cross-service communication (gRPC framing, etc.). Arrow Flight is a candidate and is discussed briefly but not mandated here.
- This RFC does not specify the catalog schema format (how schemas are stored in the Catalog Service). That's RFC 10.
- This RFC does not specify compression algorithms or codecs in detail. We note the options and defer final selection.
- This RFC does not cover streaming semantics (watermarks, event time). Streaming is deferred to RFC 23; this RFC addresses the batch and micro-batch cases.

## Commitments

### In-memory format: Apache Arrow

Every piece of data in motion inside a worker is represented as Arrow `RecordBatch`es. Not rows, not JSON, not Parquet, not custom structs. Arrow.

Consequences:

- The connector runtime produces `RecordBatch`es (either natively, for Rust connectors, or via the host API, for wasm connectors).
- The wasm host API for user transformations passes `RecordBatch`es across the boundary (details in RFC 5).
- The loader runtime consumes `RecordBatch`es and handles destination-specific conversion (e.g., Arrow → Snowflake's binary PUT format, Arrow → BigQuery Storage Write API, Arrow → Iceberg Parquet).

Rust implementation: `arrow-rs` (the official Apache implementation) is the reference. We explicitly do **not** use `arrow2` — despite its strengths, the ecosystem has consolidated on `arrow-rs` (particularly after DataFusion's migration), and tracking a fork introduces long-term maintenance cost we don't need.

### On-wire / on-disk format: Arrow IPC

When data must cross a process or disk boundary — staging files in object storage, data plane to destination, debugging exports — we use **Arrow IPC** (the Feather v2 format). Reasons:

- Zero-copy read: a downstream worker can `mmap` a staging file and access the record batches without parsing.
- Streaming-capable: IPC has a streaming variant (sequence of record batches with a shared schema) that matches our micro-batch execution model.
- Compression-friendly: per-buffer LZ4 or ZSTD compression is built in; we expect to default to ZSTD at level 3.
- Cross-language: every Arrow binding reads IPC.

**Why not Parquet for staging?** Parquet is optimized for long-term storage and query-time scan; it has row groups, statistics, dictionary encoding, and heavy metadata overhead. For staging — which is short-lived (minutes to hours), write-once-read-once, and doesn't need predicate pushdown — IPC is strictly better: faster to write, faster to read, smaller hot-path overhead. Parquet is the right choice when we *deliver* to a destination that wants Parquet (Iceberg, Delta, raw S3 drops). For transient staging, IPC wins.

### Arrow Flight: adopted for service-to-service, optional otherwise

For the specific case of data plane worker → loader → destination (where the destination speaks Arrow Flight, as BigQuery Storage Write, Dremio, and InfluxDB 3 do), we use Arrow Flight directly. For everything else, plain IPC over HTTPS/S3 is sufficient and simpler.

Arrow Flight SQL is not adopted. Our platform is not a query engine in RFC 1's scope; Flight SQL's value proposition is for query-serving systems, not ETL.

## The Platform Type System

Arrow's type system is expressive but not sufficient. Arrow distinguishes `Int32` from `Int64` but doesn't know that a Postgres `numeric(38,10)` is semantically different from a Snowflake `NUMBER(38,10)` (it isn't, actually — but it is different from a BigQuery `NUMERIC`, which has different precision/scale rules). We need a **platform type** that carries enough source-side semantic information to drive correct destination-side representation decisions.

The design pattern: a platform type is a **pair** of `(Arrow logical type, semantic annotation)`. The Arrow type is how data is stored in memory. The semantic annotation is metadata attached to the Arrow field (via Arrow's built-in `metadata` map on `Field`) that tells the loader how to represent this column in the destination.

### Platform type catalog

We define the following platform types. Each has a canonical Arrow representation, a set of acceptable source-side originations, and rules for destination projection.

**Exact integers.** `int8`, `int16`, `int32`, `int64`, and `uint8`, `uint16`, `uint32`, `uint64`. Arrow native. No semantic ambiguity.

**Floats.** `float32`, `float64`. Arrow native. We do **not** introduce "float16" as a platform type; destinations that support it are too rare to matter.

**Decimal.** Arrow `Decimal128(precision, scale)` and `Decimal256(precision, scale)`. Platform annotation carries `source_precision` and `source_scale` separately from the Arrow encoding, because Arrow Decimal has max precision 76 (Decimal256) and some sources (Oracle NUMBER unbounded) exceed this. When source exceeds Arrow max, we fall back to string with a `decimal_string` semantic annotation and the loader re-parses on delivery.

**String.** Arrow `LargeUtf8` by default (not `Utf8`), because 2GB strings from bad source data do exist and causing overflow at the extract boundary is worse than paying a slight overhead on offsets. Semantic annotations: `json` (string is JSON-shaped, destination may represent as JSON type if supported), `xml`, `uuid`, `enum(values...)`.

**Binary.** Arrow `LargeBinary`. Semantic annotations: `bytes` (opaque), `image`, `compressed(codec)`.

**Boolean.** Arrow `Boolean`. Three-valued logic (true, false, null) is the default; sources that lack null support are handled at extract time.

**Timestamp.** Arrow `Timestamp(unit, tz)`. The timezone field is **not optional** in the platform type. Timestamps are either:
- `Timestamp(Microsecond, "UTC")` — "instant in time," the default for log-based CDC and most event-shaped data.
- `Timestamp(Microsecond, None)` — "wall clock," local time with no zone. Semantic annotation: `naive_timestamp`. Used when the source explicitly has no timezone (Postgres `timestamp without time zone`, MySQL `DATETIME`).
- `Timestamp(Microsecond, "<IANA zone>")` — timezone-aware. Semantic annotation preserves the original zone.

We standardize on **microsecond resolution** because it is the intersection of what most destinations support. Nanosecond sources (Postgres with high-precision columns) are truncated with a loader warning emitted; millisecond sources are widened.

**Date.** Arrow `Date32` (days since epoch). No time component, no timezone. `Date64` is rejected — it exists for Java interop and is otherwise wasteful.

**Time of day.** Arrow `Time64(Microsecond)`. Rarely round-trips cleanly; many destinations coerce to string.

**Interval.** Arrow `Interval(MonthDayNano)` — the newest Arrow interval type, which is the only one rich enough to represent Postgres intervals correctly. Destination support varies; loaders may fall back to string.

**Array / list.** Arrow `LargeList<T>` where T is any platform type. Nested lists supported. Semantic annotation distinguishes `array` (ordered, duplicates allowed — Postgres array, BigQuery ARRAY) from `set` (unordered, deduplicated — rare in sources but representable).

**Struct.** Arrow `Struct(fields)`. Fields are platform-typed. Represents Postgres composite types, BigQuery STRUCT, Avro records.

**Map.** Arrow `Map<K, V>` where K is string or integer. Represents Postgres `hstore`, JSON objects with homogeneous values.

**JSON (first-class).** Not a new Arrow type — represented as `LargeUtf8` with semantic annotation `json`. When source is known-JSON (Postgres JSONB, MongoDB documents, typed JSON columns), we preserve the annotation so destinations with native JSON types (Snowflake VARIANT, BigQuery JSON, Postgres JSONB) can use them instead of string. When the destination lacks JSON, we deliver as string.

**Geometry / geography.** Semantic annotations over `LargeBinary` (WKB) or `LargeUtf8` (WKT). We do not invent a platform type; we preserve the source's representation and let loaders handle it.

**Union.** Arrow `Union` type. Rare in sources. Supported but deprioritized; loaders may represent as JSON.

**Null.** Every platform type is nullable by default. Non-nullability is a constraint in the schema, not a type property.

### What we do *not* support as first-class platform types

- **Vector / embedding types.** Not in scope for the ingestion wedge. Can be transported as `FixedSizeList<Float32>` when needed. Revisit when we expand toward ML.
- **Graph types.** Out of scope.
- **Spatial index types** (R-tree encodings, etc.). Out of scope.
- **Database-specific weird types** (Postgres `tsvector`, `ltree`, `xml` trees, Oracle `RAW`, etc.). Represented as binary or string with source annotation, not promoted to platform types.

The principle: **the platform type set is small and stable**. A type makes it in only when it's supported by at least 3 of our top-10 destinations. Everything else rides on top as semantic annotation.

## Type Mapping Rules

The three-layer mapping — source type → platform type → destination type — is the contract of every connector and every loader.

### Source-to-platform (connector responsibility)

Each connector defines a **static type mapping table** from source-native types to platform types, plus inference rules for dynamically-typed sources (MongoDB, JSON APIs). Requirements:

- **Total.** Every source type that a connector might encounter must map to *some* platform type. If unknown, the mapping is source type → `LargeUtf8` with annotation `unmapped_source_type(<name>)`, and a warning is emitted.
- **Stable.** A given source type always maps to the same platform type across connector versions, unless a breaking mapping change is version-gated.
- **Documented.** The mapping table is part of the connector's published schema documentation, not hidden in code.

For dynamically-typed sources, type inference uses a bounded-sample strategy: examine the first N records (default 10,000), build a merged schema, and re-promote types if the sample was unrepresentative (e.g., first batch looked int32, later batch sees int64 — promote). Promotion semantics are in RFC 10 (schema evolution).

### Platform-to-destination (loader responsibility)

Each loader defines a **platform-to-destination mapping**, with explicit behavior for types the destination does not natively support. Three possible outcomes:

1. **Native.** Platform type has a direct destination equivalent. Used.
2. **Widened.** Platform type maps to a wider destination type (e.g., `Decimal128(10,2)` → Snowflake `NUMBER(38,10)`). Loader records the widening in the run metadata so lineage is accurate.
3. **Projected.** Platform type has no destination equivalent; loader projects to string or binary with a documented encoding. The loader must record this in the run metadata, and the catalog records the projection as part of the destination schema for that table.

Loaders never silently drop data. A type that cannot be represented is an error unless explicitly configured to project. The configuration to allow projection is per-pipeline, not global.

### Changing type mappings

Changing the mapping for a given (source, target) pair is a **breaking change** to a connector or loader. It is version-gated. Pipelines pin to a connector/loader version (RFC 6, RFC 9) specifically so that mapping changes don't silently change customer data.

## Schema Representation

A schema is an Arrow `Schema` with additional metadata on every `Field`:

```
Field {
  name: string,
  data_type: Arrow DataType,
  nullable: bool,
  metadata: {
    "platform.type": "<platform type name>",
    "platform.source_type": "<source-native type>",
    "platform.source_precision": "<when applicable>",
    "platform.source_scale": "<when applicable>",
    "platform.is_primary_key": "true" | (absent),
    "platform.is_cdc_metadata": "true" | (absent),
    "platform.semantic": "<annotation, e.g. 'json', 'uuid'>",
    // additional metadata per connector
  }
}
```

All platform metadata keys are namespaced under `platform.*`. Connectors can add their own metadata under `connector.<name>.*`. Loaders read both namespaces.

Schemas are carried with every record batch (Arrow IPC does this natively). Workers, loaders, and wasm transformations receive batches with schemas; they never have to consult an external schema registry mid-flight. The Catalog Service holds schemas for cross-run reference but is not on the hot path.

## CDC Metadata

Change-data-capture streams carry per-row metadata that is not "data" in the usual sense but must flow through the pipeline: operation type (insert/update/delete), source log position, commit timestamp, transaction ID.

We represent CDC metadata as **reserved columns** with a platform-namespaced prefix:

- `_cdc.op` — `"i"`, `"u"`, `"d"`, `"t"` (truncate), `"s"` (snapshot).
- `_cdc.lsn` — source log position as `LargeUtf8` (varies by source: Postgres LSN, MySQL binlog coordinates, Oracle SCN).
- `_cdc.commit_ts` — `Timestamp(Microsecond, "UTC")`.
- `_cdc.txid` — `LargeUtf8`, optional.
- `_cdc.before` — `Struct`, optional, containing the pre-image row for updates when the source provides it.

These columns are always present in CDC streams and always absent in snapshot/batch streams. The `platform.is_cdc_metadata=true` annotation marks them so transformations and loaders can distinguish them from user columns.

Why reserved columns rather than a sidecar structure: it keeps CDC data in the same RecordBatch as the row data (zero overhead to keep them aligned), and it lets user transformations reference CDC fields if they want (e.g., to implement custom CDC handling). The prefix `_cdc.` is reserved and connectors cannot emit user columns with names in that namespace.

## Nullability and Defaults

Every platform type is nullable in the Arrow representation. Source-side non-null constraints are recorded in field metadata (`platform.source_nullable=false`) but do not change the Arrow type. This is deliberate: it means a connector that encounters a null where the source claimed non-null can still emit the row (with the anomaly logged) rather than crashing the batch.

Default values from source DDL are preserved in field metadata (`platform.source_default=<value>`) but are **not** applied by the platform. Defaults are a source-side and destination-side concern; the intermediate never manufactures data.

## Compression and Encoding

For IPC files in staging:

- **Default codec: ZSTD level 3.** Good compression ratio, fast decompression, well-supported. We will tune the level based on benchmarks but 3 is the starting point.
- **Dictionary encoding:** Arrow supports dictionary-encoded arrays natively. Connectors should emit dictionary encoding for low-cardinality string columns. This is a connector-side optimization, not a platform requirement.
- **Row group equivalent:** IPC doesn't have row groups, but we control batch size. Default target: 128MB per record batch (decompressed). Tunable per pipeline.

For cross-worker / cross-plane wire transfers:

- **Default: LZ4.** Faster than ZSTD at the cost of ratio. Wire transfers are dominated by CPU-per-byte, not bandwidth, when both endpoints are modern.

## Performance Targets

These targets drive the format's design. If we can't hit them, something is wrong.

- **Serialization overhead across the wasm boundary:** <5% of total activity CPU time for transformations operating on representative batches (1MB–128MB). Details in RFC 5.
- **Staging write throughput:** >=500 MB/s sustained per worker for IPC+ZSTD-3 to S3 (assuming sufficient S3 partitioning).
- **Staging read throughput:** >=1 GB/s sustained per worker, memory-mapped.
- **Schema-only parse time:** <1 ms for a schema with 200 fields. Workers consult schemas frequently; this must be trivial.

## Alternatives Considered

**Parquet throughout.** Rejected. Parquet's metadata overhead and row-group orientation hurt us in the staging hot path. We still *produce* Parquet for destinations that want it; we don't use it internally.

**Avro as the interchange.** Rejected. Row-oriented, not columnar. Strong schema-evolution story but the performance gap vs. Arrow on columnar operations is too large.

**Protobuf / Cap'n Proto.** Rejected. Designed for message-passing, not for bulk tabular data. No columnar representation, no wide ecosystem for data types (decimal, timestamp-with-timezone).

**A custom format.** Rejected without hesitation. The entire thesis of this RFC is that the ecosystem around Arrow is the platform's leverage. Inventing our own format gives up every cross-language integration and saddles us with maintaining it forever.

**JSON as a fallback for weird types.** Considered and partially adopted (it is our fallback for JSON-shaped data). But not as a general interchange — the cost of round-tripping every row through JSON is prohibitive.

**Two separate type systems (one for batch, one for CDC).** Rejected. A single platform type system with CDC metadata as reserved columns is simpler and loses nothing.

## Open Questions

1. **Large row handling.** A single row bigger than the batch target (e.g., a 200MB JSON document, a CDC `before` image of a huge row). Policy TBD — likely a per-row spillover to a sidecar blob referenced by URL, which requires design.
2. **Schema fingerprinting.** We need a stable hash of a schema for caching and comparison. Arrow has no canonical hash; we'll define one. Defer to RFC 10.
3. **Batch size auto-tuning.** 128MB is a reasonable default, but optimal varies by downstream. Auto-tuning based on observed loader throughput is a future optimization.
4. **Arrow Flight for internal cross-worker transfer.** If we ever shard a transformation across workers (unlikely in the near term; we prefer scaling up a single worker), Flight is the obvious choice. Not needed now.
5. **Canonical form for timezone-aware timestamps.** Arrow stores timestamps as UTC offsets with a zone string, but IANA zones can change (DST rule updates). How do we handle historical data emitted before a zone rule changed? Likely: we preserve the zone string and rely on readers having a current tzdb. Document and move on.

## References

- Apache Arrow specification: https://arrow.apache.org/docs/format/Columnar.html
- Arrow IPC format: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
- Arrow Flight: https://arrow.apache.org/docs/format/Flight.html
- arrow-rs: https://github.com/apache/arrow-rs
- Debezium CDC metadata conventions (prior art for `_cdc.*` naming): https://debezium.io/documentation/
- Fivetran's system columns (prior art for reserved metadata columns): Fivetran public docs.
- BigQuery Storage Write API (destination that speaks Arrow natively): Google Cloud docs.

## Decision

**Accepted pending review.** With RFCs 1-3 complete, we have the foundational tier: what we're building, how the components are arranged, and how data flows between them.

RFC 4 begins the Execution tier: Temporal workflow topology and durability model, which is where we commit to specific workflow shapes for pipelines.
