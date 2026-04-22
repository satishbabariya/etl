# RFC 0009: Destination Loader Protocol and Idempotency

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0003 (Data Interchange), RFC 0004 (Temporal Topology), RFC 0007 (Incremental Sync), RFC 0008 (CDC Architecture)

## Summary

This RFC specifies how staged Arrow batches get delivered to destinations — Snowflake, BigQuery, Redshift, Postgres, Iceberg, Delta, S3 — with the idempotency guarantees that make at-least-once connector emissions produce exactly-once destination state. It defines the loader protocol (trait), the per-destination delivery strategies, the merge / append / swap patterns, idempotency keys, transactional commit semantics where available, schema application, and the operational characteristics each destination imposes.

Loaders are where the platform's correctness claims either hold or fail. A connector can be perfect, but if the loader mishandles a retry, produces duplicate rows, or applies events out of order, the destination is wrong — and that's what the customer sees.

## Motivation

The connector protocol (RFC 6) and the incremental sync semantics (RFC 7) establish that the data arriving at the loader has specific properties: batches with PK metadata, `_cdc.*` columns when applicable, a schema, and a commit ordering. The loader's job is to deliver this to a destination with strong semantics, given that:

1. **Retries happen.** Temporal retries activities. The same batch may be delivered to the loader multiple times. Every loader operation must be idempotent or no-op on retry.
2. **Destinations have wildly different capabilities.** Snowflake has transactions and MERGE. BigQuery has Storage Write API with streams. S3 has no transactions at all — just object visibility. Iceberg has snapshot isolation. Postgres has full transactions. We cannot pretend they're the same.
3. **Correctness differs from performance.** A correct-but-slow loader fails the economic wedge. A fast-but-subtly-wrong loader fails the product. We design for both.
4. **Atomicity at the destination matters operationally.** A half-loaded batch visible to downstream consumers produces reports that are wrong for minutes. We commit atomically where possible and isolate partial state where not.

Destinations are the "one Anthropic reviewer said this is where you either have a product or a demo" part of the platform. We invest.

## Non-Goals

- This RFC does not list every destination we will ever support. It covers the launch set — Snowflake, BigQuery, Redshift, Postgres, Iceberg (on S3/GCS/Azure), Delta Lake, and raw S3/Parquet — with enough generality that adding new destinations is a protocol implementation, not a protocol redesign.
- This RFC does not specify the schema evolution policy at the destination. That's RFC 10. Loaders implement what RFC 10 decides.
- This RFC does not cover reverse ETL (warehouse-to-SaaS). Ruled out in RFC 1.
- This RFC does not cover the transformation layer. Transformations (RFC 12) run before loaders; loaders consume the final post-transformation batches.
- This RFC does not cover real-time streaming inserts into destinations as a first-class product. Micro-batch (seconds to minutes latency) is the bar we design for.

## Loaders Are First-Party Only

**Loaders are Rust-native, not wasm.** This is a deliberate exception to RFC 6's "connectors uniformly wasm" rule.

Reasons:

1. **Destination correctness is too load-bearing to sandbox.** A bug in a wasm loader that silently produces incorrect MERGEs is catastrophic; we need destination code reviewed, tested, and versioned under our direct control.
2. **Destination SDKs are Rust-native or C where they exist.** The Snowflake Rust driver, the BigQuery Storage Write client, the Arrow Flight implementation — these are native code. Wrapping them through a wasm boundary adds serialization overhead and gains nothing.
3. **Loaders are a small, finite set.** We expect <30 loaders total across the platform's lifetime, vs. hundreds of connectors. The economics favor direct investment per loader rather than a framework.
4. **Performance matters at the loader layer more than anywhere else.** Destination delivery is often the bottleneck of a pipeline. Every percent of overhead compounds against our unit-economics wedge.

We do not offer user-authored loaders at launch. If a customer needs a destination we don't support, they work with us to add it as a first-party integration. This is the same stance every competitive platform takes (Fivetran, Airbyte Cloud-tier, Databricks Autoloader) and we don't invent a worse wheel.

A consequence: adding a new destination is a major engineering project, not a community contribution. We price this in.

## The Loader Trait

Every loader implements a common Rust trait. The trait is narrow — most of the complexity lives behind each implementation, not in the protocol.

```rust
// Sketch; real trait has more type detail.

pub trait Loader: Send + Sync {
    type Config: DeserializeOwned + Serialize;
    type LoadReceipt: Serialize + DeserializeOwned;

    // Called at pipeline setup. Validates config, tests connectivity,
    // verifies required capabilities (e.g., destination supports MERGE,
    // credentials have sufficient privileges).
    async fn validate(&self, config: &Self::Config) -> Result<(), LoaderError>;

    // Called before a run begins. Loader prepares destination: ensures
    // target tables exist, applies any pending schema changes, sets up
    // temporary objects needed for this run.
    async fn prepare_run(
        &self,
        config: &Self::Config,
        plan: &RunPlan,
    ) -> Result<RunContext, LoaderError>;

    // The hot path. Consumes a batch reference (pointer to staging)
    // and delivers it. Idempotent by load_id — re-invocation with the
    // same load_id is a no-op or a safe reconciliation.
    async fn load(
        &self,
        ctx: &RunContext,
        batch: BatchRef,
        load_id: LoadId,
    ) -> Result<Self::LoadReceipt, LoaderError>;

    // Called when all batches for the run have been loaded.
    // Loader makes the run's data visible atomically where possible.
    async fn commit_run(
        &self,
        ctx: &RunContext,
        receipts: Vec<Self::LoadReceipt>,
    ) -> Result<CommitReceipt, LoaderError>;

    // Called if the run fails after partial load and the operator
    // requests rollback. Some destinations support this (drop staging
    // tables); others don't (data already visible).
    async fn abort_run(
        &self,
        ctx: &RunContext,
    ) -> Result<(), LoaderError>;

    // Reports destination capabilities so the workflow planner
    // picks a compatible strategy.
    fn capabilities(&self) -> LoaderCapabilities;
}

pub struct LoaderCapabilities {
    pub supports_merge: bool,             // destination-side upsert
    pub supports_transactions: bool,      // atomic multi-statement commit
    pub supports_staging_swap: bool,      // load-then-swap-visible pattern
    pub supports_schema_evolution: SchemaEvolutionLevel,
    pub supports_soft_delete: bool,       // delete via tombstone marker
    pub supports_partitioning: bool,      // partitioned target tables
    pub max_batch_size_bytes: u64,
    pub optimal_batch_size_bytes: u64,
    pub preferred_file_format: FileFormat, // IPC, Parquet, CSV, etc.
}
```

The loader is instantiated per pipeline and lives for the pipeline's lifetime inside the worker. It maintains connections, prepared statements, and per-destination state. Workers pool loader instances per (destination config, tenant) tuple.

## Load Identity and Idempotency

The core discipline of this RFC.

### `LoadId`

Every batch delivery has a `LoadId` — a deterministic identifier that is stable across retries of the same logical load. Definition:

```
LoadId = hash(pipeline_id, run_id, stream, batch_sequence_number)
```

Where `batch_sequence_number` is the connector-assigned monotonically-increasing number of the batch within the run. The same batch retried produces the same `LoadId`.

`LoadId` is passed to `load()` by the workflow activity, derived from workflow state. Temporal retries pass the same `LoadId`; loader uses it for destination-side deduplication.

### Idempotency strategies by destination capability

Every loader must achieve idempotency. The mechanism varies:

**Destinations with native idempotency keys** (BigQuery Storage Write with stream-offset model, Snowflake's `LOAD HISTORY` with `FILES=(...)` clauses): pass `LoadId` as the destination's idempotency mechanism. The destination handles dedup.

**Destinations with MERGE and staging tables**: load into a per-run staging table with `LoadId` as a primary-key column in the staging. On retry, `INSERT ... ON CONFLICT DO NOTHING` (or equivalent) ensures the row appears once in staging. The MERGE into the final table is itself idempotent (same rows with same PKs produce the same final state).

**Destinations with object storage (S3, GCS, raw Parquet)**: write to a deterministic object path including `LoadId`. Retry is a no-op on a successful write or an overwrite of the same content. Object storage is single-writer-wins on concurrent writes; we do not have concurrent retries of the same `LoadId` because Temporal serializes retries.

**Destinations without idempotency primitives**: the loader maintains an idempotency log — a destination-side table mapping `LoadId` to "loaded" status. Before loading, check the log; after loading, insert into the log atomically with the data. Postgres supports this well; other destinations vary.

Every loader's implementation notes document which strategy it uses.

### Idempotency window

`LoadId` must uniquely identify a delivery for the destination's retention window. In practice: `LoadId`s include the run_id which is time-scoped, so collisions across runs are impossible. Within a run, `(stream, batch_sequence_number)` uniquely identifies a batch. Collisions are not a concern.

### What idempotency does not cover

Idempotency handles "same data delivered twice." It does not handle:

- **Out-of-order delivery.** If batch 5 is loaded before batch 3 (somehow), CDC ordering is broken and the destination is wrong. Activity retries are sequential per stream, so this doesn't happen in practice — but the loader must not buffer batches in a way that permits reordering.
- **Different data with the same conceptual identity.** If a connector bug emits a row with the wrong cursor value, the loader correctly delivers the wrong data. Idempotency is about delivery reliability, not source correctness.

## Delivery Patterns

Four primary patterns, selected per destination × sync-mode combination.

### Pattern 1: Direct Append (full-refresh with swap, or append-only incremental)

Straightforward: write batches to the destination table. Use when:

- Full-refresh mode with stage-and-swap (below).
- Incremental append-only streams (no PK).

Pseudo-flow:
1. `prepare_run`: create per-run staging table (or equivalent).
2. `load` per batch: append batch to staging.
3. `commit_run`: atomic visibility — either swap staging into place (full refresh) or merge into target (append-only incremental).

### Pattern 2: Merge on Commit (incremental with PK)

Used for cursor-based incremental or snapshot phase of CDC, when the stream has a primary key:

1. `prepare_run`: create per-run staging table matching target schema + internal `LoadId` column.
2. `load` per batch: `INSERT INTO staging VALUES ...`, using `LoadId`-based dedup to make inserts idempotent.
3. `commit_run`: execute a destination-native MERGE statement:
   ```sql
   MERGE INTO target t USING staging s ON t.pk = s.pk
   WHEN MATCHED THEN UPDATE SET t.col1 = s.col1, ...
   WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
   ```
4. Cleanup: drop staging after commit succeeds.

Tie-breaker when staging contains duplicate PKs (possible from connector re-emission at overlap boundaries): keep the row with the highest `_cursor_value` or `_cdc.lsn`. This is done in the MERGE's `USING` clause via a windowed `ROW_NUMBER()` or equivalent destination primitive.

### Pattern 3: Apply Change Stream (CDC streaming mode)

Used for steady-state CDC. Each batch contains ordered events (inserts, updates, deletes) with `_cdc.*` metadata:

1. `prepare_run`: for streaming CDC, the run is continuous; `prepare_run` is called at each `continue-as-new` boundary and is mostly a no-op.
2. `load` per batch:
   - Partition batch by event type (i/u/d).
   - Stage the batch.
   - Apply in order: deletes first (for rows that were inserted and deleted in the same batch), then upserts.
   - For destinations supporting MERGE: a single MERGE statement with WHEN MATCHED THEN DELETE / UPDATE and WHEN NOT MATCHED THEN INSERT, driven by `_cdc.op`.
3. `commit_run`: minimal — the batch was applied in `load`. Commit just records the LSN/position advancement.

CDC streaming does not use long-lived staging tables; it operates more like continuous micro-merges. Per-batch transaction scope is ideal for destinations that support it.

### Pattern 4: Append-Only Event Log (audit-log destinations)

For destinations that retain full history (Iceberg tables configured as append-only, raw Parquet event stores, Delta Lake in append mode):

1. Every row — including deletes and updates — is appended as a new event with `_cdc.*` metadata intact.
2. No MERGE. No overwriting.
3. Consumers reconstruct current-state via windowed queries if they want it.

This is the cheapest pattern and is used when the destination semantics match (event-sourced analytics, audit tables, append-only lakes). It is explicitly chosen at pipeline configuration; the default is merge semantics where the destination supports them.

## Per-Destination Specifics

Each supported destination has its own loader implementation. Summaries:

### Snowflake

**Capabilities**: full SQL MERGE, transactions, schema evolution (ALTER TABLE), SNAPSHOT isolation. Native support for VARIANT/JSON types. Staging via internal stages or external stages (S3).

**Strategy**:
- Data arrives at internal stage via `PUT` from staging object storage.
- `COPY INTO staging_table` loads from stage.
- MERGE from staging to target.
- Swap patterns use `ALTER TABLE ... RENAME` which is metadata-only (instant).

**Notes**:
- `LOAD HISTORY` with `FILES=` provides native idempotency for the COPY step.
- Warehouse auto-suspend: loader resumes warehouse on use; customers should size warehouses for our load pattern. Document this.
- Variant / JSON: semantic annotation `json` from RFC 3 maps to Snowflake VARIANT.

### BigQuery

**Capabilities**: Storage Write API (the modern streaming ingestion), MERGE DML, schema evolution, partitioning. Does not support traditional transactions across multiple DML statements.

**Strategy**:
- Storage Write API streams for CDC and append-only loads. Exactly-once via stream-offset model — native idempotency.
- For merge-style loads: write to staging table via Storage Write, then MERGE.
- Commit uses `finalizeWriteStream` + `batchCommitWriteStreams` for atomic visibility across streams.

**Notes**:
- BigQuery charges per byte processed on MERGE. Large staging tables into small partitions: expensive. Loader uses clustering keys and partition filters to minimize MERGE cost.
- No real transactions: commit is per-stream-batch. Compensating aborts (drop staging table) work but cannot roll back a committed MERGE.

### Redshift

**Capabilities**: SQL MERGE (as of recent versions — older versions use INSERT/UPDATE pattern), transactions, COPY from S3.

**Strategy**:
- Data lands in S3 via staging.
- `COPY` from S3 into staging table.
- MERGE (or equivalent) into target.
- Stage-and-swap for full refresh uses `ALTER TABLE ... RENAME` within a transaction.

**Notes**:
- Redshift's concurrency model penalizes many small transactions; we batch commits.
- Distribution key / sort key matter significantly for MERGE performance. Loader respects destination-defined keys; does not reassign.

### Postgres (as destination)

**Capabilities**: full transactional SQL, ON CONFLICT DO UPDATE for upsert, foreign data wrappers for direct S3 read (optional).

**Strategy**:
- `COPY FROM STDIN` (streaming) or `COPY FROM PROGRAM` (reading from S3) into staging table.
- `INSERT INTO target SELECT ... FROM staging ON CONFLICT (pk) DO UPDATE SET ...`.
- All within a single transaction; commit makes visible atomically.

**Notes**:
- Postgres is an excellent loader target because its semantics are the richest. Used heavily as an operational/ODS destination.
- Foreign data wrapper approach (read Arrow directly from S3 via `parquet_fdw` or similar) is an optimization we explore post-launch.

### Iceberg (on any object store)

**Capabilities**: snapshot isolation via manifest-based commits, schema evolution (rich), partition evolution, time travel.

**Strategy**:
- Write Parquet data files directly to object storage.
- Update Iceberg manifest in a commit — atomic at the table level.
- Conflict detection on concurrent commits (optimistic); retry the manifest commit if another writer raced us.

**Notes**:
- Iceberg is the strategic open-format destination. Growing fast. We use `iceberg-rust` (the Apache implementation).
- Catalog choice matters: REST catalog, AWS Glue, Hive Metastore, Snowflake, Polaris. Loader configures which catalog.
- Native Arrow support; writes are direct without format conversion.

### Delta Lake

**Capabilities**: similar to Iceberg in shape — transaction log in `_delta_log/`, snapshot isolation, schema evolution.

**Strategy**:
- Write Parquet data files to table path.
- Append transaction log entry.
- Concurrent write conflicts resolved via transaction log.

**Notes**:
- `delta-rs` (the Rust implementation) is production-ready.
- Delta Universal Format (UniForm) emits both Delta and Iceberg metadata, which we may leverage for customers who want both.
- Configurable compaction: loader does not auto-compact; compaction is a separate maintenance task.

### Raw S3 / GCS / Azure Blob (Parquet)

**Capabilities**: none beyond object-level consistency.

**Strategy**:
- Write Parquet files to partitioned paths.
- Naming: deterministic from `LoadId`. `path/partition=X/load_id=Y.parquet`.
- No commit step beyond object visibility.

**Notes**:
- Simplest loader; also the least featureful destination.
- For customers building their own downstream pipelines. Often used alongside Iceberg.

## Transactional Commit Semantics

"Atomic visibility of the run's data" is the goal. Support varies by destination:

**Strong transactional support** (Postgres, Snowflake within a transaction, Iceberg via manifest commit): the entire run's changes become visible atomically. Staging tables are populated across many `load` calls; `commit_run` executes the final visibility step in a single transaction.

**Stream-level atomicity** (BigQuery Storage Write): each `load` commits its own stream atomically; `commit_run` finalizes visibility across streams. Best-effort atomicity across stream-level commits; a failure between the per-stream commit and the final visibility commit leaves some streams visible and others not — the loader's `abort_run` handles this by deleting the visible stream data.

**Object-level atomicity** (raw S3, older Redshift patterns): no atomicity across files. `commit_run` writes a manifest file as the visibility signal; downstream consumers read only files listed in the manifest. Partial visibility without manifest publication is "uncommitted state" by convention.

**No atomicity** (shared-database appends without transactions): loaders that target destinations this limited are avoided where possible. If we must support them, pipelines against them get a "no atomicity" warning at setup.

## Schema Application

Loaders apply schema changes at the destination, governed by RFC 10's policy but with loader-specific mechanics.

### Pre-run schema reconciliation

At `prepare_run`, the loader compares the pipeline's current schema (from the catalog) to the destination's schema. Differences are resolved:

- **Additive (new columns, widened types)**: applied via DDL at `prepare_run`. Fast on most destinations.
- **Destructive (column removal, narrowing)**: never applied by the loader. Destructive changes are policy decisions (RFC 10); the loader refuses and reports.
- **Type conversions requiring data rewrites**: not done by the loader. Handled as re-snapshot workflows.

### Mid-run schema change (CDC DDL events)

When a CDC stream emits a schema-change event:
1. Loader completes current in-flight load (flushes any batches with the old schema).
2. Loader applies the DDL at the destination (additive changes only; destructive changes pause the pipeline).
3. Loader proceeds with subsequent batches using the new schema.

This requires ordering discipline: the schema-change event must be processed at exactly the right point in the stream. The loader processes events in order and never pipelines across a schema-change boundary.

### Column-level type projections

Some platform types have no destination equivalent and are projected (RFC 3: projection). The loader applies the projection consistently:

- A column emitted as `int64` but projected to Redshift's `NUMERIC(19)` is always written as numeric; never silently switches to `BIGINT` one day.
- Projections are recorded in the catalog so lineage is accurate.

### Destination-created columns

Some destinations add their own metadata columns (e.g., Snowflake's `$file_name`, `$file_row_number` when using `COPY`). Loaders do not expose these to downstream; they are internal to the loader's delivery mechanism.

## Soft Deletes

Several destinations do not distinguish "row deleted" from "row absent." For CDC pipelines delivering deletes, loaders offer three strategies:

**Hard delete** (default for destinations supporting DELETE): issue a DELETE matching the PK. Row vanishes.

**Soft delete via tombstone column**: the destination table has a `_deleted_at` column added by the loader; deletes become updates that set this column. Users can filter out or include deleted rows in queries. Enabled via pipeline configuration.

**Soft delete via separate table**: deletes go to a parallel `<table>_deletes` table, not the main table. Main table is append-only. Users reconstruct "live" state via JOIN or materialized view.

Choice is per-pipeline, not destination. Many customers want soft deletes for auditability even on destinations that support hard delete.

## Batching and Throughput

### Optimal batch sizes

Each destination has an optimal batch size (`optimal_batch_size_bytes` in capabilities). The workflow planner asks for this and the connector targets it, but connectors may produce smaller batches if the source doesn't cooperate. The loader should handle whatever it receives without failing — batches larger than `max_batch_size_bytes` get split internally.

Guidance:

- Snowflake: 100-250 MB per COPY, smaller for frequent commits.
- BigQuery Storage Write: 10-20 MB per append, higher for batch (vs streaming).
- Redshift COPY: 1-5 GB for best throughput; we rarely hit this.
- Postgres: smaller (MB range); Postgres doesn't benefit from huge batches.
- Iceberg / Delta: 128-512 MB Parquet files are ideal.

### Parallelism

Within a single run, the loader can process multiple batches in parallel when the destination supports concurrent loads to the same target. Degree of parallelism is capacity-configured per loader.

Cross-stream parallelism is handled at the workflow level (RFC 4); the loader just processes what it's handed.

### Compaction

Many destinations benefit from post-load compaction (Iceberg files, Delta files, Snowflake clustering). **Compaction is not the loader's responsibility.** The loader writes files; compaction is a maintenance workflow triggered separately. This keeps the hot path fast and keeps compaction cost explicit.

## Error Handling

### Error categories

- **Transient**: network hiccup, destination-side 5xx. Retried per Temporal policy.
- **Throttled**: destination rate-limiting us. Retried with longer backoff; may reduce parallelism temporarily.
- **Quota exceeded**: destination quota (BigQuery slots, Snowflake warehouse limits). Retries may fail; operator notification; pipeline may pause.
- **Auth failed**: credentials invalid. Pipeline pauses; operator updates.
- **Schema mismatch**: destination schema diverged from expectation (someone manually ALTERED the destination). Pipeline pauses; operator reconciles.
- **Data error**: destination rejects data (e.g., a value violates a destination constraint we didn't know about). Non-retriable; operator decision. Rejected rows may be quarantined to a dead-letter table.
- **Destination unavailable**: whole destination is down. Retried with long backoff.

### Dead-letter handling

Per-row errors (e.g., "this specific row has a value that destination rejects") are handled via a dead-letter mechanism:

- The loader attempts the batch.
- Rows that error are isolated (destination-specific mechanism: Snowflake's `ON_ERROR=CONTINUE` with error logging, BigQuery's per-row errors, Redshift's `MAXERROR` + `STL_LOAD_ERRORS`).
- Rejected rows are written to a pipeline-specific dead-letter table with the error and original row.
- The rest of the batch proceeds normally.

Per-pipeline threshold: if dead-letter rate exceeds a configured limit (default 1% of rows in a batch), the batch fails entirely and pauses the pipeline. This prevents "10% of rows silently diverted" from being a quiet data-loss mode.

### Partial commit recovery

A run that commits some batches and fails on a later one: per the commit semantics, already-committed batches remain in the destination (either in staging for later MERGE, or in the target table if loads are incremental without staging). The workflow on retry picks up from the failed batch.

If a run commits but the final `commit_run` fails partway (e.g., MERGE succeeded but cleanup of staging failed): `commit_run` is idempotent per-destination, and retries complete the cleanup. Staging artifacts from prior runs are cleaned up via a background janitor activity scheduled per pipeline.

## Observability

Loaders emit metrics:

- Rows loaded per batch, per stream, per run.
- Bytes transferred to destination.
- Latency per `load` call, per `commit_run`.
- Destination-reported statistics where available (e.g., BigQuery slot-ms used, Snowflake warehouse-seconds).
- Dead-letter row counts.

Loader logs (structured):

- Per-batch summary at INFO level.
- DDL applied at INFO level with old and new schema fingerprints.
- Errors at WARN or ERROR with typed error category.
- Verbose SQL/API calls at DEBUG (redacted of data).

All of this flows through the standard observability path (RFC 15). Loader-specific metrics appear on the pipeline's dashboard with destination-specific context (e.g., "Snowflake warehouse load" panel appears only for Snowflake pipelines).

## Cost Attribution

Destination delivery is often the largest cost line in a pipeline. Loaders emit cost-attributable metrics:

- Bytes written to destination.
- Destination compute units consumed where reported (Snowflake credits, BigQuery bytes scanned for MERGE, Redshift query time).

These flow to the Billing / Metering Service (RFC 17). We do not mark up destination costs; we report them so customers can see where spend goes.

Additionally, loaders report operations that have non-obvious cost:

- MERGE statements on large targets (BigQuery's per-byte processing).
- Wide deletes (often more expensive than inserts).
- Small-batch writes (per-request overhead).

The operator dashboard flags expensive patterns and suggests remediations (batch size tuning, partition-key alignment).

## Testing Requirements

Every loader has a test suite, similar to connectors:

1. **Idempotency test**: the same `LoadId` applied twice produces the same destination state; row count is correct.
2. **Ordering test**: events applied in sequence with mixed ops produce correct final state.
3. **Schema application test**: each schema-change type is applied correctly.
4. **Concurrent load test**: multiple batches loaded in parallel don't interfere.
5. **Transactional rollback test**: aborted runs leave destination state clean.
6. **Dead-letter test**: rows that destination rejects are isolated and reported.
7. **Retry-after-partial-failure test**: commit failure mid-way recovers cleanly on retry.
8. **Throttling test**: simulated rate limiting produces backoff, not data loss.
9. **Schema-mismatch detection test**: destination ALTERED out-of-band produces a clear error, not silent miswrites.
10. **Large-batch test**: batches at and above `max_batch_size_bytes` are handled (split or fail clean).

Destination-specific tests extend this: each destination has its own quirks (Snowflake's VARIANT handling, BigQuery's clustering, Iceberg's manifest conflicts) that get dedicated test coverage.

## Alternatives Considered

**Allow user-authored loaders (wasm).** Tempting for connector parity. Rejected: destination correctness is too load-bearing; user-authored Rust via review would be the path if we ever want custom loaders.

**Single unified loader abstraction (one protocol, per-destination adapters).** Implicitly done; the Loader trait is the unified abstraction. The alternative — per-destination ad-hoc code — would diverge quickly.

**Separate loaders for CDC and for batch.** Considered because the patterns are different. Rejected in favor of one loader per destination handling both modes. Sharing the destination-connection pool, credential management, schema application, and error handling outweighs the protocol complexity.

**Kafka as intermediate transport.** Staging to Kafka, destinations consume from Kafka. Rejected for the same reason as in RFC 8: adds a durable log layer we don't need. Staging in S3 plus Temporal state is sufficient.

**Push model where loaders register with connectors.** Rejected: decouples the workflow from delivery, breaks the commit ordering guarantees. Pull model (workflow drives loader explicitly) preserves ordering.

**Streaming-first delivery with micro-commits.** Some destinations (BigQuery Storage Write) naturally streaming. We do use this internally; the protocol shape accommodates it (batch-granularity idempotent calls map naturally onto stream-append-plus-offset model). But the external contract is "run-based with committed batches" — simpler to reason about.

## Open Questions

1. **Multi-destination writes from a single pipeline.** "Write to Snowflake AND S3 in parallel." Supported at the workflow level (two load stages in one run) but per-destination failure handling is messy. Flagged for a follow-up.
2. **Destination-side compaction coordination.** Iceberg / Delta tables need periodic compaction. Do we orchestrate it, leave it to customers, or partner with a maintenance vendor? Likely "provide hooks, let customers decide"; defer.
3. **Cross-region cost optimization.** Data plane in region A, destination in region B: egress costs. Minimize with region-aware deployment; full treatment in deployment RFC.
4. **Schema evolution for historic data.** Adding a column with a default — do we backfill old rows at destination or leave NULL? Destination-dependent. Explicit user choice; defaults to "new rows only."
5. **Rate-limit-based adaptive batch sizing.** Auto-tune batch size based on destination responsiveness. Worthwhile optimization, not a day-one requirement.

## References

- Snowflake COPY INTO documentation (native idempotency via `LOAD HISTORY`).
- BigQuery Storage Write API: https://cloud.google.com/bigquery/docs/write-api
- Apache Iceberg spec: https://iceberg.apache.org/spec/
- Delta Lake transaction log: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
- `iceberg-rust`: https://github.com/apache/iceberg-rust
- `delta-rs`: https://github.com/delta-io/delta-rs
- Redshift MERGE documentation.
- Debezium's sink connector patterns (prior art for CDC loader design).

## Decision

**Accepted pending review.** This completes the data path. Execution tier has two more RFCs:

- RFC 10: Catalog, Schema Registry, and Schema Evolution — how we track what schemas exist, how they change, and how those changes propagate.
- RFC 11: Secrets, Connections, and Credential Management — how source/destination credentials are stored, rotated, and accessed.

The Execution tier is nearly done; after RFC 11 we move into the Platform tier (catalog, transformations, DSL, etc.).
