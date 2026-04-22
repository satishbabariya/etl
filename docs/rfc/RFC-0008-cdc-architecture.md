# RFC 0008: CDC Architecture (Log-Based Sources)

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0003 (Data Interchange), RFC 0004 (Temporal Topology), RFC 0006 (Connector Protocol), RFC 0007 (Incremental Sync)

## Summary

This RFC specifies how we do log-based change-data-capture: consuming a source's write-ahead log or equivalent to emit an ordered stream of change events (inserts, updates, deletes). It covers the workflow topology (long-lived parent with finite snapshot child), replication-slot-style resource management, the initial-snapshot-plus-catch-up handoff, LSN-equivalent position handling, per-source semantics for Postgres / MySQL / MongoDB / SQL Server / Oracle, and the operational model for CDC pipelines that run continuously for months.

CDC is the delete-aware, change-ordered alternative to cursor-based sync (RFC 7). It is also the single most operationally demanding feature in the platform: a CDC pipeline holds resources on the source (replication slots) that can grow unbounded and damage the source if we mishandle them. We are conservative, explicit, and defensive.

## Motivation

Cursor-based sync has structural limitations that no amount of careful design can overcome:

- It cannot see hard deletes.
- It cannot distinguish "row updated once" from "row updated 50 times" — you see the latest state only.
- It has latency bounded by the sync schedule (minutes to hours).
- It loads the source with scan queries on every sync.

Log-based CDC fixes all four by consuming the source's write log:

- Every change (insert, update, delete, schema change) appears in the log.
- Order is preserved.
- Latency is bounded by log consumption rate (seconds).
- Source load is near-zero for the CDC read itself; all the cost is replaying the log.

CDC is also the only path to compete credibly with Fivetran for database sources. Fivetran's CDC connectors are a meaningful part of their moat. If our platform only supports cursor-based sync for databases, we lose every deal where "sub-minute latency" or "delete tracking" is a requirement — which is most mid-market and all enterprise.

The cost is complexity:

1. **Replication slots are a source-side resource we do not control.** A CDC consumer that stops consuming while the source keeps writing accumulates WAL/binlog storage on the source. Left unchecked, this fills the source's disk and takes down the database. This is a real incident that has happened to every CDC-capable vendor at some point.
2. **Initial snapshot + log catch-up is operationally subtle.** The snapshot produces "current state" of N billion rows; the log starts at some position; the two must stitch together without gaps or duplicates.
3. **Schema changes mid-stream are harder than in cursor mode.** A DDL applied to the source between events must be reflected in the change stream, and events before the DDL must be interpretable under the old schema.
4. **Transactions.** A transaction in the source generates multiple log events; commit/abort semantics matter for downstream correctness.
5. **Source-specific quirks are legion.** Postgres TOAST values, MySQL GTID vs. binlog coords, Oracle LogMiner vs. GoldenGate, MongoDB oplog vs. change streams — each has its own failure modes.

This RFC gets these right, or at least sets us up to get them right incrementally.

## Non-Goals

- This RFC does not specify every CDC-capable source in detail. Postgres, MySQL, SQL Server, MongoDB, and Oracle get enough detail to anchor the design; per-source engineering notes live in the connectors themselves.
- This RFC does not cover outbound CDC (us *being* a CDC source, e.g., change feeds from our platform to downstream systems). That's a future product direction if at all.
- This RFC does not mandate Debezium. We evaluate using Debezium components below but commit only to interoperable semantics, not to the implementation.
- This RFC does not redefine the connector protocol. CDC connectors use the RFC 6 protocol with the `incremental-cdc` sync mode. This RFC specifies what "incremental-cdc" means.

## The CDC Workflow Topology

A CDC pipeline does not fit the standard `PipelineRunWorkflow` shape from RFC 4. It is long-lived (weeks to months), it has distinct phases (snapshot, then streaming), and it needs to retire and re-initialize history periodically. We use a parent-plus-child pattern with explicit justification under RFC 4's child-workflow rules.

### Topology

```
CdcPipelineWorkflow (parent, long-lived)
├── Phase 1: CdcSnapshotWorkflow (child, finite)
│     └── Iterator activity: snapshot chunks using bounded reads
└── Phase 2: CdcStreamingLoop (parent continues, streams log events)
      └── Iterator activity: CdcReadWindow (consumes log up to bound,
                             commits batches + positions, returns)
```

### Why a child workflow for snapshot

RFC 4 justified child workflows in three cases; "finite-child-of-long-lived-parent" is case #1. A snapshot of a 10B-row table produces a large Temporal history if run as a loop of activities in the parent — enough to force `continue-as-new` churn on the parent, which we want to avoid on long-lived CDC workflows because it complicates the streaming state. Putting snapshot in its own child workflow isolates its history; on snapshot completion the child terminates and its history is out of the parent's path.

### Why not two separate root workflows

Alternative: `CdcSnapshotWorkflow` and `CdcStreamingWorkflow` as peer roots, with the scheduler starting streaming after snapshot completes. Rejected: the handoff from snapshot to streaming has strict correctness requirements (the streaming start position must be ≤ the snapshot capture position), which is easier to enforce inside a single workflow than across workflow boundaries.

### Workflow lifecycle

`CdcPipelineWorkflow` states:

- `initializing` — setting up replication slot / CDC source configuration.
- `snapshotting` — snapshot child running.
- `streaming` — consuming log events.
- `streaming_backlogged` — streaming, but falling behind; see "Backpressure" below.
- `paused` — operator-initiated pause; slot is still held, log accumulates.
- `failed_needs_reinit` — a failure that cannot be recovered by retry; operator decision required.
- `failed_fatal` — slot was lost; re-initialization with new snapshot is required.

State transitions are explicit and persisted in workflow state, queryable by operators.

### `continue-as-new` policy for CDC

CDC workflows run indefinitely. History must be bounded.

- The streaming loop uses `continue-as-new` on a schedule: every 6 hours OR every 10M events processed OR every 2,000 activities executed, whichever hits first.
- The payload carried across continuation is the minimum state: current log position, schema fingerprint per stream, run metadata, pending operator-signal state.
- Snapshot workflows do not use `continue-as-new` — they are finite-bounded and expected to complete before hitting history limits; if an individual snapshot is expected to exceed the history budget, it is split into multiple snapshot windows (see "Very Large Snapshots" below).

## Replication Slot / Log Position Management

This is the most operationally sensitive section of the RFC. The platform holds a resource on the source; mismanagement damages the source.

### The slot-as-resource abstraction

Every CDC-capable source has a concept of a **consumer position** that prevents the source from recycling log entries ahead of the consumer. Per-source vocabulary varies:

- Postgres: a **replication slot** (logical slot for our use case). Holding a slot pins WAL.
- MySQL: a **server ID** + **binlog coordinates**. No server-side resource until row-based binlog retention policy kicks in; binlog retention is time-based or size-based, set by the DBA.
- SQL Server: a **capture instance** via SQL Server Change Data Capture (CDC). Retention is configured on the source.
- MongoDB: an **oplog cursor** (for classic oplog) or **change stream resume token** (modern). Oplog is a capped collection; retention is size-based.
- Oracle: **LogMiner** or **GoldenGate** — both hold resources; LogMiner is lighter-weight, GoldenGate is fuller-featured.

We unify these under the **`ReplicationPosition`** concept: an opaque, source-specific value representing "we have consumed log up to here." The platform stores it; the connector interprets it.

### Slot lifecycle

The platform explicitly manages slots via dedicated activities:

1. **`EnsureSlotActivity`** — runs at workflow initialization. Creates the slot if absent; verifies it's healthy if present. For Postgres: creates logical replication slot, configures the decoder plugin (pgoutput). For MySQL: verifies binlog format is ROW and that retention is adequate.
2. **`AdvanceSlotActivity`** — runs periodically (every committed position advance). Tells the source "you can recycle log up to position P." For Postgres: calls `pg_replication_slot_advance`.
3. **`ReleaseSlotActivity`** — runs on pipeline teardown. Drops the slot cleanly. For Postgres: `pg_drop_replication_slot`.

Slot advance is **not** a free operation on the source. We rate-limit it: advance at most every 30 seconds during steady-state streaming, even if we've committed many positions in that window.

### Slot growth alerting

The platform monitors slot lag: how much log has accumulated in the slot but not yet been consumed by our streaming loop. We alert the operator at three thresholds, configurable per pipeline:

- **Warning** (default 1 GB or 1 hour of lag, whichever first): something is slow; investigate.
- **Severe** (default 10 GB or 6 hours): operator attention required now.
- **Emergency** (default 50 GB or 24 hours): the source is at risk. The platform can optionally auto-pause the pipeline and drop the slot, forcing a reinit. Auto-drop is opt-in per pipeline; the default is "alert, do not auto-drop."

These defaults are per-source and will be refined in operations. The point is the thresholds exist and are actionable, not that the numbers are exactly right on day one.

### Orphaned slot recovery

A slot whose associated pipeline was deleted without clean teardown is **orphaned** and will grow forever. The platform runs a reconciliation job that compares live slots on each source (queried via a source-side reconciliation activity) to active pipelines in the catalog. Orphans are logged and, after a confirmation window (default 72 hours) to handle transient catalog inconsistencies, automatically dropped.

This reconciliation is critical for preventing customer-visible outages. It runs at least daily per source.

### Multiple slots per source

Some sources impose slot limits (Postgres defaults to 10 concurrent logical replication slots; configurable). When a single source has many CDC pipelines, we can exhaust the limit.

Mitigation:

- Default to **one slot per source per tenant**, not one slot per pipeline. A single slot can feed multiple pipelines as long as they share the same filter criteria. Sharing is a later optimization; v1 is one slot per pipeline and we document the slot-count requirement to customers.
- Publication filter in Postgres (`CREATE PUBLICATION FOR TABLE ...`) is used to scope a slot to the needed tables, rather than one slot per table.
- For sources with strict limits (Oracle LogMiner), we will more aggressively share a single "consumer" across pipelines.

## Initial Snapshot and Streaming Handoff

The hardest part of CDC: get the state of the entire source, then pick up log streaming at exactly the right position, without gaps and without uncontrolled duplication.

### The capture-and-stream pattern

1. **Capture reference position.** At snapshot start, the connector records the source's current log position as `snapshot_start_position`. This is done *before* snapshot queries begin. For Postgres: the slot is created in a single transaction that includes `SELECT pg_export_snapshot()` — the snapshot and the slot are bound to the same WAL position.
2. **Snapshot with that position pinned.** The snapshot reads rows using the pinned snapshot handle so it sees a consistent point-in-time view. Concurrent writes to the source during snapshot are not visible in snapshot results but are captured in the log starting from `snapshot_start_position`.
3. **Snapshot emits rows with op=`s` (snapshot marker).** All snapshot rows carry the `_cdc.op = "s"` metadata column (RFC 3) and the `_cdc.lsn = snapshot_start_position` value.
4. **Streaming begins at `snapshot_start_position`.** When snapshot completes, streaming consumes the log from the recorded position forward. Rows updated during snapshot appear twice: once in the snapshot (with their value at `snapshot_start_position`), once in the stream (with their value after the change). Destination-side PK merge handles this: snapshot rows are written first, stream rows replace them.
5. **Destination dedup resolves.** The loader's MERGE operation handles the overlap, producing a correct final state.

### Per-source variation

- **Postgres:** Native primitive (`pg_export_snapshot` within the replication-slot-creating transaction). Clean, well-documented. This is the gold standard.
- **MySQL:** No single-transaction snapshot + binlog coords primitive. We use `FLUSH TABLES WITH READ LOCK` + `SHOW MASTER STATUS` to record position, then `UNLOCK TABLES` and snapshot using consistent read (`REPEATABLE READ` + `START TRANSACTION WITH CONSISTENT SNAPSHOT`). Or, with Percona toolkit / MySQL consistent snapshot features, a cleaner variant. The connector encapsulates the per-version details.
- **SQL Server:** Use a snapshot isolation read after reading the max `__$start_lsn` from the CDC capture instance tables.
- **MongoDB:** `$resumeAfter` / `$startAtOperationTime` on change streams. Record cluster time before snapshot, resume stream from that time.
- **Oracle:** SCN-based. Capture current SCN, snapshot using flashback query (`AS OF SCN`), then stream from that SCN.

### Handoff correctness property

The invariant we commit to: **every change to the source after `snapshot_start_position` appears in the stream, and every change before it is reflected in the snapshot.** Changes at `snapshot_start_position` itself are handled by overlap (dedup by PK).

### Snapshot-during-streaming (add-a-table flow)

A running CDC pipeline has a user add a new table to its scope. That table needs a snapshot before it can be streamed.

Pattern:

1. The streaming loop pauses the new table's inclusion in its filter (the existing tables continue streaming).
2. A `CdcSnapshotWorkflow` child is launched just for the new table, pinning a new reference position.
3. On completion, the streaming loop begins consuming the new table's events from the pinned position.
4. Existing tables are unaffected throughout.

This is a per-table snapshot; the pipeline-global position advances throughout.

### Very Large Snapshots

Snapshots of 10B-row tables take days. To keep child workflow history bounded:

- Snapshot work is broken into **snapshot chunks** of bounded size (default 10M rows or 30 minutes, whichever first).
- Each chunk is a bounded `read` invocation per RFC 6.
- The child workflow loops over chunks until the snapshot is complete.
- If the child approaches the history budget, it uses `continue-as-new` at a chunk boundary.
- During multi-day snapshots, the parent's streaming position continues to be advanced on the source (the slot is held; log accumulates). Operators are alerted when slot lag from a long-running snapshot exceeds warning threshold.

### Skip-snapshot mode

Some use cases explicitly do not want the initial snapshot: "I only care about changes from now forward." This is supported:

- Pipeline is configured with `initial_sync = streaming_only`.
- The connector captures the current log position and starts streaming from there.
- The destination table is empty at pipeline start; it accumulates as changes arrive.

This is a conscious data-loss trade: rows that existed before streaming start are never delivered unless separately seeded.

## Event Model and Semantics

### Event types

Every CDC event is a row in an Arrow batch with `_cdc.*` metadata (RFC 3):

- `_cdc.op = "i"` — insert. Full row in the batch.
- `_cdc.op = "u"` — update. Full row (after state). If the source provides it, `_cdc.before` holds the pre-image.
- `_cdc.op = "d"` — delete. The row's primary key and any columns the source provides at delete time; non-PK columns may be null.
- `_cdc.op = "t"` — truncate. Emitted when the source truncates a table; downstream loaders treat this as "delete everything with matching table identity at this LSN."
- `_cdc.op = "s"` — snapshot. Full row from initial snapshot.
- `_cdc.op = "c"` — schema change. Payload describes the DDL; row has no data.

### Per-row metadata

Every CDC row carries:

- `_cdc.lsn`: source log position at which the change was committed.
- `_cdc.commit_ts`: commit timestamp from the source.
- `_cdc.txid`: transaction ID (when available).
- `_cdc.before`: optional pre-image struct (for updates and deletes, if the source provides it).

### Transaction boundaries

A single source transaction generates multiple log events. We need to decide: does the stream expose transaction boundaries to downstream consumers?

**Default: No.** Events are emitted in commit order per stream, and the loader applies them in order, but transaction grouping is not preserved. The reasoning: most destinations (columnar warehouses especially) don't support multi-statement transactions efficiently, and the abstraction "rows in order" is easier to reason about than "batches bounded by source transactions."

**Opt-in: transaction boundaries preserved.** For destinations that can use them (Postgres, transactional Iceberg tables), the pipeline can be configured to preserve transaction boundaries. The loader then applies all events within a transaction atomically. This is a per-pipeline configuration, not a default.

Events emitted in the default mode carry `_cdc.txid` so consumers can reconstruct transaction grouping in queries if needed. The information isn't lost; we just don't use it at load time by default.

### Ordering guarantees

Per stream, events are emitted in source commit order. Across streams (tables) within a pipeline, ordering is **not** preserved — streams may be emitted in parallel and arrive at the destination out of order. This is acceptable because cross-table consistency at the destination is the destination's concern (foreign-key constraints, materialized view refresh, etc.).

Pipelines that require cross-table transactional consistency at the destination must use transaction-boundary-preserving mode, which serializes stream emission.

### The "update without before image" problem

Some sources emit updates without the pre-update value. Destination merge by PK works regardless (we write the new state). But some transformations want the pre-image to compute deltas. We expose the source's capability to the user:

- If the source does not provide before-images, the pipeline configuration surfaces this, and transformations that require before-images fail validation.
- Postgres: configurable via `REPLICA IDENTITY`. Default (`DEFAULT`) only includes PK in before-images; `FULL` includes the whole row but costs more. The connector surfaces this and the user chooses.
- MySQL: row-based binlog always includes before-images for updates.
- MongoDB: `fullDocumentBeforeChange` option on change streams (MongoDB 6+).

## Schema Change Handling

Schema changes in CDC are fundamentally different from cursor-based (RFC 7). In cursor mode, we see state; we don't see DDL. In CDC, DDL events appear in the log and must be handled in order.

### Event: schema change

The connector decodes DDL events from the log and emits `_cdc.op = "c"` rows with payload describing the change: table, change type, old schema fingerprint, new schema fingerprint, full new schema.

### Handling at the loader

The loader processes events in order. On seeing a schema change event:

1. All pending data-event batches for that table are flushed with the old schema.
2. The destination schema is altered to match the new schema (per RFC 10 evolution policy).
3. Subsequent data events use the new schema.

This requires the loader to synchronize schema changes with data application. Complicated but correct. The alternative — reordering schema events — produces wrong data.

### DDL not captured by the log

Not every DDL is visible in the log. Postgres captures `CREATE TABLE`, `ALTER TABLE`, etc., in its logical decoding only with extension (`pglogical`) or explicit handling. Vanilla pgoutput emits limited DDL.

The connector detects this case: if it sees data events whose schema differs from the expected schema and no preceding schema-change event was emitted, it:

1. Pauses stream consumption.
2. Queries the source for the current table schema.
3. Emits a synthetic schema-change event describing the diff.
4. Resumes consumption.

This is an approximation; DDL that happened between log events cannot be precisely located in the log. We surface this as "schema change detected at approximate LSN X" and proceed.

### Unsupported DDL

Some DDL has no clean streaming equivalent: table rename, column rename, column type change with data conversion. Policy:

- **Rename:** treated as DROP + CREATE. The old table's events stop; the new table appears as a new stream requiring snapshot.
- **Column rename:** treated as column drop + column add. Data from before the rename is not re-populated under the new name.
- **Type change with conversion:** pauses the pipeline for operator review. Operator can accept the change (destination widens if possible, else re-snapshot) or reject (pipeline halts for manual intervention).

These policies are conservative. They favor correctness over continuity.

## Backpressure and Slot Lag

A CDC pipeline can fall behind for many reasons: destination slow, worker undersized, source burst. Our response must preserve correctness.

### Detection

Two signals:

- **Slot lag (source-side):** WAL/binlog bytes between current source position and consumer position. Queried from the source periodically.
- **Processing lag (destination-side):** time between event commit timestamp and event load timestamp.

### Response tiers

**Tier 1: healthy.** Slot lag < warning threshold, processing lag < 1 minute. Normal operation.

**Tier 2: backlogged.** Slot lag in warning range OR processing lag growing. Pipeline state → `streaming_backlogged`. Operator is notified but nothing auto-changes.

**Tier 3: degraded.** Slot lag in severe range. Pipeline continues consuming but with increased batch sizes and reduced parallelism on non-critical work. Operator is paged.

**Tier 4: critical.** Slot lag approaching emergency threshold. Per pipeline configuration:
- Default: operator is paged; no auto-action.
- Opt-in: pipeline auto-pauses and drops slot. Full reinit required on resume. This trades data-completeness for source-availability — the right trade for some customers, wrong for others.

### Never silently drop events

At no tier does the platform skip events to catch up. Catching up by dropping is data loss. We slow down, we alert, we may eventually require a reinit — but we do not fabricate progress.

## Specific Source Notes

Brief per-source notes. Full engineering docs live with each connector.

### Postgres

- Use logical replication slot with `pgoutput` (native, no extensions required).
- Require `wal_level = logical` and `max_replication_slots` configured adequately.
- Use `PUBLICATION` objects to scope tables.
- Slot advance via `pg_replication_slot_advance` (available PG 11+).
- TOAST'd values: when a TOAST column is not modified, the log entry doesn't include it. The connector detects this and either (a) fetches the value via a lookup query (expensive, complete), (b) emits a sentinel value and lets the destination preserve the prior value (cheap, requires loader support). Default is (b) for efficiency; (a) is opt-in.
- Large transactions: streaming (in-progress transaction streaming) is supported on PG 14+. We use it to avoid memory blowup on large transactions.

### MySQL

- Require `binlog_format = ROW`, `binlog_row_image = FULL` for complete before-images.
- Use GTID-based positioning where possible (MySQL 5.7+); fall back to file+offset otherwise.
- Binlog retention is time- or size-based and configured by the DBA; we surface requirements and alert if retention is below our consumption rate.
- DDL is captured in binlog as `Query_event`s; parsed by a SQL parser in the connector.
- Heartbeats: configure MySQL to emit heartbeat events so we can distinguish "no changes" from "connection stale."

### MongoDB

- Use change streams (MongoDB 4.0+); oplog-direct consumption is deprecated.
- Require replica set or sharded cluster; standalone is not supported.
- Resume tokens are opaque, source-versioned. The connector stores them as-is.
- `fullDocumentBeforeChange` requires MongoDB 6+ and collection-level configuration; surface this in pipeline setup.
- Collection rename, index change: handled via full-resnapshot of affected collection.

### SQL Server

- Use SQL Server CDC feature (`sys.sp_cdc_enable_db`, `sys.sp_cdc_enable_table`).
- Requires SQL Server Agent running; CDC capture jobs read the transaction log and write to change tables.
- Retention configured per capture instance (default 3 days).
- Read from change tables via `cdc.fn_cdc_get_all_changes_*` functions.
- Note: SQL Server CDC is distinct from Change Tracking, which is coarser-grained and not suitable for CDC.

### Oracle

- LogMiner is the default; GoldenGate is a premium option for customers with existing GoldenGate investments.
- LogMiner: require ARCHIVELOG mode, supplemental logging enabled.
- LogMiner has performance limits (throughput caps) for high-write workloads; surface this in capacity planning.
- Complex types (objects, nested tables): limited support; surface limitations.

## Failure Modes and Operator Actions

CDC pipelines fail in ways cursor pipelines don't. We enumerate:

### Slot dropped by source

Source DBA dropped the slot; the consumer returns a "slot not found" error. Pipeline transitions to `failed_fatal`. Recovery: full reinit (new snapshot). Not automatic.

### WAL/binlog retention exceeded

The source recycled log segments our consumer hadn't read yet. Connector returns a "position not available" error. Same state and recovery as slot dropped.

### Schema change we can't handle

A DDL produces a change we can't safely apply (e.g., incompatible type change). Pipeline transitions to `failed_needs_reinit`. Operator decides: accept and re-snapshot, reject and halt.

### Source version upgrade

Source was upgraded to a new major version; our connector may need a new version to support new log format. Detected by connector metadata mismatch. Pipeline pauses; operator upgrades connector; pipeline resumes from last committed position.

### Transient source unavailability

Connection dropped, source restarting, etc. Activity retries; workflow continues; slot lag accumulates during the gap. Alerting escalates per backpressure tiers.

### Connector crash mid-batch

Worker dies while streaming. Standard Temporal recovery: activity is retried. Idempotent emission (positions in state, destination PK merge) handles replay cleanly.

### Position corruption

The stored ReplicationPosition, when replayed, returns an error or unexpected data. Pipeline transitions to `failed_needs_reinit`. This is rare but possible (storage corruption, version incompatibility); reinit is the only safe response.

## Debezium: Components Without the Framework

Debezium is the most mature open-source CDC toolkit. We use its per-source decoding work but do not adopt its framework.

### Use

- Debezium's binary protocol parsers for Postgres pgoutput, MySQL binlog row format, and Oracle LogMiner decoding. These are the parts that represent years of edge-case handling we would otherwise reinvent.
- Debezium's documented semantics as a reference for how specific edge cases should behave.

### Do not use

- Debezium Server / Debezium Connect: JVM-based, Kafka-centric, architecture does not fit ours.
- Debezium's internal state management, connector framework, offset storage: these are Kafka Connect primitives, not applicable here.

### Practical implementation

Where Debezium's parsers are Java, we interface via one of:

- Rust-native reimplementation of the parsing layer, informed by Debezium's code. Preferred for widely-used sources where Rust ecosystem is adequate (Postgres has `postgres-replication`, several MySQL binlog crates exist).
- FFI / subprocess to Debezium for sources where the Rust implementations are immature (Oracle LogMiner is the case most likely to need this). Accepted as technical debt with a plan to replace.

Per-source engineering decisions on this are documented in the connector itself; this RFC just establishes the policy.

## Testing Requirements for CDC Connectors

Supplementing RFCs 6 and 7, CDC connectors must pass:

1. **Snapshot consistency test:** concurrent writes during snapshot — final state in destination matches final source state after all writes propagate.
2. **Streaming correctness test:** all event types (insert/update/delete/truncate/DDL) are emitted correctly with correct metadata.
3. **Large transaction test:** a single source transaction with many rows emits all rows in order, correctly handles memory.
4. **Slot-advance test:** committed positions cause the source slot to advance; lag decreases.
5. **Reinit test:** pipeline reinitialization after slot loss produces a final state matching the source.
6. **Schema-change test:** a DDL applied during streaming produces correct events; downstream state reflects the change.
7. **Resume test:** worker restart mid-stream results in no data loss and no uncontrolled duplication (dedup resolves).
8. **Backpressure test:** destination throttling produces increased slot lag but no errors; recovery on un-throttling catches up.
9. **Source-restart test:** source restart during streaming results in reconnection and clean resume from last position.
10. **Orphan-slot reconciliation test:** a pipeline deleted without clean teardown leaves a slot that reconciliation detects and cleans up.

## Open Questions

1. **Slot sharing across pipelines.** A real opportunity for cost reduction on source side. Complex because of filter divergence between pipelines. Flagged for a follow-up when we have operational data.
2. **Backfilling deletions into existing destinations.** If a customer switches a pipeline from cursor-based to CDC, the destination has accumulated soft-deleted rows. A one-time reconciliation option would clean these up. Nice-to-have; punt.
3. **Cross-source CDC pipelines.** A single pipeline consuming from two databases. Transaction ordering across sources is impossible; fallback semantics need design. Not on the near-term roadmap.
4. **PITR (point-in-time recovery) for CDC.** Rewind the destination to its state at source time T. Requires keeping CDC events in a replayable store, not just applying to destination. Significant feature; likely a separate product direction.
5. **CDC-over-polling fallback.** Some sources don't have usable log-based CDC. A "trigger-based CDC" (source-side trigger populates an audit table, connector reads from it incrementally) is an ugly but working pattern. Evaluate per source; not a platform-wide commitment.

## Alternatives Considered

**Adopt Debezium Server as the CDC runtime.** Covered above: wrong architectural fit, JVM, Kafka-centric. We use Debezium's parsing knowledge without adopting the framework.

**Skip CDC at launch, cursor-only.** Rejected in RFC 1: losing database CDC means losing the most competitive segment of Fivetran's business. CDC is non-optional for our wedge.

**Use Kafka as the CDC transport internally.** Debezium's model: source → Kafka Connect → Kafka → downstream. Rejected: adds a persistent log layer we don't need. Our staging (S3/object storage) plus Temporal workflow state is our durability; adding Kafka doubles operational complexity.

**Native "streaming" Temporal workflows instead of the iterator-activity pattern.** The streaming loop could conceptually be modeled as a continuously-running activity. Rejected: Temporal activities have bounded lifetimes; iterator pattern (small activities in a loop) gives us heartbeats, retries, and observability per-iteration, which is the right granularity.

**Single workflow for snapshot and streaming (no child).** Rejected above: history growth during snapshot conflicts with the parent's long-lived nature.

**Eager delete propagation via CDC, lazy via cursor.** Rejected at the design level: we use cursor or CDC per-stream, not a mix. Mixing creates semantic confusion about what "delete" means at the destination.

## References

- Debezium documentation and source: https://debezium.io/
- Postgres logical decoding: https://www.postgresql.org/docs/current/logicaldecoding.html
- MySQL binlog documentation: https://dev.mysql.com/doc/refman/8.0/en/binary-log.html
- MongoDB change streams: https://www.mongodb.com/docs/manual/changeStreams/
- SQL Server CDC: Microsoft Docs.
- Oracle LogMiner: Oracle Database documentation.
- Stripe engineering blog posts on CDC at scale (publicly discussed cases).
- Netflix DBLog (prior art for snapshot + streaming unification): public engineering posts.

## Decision

**Accepted pending review.** RFC 9 next: Destination Loader Protocol and Idempotency — which completes the data path by specifying how staged batches are delivered to Snowflake, BigQuery, Iceberg, and other destinations with the idempotency guarantees this RFC and RFC 7 depend on.
