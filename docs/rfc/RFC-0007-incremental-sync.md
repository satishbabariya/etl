# RFC 0007: Incremental Sync and Cursor Semantics

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0003 (Data Interchange), RFC 0004 (Temporal Topology), RFC 0006 (Connector Protocol)

## Summary

This RFC specifies the correctness semantics of incremental synchronization: what guarantees we make about the data that lands in a destination relative to the data that exists in a source, under what conditions those guarantees hold, and how connectors achieve them. It defines the cursor model, the deduplication model, the tombstone model, the initial-snapshot-plus-catch-up model, and the edge cases that routinely break naive incremental sync implementations.

This is the RFC that prevents silent data loss. ETL platforms that do not specify these semantics end up with data loss — they just don't notice it, or they notice it years later when a customer audits and finds 0.01% of rows missing. We are specific, we are explicit, and we are conservative about what we claim.

## Motivation

"Incremental sync" sounds simple: track a cursor (usually an `updated_at` timestamp), pull rows newer than the last cursor, advance the cursor. In practice, every step of this has failure modes:

1. **What does "newer than" mean** when the source clock has millisecond resolution and multiple rows can share a timestamp?
2. **What happens to rows updated during the sync window** — are they captured in this run, the next run, or both (with duplicates)?
3. **What about rows deleted from the source?** A cursor-based sync cannot see deletes.
4. **What about rows whose cursor column goes backwards** (corrected data, reset sequences, restored backups)?
5. **What about clock skew** between source and sync driver?
6. **What about schema changes to the cursor column itself?**
7. **What about sources where the cursor is not strictly monotonic per row?** (Most real systems.)

A Fivetran competitor that gets these right is indistinguishable from Fivetran on correctness. A competitor that handwaves them produces data that looks right most of the time and quietly loses rows the rest of the time. The difference is not visible in demos; it is visible in customer audits after a year of production.

We commit to **at-least-once delivery with primary-key-based exactly-once semantics at the destination**, documented precisely, with connector-level and pipeline-level requirements that make the commitment achievable.

## Non-Goals

- This RFC does not cover log-based CDC. That is RFC 8. CDC has fundamentally different semantics (change events are the source of truth, not periodic scans).
- This RFC does not cover full-refresh sync. Full refresh is simple: truncate destination, copy source, done. Atomicity and failure recovery for full refresh are specified in RFC 9 (loader protocol).
- This RFC does not cover transformation correctness. Transformations run after extraction; their correctness is a separate concern handled in RFC 12.
- This RFC does not prescribe how individual sources solve these problems. Per-source strategies are documented per connector. This RFC establishes the semantics every cursor-based connector must satisfy, and the platform-level guarantees built on top.

## Delivery Guarantee: At-Least-Once + PK-Based Exactly-Once-at-Destination

The platform guarantees:

1. **At-least-once delivery of every row present in the source at some point during or after the sync window.** Every such row appears in the destination at least once.
2. **Exactly-once representation at the destination for streams with a primary key.** The destination row count for a PK matches the source's latest observed state; duplicate deliveries are merged by PK at load time.
3. **No guarantee of zero latency.** A row created in the source after the current sync window appears in a subsequent sync, not this one.
4. **No guarantee of ordering across streams.** Stream A's rows can arrive after Stream B's rows even if Stream A's source changes happened first.
5. **Ordering within a stream is preserved when the source provides it.** Connectors emit batches in cursor order; loaders preserve that order during delivery.

### What this means in practice

- If you set up a sync, walk away for a day, and come back, every row that existed in the source yesterday is in the destination today.
- If two sync runs overlap or retry, a row may be emitted multiple times, but after destination-side merge the row count is correct.
- If a row is updated twice in the source between syncs, you see the latest state. (You do *not* see both intermediate states — that's CDC territory, RFC 8.)
- If the source has no primary key, duplicates accumulate on retry. Pipelines using PK-less streams opt into append-only semantics or use full-refresh only.

### Non-guarantees we name explicitly

- **No guarantee on deleted rows.** A cursor-based incremental sync cannot see deletes. Streams requiring delete-awareness must use CDC mode (RFC 8) or accept that deletions in source do not propagate. We surface this prominently in the UI.
- **No guarantee under cursor-column lies.** If the source lies about its own cursor (e.g., populates `updated_at` with past times, or resets sequences), we will lose rows. This is a source-integrity problem we cannot solve from outside.
- **No guarantee across major connector-version upgrades without reinitialization.** A major-version bump to a connector may require a full re-snapshot to re-establish cursor correctness (RFC 6).

## Cursor Types and Their Properties

A cursor is a value that defines the boundary between "already synced" and "not yet synced." Cursors come in three flavors, each with different guarantees.

### Strictly Increasing

Every row has a cursor value strictly greater than every previously emitted row. No two rows share a value.

Examples:
- An auto-incrementing primary key used as a cursor.
- A source-generated sequence guaranteed unique (`xmin` in Postgres with caveats, Oracle `ORA_ROWSCN` in commit order, monotonic transaction IDs).

Properties:
- Resume is trivially correct: filter `cursor > last_value`.
- No overlap region. No deduplication needed (the stream itself has no duplicates by cursor).
- Rare in practice.

### Non-Decreasing (the common case)

Every row has a cursor value greater than or equal to every previously emitted row. Multiple rows can share a value.

Examples:
- `updated_at` timestamps with second or millisecond resolution.
- `modified_date` fields in most SaaS APIs.
- Any time-based field where two updates in the same second are possible.

Properties:
- Resume is *not* trivially correct. A filter `cursor > last_value` drops rows that share the cursor boundary value with rows already emitted.
- A filter `cursor >= last_value` captures them but also re-emits rows already emitted.
- The correct pattern: `cursor >= last_value`, plus primary-key-based deduplication at the destination.
- **Requires a primary key.** Without a PK, non-decreasing cursors cannot be safely resumed.

### Unreliable

Cursor values can go backwards for reasons outside our control: restored backups, clock skew, manual corrections.

Examples:
- `updated_at` populated by application code that writes arbitrary values.
- Fields that are "usually" monotonic but not enforced.

Properties:
- Cannot be used for correct incremental sync. A rollback of the cursor column causes permanent data loss on the rollback point.
- Connectors that encounter unreliable cursors should refuse to use them for incremental mode, surfacing an error to the user asking them to choose a different cursor or use full-refresh.

## The Overlap Window Pattern

For non-decreasing cursors (the 80% case), we use an **overlap window** on resume: the connector re-reads the cursor boundary to capture rows that shared the previous high-water mark.

### Mechanism

1. On first sync: extract everything up to the current clock — the start cursor is the source's earliest point; the end cursor is observed high-water mark at the end of the extraction.
2. Emit rows in cursor order.
3. Commit the new cursor value: the highest cursor observed.
4. On resume: next sync starts with `cursor >= last_committed_cursor`, not `cursor > last_committed_cursor`.
5. Rows at the boundary are re-emitted. Destination-side PK dedup removes the duplicates.

### Why not `cursor > last_value` with a saved primary-key-at-boundary set

One alternative: "I remember the PKs of rows I emitted at the boundary, exclude them on resume." Rejected:

- The boundary set can be large (all rows with identical cursor values could be millions in pathological cases).
- Persisting it bloats state.
- The platform's state slot is meant to be small-KV, not boundary-row storage.
- PK-based destination dedup is the cheaper mechanism and is already required for other reasons.

### The window size is "exactly the boundary value"

We do not extend the overlap backwards by N seconds as some systems do. Reasons:

- "N seconds" is arbitrary and inevitably wrong for some source.
- The source's own timestamp resolution defines the correct overlap: rows sharing the exact cursor value.
- Destination dedup handles the overlap cheaply.

The only case where a wider overlap is needed is clock skew between the sync driver and the source (see below). We handle that case explicitly at the connector level, not by making the overlap configurable.

## Primary-Key-Based Destination Deduplication

Destination-side dedup by primary key is how we convert at-least-once connector emissions into exactly-once-at-destination representation.

### Requirement

Every stream in non-decreasing-cursor incremental mode **must** have a declared primary key. The connector's `discover` step establishes this; pipelines configured without a PK on a non-decreasing-cursor stream are rejected at setup.

### Mechanism at the loader

The loader (RFC 9) performs a MERGE-style upsert on every batch: rows matching an existing primary key replace the existing row; rows with new primary keys are inserted. The "winning" version when duplicates arrive in the same load is the row with the highest cursor value; ties broken by the connector-assigned emission order.

### What if a source's primary key is compound

Compound primary keys are supported. The catalog records the PK as an ordered list of fields; the loader uses the composite as the merge key.

### What if the source has no primary key

Three options, configured per pipeline at setup time:

1. **Append-only mode.** All rows append to the destination. Duplicates on retry are visible; consumers must deduplicate themselves or accept them.
2. **Synthetic PK.** The connector or loader computes a deterministic hash over a specified subset of columns and uses it as a synthetic PK. Correctness depends on the hashed columns being sufficient to identify a row, which is a user-assertion.
3. **Full-refresh only.** The stream cannot be synced incrementally; use full-refresh mode on each run.

The UI surfaces this choice explicitly when a PK-less stream is added to a pipeline. We do not default.

## Clock Skew Between Source and Driver

The sync driver (our worker) and the source (customer's database or SaaS) may have different clocks. If the worker uses its own wall clock to determine "up to what cursor to extract," it will lose rows.

### Rule: cursor values come from the source, never the driver

When extracting with a timestamp-based cursor, the extraction filter is `cursor_field < max_seen_value_from_source`, not `cursor_field < driver_current_time`. The high-water mark for the completed sync is the highest cursor value observed in emitted rows, not the driver's current time.

### Consequence: the sync always lags

If a sync completes at driver time T, and the highest observed source timestamp was T - 30 seconds, the next sync starts from T - 30 (with overlap semantics). Rows the source wrote between T - 30 and T are captured by the next sync. This is a strict improvement over driver-time cursoring, which would silently skip them.

### Exception: sources without a queryable high-water mark

Some sources don't let you ask "what's your current high water?" — you only see the rows you pull. For these, the connector tracks the maximum cursor value emitted during the run and uses that as the committed high water. No different in practice from the main rule.

### Exception: strictly monotonic, source-generated IDs

If the cursor is a source-assigned sequence (not a timestamp), clock skew is irrelevant. The driver's clock does not participate in the extraction filter at all.

## Cursor Choice: Which Field

The connector's `discover` step nominates candidate cursor fields per stream. The platform ranks and selects:

### Preferred cursor fields (in order)

1. **Source-internal monotonic IDs** (e.g., a write-ahead log position, if queryable as a field): strictly increasing, immune to clock skew. Best.
2. **`updated_at` / `last_modified` / equivalent**: non-decreasing, standard, well-understood. Good.
3. **`created_at`** when the source does not track updates: non-decreasing but cannot capture post-creation mutations. Use only if source is effectively append-only.
4. **Composite cursors** (e.g., `(updated_at, id)`): useful when a single field is non-decreasing but a composite is strictly increasing. Supported if the connector declares it.

### Cursor fields we refuse

- Fields whose monotonicity depends on application correctness (any user-writable field, e.g., `status` transitioning through states). Refuse unless explicitly acknowledged by the user with a warning.
- Fields involved in the primary key. Reason: PK fields must not change for a row; if they do, that's a delete + insert from our perspective, breaking incremental semantics.
- Nullable fields. A nullable cursor means rows with null cursor values are invisible to incremental sync. Either the connector excludes them (explicit opt-in) or refuses to use the field.

### User override with warnings

Users can override cursor choice when they know something the platform doesn't (e.g., "this field is monotonic in our deployment because of an application invariant"). Overrides are logged and surfaced on the pipeline's observability page. The platform's correctness claims are conditional on the user's assertion; we do not verify it.

## Initial Snapshot + Incremental Catch-Up

When a pipeline is first set up in incremental mode, there is no cursor to resume from. The connector must perform an initial snapshot, then transition to incremental mode.

### Sequence

1. **Initialization:** the connector captures a stable reference point. For database sources, this may be a transaction snapshot or an LSN. For SaaS sources, it is typically the current highest cursor value.
2. **Snapshot phase:** full scan of the source, emitting rows. The snapshot can be large (billions of rows). It uses the bounded-read mechanism (RFC 6) to make progress across many activity invocations.
3. **Catch-up phase:** once the snapshot is complete, the connector transitions to incremental mode, starting from the initialization point. Any rows modified during the snapshot are captured here.
4. **Steady state:** subsequent runs are plain incremental.

### Snapshot atomicity

A snapshot in progress emits data that the destination must not yet consider "complete." Two patterns supported:

- **Stage-and-swap.** Snapshot writes to a staging table; only on snapshot completion, it is swapped into the main destination table atomically. Loader detail in RFC 9.
- **Idempotent append.** Snapshot writes directly to the destination; partial completion is acceptable because the PK-based merge model handles the "rest of the snapshot arrives later" case. Less preferred because the table is in a confusing intermediate state during snapshot.

Default is stage-and-swap. Idempotent append is opt-in for specific use cases (e.g., monitoring pipelines where partial data is better than no data during snapshot).

### Snapshot resume

A snapshot that is interrupted mid-way must resume without restarting. The mechanism is a **snapshot token** — an opaque value the connector emits periodically during snapshot, representing the current position. On resume, the connector restarts from the token.

Snapshot tokens are source-specific:
- For SQL sources, typically a PK-based position: "I've read all rows with PK ≤ X."
- For SaaS sources with consistent pagination, the pagination cursor.
- For sources without stable pagination, the snapshot cannot resume and must restart on failure. Connectors surface this as a capability flag; pipelines against such sources accept restart-on-failure for snapshots.

## Sequence Handling: Backwards-Moving Cursors

A cursor value going backwards is an integrity violation and a real operational risk.

### Detection

When a connector on resume observes rows with cursor values *less than* the committed high-water mark, this is a backwards-cursor event. It's detected by the connector (which is filtering on the cursor and seeing unexpected rows) or by the platform on cursor commit (when an emitted batch's max cursor is less than the prior commit).

### Response

1. **Log the event prominently.** This is an incident, not a warning.
2. **Pause the pipeline.** Do not advance the cursor forward; do not continue emitting.
3. **Alert the operator.** Pipeline enters `awaiting_operator_decision` state.
4. **Operator options:**
   - Resnapshot: full refresh, then resume incremental from a fresh cursor. Safest.
   - Accept and continue from the backwards point: acknowledging that some rows have probably been re-processed or missed. Operator signs off.
   - Investigate: pause indefinitely while the operator examines the source.

We do not auto-resolve backwards cursors. The correct response depends on the source's reality, which we cannot infer.

## Window-Based Sync (Bounded Time Ranges)

For very large historical backfills, a single "snapshot" is impractical. Some connectors support window-based sync: extract a bounded time range per run, then advance to the next window.

### Pattern

Instead of "read everything from cursor X onward," the connector reads "cursor in [X, X + W)" where W is a window size (e.g., one day). The pipeline schedules runs that cover consecutive windows until caught up.

### When used

- Historical backfills spanning months or years where a single snapshot cannot complete in reasonable time.
- Sources that enforce cursor ranges themselves (some SaaS APIs only allow querying within a time window).

### State implications

Window state extends the base cursor state with a `window_end` value. The connector is mid-window when processing; on completion, window advances, cursor advances, and the next run starts.

### Parallelism

Windows can be processed in parallel — multiple runs each handling a different window — when the source supports it and the destination tolerates out-of-order arrival (the PK-merge pattern does). Historical backfills are the primary case where this parallelism is worth it.

## Deletion Handling in Cursor-Based Mode

Cursor-based sync cannot see hard deletes. We offer three policies, configured per stream:

### 1. Ignore (default)

Deletes in source do not propagate. The destination accumulates rows that have been deleted upstream. This is acceptable for analytics use cases that tolerate historical data.

### 2. Periodic reconciliation

On a user-configured cadence (e.g., weekly), the connector performs a full scan of source primary keys and compares to destination primary keys. Rows present in destination but not in source are marked deleted.

Expensive for large streams. Opt-in per stream. Requires source to support a PK-only scan cheaply.

### 3. Soft-delete markers

If the source has a soft-delete convention (e.g., `deleted_at IS NOT NULL`), the connector treats those rows as deletes and propagates the soft-delete to destination. Simple; works only when source has the convention.

### CDC is the real answer

For streams that need timely delete propagation, CDC mode is the correct tool. Cursor-based mode is inherently blind to deletes; no amount of protocol cleverness fixes that. Users who require delete-awareness should use CDC-capable sources (RFC 8).

## Schema Changes During Sync

Covered at the protocol level in RFC 6, but with specifics for incremental mode.

### Adding a column

Incremental sync continues normally. New column is populated in rows emitted after the change; prior rows in destination have the column NULL (destination-dependent default). Cursor is unaffected.

### Removing a column

Incremental sync continues. Prior rows in destination retain the column's values; new rows have it NULL or absent depending on destination semantics. This is a decision for RFC 10 (schema evolution).

### Changing a column's type (compatible)

Incremental sync continues. Destination column widens (RFC 10). Prior rows are not rewritten.

### Changing the cursor column's type

Incremental sync **pauses**. A cursor-type change can break comparison semantics (integer → string, for example). Operator must acknowledge and either re-snapshot or accept the risk and continue with a new cursor type. Same flow as backwards-cursor detection.

### Changing the primary key

Incremental sync **pauses**. A PK change means "what uniquely identifies a row" has changed, which means destination dedup is now based on the wrong key. Mandatory resnapshot.

## Deduplication State at the Destination Loader

For completeness: the loader's role in making at-least-once into exactly-once-at-destination.

### The merge key

Every batch emitted in incremental mode carries, in its schema metadata (RFC 3), the primary-key field list. The loader uses this as the merge key regardless of the physical destination.

### The tie-breaker

Within a single load batch, if duplicate primary keys appear (e.g., the same row was updated twice during the extract), the winner is the row with the highest cursor value. Ties on cursor value are broken by emission order (the connector emits in cursor order; later in the emission sequence wins).

### Cross-batch semantics

When a new batch arrives with PKs that already exist in the destination, the new row replaces the existing one. This is a MERGE operation. The loader does not compare cursor values across batches (that would require reading destination state during load), because the connector + overlap pattern already ensures that a later-arriving batch cannot contain a *strictly older* row for the same PK.

### Failure modes the pattern doesn't fix

- **Rows re-emitted with stale cursor values due to connector bugs.** If a connector emits the same row twice with the second emission having an *older* cursor value than the first, the loader's "latest row wins by emission order" rule may deliver the stale version. Mitigation: connectors are tested (RFC 6) to emit in cursor order, and this is a connector bug we treat as a severity issue.
- **Non-idempotent source APIs.** If a source's pagination returns different rows on retry (e.g., eventual-consistency issues), we may miss rows. Connector-specific problem; specific connectors handle specific source quirks.

## Testing Requirements for Incremental Mode

Every incremental-capable connector must pass:

1. **Fresh start test:** no prior state, full initial extraction produces correct data.
2. **Resume test:** mid-extraction interruption and resume produces the same final data (modulo duplicates, which the destination dedups).
3. **Overlap boundary test:** rows sharing the exact cursor value at a commit boundary are all delivered (at least once) across the commit.
4. **Clock skew test:** simulated source clock drift (driver fast, driver slow) produces correct results.
5. **Backwards cursor test:** injected backwards cursor triggers the pause-and-alert path, does not silently advance.
6. **Schema change test:** additive schema change during sync produces correct data with new schema applied from the change point forward.
7. **Large batch test:** a batch containing many rows with identical cursor values does not cause data loss or infinite loop.
8. **Snapshot interruption test:** snapshot interrupted partway resumes correctly via snapshot token.

These are publication gates, supplementing the general connector tests from RFC 6.

## Platform-Level Observability Guarantees

For every incremental stream, the platform surfaces:

- **Current cursor value** (human-readable when possible, else opaque).
- **Lag estimate:** current driver time minus committed cursor value, for time-based cursors.
- **Rows emitted per sync run.**
- **Overlap cost:** number of rows re-emitted due to overlap window, visible to the operator so they can diagnose unexpectedly large overlap batches.
- **Last-seen schema.**

These are not decorative — they are how operators notice something is wrong. A stream whose cursor hasn't advanced in 24 hours against a source that updates hourly is a silent failure we want visible. Lag estimates surface that.

## Alternatives Considered

**Guarantee exactly-once delivery end-to-end.** True exactly-once would require destination-side transactional writes coordinated with cursor commits. Many destinations (BigQuery Storage Write, Snowflake copy, S3) don't support the needed primitives. Rejected in favor of at-least-once + destination PK dedup, which is achievable everywhere and equivalent from the consumer's perspective when PK is present.

**Always extend overlap by N seconds.** Simple to implement, catches most clock-skew issues. Rejected: N is always wrong for some source, adds avoidable re-emission cost, and doesn't address the root cause (using driver clock). The explicit source-clock-based approach is correct regardless of skew.

**Auto-detect backwards cursor and re-snapshot silently.** Would be invisible to operators, which is the problem — a silent re-snapshot masks a real data-integrity event that the operator should know about. Rejected.

**Track the PK set at the cursor boundary instead of using destination dedup.** Mentioned above. Rejected: unbounded state, duplicates the work destination dedup does anyway.

**Let connectors persist their own state in external stores.** Would decouple state from platform lifecycle. Rejected: state must be atomically committed with data for correctness; platform-owned state is the only way to guarantee this.

**Default to full-refresh when incremental is uncertain.** Would be "safer" but misses the whole point of the platform: economically unviable at Fivetran-competitor scale. Incremental with clearly documented caveats is the right product.

## Open Questions

1. **Deletion-detection heuristics for sources without soft-delete conventions.** Are there cheap signals (row count discrepancies, checksum columns) we can use to trigger a reconciliation automatically? Probably not worth it; punt to explicit configuration.
2. **Mixing incremental streams with CDC streams in one pipeline.** Multiple streams from the same source, some via cursor, some via CDC. Cursor commit and CDC commit have different semantics. Likely supported, but commit-atomicity edge cases need working through in RFC 8.
3. **Cursor type widening mid-sync.** Source changes `updated_at` from `timestamp(3)` to `timestamp(6)`. Compatible but the cursor values now have more resolution. Safe to continue? Probably yes; worth a test case.
4. **Rows with cursor value in the future.** Source has a row with `updated_at = driver_time + 1 hour` (clock skew, explicit future-dating, bug). Treat as "emit now, advance cursor to that value"? Lean yes but could cause lag misreporting; flag for follow-up.
5. **Delta-detecting against destination without full source scan.** If source and destination both support PK-range listing, can we detect deletes via range checksums? Source-specific optimization for power users; defer.

## References

- Fivetran cursor-based sync documentation (prior art for the user-visible model).
- Airbyte incremental sync documentation (publicly documents the overlap-window pattern).
- Debezium snapshot modes (relevant for initial-snapshot-plus-catch-up semantics; expanded in RFC 8).
- Stripe incremental reconciliation patterns (public engineering blog posts on the problem domain).
- Singer tap specification on state messages.

## Decision

**Accepted pending review.** RFC 8 next: CDC Architecture, which covers log-based sources and provides the delete-aware, ordered-change-event alternative to cursor-based sync specified here.
