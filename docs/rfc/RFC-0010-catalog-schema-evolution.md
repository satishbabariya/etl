# RFC 0010: Catalog, Schema Registry, and Schema Evolution

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0002 (Core Architecture), RFC 0003 (Data Interchange), RFC 0006 (Connector Protocol), RFC 0007 (Incremental Sync), RFC 0008 (CDC Architecture), RFC 0009 (Destination Loaders)

## Summary

This RFC specifies the Catalog Service: the system of record for what a pipeline is, what streams it touches, what schemas those streams have, how schemas change over time, and what policies govern destination-side evolution. It defines the entity model (tenants, workspaces, connections, pipelines, streams, schemas, runs), the schema-diffing algorithm, the evolution-policy taxonomy, the handling of destructive changes, and the lineage graph derived from run metadata.

The Catalog is the boring CRUD service of the platform — and that's the goal. It's where product-tier features (UI, API, observability, lineage) hang off the data-tier work. Nothing in this RFC should surprise anyone who has built SaaS-style metadata services; everything should compose cleanly with the decisions made in prior RFCs.

## Motivation

Every previous RFC has deferred decisions to "the catalog": pipeline definitions, schema storage, schema evolution policy, connection metadata, lineage. This RFC pays those debts. Concretely:

1. **The data plane depends on the catalog for every pipeline run.** Workers pull pipeline definitions, schemas, and connection metadata from the catalog at run start. A bad catalog design produces correctness bugs (stale schemas, wrong cursor fields) and operational drag (chatty cross-plane traffic, cache invalidation problems).
2. **Schema evolution is the #1 source of pipeline incidents.** A column added in the source, a column renamed, a type widened — these happen constantly and each has to do something sensible. Handwaving "we handle schema evolution" is how competitors acquire angry users.
3. **The catalog is the surface for lineage and impact analysis.** "What pipelines will break if I change this source table?" is a question customers ask; answering it requires a real lineage graph, which requires a real catalog.
4. **Connections span many pipelines.** A single Postgres connection feeds five pipelines; a credential rotation has to apply to all five. Correctly modeling connections vs. pipelines prevents per-pipeline credential sprawl.

## Non-Goals

- This RFC does not cover secrets storage. Catalog holds secret *references*; actual secret material lives elsewhere (RFC 11).
- This RFC does not cover authentication, authorization, or RBAC. Those are user-facing access concerns addressed by the Auth service (RFC 2) and in the security RFC (RFC 19, future).
- This RFC does not cover billing metering. Catalog emits run metadata; billing (RFC 17, future) consumes it.
- This RFC does not cover the query surface for lineage/impact analysis UX. We store the graph; UI/API for it is a product concern.
- This RFC does not cover the transformation DAG specification. Transformations (RFC 12, future) reference catalog entities; the catalog stores references, not transformation logic.

## Entity Model

Every entity has a stable `id` (UUID, time-ordered for cache friendliness), a `created_at`, an `updated_at`, a `version` (monotonic int for optimistic concurrency), and a `status` where lifecycle applies.

### Hierarchy

```
Tenant
├── Workspace
│   ├── Connection
│   │   └── (references one Connector)
│   └── Pipeline
│       ├── Source (references Connection + SelectedStreams)
│       ├── Destination (references Connection + DestinationConfig)
│       ├── Transformation (optional, references TransformationPackage)
│       ├── Schedule
│       └── Run (historical, many)
└── (tenant-scoped entities)
    ├── ConnectorVersion (published connector registry entries)
    ├── TransformationVersion (published transformation packages)
    └── SecretRef (references, not material)
```

### Tenant

Top-level isolation boundary. One tenant per customer organization. Tenants are created by the platform operator (us), not by users.

Fields: `name`, `plan_tier`, `data_plane_region`, `data_plane_mode` (`hosted`, `byoc`, `self_hosted`), `suspended_at`. Cross-cuts billing, quotas, and deployment.

### Workspace

Organizational subdivision within a tenant. A tenant has at least one workspace; large customers have many. Workspaces scope permissions, pipelines, and connections — pipelines in workspace A can't accidentally reference connections in workspace B.

Fields: `name`, `description`, `created_by`.

### Connection

A reusable, credentialed handle to an external system. One Connection per (external system, credential set). Multiple pipelines reference the same Connection to share credentials, connection-pool capacity, and rate-limit budgets.

Fields:
- `connector_ref`: `(publisher, name, version_constraint)` — e.g., `platform/postgres@^2`.
- `config`: non-secret configuration as JSON.
- `secret_refs`: map of logical names → SecretRef IDs (RFC 11).
- `validation_status`: result of last `validate-config` call.
- `capabilities_cache`: cached output of `describe` for the connector version in use.

Connections are validated on create/update via the connector's `validate-config`. Validation failures block pipeline creation against the connection.

### Pipeline

The unit of ETL execution. A pipeline references one source connection, one destination connection, optional transformation, and a schedule.

Fields:
- `name`, `description`.
- `source`: `{connection_id, selected_streams, sync_mode_overrides}`.
- `destination`: `{connection_id, destination_config}`.
- `transformation`: optional `{transformation_ref, transformation_config}`.
- `schedule`: `{cron | interval | trigger_from_pipeline | manual_only}`.
- `evolution_policy`: per-pipeline schema-evolution policy (see below).
- `enabled`: bool.
- `paused_reason`: optional typed reason (billing, manual, schema-event-pending, etc.).

Pipeline edits are versioned (see "Versioning" below). A run captures the pipeline version at run start.

### Stream (within a pipeline)

A stream is a source-emitted entity (a table, collection, endpoint) selected into a pipeline. Streams have their own per-pipeline config, schema history, and run state.

Fields:
- `name`, `namespace`.
- `sync_mode`: active sync mode (may override pipeline default).
- `cursor_config`: chosen cursor field, overlap handling, clock-skew tolerance.
- `primary_key_config`: declared or overridden PK.
- `schema_head`: pointer to current Schema entity.
- `destination_table`: target table/path name (defaulted, overridable).
- `enabled`: bool.

Stream configs are independent: you can disable one stream in a pipeline without disabling the others.

### Schema

A versioned record of a stream's shape. Schemas are **append-only and immutable** — every change is a new Schema entity. A stream has a chain of Schema versions; the current one is the schema_head.

Fields:
- `stream_id`.
- `arrow_schema`: serialized Arrow schema (with RFC 3 metadata).
- `fingerprint`: canonical hash (see "Fingerprinting" below).
- `parent_schema_id`: previous Schema in the chain.
- `change_summary`: typed description of the change from parent.
- `detected_at`, `detected_by_run_id`.
- `applied_to_destination_at`: nullable; null until the loader has applied the schema to the destination.

### Run

The record of a pipeline execution. Immutable once completed.

Fields:
- `pipeline_id`, `pipeline_version`.
- `trigger`: `scheduled`, `manual`, `backfill`, `cdc_resume`, `cascade`.
- `started_at`, `completed_at`, `status`.
- `per_stream_results`: rows, bytes, cursor advance, schema events, errors.
- `temporal_workflow_id`: for cross-referencing Temporal history.

### ConnectorVersion, TransformationVersion

Registry entries for published connectors and transformations. Tenant-scoped (first-party connectors are visible to all tenants; third-party connectors may be private to a tenant or shared).

Fields:
- `publisher`, `name`, `version` (semver).
- `manifest`: full connector manifest (descriptor, config schema, capabilities).
- `artifact_ref`: pointer to the AOT-compiled wasm artifact in the registry.
- `signature`: cryptographic signature (RFC 19, future).
- `published_at`, `deprecated_at`, `removed_at`.

### SecretRef

A catalog-side handle for a secret. Does not contain secret material. Detail in RFC 11.

## Versioning

The catalog is **versioned at the pipeline level**. Every edit to a pipeline produces a new version; old versions are retained for audit and for already-running workflows that pinned to them.

### Versioning semantics

- Every pipeline edit increments `version` atomically (database-level constraint).
- Runs record the version they started against.
- Pipeline activities load the version pinned at workflow start (RFC 4) — an edit during a run does not affect the running workflow.
- Version history is retained for 90 days by default; extended retention per plan tier.

### What is pipeline-versioned

- Source selected streams, sync modes, cursor configs.
- Transformation reference.
- Destination config.
- Evolution policy.
- Schedule.

### What is not pipeline-versioned

- Connection config. Connection edits apply immediately across all pipelines using the connection.
- Secrets (via SecretRef). Credential rotation applies immediately.
- Schema history. Schemas are append-only and identified by fingerprint; they are referenced by pipelines but not owned.
- Run history (immutable by definition).

### Why connection edits are unversioned

If a connection's credentials rotate, all pipelines using it should use the new credentials immediately. Pinning each pipeline to a credential version would require coordinated updates across pipelines and delay credential rotation — a security anti-pattern.

## Schema Fingerprinting

A stable, canonical hash of a schema. Used for cache keys, change detection, and equality comparisons.

### Algorithm

1. Normalize the Arrow schema:
   - Sort field-level metadata keys.
   - Canonicalize numeric type representations (e.g., always emit `Decimal128(p,s)` with explicit precision/scale).
   - Remove platform-injected metadata that doesn't affect semantics (e.g., `platform.detected_at`).
2. Serialize the normalized schema to Arrow IPC.
3. Hash with BLAKE3. 256-bit output.

Fingerprints are 64-char hex strings stored on every Schema entity. Two schemas with the same fingerprint are equivalent for all platform purposes.

### What the fingerprint includes

- Field names, order, types, nullability.
- Platform-semantic metadata: `platform.type`, `platform.is_primary_key`, `platform.is_cdc_metadata`, `platform.semantic`.
- Source-origin metadata: `platform.source_type`, `platform.source_precision`, `platform.source_scale` (these *do* affect loader behavior).

### What the fingerprint excludes

- Catalog-injected metadata: `platform.detected_at`, internal IDs.
- Arrow format-internal metadata not affecting semantics (field ordering within metadata maps, etc.).
- Connector-private metadata under `connector.<n>.*` (included if it changes semantically; excluded if cosmetic).

## Schema Diffing

When a run observes a schema different from `schema_head`, the platform computes a diff. Diffs are **typed changes**, not free-form text, because downstream decisions (evolution policy application, operator-facing display) need structured input.

### Diff type taxonomy

**Additive changes (non-breaking):**
- `field_added(field)`: new column appeared. Field includes its full type and metadata.
- `field_nullability_relaxed(name)`: was non-null, now nullable.
- `field_type_widened(name, old_type, new_type)`: `int32 → int64`, `decimal(10,2) → decimal(20,2)`, etc. Widenings are enumerated explicitly; we do not accept "new type is a superset" as a blanket rule because the destination's interpretation varies.

**Breaking changes:**
- `field_removed(name)`: column disappeared.
- `field_type_narrowed(name, old_type, new_type)`: reverse of widening. Rare in source systems; usually a bug or a misreading.
- `field_type_incompatible(name, old_type, new_type)`: no valid widening exists (e.g., `string → int32`).
- `field_nullability_tightened(name)`: was nullable, now non-null. Breaking because existing nulls in destination can't be enforced retroactively.
- `primary_key_changed(old_pk, new_pk)`: PK composition changed.
- `field_renamed(old_name, new_name)`: detected when a field disappears and another appears with a compatible type. Requires heuristics; we represent it as `field_removed + field_added` by default and offer a manual rename-reconciliation flow (below).

**Semantic changes (ambiguous):**
- `field_semantic_changed(name, old_semantic, new_semantic)`: e.g., a string column's `platform.semantic` changed from `json` to no annotation. Could be widening (we now treat as opaque string) or information loss; policy-dependent.
- `cdc_metadata_changed`: `_cdc.*` fields added or removed as the source's CDC capability changes. Handled specially (see CDC section).

### Computing the diff

Pairwise field diff on `schema_head` vs. newly observed schema. Field matching by name; unmatched fields produce `field_removed` or `field_added`. Matched fields are compared for type, nullability, metadata.

Widening detection uses a lookup table of (old_type, new_type) → `widening` / `narrowing` / `incompatible`. Defined per the platform type catalog in RFC 3. Example entries:

| old | new | result |
|---|---|---|
| int32 | int64 | widening |
| int64 | int32 | narrowing |
| decimal(10,2) | decimal(20,2) | widening |
| decimal(10,2) | decimal(10,4) | widening |
| decimal(10,2) | decimal(10,1) | narrowing |
| decimal(10,2) | decimal(5,2) | narrowing |
| string | large_string | widening |
| timestamp(milli, UTC) | timestamp(micro, UTC) | widening |
| timestamp(naive) | timestamp(UTC) | incompatible (semantic change) |

The table is extensive; the principle is that widenings never lose information and narrowings might.

### Rename heuristic

Optional. Off by default because false positives are worse than false negatives (a false rename causes data loss at the destination). When enabled per pipeline, the heuristic looks for: (a) one removed field and one added field in the same diff, (b) same type and nullability, (c) similar metadata. If detected, the operator is offered a "was this a rename?" prompt. Operator-confirmed renames apply as destination-side `ALTER TABLE ... RENAME COLUMN`.

## Evolution Policy

Per-pipeline configuration that governs what happens when schema changes are detected. The platform offers a small set of named policies; per-field overrides are supported for advanced cases.

### Named policies

**`propagate_additive`** (default). Additive changes apply automatically: new columns added to destination, widenings applied. Breaking changes pause the pipeline for operator review.

**`strict`.** Any change — even additive — pauses the pipeline. Used by customers with strict change-management processes who want human sign-off on every schema event.

**`propagate_all`.** Additive changes apply automatically; widening applies; breaking changes attempt automatic resolution per the "breaking-change automation" rules below. Falls back to pause if resolution is impossible. Used by customers who prioritize pipeline continuity over caution.

**`freeze`.** Schema changes are detected but never applied to destination. Extra source columns are dropped silently (with per-run logging). Destination stays fixed at the schema-at-pipeline-creation. Used for stable downstream consumers that can't tolerate destination schema drift.

### Per-field overrides

A pipeline's evolution policy can be refined per field:

- `ignore(<field>)`: changes to this field don't trigger any action.
- `freeze(<field>)`: this field's schema is frozen even in otherwise propagating policies.
- `protect(<field>)`: destructive changes to this field always require manual approval, even under `propagate_all`.

Overrides are listed in the pipeline's `evolution_policy` config and surface in the UI as a "field protection" section.

### Breaking-change automation (under `propagate_all`)

When `propagate_all` encounters a breaking change, it attempts resolution before falling back to pause:

- `field_removed`: destination column is kept (not dropped). Subsequent loads set it to NULL. User can manually drop the column via an explicit action.
- `field_type_narrowed`: if new values still fit the old type, the destination type is unchanged. If they don't, the pipeline pauses.
- `field_nullability_tightened`: destination nullability unchanged (keeps nulls). Loader writes nulls where source provides non-nulls. Approximate; user notified.
- `field_type_incompatible`: pause. No automatic resolution possible.
- `primary_key_changed`: pause. Mandatory resnapshot is required (RFC 7).

The philosophy: `propagate_all` tries hard to keep pipelines running, but never invents data or drops data silently. It keeps the destination "reasonable" and lets the user decide when a drift has gotten too far.

### Destination-type-mapping changes

Not a source-schema change, but related: a loader version upgrade may change the platform-to-destination type mapping (RFC 3, "Changing type mappings is a breaking change"). The catalog detects this at loader version change and surfaces it the same way as a source schema change — as a proposed evolution requiring policy evaluation.

## Destination Schema Application

Schemas exist in two places: source-observed (in the catalog) and destination-realized (the actual DDL state of destination tables). Keeping these in sync is the loader's job, governed by the catalog's policy decisions.

### State machine per Schema entity

- `proposed`: schema was observed; policy evaluation pending.
- `approved`: policy (automatic or manual) approved application.
- `applying`: loader is applying DDL.
- `applied`: destination matches.
- `rejected`: policy or operator rejected; pipeline paused.
- `superseded`: a newer schema has been approved; this one is not applied.

Transitions happen via catalog events; the Temporal workflow for a pipeline run checks state at `prepare_run` (RFC 9) and fails cleanly if the schema isn't `applied` yet.

### Destination-side divergence detection

Occasionally, someone ALTERs a destination table out of band. The catalog's view no longer matches reality. We detect this at `prepare_run`:

1. Loader queries destination's current schema.
2. Catalog compares to the expected schema (the approved Schema entity).
3. Divergence → pipeline pauses with `destination_schema_divergence` status; operator reconciles.

Options for reconciliation:
- Accept destination as canonical: update catalog schema_head to match destination, re-run pending changes.
- Revert destination to expected: loader applies DDL to undo divergence.
- Manual merge: operator specifies field-by-field resolution.

## CDC-Specific Schema Handling

CDC sources emit schema-change events in-stream (RFC 8). These produce schema diffs the same way as cursor-based detection, but they are bound to a specific log position and must be applied in order.

### In-band application

The loader processes CDC events in order. A schema-change event in the stream triggers:

1. Flush any buffered old-schema batches to destination.
2. Submit the schema change to the catalog for policy evaluation.
3. If approved and applicable: apply DDL at destination, then resume with new schema.
4. If rejected or pending approval: pause the pipeline at the schema-change LSN. The catalog holds the in-flight LSN; resumption starts from there.

Because CDC pipelines are long-lived (months), schema events accumulate. The catalog maintains them as an ordered log per stream, not just a schema_head pointer.

### Schema lag

A CDC pipeline that is backlogged (RFC 8 backpressure) may have old schemas in flight while the source has already moved to newer ones. The catalog's schema_head tracks what has been applied to destination; the source's current schema may be ahead. This is normal; it's reconciled as the pipeline catches up.

## Lineage

Lineage is the graph of "what produced what." Built from run metadata; stored as a queryable graph in the catalog.

### Graph structure

Nodes:
- **Source stream** (external, identified by connection + stream name).
- **Destination table** (external, identified by destination connection + table).
- **Pipeline** (catalog entity).
- **Transformation** (catalog entity, when present in the pipeline).

Edges:
- Source stream → Pipeline (`consumed_by`).
- Pipeline → Destination table (`produces`).
- Pipeline → Transformation → Destination table (when transformation exists).
- Pipeline → Pipeline (`cascades_to`, when RFC 4 cascade triggers are configured).

### Derivation

Each completed run emits a lineage-event activity (RFC 4). Events are appended to a run-event log. A derivation job rebuilds the lineage graph periodically (default: every 5 minutes, eventually consistent).

Why a derivation job rather than updating the graph transactionally on each event: run events are high-volume; the graph is read at human-query rate. Eventual consistency is fine here.

### Column-level lineage

Graph-level lineage (pipeline → destination table) is straightforward. Column-level lineage (source column X → destination column Y) requires knowing the transformation's semantics. We support it when:

- No transformation (direct pass-through): column-level lineage is trivial.
- First-party declarative transformations: column lineage is derivable from the transformation definition (RFC 12, future).
- Arbitrary wasm transformations: column-level lineage is not automatically derivable. Transformations may opt-in by declaring their column-level mapping in their manifest.

For arbitrary transformations without declared lineage, we offer graph-level lineage only and surface this limitation in the UI.

### Impact analysis

"What pipelines will break if this source changes?" is a query against the lineage graph:

1. Start at the source stream or column.
2. Traverse outgoing `consumed_by` and subsequent edges.
3. Return all reachable pipelines and destination tables.

A UI exposes this as a "downstream impact" view on any catalog entity.

## Catalog API Surface

The catalog is consumed by:
- The control plane UI and user-facing API (read/write).
- The data plane workers (read mostly; write of run metadata).
- Other control plane services (Scheduler, Observability) for metadata enrichment.

### API shape

- HTTP/JSON for user-facing (through API Gateway, RFC 2).
- gRPC for worker-facing (internal), same schema under the hood.
- Read-heavy; optimistic concurrency via `version` field on writes.

### Consistency

- **Strongly consistent within a catalog shard** (single-region Postgres primary). Reads after writes see the write.
- **Eventually consistent across derived views.** Lineage graph and cache layers are behind the primary.
- **Cached at the worker.** Workers cache loaded entities with a TTL (default 5 minutes). Cache invalidation for critical changes (pipeline paused, connection credentials rotated) uses an event-based invalidation channel (publish on catalog; workers subscribe).

## Storage

### Backing store

Postgres. Single logical database per data-plane region (for our-hosted); per-tenant for BYOC.

### Schema (sketch)

Tables: `tenants`, `workspaces`, `connections`, `connector_versions`, `pipelines`, `pipeline_versions`, `streams`, `schemas`, `runs`, `run_events`, `secret_refs`, `lineage_edges`.

Large-ish tables partitioned by time where relevant (`runs`, `run_events`). `schemas` partitioned by `stream_id` at scale.

### Scale targets

At 10K tenants with 100 pipelines each and daily runs:
- Pipelines: 1M rows.
- Runs: ~365M/year.
- Run events: ~5B/year.

Postgres handles this with partitioning, decent indexes, and occasional archival of old run events to object storage. Not a new problem.

### Retention

- Pipelines, connections, schemas: retained indefinitely.
- Runs and run events: default 90 days hot in Postgres, archive to object storage beyond that. Per-plan-tier retention tuning.
- Catalog audit log: 7 years default, compliance-grade.

## Versioning and Migrations

Catalog schema evolves. Migration discipline:

- Forward-only migrations. No downgrades (hard to get right; we don't promise them).
- Backward-compatible for one version pair: version N+1 code must read version N data and vice versa during deploy windows.
- Zero-downtime migrations via standard patterns: add column nullable, backfill, deploy new code, alter to non-null, drop old column.
- Migration review is a required gate in the control plane CI.

## Error Scenarios and Operational Concerns

### Catalog unavailable

Data plane workers have cached pipeline definitions; in-flight runs continue (RFC 2 invariant). New runs can't start until catalog recovers. Writes queue at the API Gateway with retry.

### Run event loss

Run events feed lineage derivation and observability. Occasional loss is tolerable (lineage is approximate; billing is reconciled separately). We do not treat run events as must-deliver; billing events (RFC 17) are the durable channel.

### Concurrent edits

Optimistic concurrency via `version` field. Conflicting edits to the same pipeline produce a clear error ("someone else edited this"); user retries after reviewing.

### Schema chain corruption

In principle, the `parent_schema_id` chain for a stream could develop loops or orphans under a bug. The catalog enforces at write time: `parent_schema_id` must reference an existing schema for the same stream; `schema_head` must be reachable from any ancestor.

### Large schemas

Some sources have tables with thousands of columns or deeply nested types. Arrow schemas remain manageable up to ~10K fields per table; beyond that, Postgres row size limits (1GB per row practical limit) may become relevant. Catalog enforces a 10K-field cap per schema with a clear error; few real tables exceed this.

## Alternatives Considered

**Store schemas as JSON Schema instead of Arrow IPC.** JSON Schema is more readable but less expressive for the types we care about (decimals with precision, timezone-aware timestamps, Arrow-specific semantics). We'd end up translating back and forth; keep Arrow IPC as the canonical form.

**Central schema registry shared across tenants.** Tempting for common source types (everyone's Postgres `pg_stat_statements` has the same shape). Rejected: cross-tenant sharing creates cache invalidation problems, and the savings are marginal because schemas are small.

**Merge Catalog with Scheduler / Auth / Observability into a single service.** Would be operationally simpler. Rejected in RFC 2 for scaling and clarity reasons; re-raised here because catalog and scheduler do share some state. The split remains correct: catalog is CRUD, scheduler is decisioning about when to run; they have different scaling profiles.

**Store lineage in a purpose-built graph database** (Neo4j, TigerGraph). Rejected: our lineage is derivable from run events stored in Postgres and we don't need graph-query superpowers. An adjacency-table model in Postgres handles our query patterns fine.

**Version schemas per pipeline (not globally per stream).** Would mean two pipelines on the same source see potentially different schema chains. Rejected: makes schema events hard to reason about and wastes storage. Schema chains are per-stream; pipelines reference them.

**Allow pipeline edits to affect running workflows (no version pinning).** Simpler. Rejected: edits during runs produce non-deterministic behavior and debug hell. Pinning is the right discipline.

## Open Questions

1. **Pipeline templates.** Customers want "create 50 pipelines that look like this one with these 50 tables." Templates are a product feature on top of the catalog; scope TBD.
2. **Workspace-to-workspace references.** Can a pipeline in workspace A reference a connection in workspace B? Default no; some customers want controlled cross-workspace sharing. Defer.
3. **Schema diff visualization.** The UI for reviewing a proposed schema change is make-or-break for the UX. Design not in this RFC; flag for product.
4. **Catalog query complexity limits.** Lineage traversals can become expensive at large scale. Implement query-depth and result-size caps; tune empirically.
5. **Backup / restore story.** Catalog is the system of record; backup cadence, retention, restore procedures need explicit design for enterprise customers. Flag for operational RFC.
6. **Audit log retention vs. privacy.** 7-year audit log may conflict with right-to-be-forgotten requirements for tenants in some jurisdictions. Reconcile with the future security/compliance RFC.

## References

- Confluent Schema Registry (prior art for per-subject schema chains): https://docs.confluent.io/platform/current/schema-registry/
- AWS Glue Data Catalog (prior art for multi-tenant catalog shapes).
- OpenLineage specification: https://openlineage.io/ (possible external compatibility target for lineage export).
- Apache Atlas (prior art for data governance catalog).
- Arrow canonical schema representation: https://arrow.apache.org/docs/format/CanonicalExtensions.html

## Decision

**Accepted pending review.** RFC 11 next: Secrets, Connections, and Credential Management — the last RFC in the Execution tier before we cross into Platform (catalog is done; transformations, DSL, state storage architecture, and observability await).
