# RFC 0012: Transformation Layer and UDF Model

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0003 (Data Interchange), RFC 0004 (Temporal Topology), RFC 0005 (Wasm Runtime), RFC 0006 (Connector Protocol), RFC 0009 (Destination Loaders)

## Summary

This RFC specifies the optional transformation stage that sits between extract and load. It defines the transformation model (declarative operators plus user-authored UDFs in wasm), the operator catalog (select, filter, project, cast, rename, mask, enrich, dedupe, flatten), the UDF interface, the composition model (transformations as DAGs of operators), and the integration with the existing workflow, staging, and loader infrastructure.

Transformations are the bridge between "we move data" (Fivetran positioning) and "we move and shape data" (the first meaningful step toward Databricks territory). They are not a separate product — they are a first-class, optional stage of every pipeline.

## Motivation

Fivetran's explicit position is "ELT not ETL" — move raw data to the warehouse, transform there with dbt. This is a reasonable architecture for many use cases and Fivetran's lack of a transformation layer is a deliberate focus choice, not an oversight. But it leaves customer pain:

1. **Secrets and PII.** A customer syncing production data to a development warehouse wants to mask PII *before* it leaves their network, not after it lands in the warehouse. Destination-side masking is leaked-and-cleaned-up; pre-destination masking is never-exposed.
2. **Cost.** Rows written to a warehouse and then filtered out of analytical views still cost storage and load compute. Pre-destination filtering saves real money at scale.
3. **Schema shaping.** Destination-side transformations can't change the schema that's loaded. If the source emits 200 columns and the warehouse table needs 20, dbt has to maintain a view that selects 20 — and the 200-column source table is still there costing storage.
4. **Multiple destinations.** A pipeline feeding both Snowflake and S3 with different schemas requires two separate pipelines in the ELT-only world. Transformations in-pipeline let one extract feed two differently-shaped loads.
5. **Reference data joins.** Enriching extracted rows with a small lookup table (mapping, translation, categorization) is awkward in dbt and obvious in-pipeline.

We offer transformations as an **optional stage**. Customers who prefer ELT-to-destination simply don't use them; customers who want transformations get them without operational overhead.

The design balance we need: powerful enough to be worth using, constrained enough to stay cheap, debuggable, and composable. A Turing-complete Python UDF is powerful but ruinous to performance, observability, and cost. A no-UDF declarative-only model is cheap but leaves real use cases uncovered. We support both.

## Non-Goals

- This RFC does not cover analytical SQL transformations that live in the destination (dbt territory). Those remain downstream of us.
- This RFC does not cover stream processing (windowed aggregates, event-time watermarks). RFC 1 explicitly deferred streaming; transformations here are micro-batch, stateless or small-state.
- This RFC does not cover ML inference as a first-class operator. Can be done via UDF; not a distinguished platform feature.
- This RFC does not cover destination-side transformations (VIEWs, MERGE-with-SELECT transformations). Those are the loader's concern (RFC 9) and the destination's SQL surface.
- This RFC does not cover transformation authoring UX (visual DAG builder, SQL-like DSL). Authoring surfaces are a product concern; this RFC specifies the execution model they all compile to.

## Design Principles

**Declarative by default, UDF by exception.** The common case is "filter out PII, drop unused columns, cast a type, rename a field." These are well-served by declarative operators that the platform implements natively (Rust, Arrow-native). UDFs exist for the cases declarative can't cover.

**Wasm for UDFs, same runtime as connectors.** No new execution environment. UDFs use the RFC 5 wasm runtime with a restricted capability set (transformations are deterministic — no network, no time, no random, per RFC 5).

**Columnar throughout.** Transformations operate on Arrow `RecordBatch`es. No per-row Python-like dispatch. The platform's cost-advantage over Spark on small-to-medium data depends on keeping the columnar fast path unbroken.

**Schema is statically verifiable.** Transformation DAGs have schemas that can be computed without running them. A pipeline's expected output schema is known at configuration time, not at runtime. This is how we keep destination schema evolution (RFC 10) sane when transformations are in the pipeline.

**Transformations are a DAG, not a chain.** Multiple inputs, multiple outputs, fan-in, fan-out. Most real transformations are linear but the model supports branching without special cases.

**Idempotent and side-effect-free.** A transformation invoked twice on the same input produces the same output. This is what makes Temporal activity retries safe (RFC 4).

## The Execution Model

Transformations live between the extract and load stages in a pipeline run (RFC 4). Activity-level sketch:

```
for each extract stage → staging batch
for each transformation stage:
    load input batch refs from staging
    execute transformation DAG
    write output batches to staging
for each load stage → consume transformed staging
```

A transformation is one or more activities in the Temporal workflow. Like extract and load, transformation activities are idempotent, heartbeat, and respect limits.

### Transformation packages

A transformation is packaged as a `TransformationPackage`: a versioned catalog entity (RFC 10) containing:

- A **DAG specification** — the operator composition and configuration.
- Zero or more **UDF modules** — wasm components implementing user-authored operators.
- A **manifest** — input-schema requirements, output-schema derivation, resource limits, dependencies.
- **Tests** (publication gate, described below).

Pipelines reference transformation packages by `(publisher, name, version)` — same versioning discipline as connectors (RFC 6). This is important: upgrading a transformation is an explicit operator action, because a transformation change changes destination schemas.

### DAG execution

The DAG specification is a directed acyclic graph of operator nodes. Each node:

- Has a unique name within the DAG.
- Has an operator type (declarative or `user_udf`).
- Has operator-specific configuration.
- Has declared inputs (edges from upstream nodes; for root nodes, references to extract streams by name).
- Produces one or more outputs (edges to downstream nodes; for leaf nodes, named outputs the loader consumes).

The platform's transformation engine walks the DAG in topological order, passing Arrow batches node-to-node. For linear DAGs, this is straightforward. For branching DAGs (one input, multiple outputs), batches are fanned out; for fan-in, the engine materializes inputs to a synchronized boundary.

## Operator Catalog

The first-party declarative operators. These cover 80%+ of realistic use cases and are implemented natively in Rust over Arrow.

### Filter

Drop rows not matching a predicate. Predicate is an expression over row fields.

```
filter:
  predicate: "status != 'deleted' AND created_at > '2024-01-01'"
```

Expression language is a small SQL-like subset: comparison, logical, arithmetic, `IN`, `LIKE`, `IS NULL`. The predicate is parsed and compiled to a columnar filter (DataFusion-style). No UDF calls in expressions at this level — for UDF-based filtering, use a separate `user_udf` node.

### Project

Select, drop, or rearrange columns. The most common operator.

```
project:
  keep: [id, email, created_at, updated_at]
  # or:
  drop: [internal_notes, debug_info]
  # or:
  # keep_matching: "^(id|email|.*_at)$"  (regex)
```

Emits a batch with only the specified columns, in the specified order.

### Rename

Change column names. Destination schema names may differ from source names.

```
rename:
  mapping:
    customer_id: customer_external_id
    dob: date_of_birth
```

### Cast

Change a column's type explicitly. Platform applies widening/narrowing per RFC 3 rules; narrowing requires explicit opt-in to acknowledge potential precision loss.

```
cast:
  fields:
    - name: revenue
      to_type: decimal(19,4)
    - name: status
      to_type: string  # from integer status codes
```

### Mask / Hash / Redact

Privacy-preserving operators. Essential for compliance use cases.

```
mask:
  fields:
    email:
      strategy: hash(sha256)
    phone:
      strategy: redact(keep_last=4)
    ssn:
      strategy: remove
    address:
      strategy: replace(value="REDACTED")
```

Platform-provided strategies: `hash` (with algorithm and optional salt), `redact` (keep prefix/suffix characters), `remove` (null out), `replace` (static value), `tokenize` (deterministic pseudonymization using a platform-provided key). All implementations are deterministic (same input produces same output across runs) so destination-side PK merge still works correctly.

### Enrich (lookup join)

Join with reference data provided as a small configured table. For joining with large tables, use destination-side SQL — we are not a general join engine.

```
enrich:
  with: country_codes
  # Source data file registered with the pipeline.
  # Loaded into memory (held per-worker, limit 100 MB).
  on:
    left: country
    right: code
  add:
    - country_name
    - region
```

Reference data is supplied as a named artifact — typically a small CSV or Parquet file uploaded with the pipeline configuration. The platform materializes it into Arrow and holds it in worker memory for the duration of the activity.

Hard limit: reference data must be under 100 MB uncompressed. For larger joins, the customer has options: destination-side SQL, a lookup-performing UDF that uses external services, or building the lookup into their source system.

### Dedupe

Remove duplicate rows by primary key. Useful for sources that over-emit.

```
dedupe:
  by: [customer_id]
  keep: last_by(updated_at)  # or: first, any
```

This is a batch-local dedup; it does not do cross-batch dedup (destination MERGE handles that). Purpose: remove intra-batch duplicates that the loader's MERGE would otherwise struggle with (e.g., batches containing 10 updates for the same row — we keep the latest, loader MERGEs one row to destination).

### Flatten (struct / array unnesting)

Nested Arrow types → flat tabular.

```
flatten:
  field: address
  prefix: addr_   # address.street -> addr_street
```

For arrays:

```
unnest:
  field: tags
  emit: row_per_element
```

Unnesting expands one input row to N output rows. Downstream operators see the unnested structure.

### Split

Emit rows to different outputs based on a predicate.

```
split:
  outputs:
    active_users:
      predicate: "status = 'active'"
    inactive_users:
      predicate: "status = 'inactive'"
    other:
      default: true
```

Used to feed different destinations from a single extract.

### Add-column (computed expression)

Add a column whose value is a pure expression over existing columns.

```
add_column:
  name: is_premium
  expression: "plan_tier IN ('gold', 'platinum')"
  type: boolean
```

The expression language is the same small SQL-like subset as `filter`. Not Turing-complete; for anything beyond arithmetic and boolean logic, use a UDF.

### Validate

Assert row-level or batch-level invariants. Violations fail the pipeline (default) or quarantine to dead-letter.

```
validate:
  row_rules:
    - "email LIKE '%@%.%'"
    - "created_at IS NOT NULL"
    - "age BETWEEN 0 AND 150"
  on_violation: dead_letter  # or: fail
```

Dead-letter rows go to the dead-letter table (RFC 9, same mechanism as loader rejections). `validate` is a distinct operator (not a side effect of `filter`) because its semantics are "this should have been true, log if not" vs. filter's "drop these."

### Aggregate (limited)

Reduce rows. **Restricted** compared to general aggregate operators: only stateless, batch-local aggregation.

```
aggregate:
  group_by: [region, plan_tier]
  compute:
    user_count: count(*)
    avg_revenue: avg(revenue)
```

This is batch-local — each batch aggregates independently. Cross-batch aggregation is not supported (that's stateful stream processing, deferred). The use case is collapse-within-a-CDC-batch, per-stream summaries emitted alongside detail data, and similar patterns that don't need global state.

If a customer needs cross-batch aggregation, the answer is "do it at the destination" (BigQuery can aggregate 1B rows easily) or "use a downstream tool built for it." We do not pretend to be a stream processor.

## User-Defined Functions (UDFs)

When declarative operators don't cover a use case, users author UDFs.

### UDF types

Three UDF shapes:

- **Scalar UDF**: input is a column value, output is a column value. Called once per row. Example: complex string parsing, custom number formatting.
- **Batch UDF**: input is a batch (columns worth of data), output is a batch (same row count, same or modified columns). Called once per batch. Preferred for performance.
- **Transform UDF**: input is a batch, output is a batch (potentially different row count, different columns). Called once per batch. Most general; used when the transformation genuinely restructures data.

### Why batch UDF is preferred

A scalar UDF compiled to wasm has per-call overhead: argument marshaling, return-value marshaling, a host-guest boundary crossing. At millions of rows per batch this dominates. Batch UDFs cross the boundary once per batch and operate on columnar data inside wasm. Speed difference is orders of magnitude for CPU-bound work.

Our SDK provides ergonomic batch-UDF authoring: the user writes code that looks like "compute column X from columns Y and Z" and the SDK compiles it to a batch-granularity wasm call. Scalar UDFs are supported as a convenience for truly per-row logic (and discouraged in documentation).

### UDF interface (WIT sketch)

```wit
package platform:transformation@0.1.0;

world transformation {
  // Imports from platform host (restricted — RFC 5 transformation context).
  import platform:core/log;
  import platform:core/progress;
  import platform:core/errors;
  import platform:data/batches;

  // Exports.
  export describe: func() -> transformation-descriptor;
  export validate-config: func(config: config-value) -> result<_, config-error>;
  export process: func(ctx: process-context) -> result<_, process-error>;
}

record transformation-descriptor {
  name: string,
  version: string,
  shape: udf-shape,
  // Schema transformation rules the platform uses to infer output schema
  // from input schema — see "Static Schema Derivation" below.
  schema-derivation: schema-derivation,
  config-schema: json-schema,
}

enum udf-shape {
  scalar,     // row-at-a-time
  batch,      // batch-at-a-time, preserves row count
  transform,  // batch-at-a-time, row count may change
}

record process-context {
  config: config-value,
  input: batch-reader,     // stream of input batches
  output: batch-writer,    // stream of output batches
}
```

The UDF exports `process`, reads from `input`, writes to `output`. The wasm runtime wires these up per RFC 5.

### Capabilities denied in UDFs

Transformations run in **transformation context** (RFC 5), which denies:

- Network access (`platform:net/http` not linked).
- Secrets (`platform:secrets/access` not linked).
- State (`platform:state/cursor` not linked).
- Time (`platform:time/clock` not linked).
- Randomness (`platform:crypto/random` not linked).

This is enforced by the host not linking those interfaces, not by guest discipline. A UDF that tries to use them fails to instantiate with a clear error.

### Why these denials matter

Determinism is a platform guarantee. It's what makes:

- Temporal activity retries correct (the same input produces the same output).
- Pipeline rerun produce consistent destinations.
- Debug replay (replay a failed batch with known input) reproduce the failure.
- Schema evolution safe (we know what the output schema will be).

A UDF with access to time or randomness breaks all four.

### Explicit escape hatches

Some valid use cases appear to require time/network/state:

- **Current time.** If the UDF needs "now," it's provided as an input column (the host passes `activity_start_time` as a virtual column). This keeps determinism: the UDF's output depends on its input, not on when it ran.
- **Random values.** The host provides a deterministic pseudo-random value per row, seeded from row contents. If a UDF truly needs non-deterministic randomness (rare), it doesn't belong in the transformation stage.
- **Lookups.** Not supported in UDFs. Use the `enrich` operator (reference data) for small lookups; use destination-side joins for large lookups; push external-service calls to the source via pre-extraction if genuinely needed.
- **Stateful computation.** Not supported. Batch-local state only. Cross-batch state is stream processing.

## Static Schema Derivation

This is load-bearing for RFC 10 (schema evolution). The platform must compute "what will this pipeline's output schema be?" without running the pipeline.

### Declarative operators

Each declarative operator has a schema-transformation rule:

- `filter`: output schema = input schema.
- `project.keep=[a,b,c]`: output schema = input schema restricted to `[a,b,c]`.
- `rename.mapping`: output schema = input schema with renamed fields.
- `cast.fields`: output schema = input schema with updated types.
- `mask`: output schema = input schema (types may change: `hash` produces string; `tokenize` produces string; others preserve type).
- `enrich`: output schema = input schema + added columns from reference data.
- `dedupe`: output schema = input schema.
- `flatten`: output schema = input schema with nested field expanded.
- `unnest`: output schema = input schema with array field replaced by element schema.
- `split`: multiple output schemas, each equal to input schema (predicate doesn't change shape).
- `add_column`: output schema = input schema + new column (type from expression).
- `validate`: output schema = input schema.
- `aggregate`: output schema = `[group_by columns] + [computed columns]`.

The platform walks the DAG, composing these rules, and produces the output schema of each node.

### UDFs

UDFs declare their schema-derivation rule in their descriptor:

- **Identity** (input schema = output schema). Most batch UDFs that modify values in place.
- **Replace field** (input schema with one field replaced by output field). Most scalar UDFs.
- **Add field** (input schema + new field).
- **Drop field** (input schema minus a field).
- **Explicit** (the UDF declares its output schema statically, regardless of input). Used by transform UDFs that fully reshape data.
- **Derived from input** (the UDF provides code — compiled and run at derivation time — that computes output schema from input schema). Advanced; requires that the derivation code be deterministic and terminating.

Most UDFs fit one of the first four; explicit and derived are used sparingly.

### Schema drift through transformations

When a source schema changes (RFC 10), the platform re-derives the transformation's output schema and computes the diff at the loader's input. Policies apply the same way: additive source change producing additive loader input is auto-propagated; a breaking source change that breaks the transformation's assumption produces a pause with an explicit message ("source added column X; transformation 'redact-pii' has rule for column Y — applying additive propagation").

Transformations may have their own assumptions that source evolution breaks. Example: a `mask` operator configured for column `email`; source removes `email`. The transformation's configuration now references a non-existent column. Detected at schema derivation; pipeline pauses with the specific mismatch.

## Resource Limits

Transformations run in wasm instances per RFC 5 rules. Additional transformation-specific policies:

- **Default budget**: 60 seconds CPU, 512 MB memory per activity. Tunable per transformation, capped at platform maxima.
- **Input batch size hint**: transformations receive batches sized for the transformation's optimal throughput. Default: inherited from extract-stage sizing (RFC 3). Transformations that prefer smaller batches (because they explode row counts via `unnest`) can request smaller inputs.
- **Reference data memory**: `enrich` loads reference data into worker memory. Aggregate reference data across all active pipelines on a worker is capped at 1 GB per worker.

## Composition and Reuse

### Transformation libraries

A `TransformationPackage` can be a standalone package (pipeline-specific) or a published library. Published libraries:

- `platform/pii-masking@1.2.0`: standard PII masking patterns.
- `platform/gdpr-redaction@1.0.0`: GDPR-compliant field redaction.
- Customer-authored internal libraries shared across their pipelines.

A pipeline references a transformation package the same way it references a connector: by `(publisher, name, version_constraint)`.

### Parameterization

Transformation packages accept configuration. A single `platform/pii-masking` library might accept a list of fields to mask; different pipelines provide different lists. Config schemas are the same JSON Schema mechanism as connectors (RFC 6).

### Composition

A transformation package can invoke other transformation packages as sub-DAGs. This is how "compose pii-masking + gdpr-redaction" becomes "one composite transformation." Composition is declarative in the package manifest — no runtime resolution complexity.

## Debugging and Dev Experience

### Preview mode

A pipeline in configuration can run a **preview**: extract a small sample (default 1000 rows), run the transformation DAG, show the output. Preview runs do not load to the destination. They are free of side effects and run in a sandboxed context.

### Per-node observability

The platform records, for each transformation node, per-run metrics:

- Input row count and byte count.
- Output row count and byte count.
- Execution time.
- Expression compile cache hit rate.
- Dead-letter row count (for `validate` with dead-letter policy).

These surface as a DAG-shaped dashboard: which node is the bottleneck, which is dropping rows unexpectedly, which is producing unexpected row counts.

### Stepwise replay

For failed batches, the platform retains (subject to staging retention) the batch at each stage. Operators can request "replay this batch through this transformation" to reproduce failures. Since transformations are deterministic, a reproduced failure is the same failure.

### Test harness

Transformation packages ship with a test suite (publication gate). Tests provide input fixtures and expected outputs; the platform runs them in CI.

Required test categories:

1. **Happy-path**: representative input produces expected output.
2. **Schema-drift**: input with additional columns is handled per declared policy.
3. **Empty-input**: empty batch produces empty output (not an error).
4. **Null handling**: nulls in key columns are handled correctly per operator semantics.
5. **Large batch**: a batch at the expected max size doesn't exceed resource limits.
6. **Schema-derivation**: declared output schema matches actual output.

## Integration with Existing Subsystems

### Workflow (RFC 4)

Each transformation stage becomes one or more Temporal activities. For linear DAGs, one activity per stage is typical. For branching DAGs with heavyweight nodes, the platform may parallelize node execution across activities.

### Staging (RFC 3, RFC 14)

Transformation activities read from staging (extract output), write to staging (transformation output). Staging layout: `staging/{run_id}/extract/{stream}/` → `staging/{run_id}/transform/{node}/`. Loader reads from the transformation-output staging, not directly from extract.

### Catalog (RFC 10)

Transformation packages are catalog entities (`TransformationVersion`). Pipelines reference them. Pipeline versions pin to transformation versions.

### Schema evolution (RFC 10)

Output schemas are derived statically and registered in the catalog as the pipeline's effective source schema for loader purposes. Changes propagate through evolution policies the same as direct-source-to-loader pipelines.

### Loader (RFC 9)

Loaders see transformation output as their input. They do not know (or care) whether their input came from direct extract or through a transformation DAG. Same schema, same batches, same load semantics.

### CDC (RFC 8)

CDC events flow through transformations with full `_cdc.*` metadata preserved. Transformation operators respect CDC semantics:

- `filter` applied to a `_cdc.op = "d"` row produces a delete event if the filter matches, or drops the delete if it doesn't.
- `project` must preserve `_cdc.*` columns unless explicitly dropped.
- `validate` dead-letter-ing a delete event: the delete is sent to dead-letter and *not* applied to destination. This can cause destination drift (row still exists in destination that has been deleted in source). Surfaced as a warning when configuring.

Default operator behavior preserves `_cdc.*` automatically; user-configured projection must explicitly retain them.

### Secrets (RFC 11)

Transformations have **no access** to secrets. This is an RFC 5 enforcement. For transformations requiring cryptographic operations with a secret key (e.g., HMAC tokenization), the host pre-resolves the key from the backend and passes it to the transformation as a per-activity deterministic value — not as a reference the UDF can query. The key's value is present in the transformation's activity input; in Temporal history it appears as a `SecretValue` wrapper that does not serialize to plaintext. This narrow exception preserves the "no secrets in catalog / workflow history" invariant of RFC 11 by keeping the plaintext only in in-memory activity context.

Covered here specifically because tokenization with a secret key is a canonical and important use case; most transformations don't need secret material at all.

## Performance Expectations

Targets for a transformation activity (not yet benchmarked; aspirational):

- **Declarative-only DAG throughput**: at least 1M rows/sec per core for typical operator mix on ~100-column batches. DataFusion-comparable.
- **Batch UDF overhead**: <10 ms per batch-boundary crossing (mostly Arrow IPC serialization, per RFC 5 Tier 1).
- **Scalar UDF overhead**: 1-5 μs per row for typical UDF. (Why we prefer batch UDFs.)
- **Schema derivation time**: <100 ms for a DAG with 50 nodes.

Missing these targets by 2× is acceptable at launch; by 10× is not. The declarative path is where the most speed matters because it's what customers use most.

## Alternatives Considered

**Push transformations entirely to the destination (no in-pipeline transformations).** This is Fivetran's position and is defensible. Rejected for reasons above: PII masking, cost, multi-destination from one extract.

**Support only declarative operators; no UDFs.** Simpler, faster, more analyzable. Rejected: real use cases require custom logic, and we want to capture those use cases without forcing customers to build a separate pipeline stage.

**Use Python as the UDF language (Pyodide-compiled to wasm).** Most accessible language. Rejected at this level: Python via Pyodide is slower than compiled wasm from Rust/TinyGo, and Python's ecosystem assumes system calls we deny. Python UDFs can be supported through our multi-language SDK (RFC 5) — authors write Python, the SDK compiles to wasm — but Python is not privileged.

**Stateful transformations (stream-like).** Deferred. Cross-batch state is stream processing. The complexity is large and the launch use cases don't require it.

**Use SQL as the transformation language (DuckDB / DataFusion embedded).** Would give customers the full SQL surface. Rejected as the primary model because SQL doesn't compose well with our DAG-of-operators model and doesn't give us static schema derivation without a full SQL planner. SQL-compatible expression syntax in `filter`, `add_column`, and predicates is supported as a subset — we don't ship SQL-as-transformation at launch.

**Let loader do transformation (MERGE with column expressions).** Some operators could be pushed down. Rejected as the base model: destination-side transformation is destination-specific, doesn't compose cross-destination, and doesn't help the "transform before destination" use cases (masking, filtering for cost).

**Invent a new transformation IR.** Over-engineering. We use Arrow as the transformation substrate and DataFusion's expression model for predicates. Industry-standard components, no novel DSL.

## Open Questions

1. **SQL-as-transformation.** A customer subset clearly wants this: "let me write transformations in SQL, you compile them to your DAG." Possible future work; not a launch requirement. Requires significant planning (literally — a SQL planner).
2. **Cross-stream transformations.** A transformation that takes multiple streams as input and produces a joined output. Some join use cases are tractable (broadcast joins with one small stream) but joins in general are out of scope per stream processing deferral. Need a clear policy.
3. **Per-row side effects in transformations.** None. But the equivalent "emit this row to a monitoring channel" could be useful. Potentially via a `tap` operator that splits output to a secondary destination. Defer.
4. **Transformation versioning and destination-schema coupling.** A transformation version bump can change destination schema. Do we require destination-schema migration to be coordinated with the bump? Probably yes; how exactly is a coordination question with the catalog.
5. **Lazy vs. eager DAG execution.** Eager is simpler; lazy (with fusion) could be significantly faster for multi-operator DAGs by eliminating intermediate materialization. Punt; start eager, optimize later if bottlenecks appear.
6. **Reference data freshness.** `enrich` uses uploaded reference data. How is it refreshed? Manual re-upload? A separate pipeline populating it? Leave to pipeline configuration; worth a small feature RFC later.

## References

- Apache Arrow DataFusion: https://arrow.apache.org/datafusion/
- dbt (reference for destination-side transformation — the alternative we're not replacing): https://docs.getdbt.com/
- BigQuery Dataform: reference for another in-pipeline transformation approach.
- Singer transformations: prior art for ETL-embedded transformation.
- Polars: reference for columnar expression engines: https://pola.rs/
- Substrait: cross-engine relational IR — relevant if we ever move to a unified transformation IR: https://substrait.io/

## Decision

**Accepted pending review.** RFC 13 next: Pipeline DSL and Configuration Language — the authoring surface for pipeline definitions, which must compose cleanly with everything in RFCs 1-12.
