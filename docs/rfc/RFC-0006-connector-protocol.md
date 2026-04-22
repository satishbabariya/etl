# RFC 0006: Connector Protocol

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0003 (Data Interchange), RFC 0004 (Temporal Topology), RFC 0005 (Wasm Runtime)

## Summary

This RFC specifies the interface every connector implements: the WIT-defined contract exported by the guest and invoked by the host. It defines the connector lifecycle (configure → discover → read), the shape of configuration and state, the cursor model, schema negotiation, and the error taxonomy connectors must use.

A connector protocol has outsized impact on platform quality. Airbyte's per-record JSON-over-stdio protocol is a major source of their cost disadvantage. Fivetran's closed Java-based connector framework is a major source of their moat. Singer's tap/target spec is simple but leaves too many decisions ambiguous. Our protocol must be simpler than Fivetran's, faster than Airbyte's, and less ambiguous than Singer's.

## Motivation

The connector is where external reality meets our platform. Each connector faces real-world concerns: pagination, authentication refresh, rate limiting, partial failures, schema drift, incremental state, historical backfill, the gap between "what the API documents" and "what the API actually does." A good protocol makes these concerns explicit and gives connector authors tools to handle them; a bad protocol pushes them into connector-by-connector improvisation that produces inconsistent behavior across the connector library.

We need the protocol to satisfy:

1. **Language-agnostic authoring.** The WIT interface must be expressible and idiomatic in Rust, TypeScript, Go, Python, and any future wasm-targeting language we care about.
2. **Streaming by default.** A connector returns batches as it produces them, not all-at-once. Memory bounded, latency visible.
3. **Explicit state ownership.** The connector does not own durable state. The host owns it. Connectors compute state updates and return them; the host persists them atomically with the corresponding data.
4. **Incremental and full-refresh modes, both first-class.** Incremental is the economic win; full-refresh is the correctness fallback. Both paths must be well-specified.
5. **Schema as data, not as code.** Schemas are declared, discovered, and evolved as values — so the platform can inspect, diff, and persist them — not as guest-side code decisions.
6. **Partial-success semantics.** A connector can emit N batches and then hit an error; the host must be able to commit the N batches and resume from the failure point. Not "all or nothing."

## Non-Goals

- CDC (change-data-capture) specifics are in RFC 8. This RFC defines the common protocol that CDC connectors *also* implement, plus points where CDC connectors extend it.
- The connector registry (packaging, signing, distribution, versioning of individual connectors) is a later RFC. This RFC covers the runtime protocol, not the supply chain around it.
- This RFC does not list the launch connectors or their individual quirks. Each connector has its own documentation; this RFC is the interface they all share.
- Destination delivery is RFC 9. Connectors emit; loaders consume.

## Design Principles

### Small export surface, large import surface

The connector *exports* a small number of functions (effectively: `discover`, `read`). It *imports* a rich host API (RFC 5) for HTTP, secrets, state, logging. This asymmetry is deliberate. The export surface is the "what the connector is"; the import surface is "what the connector has access to." Small exports make connectors easy to author and test. Large imports make the host's capabilities reusable across all connectors.

### Pull-based, not push-based

The host pulls batches from the connector. The connector does not push into a sink. Reasons:

- Backpressure is natural: the host stops pulling when it needs to.
- The host controls rate: if the downstream loader is slow, the connector isn't driving into nowhere.
- Cancellation is straightforward: the host stops pulling and terminates.

### Everything in the protocol is serializable

Configuration, state, schema, cursor values — all values that cross the host-guest boundary or get persisted are serializable and visible to the host. The host can log them, diff them, snapshot them for debugging. Connectors that try to hide internal state in opaque blobs defeat this; the protocol actively prevents it by defining state shape at the protocol level.

## The Protocol (WIT sketch)

The full WIT is long; this section shows the essential shape. The canonical `.wit` is published in the platform SDK.

```wit
package platform:connector@0.1.0;

// The connector world: what imports are available, what exports are required.
world connector {
  // Host imports (from RFC 5 Host API).
  import platform:core/log;
  import platform:core/progress;
  import platform:core/errors;
  import platform:data/batches;
  import platform:net/http;
  import platform:secrets/access;
  import platform:state/cursor;
  import platform:time/clock;
  import platform:crypto/random;

  // Connector exports.
  export describe: func() -> connector-descriptor;
  export validate-config: func(config: config-value) -> result<_, config-error>;
  export discover: func(ctx: discovery-context) -> result<catalog, discover-error>;
  export read: func(ctx: read-context) -> result<_, read-error>;
}

record connector-descriptor {
  name: string,
  version: string,
  sdk-version: string,
  supported-modes: list<sync-mode>,
  config-schema: json-schema,
  documentation-url: option<string>,
}

enum sync-mode {
  full-refresh,
  incremental-cursor,
  incremental-cdc,
}

record discovery-context {
  config: config-value,
  // Optional: existing catalog from a prior discovery, so the connector
  // can do schema-preserving re-discovery instead of from-scratch.
  prior-catalog: option<catalog>,
}

record catalog {
  streams: list<stream-descriptor>,
  discovered-at-utc-micros: u64,
}

record stream-descriptor {
  name: string,                        // canonical, unique within catalog
  namespace: option<string>,           // e.g., database schema
  schema: arrow-schema-ipc,            // serialized Arrow schema
  primary-key: option<list<string>>,   // null = no PK
  supported-modes: list<sync-mode>,
  cursor-fields: list<cursor-field-descriptor>,   // for incremental-cursor mode
  source-metadata: list<tuple<string, string>>,   // free-form, used by catalog service
}

record cursor-field-descriptor {
  field-path: string,                  // e.g., "updated_at"
  type-hint: cursor-type-hint,
  monotonicity: monotonicity,
}

enum monotonicity {
  strictly-increasing,                 // safe for resumable cursor
  non-decreasing,                      // safe with duplicate handling
  unreliable,                          // use with caution
}

record read-context {
  config: config-value,
  selected-streams: list<selected-stream>,
  // Limits the host imposes on this invocation.
  limits: read-limits,
  // If present, host is asking for a bounded chunk of work
  // (used for long-running extracts; see "Bounded Read" below).
  bound: option<read-bound>,
}

record selected-stream {
  name: string,
  namespace: option<string>,
  mode: sync-mode,
  mode-state: mode-state,              // cursor position, snapshot phase, etc.
  projection: option<list<string>>,    // null = all columns
  // Schema the host expects the connector to emit.
  // On mismatch, the connector emits a schema-change event instead of data.
  expected-schema: arrow-schema-ipc,
}

variant mode-state {
  full-refresh-fresh,
  full-refresh-resume(snapshot-token),
  incremental-cursor(cursor-position),
  incremental-cdc(cdc-position),
}

record read-limits {
  max-batches: option<u32>,
  max-rows: option<u64>,
  max-bytes: option<u64>,
  max-wall-time-ms: option<u32>,
}

record read-bound {
  // Bound descriptors are per-stream; the connector should stop emitting
  // a stream once its bound is reached.
  per-stream: list<tuple<string, stream-bound>>,
}
```

The above is abbreviated. The essential shape: four exports (`describe`, `validate-config`, `discover`, `read`), two data-carrying types (`catalog`, `read-context`), and explicit mode and state machinery.

## Lifecycle

### 1. `describe` — static self-description

Called once per (connector, version), typically at registry publish time or worker startup. Returns a static descriptor: name, version, supported sync modes, JSON Schema for configuration.

This call is pure: no network, no config, no state. Its output is cacheable and part of the connector's published metadata.

### 2. `validate-config` — configuration validation

Called when a user saves or edits a connection configuration. Takes a proposed config, returns success or a structured error pointing at which fields are wrong and why.

This call *may* make network requests (to verify credentials) but must complete in <30 seconds. It does not read data; it does not establish a sync. It's the "can I at least authenticate?" check.

### 3. `discover` — catalog discovery

Called on demand (when a user sets up a new pipeline or requests a re-discovery). Connects to the source, enumerates available streams, emits schemas.

Key rules:

- Discovery is read-only. A well-behaved connector does not create, modify, or delete source-side objects during discovery.
- Discovery is **merge-friendly**. If the host provides a `prior-catalog`, the connector should preserve stream names and cursor field selections where compatible, so downstream pipeline configurations don't churn.
- Discovery time scales with source size. A connector against a 10,000-table database takes longer than one against a SaaS app with 20 entities. Connectors must heartbeat through the host API during long discoveries. The host applies a default 10-minute cap per discovery; connectors that genuinely need more request it via manifest declaration.

### 4. `read` — the hot path

Called by the host inside the extract activity (RFC 4). Produces batches for the selected streams and updates state as it goes.

The `read` call returns when:

- All selected streams are exhausted (for full refresh or bounded incremental reads), OR
- The host-imposed limits are hit (max batches, max rows, max bytes, max wall time), OR
- A non-retriable error occurs.

The call does **not** return one batch per invocation. One `read` call produces many batches through the `platform:data/batches` writer resource (RFC 5). This is the streaming pattern: the connector calls `batch-writer.write(b)` repeatedly, and each write is an observable emission that the host can consume and stage incrementally.

## The Cursor and State Model

Cursors are how we resume work mid-flight. Their design is the most subtle part of this RFC.

### State lives at the host, computed by the connector

The connector does not persist its own state. Every `read` invocation starts with state passed in via `mode-state`, and the connector emits updates to state through a dedicated state-emission interface as it makes progress. The host persists these updates.

```wit
interface platform:data/state-emission {
  record state-update {
    stream: string,
    new-mode-state: mode-state,
    commit-with: commit-fence,
  }
  enum commit-fence {
    // Update can be committed as soon as preceding batches are durable.
    at-batch-boundary,
    // Update must wait until the read call fully completes.
    at-read-completion,
  }
  emit: func(update: state-update)
}
```

The `commit-fence` matters because cursors often have partial-progress semantics (e.g., "we've processed all rows with `updated_at <= T`") that are only meaningful at stable batch boundaries, not mid-batch.

### Three state flavors

**Full-refresh state** carries minimal information — whether the refresh is fresh or resuming from a snapshot token. Full refreshes are atomic at the stream level: either the whole stream loaded or the stream is re-loaded from scratch.

**Incremental-cursor state** carries a cursor value — usually a timestamp, integer, or opaque string. The connector emits rows with cursor-field values ≤ high water, advances the cursor to the new high water, emits a state update.

Two sub-flavors:

- *Strictly increasing cursors* (e.g., monotonic IDs) can resume safely by filtering `cursor_field > last_cursor_value`.
- *Non-decreasing cursors* (e.g., `updated_at` timestamps where multiple rows can share a value) require overlap handling: the connector re-reads rows at the cursor boundary on resume and deduplicates by primary key. This is a protocol-level pattern; the host trusts the connector to handle it.

**Incremental-CDC state** carries a log position (LSN, binlog coordinate, SCN). Details in RFC 8; the protocol slot is defined here.

### Cursor shape is opaque to the host

From the host's perspective, cursor state is a typed-but-connector-specific bag. The host serializes and persists it; it does not interpret it. This is deliberate: the host does not try to understand Oracle SCN semantics vs. Postgres LSN semantics. It hands state to the connector and trusts the connector to know what it means.

Consequence: cursor state is versioned by connector version. If a connector bumps its major version with a state-format change, the host treats the pipeline as requiring a one-time reinitialization (full refresh + catch up), because old state is not readable by new connector code. Minor version changes must be state-format-compatible.

### State is scoped per stream, not per connection

A pipeline syncs many streams from one connection. Each stream has its own independent state. A failure in stream A's sync does not affect stream B's cursor. This lets the host parallelize streams and commit their progress independently.

## Schema Handling

### Schemas are declared in the catalog and re-declared at read time

Discovery produces schemas. Read-time context carries an `expected-schema` per stream. The connector compares the source's current shape to the expected schema. Three outcomes:

1. **Match.** Emit batches conforming to the expected schema.
2. **Compatible drift.** Source has added a column, or widened a type compatibly. Connector emits a `schema-event` signaling the change, then emits batches conforming to the *new* schema (which is a superset of the expected). The host handles the evolution per RFC 10.
3. **Incompatible drift.** Source has dropped a column, renamed one, or changed a type incompatibly. Connector emits a `schema-event` signaling the incompatibility and returns a typed error. The host pauses the pipeline and requires human intervention.

```wit
interface platform:data/schema-events {
  record schema-event {
    stream: string,
    change: schema-change,
    detected-at-utc-micros: u64,
  }
  variant schema-change {
    field-added(field-info),
    field-removed(string),                  // field name
    field-type-changed(tuple<string, arrow-data-type, arrow-data-type>),
    field-nullability-changed(tuple<string, bool>),
    primary-key-changed(option<list<string>>),
    full-reshape(arrow-schema-ipc),         // new schema, connector gives up on diffing
  }
  emit: func(event: schema-event)
}
```

The host decides what to do with incompatible changes (RFC 10). The connector's responsibility ends at accurately reporting them.

### No implicit schema fixing

The connector does not silently coerce data to fit a schema that no longer matches. If the source has a string where the schema says integer, the connector emits a schema event and stops, rather than emitting bad data. This prevents the "Fivetran quietly stringified my column" class of incident.

## Error Taxonomy

Errors are typed. The connector throws typed errors that the host's retry policy understands.

### Error categories

- **`transient`** — network hiccup, temporary 5xx. Host retries per activity retry policy (RFC 4).
- **`rate-limited(retry-after)`** — source has told us to back off. Host respects `retry-after` if provided.
- **`auth-failed`** — credentials invalid or expired. Not retried. Host alerts the operator.
- **`config-error`** — connector configuration is wrong (e.g., account doesn't have permission on stream X). Not retried. Host marks pipeline as needing config update.
- **`schema-incompatible`** — discovered source schema can't merge with destination expectation. Host pauses pipeline for operator review.
- **`source-corrupted`** — connector could not make sense of source response (malformed data). Not retried by default; operator decision.
- **`quota-exceeded`** — source-side quota (common for some SaaS). Typed separately from `rate-limited` because quotas are usually not retry-after-bounded.
- **`source-unavailable`** — source is clearly down (connection refused, DNS, etc.). Retried with longer backoff.
- **`unexpected`** — fallback for connector bugs. Retried once, then escalated.

Each error carries a human-readable message, an optional details map, and a `retriable` boolean that the connector sets (host's retry policy is advisory but the connector can override for its specific knowledge, e.g., "this 500 is actually permanent, don't retry").

### Partial success

A connector can emit N batches and then fail. The host commits the N batches' progress (via the state updates emitted along with them) and retries from the emitted state. The failure does not invalidate successful batches.

This requires connectors to emit state updates at safe points — typically after each batch, marked `at-batch-boundary`. Connectors that only emit final state (at-read-completion) force whole-call retries on failure. Batch-boundary emission is the recommended pattern, enforced by default in the language SDKs.

## Bounded Reads (Long-Running Extracts)

Referencing RFC 4's iterator activity pattern: the host may invoke `read` with a `read-bound` that limits work per invocation. The connector:

- Processes streams until any bound is hit (max batches, max rows, max wall time, or per-stream bound).
- Emits final state updates before returning.
- Returns success; the host decides to invoke `read` again with the advanced state.

A well-behaved connector responds to bounds within the batch it's currently emitting — it does not start a new batch if any bound would be exceeded. This keeps progress granular.

### Why bounded reads matter

Historical backfills of a 10-billion-row source can take days. Temporal activities should not take days (RFC 4). Bounded reads let the host drive long-running work in chunks, heartbeating and checkpointing between chunks, without the connector needing to know about Temporal at all. From the connector's perspective, it got asked to do a bounded amount of work; it did it and returned.

## Configuration

Configuration is a JSON value whose schema is declared by the connector (returned from `describe`). The host validates config against the schema before invoking the connector; the connector re-validates in `validate-config` (which also does live checks like auth).

### Secrets in configuration

Config fields can be marked as secret in the connector's JSON Schema (via a custom `secret: true` annotation). The UI treats these fields as password inputs. At runtime, the host *does not* inline secret values into config; it substitutes **secret references** (opaque IDs). The connector resolves references via `platform:secrets/access` in `read`. This keeps plaintext secrets out of the config blob that gets logged, diffed, versioned, and serialized into Temporal history.

Connectors that include a plaintext secret in a non-secret config field are non-compliant and will fail publication review.

### Config versioning

Config schema evolves with connector versions. Rules:

- **Adding a field with a default:** non-breaking. Existing pipelines pick up the default.
- **Adding a required field:** breaking. New connector major version; existing pipelines require re-validation before upgrade.
- **Removing a field:** breaking.
- **Changing a field's type:** breaking.
- **Renaming a field:** breaking; an alias-based migration path may be provided at the author's discretion.

## Connector Identity and Namespacing

Every connector is identified by `{publisher}/{name}@{version}`, e.g., `platform/postgres@2.3.1` for a first-party Postgres connector or `acme-corp/quickbooks-enhanced@0.4.0` for a customer-authored one.

- `publisher` namespaces prevent collision and identify trust tier (platform-published vs. third-party).
- `name` is the connector family name.
- `version` is semver.

Pipelines pin to a connector identity. Upgrading a pipeline to a new connector version is an explicit operator action (RFC 4's versioning discussion applies here too).

## Testing Contract

Every connector ships with a test suite that the platform enforces as a publication gate. Required tests:

1. **`describe` round-trip:** `describe()` output is valid per the platform's descriptor schema.
2. **`validate-config` positive and negative:** at least one valid and one invalid config fixture, both producing the correct outcome.
3. **`discover` against recorded fixtures:** recorded HTTP responses feed a mocked host; `discover` produces the expected catalog.
4. **`read` happy path:** recorded fixtures exercise at least one full-refresh and one incremental read per supported stream, producing the expected batches and state updates.
5. **`read` resume:** given an intermediate state, `read` produces only new rows.
6. **Error injection:** for each error category in the connector's declared set, a test reproduces the condition and verifies correct typed error emission.
7. **Schema-change handling:** a fixture where the source schema differs from the expected schema produces a `schema-event`, not silent data emission.

Tests run in CI in the publication pipeline. Failing tests block publication. These are the bare minimum; connector authors are encouraged to test more aggressively.

## Host Responsibilities (the other side of the contract)

For completeness, the host commits to:

1. **Calling only allowed lifecycle transitions.** `describe` any time; `validate-config` in config flows; `discover` before/during pipeline setup and on demand; `read` during pipeline runs.
2. **Never mutating guest state between calls.** State emitted by the connector is persisted verbatim and passed back on the next call.
3. **Honoring declared limits.** If the connector manifest declares "my read activity needs 1GB memory," the host provides it or fails clean.
4. **Surfacing logs and progress reports.** Log entries and progress updates emitted via the host API appear in operator-facing observability.
5. **Persisting state atomically with corresponding data.** A state update associated with a batch is only committed when the batch is committed. A connector does not see a state "commit" without the data being durable.
6. **Respecting schema events.** When a schema event is emitted, the host acts on it per the pipeline's schema evolution policy (RFC 10), not silently.

## First-Party Connector Path

First-party connectors are compiled to wasm from Rust and execute through the same wasm runtime as third-party connectors. This is the **invariant**: we do not maintain a separate Rust-native code path for first-party connectors.

Why not: two code paths means two sets of bugs, and it removes our internal incentive to keep the wasm path fast. By dogfooding the wasm path on our own connectors, we guarantee it stays excellent.

Exceptions: extremely performance-sensitive destination loaders are **not** wasm (RFC 9). Connectors are uniformly wasm.

The first-party connector repository uses the same SDK and testing infrastructure as third-party connectors, plus internal standards for test coverage, fixture recording, and security review.

## Alternatives Considered

**Adopt Singer / Airbyte protocol directly.** Singer's tap/target spec is subprocess-based and row-at-a-time JSON. Airbyte extends it with JSON Schema and state messages but keeps the subprocess + line-delimited JSON model. Rejected: both pay serialization overhead on every row, preventing us from delivering the performance wedge. We also want structured types (via WIT) that JSON can't express well.

**Adopt Fivetran's connector SDK.** Fivetran's SDK is Java and deeply coupled to their runtime; adoption would lock us into their runtime properties. Rejected.

**Use a higher-level abstraction (e.g., "connector describes its data source; platform generates `read`").** Tempting: we write Postgres-connector-like logic once and generate connectors for all SQL sources. Rejected as a core protocol: the abstraction leaks for non-SQL sources (SaaS APIs, file sources, CDC streams). We can build such higher-level *tooling* on top of the protocol, but the protocol itself must be low-enough-level to support all source shapes.

**Make `read` return a single batch per call, host calls in a loop.** Simpler protocol surface; one batch per function call. Rejected: cross-boundary call overhead dominates at high batch rates. A single `read` call emitting many batches via the writer resource is one boundary crossing; the alternative is N crossings. Also, connectors benefit from maintaining within-call state (parsed pagination cursors, open connections) that would need to be re-serialized on every call.

**Rich typed state schemas in the protocol (non-opaque).** The host understands cursor types and can reason about them. Rejected: the host should not grow to understand Oracle SCN semantics, MySQL binlog coordinates, Postgres LSN, Salesforce `SystemModstamp`, etc. Opaque state with connector-internal semantics is the right level.

**Mandatory primary keys on every stream.** Would simplify deduplication logic platform-wide. Rejected: real sources have PK-less data (event streams, log-shaped exports). The protocol must represent them; pipelines using them accept restricted sync modes (full refresh only, typically).

## Open Questions

1. **Connector-level caching of expensive calls.** Some connectors compute expensive things once per `read` (e.g., token introspection). Should the protocol offer a "connector-lifetime cache" beyond instance lifetime? Defer; the `platform:state/cursor` API can cover most cases.
2. **Parallel stream reads within a single `read` call.** Currently one `read` call, many streams, but we don't specify whether the connector reads them in parallel or serially. The connector decides, but should there be a host-provided parallelism hint? Likely yes; leave as SDK convention for now.
3. **Streaming-push fallback for sources that can't be pulled.** Some sources (webhooks) are push-only. We'd need a separate "inbound" model. Defer to a dedicated RFC if/when we add webhook-source support.
4. **Config-dependent streams.** Connectors whose stream set depends on config (e.g., "connect to database X; which tables depends on X's schema"). Handled today by `discover` running with config. Edge case: config changes should trigger re-discovery. Flag for the catalog-service RFC.
5. **Very-large-row handling at the connector boundary.** Rows bigger than the Arrow batch target (flagged in RFC 3). Does the connector emit them as separate single-row batches? As references to sidecar blobs? Defer until we have empirical cases.

## References

- Apache Arrow ADBC (another take on a pull-based connector API, narrower scope): https://arrow.apache.org/adbc/
- Singer Specification: https://github.com/singer-io/getting-started
- Airbyte Protocol: https://docs.airbyte.com/understanding-airbyte/airbyte-protocol
- Fivetran Connector SDK (public docs): Fivetran's developer site.
- Debezium connector model (for CDC reference): https://debezium.io/documentation/
- WIT language reference (from RFC 5): https://component-model.bytecodealliance.org/design/wit.html

## Decision

**Accepted pending review.** RFC 7 next: Incremental Sync and Cursor Semantics — which drills into the correctness properties of cursor-based incremental extraction that this RFC sketched at the protocol level.
