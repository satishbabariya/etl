# RFC 0015: Observability, Lineage, and Audit

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0002 (Core Architecture), RFC 0004 (Temporal Topology), RFC 0008 (CDC Architecture), RFC 0010 (Catalog), RFC 0011 (Secrets), RFC 0014 (State Storage)

## Summary

This RFC specifies the three observability surfaces the platform exposes: **operational observability** (what operators use to debug and tune — metrics, logs, traces), **data observability** (what customers see about their pipelines — run status, data volume, schema changes, lineage), and **audit** (what compliance teams and regulators see — who did what, when, with what data). It defines the metric and log schemas at a structural level, the ingestion and query paths, the retention and access model, the lineage derivation from run events, and the tamper-evident audit pipeline.

Observability is not a cross-cutting concern in the sense of "added after the fact." Every prior RFC has created observability requirements; this RFC makes them coherent and gives them a single set of ingestion/storage/query paths.

## Motivation

In a data platform, observability has three distinct audiences with overlapping but not identical needs:

1. **Operators (us).** When a pipeline breaks, we need to know what broke, where, why, and whether it's our problem or the customer's source/destination. Operational signals are dense, noisy, and short-retention.
2. **Customers.** They need to know their pipelines are running, their data is arriving, their schemas are drifting or not. Customer-facing signals are summarized, clean, and moderate-retention.
3. **Auditors.** They need evidence that specific things happened (or didn't) for compliance, forensic investigation, or contract dispute. Audit is append-only, tamper-evident, and long-retention.

A naive implementation builds one of these and shoehorns the others in. The result is that operators have access to raw logs that should be private to customers, customers have sparse dashboards backed by "the same thing the operators see," and auditors are handed whatever can be excavated from operational logs during an incident. We avoid this.

Additionally, observability is where the platform's reliability claims meet reality. "We guarantee at-least-once delivery" is a promise; the metric "rows emitted vs. rows loaded per run" is evidence. Without the evidence, the promise is marketing.

## Non-Goals

- This RFC does not evaluate specific observability vendors (Datadog, New Relic, Honeycomb, Grafana Cloud) by name. We specify the protocols we emit (OpenTelemetry, Prometheus); customers and we choose compatible backends.
- This RFC does not cover alerting rules content. We specify the infrastructure; specific alerts and their thresholds are an operational concern.
- This RFC does not cover incident response procedures. That's an operational runbook, downstream of this RFC.
- This RFC does not cover machine learning on observability data (anomaly detection, etc.). Interesting future work; not launch scope.
- This RFC does not cover the user-facing UI for dashboards. Product surface; downstream.

## Design Principles

**Three audiences, three planes, one source of truth.** Operational, customer-facing, and audit surfaces all derive from the same underlying event stream, but are processed, retained, and exposed through distinct pipelines with distinct access controls.

**Standardize on OpenTelemetry.** We emit OTel metrics, OTel logs, and OTel traces. No proprietary protocols. Customers can tap our data plane's OTel stream directly if they want to integrate with their existing observability stack.

**Separate audit from observability.** Audit has stronger durability, tamper-evidence, and retention requirements than operational or customer-facing observability. It runs on its own pipeline (as RFC 11 commits) and is never conflated with the others.

**Every signal is tenant-tagged.** Every metric, log line, trace span, and audit event carries a tenant identifier. Multi-tenant queries are structurally impossible without explicit cross-tenant permissions.

**No customer row data in observability.** Observability sees metadata (row counts, byte counts, schema fingerprints, error types) — never row contents. A log line cannot contain a customer's email address because logs are not the right place for row data even for legitimate purposes. If a customer needs to see specific rejected rows, the dead-letter system (RFC 9) is the right surface.

**Customer visibility has a structured, versioned API.** Customers don't query our Prometheus directly or grep our logs. They see a curated API that we can evolve without breaking their integrations, and that we can rate-limit, authorize, and cache.

## The Three Planes

### Operational observability

**Audience:** our engineers, SREs, support staff.

**What:** raw metrics at all cardinalities, detailed structured logs, distributed traces across the entire request path, Temporal workflow histories, wasm runtime diagnostics.

**Where:** our internal observability stack. The specific backend is a deployment choice (RFC 18); the architecture is uniform.

**Retention:** 30d hot, 13mo downsampled for metrics; 30d for logs; 7d for traces.

**Access:** internal authenticated users only. Per-tenant filtering via authorization (support for tenant X can see tenant X's operational data when actively supporting; auditing on access).

### Customer-facing data observability

**Audience:** the customer operating the pipeline.

**What:** run outcomes, data volume moved, schema change events, lineage graph, pipeline health, error summaries, dead-letter row counts and contents, historical trend data.

**Where:** the customer UI + a stable API that they can integrate with their own tools.

**Retention:** per plan tier — 30d/90d/1y standard retention for hot access; archive available in enterprise tier.

**Access:** per customer, via the platform's normal auth/RBAC (RFC 2).

### Audit

**Audience:** customer's compliance team, our compliance staff, regulators (via the customer), potentially courts.

**What:** every security-relevant event — secret accesses, configuration changes, user actions, data residency movements, access to customer data by us.

**Where:** append-only audit store with tamper-evidence (RFC 11 committed to hash-chained events with external anchors).

**Retention:** 7 years default, longer for specific compliance tiers.

**Access:** customer audit admins (their own tenant's data); our compliance team (with its own audit trail); regulators via customer-mediated request.

## The Event Bus

A single event bus is the substrate from which all three planes derive. Events are emitted by the data plane and control plane; routed through a per-tenant queue; processed by fan-out workers that populate each plane's stores.

### Event envelope

Every event has a common envelope:

```
Event {
  event_id: UUID (time-ordered),
  occurred_at: Timestamp,
  tenant_id: UUID,
  workspace_id: Option<UUID>,
  source: EventSource,   // worker, scheduler, catalog, auth, etc.
  kind: string,          // typed event name
  attributes: map<string, AttributeValue>,
  resource: ResourceRef, // what the event is about
  severity: enum(trace, debug, info, warn, error, critical),
  // For audit-relevant events:
  actor: Option<Actor>,
  outcome: Option<Outcome>,
}
```

Structured, typed, tenant-scoped, traceable.

### Emission discipline

Events are emitted by the component at the point the event occurs. Rules:

- **Non-blocking.** Emitters write to a local in-process buffer; a background task flushes to the bus. An observability backend outage never blocks the hot path.
- **Bounded buffering.** If the bus is unreachable, each emitter buffers up to a cap (default 10 MB). Beyond the cap, older non-audit events are dropped (logged as a "drop counter" metric). Audit events are never dropped — they spill to local durable storage and retry.
- **No synchronous dependencies.** A data-plane worker's pipeline activity does not wait for observability emissions to succeed. Observability failure is degraded mode, not operational failure.

### Routing

An event's `kind` plus its attributes determine which plane(s) it's routed to. Most events go to operational only. A smaller set (run outcomes, schema changes, data volumes) go to customer-facing. A smaller set still (secret accesses, config changes, cross-tenant accesses) go to audit. An event can be routed to multiple planes.

Routing is declarative: a routing table (`EventRoutingTable`) maps event kinds to target planes. The routing table is versioned and changes are audited.

## Metrics

Metrics are the primary operational signal. High-volume, low-per-record-value, aggressive retention.

### Naming and labels

Metric names follow a `platform_<component>_<unit>` pattern:

- `platform_worker_activity_duration_seconds`
- `platform_worker_batch_rows_emitted_total`
- `platform_loader_merge_duration_seconds`
- `platform_cdc_slot_lag_bytes`
- `platform_temporal_workflow_history_events_total`

Every metric has a standard label set:

- `tenant_id`
- `workspace_id`
- `pipeline_id` (where relevant)
- `region`
- `deployment_mode` (hosted / byoc / self_hosted)

Component-specific labels add dimensional detail (e.g., `connector_name`, `stream_name`, `loader_destination`).

### Cardinality discipline

Cardinality is the cost driver for metrics. Rules:

- No label values with unbounded cardinality (row IDs, timestamps, free-form strings from user data).
- `tenant_id` is bounded by our customer count; acceptable.
- `pipeline_id` is bounded per tenant; acceptable at our scale.
- `stream_name` is free-form but bounded per tenant; capped at 10,000 distinct values per tenant per day (beyond cap, rolled up to `_other_`).
- `error_kind` is enumerated, bounded.
- No per-batch or per-row labels ever.

### Core metric set

The metrics the platform emits by default, grouped by component:

**Worker:**
- `worker_activity_duration_seconds` (histogram)
- `worker_activity_attempts_total` (counter, by outcome)
- `worker_instance_pool_size` (gauge)
- `worker_memory_committed_bytes` (gauge)
- `worker_wasm_compile_duration_seconds` (histogram)

**Connector:**
- `connector_read_duration_seconds` (histogram)
- `connector_batch_rows_emitted_total` (counter)
- `connector_batch_bytes_emitted_total` (counter)
- `connector_http_requests_total` (counter, by response-class)
- `connector_http_request_duration_seconds` (histogram)

**Loader:**
- `loader_load_duration_seconds` (histogram)
- `loader_rows_loaded_total` (counter)
- `loader_bytes_loaded_total` (counter)
- `loader_merge_duration_seconds` (histogram, where applicable)
- `loader_dead_letter_rows_total` (counter)

**CDC:**
- `cdc_slot_lag_bytes` (gauge)
- `cdc_slot_lag_seconds` (gauge)
- `cdc_events_processed_total` (counter, by op type)
- `cdc_snapshot_progress_ratio` (gauge, during snapshot phase)

**Temporal:**
- `temporal_workflow_running_count` (gauge)
- `temporal_workflow_history_size_bytes` (gauge)
- `temporal_activity_schedule_to_start_seconds` (histogram)

**Catalog:**
- `catalog_api_request_duration_seconds` (histogram, by endpoint)
- `catalog_db_connection_pool_active` (gauge)

**Observability meta:**
- `observability_events_dropped_total` (counter, by reason)
- `observability_buffer_bytes` (gauge)

Full list is longer but these anchor the shape. Each metric's definition lives in a versioned metrics-schema document that the platform publishes.

### Scraping / push

We emit via OTel. Standard choices:

- **Push model by default** — workers push to an OTel collector running in the data plane.
- The collector aggregates and forwards to the tenant's chosen backend (ours, theirs, or both).

Customer-facing Prometheus scrape endpoints are available on request for direct integration.

## Logs

Logs are the second operational signal. Lower volume than metrics, higher per-record value. Primarily used for incident investigation.

### Structured logging

All logs are structured JSON. No free-form string logs. Fields:

- Standard envelope (event ID, tenant, timestamp, severity, component).
- Typed attributes.
- Exception details where applicable (type, message, stack, cause chain).
- Correlation IDs: `run_id`, `workflow_id`, `activity_id`, `batch_id`.

### Sensitive data discipline

- No row-level data in logs. Enforced at the logger level (RFC 11): `PlaintextSecret` and row-batch types do not implement the logger's value trait; attempting to log them fails at compile time.
- URL parameters may contain secrets (OAuth tokens in query strings); URLs are sanitized before logging via a known-parameter denylist.
- Customer error messages from external services (e.g., database error messages) can contain row values; these are logged at DEBUG level only and retained with stricter access control.

### Log levels

- `TRACE`: wire-level detail, off by default, enabled for targeted debugging.
- `DEBUG`: detailed per-operation logs, enabled in dev/staging, off in prod steady state.
- `INFO`: per-activity summaries, significant state transitions, always on.
- `WARN`: recoverable problems (retried errors, rate limits hit, schema drift detected).
- `ERROR`: unrecoverable operation failures (activity failed, cannot retry).
- `CRITICAL`: platform-level failures (cluster issues, invariant violations).

### Retention

30 days default. Per-tenant retention tier may extend. Logs older than retention are deleted; they do not go to audit (audit is a separate pipeline, not a log archive).

## Traces

Distributed tracing for request-path observability.

### Trace scope

A trace follows a logical operation across components:

- User API call → Catalog API → database.
- Scheduler trigger → Temporal workflow start → activity → worker → connector → HTTP calls → staging write.
- Loader invocation → destination SQL → destination response.

Each becomes a trace; spans correspond to each component's work.

### Sampling

Full traces at low volume would overwhelm the trace store. We sample:

- **Head-based sampling** for steady-state: 1% of requests/workflows traced.
- **Tail-based sampling** for error paths: 100% of traces where any span reports an error.
- **Per-tenant override** in incident response: 100% sampling for a specific tenant for the duration of an investigation.

Sampled-out traces are still counted in metrics; traces are for debugging, not accounting.

### Propagation

Standard W3C Trace Context. Propagated through Temporal's context-passing mechanisms into activities, and via HTTP headers when calling external services (where the external service supports it).

### Trace-log correlation

Log records include the current trace ID and span ID. A span-level error in the trace UI links directly to the corresponding logs.

## Customer-facing data observability

This is the product surface. What customers see about their pipelines.

### Dashboard

A per-pipeline dashboard showing:

- Run history (success / fail / in-progress).
- Rows / bytes moved per run, with trend lines.
- Schema changes applied.
- Schema changes pending operator action.
- Dead-letter row count per recent run.
- Destination write throughput / latency.
- CDC-specific: slot lag, streaming status, events/sec, snapshot progress.
- Error summary: typed errors by frequency, with drill-down to the specific run.

### Pipeline health

A coarse-grained "health" signal per pipeline: `healthy`, `degraded`, `unhealthy`, `paused`. Derived from recent runs' success rate, error-free CDC streaming, absence of pending schema events. The health score is opinionated — we surface why a pipeline is `degraded` even if every individual metric looks borderline-fine.

### Data volume accounting

For billing transparency (RFC 17) and for customer understanding:

- Rows extracted per pipeline per day.
- Bytes extracted per pipeline per day.
- Bytes loaded per pipeline per day.
- Compute-seconds consumed per pipeline per day.

These are aggregated from the event bus into a per-tenant metrics store, queryable via the customer API.

### Queries / filters

The customer API supports queries:

- "Show me all pipelines with pending schema events in my workspace."
- "Show me the last 100 runs of pipeline X."
- "Show me bytes-loaded per day for the last 30 days, aggregated by destination."

The API is paginated, rate-limited, and authorized per standard tenant auth. Query complexity is bounded; pathological queries are rejected with clear messages.

### Pipeline notifications

Users configure notifications at the pipeline level:

- On run failure.
- On schema change requiring decision.
- On dead-letter row rate exceeding threshold.
- On CDC slot lag crossing threshold.

Delivery channels: email, webhook, Slack/Teams integration. Notifications are events (not polling); they fire from the event bus.

## Lineage

Lineage is the graph of "what produced what" — introduced in RFC 10, fully specified here.

### Lineage event model

Every pipeline run emits lineage events at stage boundaries:

```
LineageEvent {
  run_id, pipeline_id, tenant_id,
  stage: extract | transform | load,
  inputs: list<DatasetRef>,
  outputs: list<DatasetRef>,
  occurred_at,
  metadata: {...}  // optional column mappings, transformation IDs, etc.
}

DatasetRef {
  kind: source_stream | destination_table | staging_ref | transformation_output,
  identifier: string,  // connection+table, or staging path, or transformation+output
  schema_fingerprint: string,
}
```

### Graph construction

A background job (the lineage derivation worker) consumes lineage events and materializes a graph in Postgres (per RFC 10). Graph is refreshed with eventual consistency; default lag is 5 minutes.

### Graph queries

The customer API exposes:

- **Upstream lineage**: for a destination dataset, what pipelines produce it? What source datasets feed them?
- **Downstream lineage**: for a source dataset, what pipelines consume it? What destinations are affected?
- **Transformation chain**: given a destination column, what transformation produced it?

Column-level lineage is provided only when the transformation declares its column mapping (RFC 12). Otherwise, graph-level only, with a clear indicator that column-level is unavailable.

### External compatibility

We emit lineage in **OpenLineage** format as an optional outbound feed. Customers integrating with DataHub, Amundsen, Marquez, or Collibra can consume our lineage directly. This is a non-trivial integration win that costs little to build because the model is already close.

### Impact analysis

"What breaks if this source changes?" is a graph traversal. Exposed as a UI feature and an API endpoint. Used by customers before making source-side changes to understand downstream consequences.

## Audit

The audit pipeline is specified in RFC 11 (for secret accesses) and mentioned in RFC 10 (for config changes). This section consolidates.

### What is audited

- **Authentication events**: user login, API key use, SSO federation, MFA challenges.
- **Authorization events**: permission denied on any API call.
- **Configuration changes**: catalog writes (pipeline create/update/delete, connection changes, schema approvals).
- **Secret accesses**: per RFC 11.
- **Data-plane access to customer data by our staff**: a platform engineer connecting to a worker for debugging produces an audit event identifying the engineer, the tenant, the reason (ticket number or annotation), and the duration.
- **Cross-tenant access**: if our staff access one tenant's data while supporting another, both tenants' audit logs receive the event.
- **Billing events**: for financial audit.

### What is not audited

Not everything is audit-worthy. Specifically:

- Read-only queries against a tenant's own data (self-access produces operational logs, not audit).
- Platform-internal operations with no external effect (background job running, metric emission).
- Temporary credential use (we audit the refresh that produced the credential, not every use of it — the use is bounded by the credential's lifetime).

### Audit event structure

Audit events are a subset of the general event envelope, with required fields:

- `actor`: who (user ID, service identity).
- `action`: what (typed verb: `secret.read`, `pipeline.update`, `user.login`, etc.).
- `resource`: on what (typed reference).
- `outcome`: `success` / `denied` / `error`.
- `context`: auth method, IP address (where applicable), geographic region.

### Audit store

Per RFC 11: append-only, hash-chained, daily external anchor. Implementation-wise:

- A dedicated audit service receives events, commits them to durable storage (Postgres + object-storage archive), and appends to the hash chain.
- The hash chain's tip is published daily to an external anchor (blockchain transaction, git-signed tag in a public repository, or equivalent).
- Verification of the chain: periodic (weekly) job walks the chain and confirms consistency; alerts on mismatch.

### Access

- Customer audit admins query their own tenant's audit events through a dedicated API (not the general customer API — different rate limits, different retention, different auth flow).
- Our compliance staff have cross-tenant read access but every access is itself audited (meta-audit). This is a "can't read without leaving fingerprints" posture.
- Regulators are mediated through the customer. We do not provide direct regulator access.

### Audit retention and privacy tension

7-year retention is standard for financial/compliance purposes. This conflicts with data-minimization regulations (GDPR, where a user may request erasure of their data). Resolution:

- Audit events do not contain user-data payloads. They reference actors and resources by identifier. An actor's account can be deleted (from the Auth system); audit events referencing the deleted actor's ID remain, but the link to personal information is severed.
- For tenants with specific legal obligations around their own data, we support a shorter audit retention as an enterprise-tier configuration. Regulators typically permit this with documented justification.

## Backpressure and Degradation

Observability emits high event volume. Spikes happen. Our response to pressure:

- **Operational metrics**: continue to emit at reduced cardinality if needed (drop high-cardinality labels, aggregate more aggressively).
- **Logs**: drop DEBUG-level logs first; drop INFO-level next; never drop WARN+.
- **Traces**: reduce sampling percentage under pressure.
- **Audit**: never dropped. Audit events spill to local durable storage and retry; the audit backend is sized for peak plus margin.
- **Customer-facing metrics**: aggregated at ingest; if ingest is slow, customer dashboards lag — but this is visible to customers, not silent.

## Staff Access to Customer Data

The politically-sensitive section, made explicit.

### Principle

Our staff default to no access to customer data. Access is granted:

- With explicit customer authorization (support ticket, contract clause).
- With break-glass procedure for specific incident response.
- Time-bounded (typically 4 hours, auto-expiring).
- Audited both in the customer's tenant audit and in our internal access audit.

### Mechanism

A separate "staff-access" control plane service issues short-lived tokens to engineers with documented justification. Workers validate these tokens before allowing elevated queries. Token issuance is an audit event; token use is an audit event; token expiry removes access.

### What staff can access under authorization

- Operational observability for the tenant (metrics, logs, traces).
- Temporal workflow histories.
- Dead-letter rows (with restrictions — dead-letter is the customer's property).
- Run metadata.

### What staff cannot access even with authorization

- Plaintext secret material (RFC 11 — structurally inaccessible).
- Customer source data in transit (destroyed from memory after activity completion).
- Destination data (no channel from platform to destination that bypasses normal auth).

## Health and SLOs

Platform-level SLOs — our commitments to uptime and quality.

### Defined SLOs

- **Pipeline scheduler availability**: 99.9% (scheduler accepts new runs when it should).
- **Data-plane worker availability**: 99.9% (workers are picking up activities).
- **Catalog API availability**: 99.95% (read-path).
- **Catalog API write availability**: 99.9%.
- **Audit event durability**: 100% (zero tolerance for audit loss; we spill to durable local storage rather than drop).
- **Observability ingestion success rate**: 99% (1% drop budget for non-audit events under pressure).

### SLO tracking

We emit SLI metrics derived from the operational plane. The platform's own operational dashboards track SLOs. Customer-facing SLOs are surfaced on a public status page plus per-tenant status indicators.

### Incident classification

- **Sev 1**: platform unavailable, data at risk, audit integrity compromised.
- **Sev 2**: significant feature degraded, measurable customer impact.
- **Sev 3**: specific customers affected, workaround exists.
- **Sev 4**: no customer impact; internal remediation.

Incident response procedures are operational runbooks, not RFC content. The observability infrastructure must support incident response: metrics, logs, traces, and audit all queryable during high-stress incidents.

## Data Export and Integration

Customers integrate our observability with their own tools.

### Export surfaces

- **OTel endpoint**: customers can configure a second OTel destination to which we forward their data-plane metrics/logs/traces. They consume via Datadog, New Relic, Honeycomb, their own Grafana, etc.
- **Webhooks**: subscribe to specific event kinds (run completed, schema changed); receive HTTP callbacks.
- **OpenLineage feed**: subscribe to lineage events; forward to DataHub or similar.
- **Audit export**: daily or weekly export of audit events to customer's chosen archival system.

### Rate limits and cost

Customer-facing API queries are rate-limited per tenant per endpoint. High-rate integrations use webhooks or OTel push (which scale naturally) rather than polling the API.

## Alternatives Considered

**Single unified store for all observability planes.** Simpler. Rejected: audit's durability and tamper-evidence requirements conflict with operational metrics' high-volume, low-per-record-value access pattern. Forcing them into one store either over-protects metrics (expensive) or under-protects audit (dangerous).

**Proprietary observability protocol.** We have some complex events (CDC slot lag, schema evolution) that don't map cleanly to standard metric/log/trace. Rejected: OTel supports arbitrary attributes on every signal type; we model our custom events as specialized metrics/logs within OTel. Standardization wins.

**Skip distributed tracing.** Metrics + logs cover 80% of needs. Rejected: 80% is not enough for complex debugging of cross-component interactions. Tracing cost (via sampling) is manageable.

**Every event is audit-worthy.** Conservative, hard to argue with. Rejected: audit storage and review cost scales with event volume; including every DEBUG log in audit makes audit meaningless. Audit is what matters for compliance; observability is what matters for operations; they're deliberately different sets.

**Customer direct access to our Prometheus.** Maximally transparent. Rejected: couples customer integrations to our internal tool choices, exposes internal cardinality details, makes retention tier changes customer-visible. A curated API is strictly better.

**Real-time streaming audit (vs. batch commits).** Real-time is always better, right? Rejected for audit: committing audit in micro-batches (say, every 100ms) is well within the freshness needs of compliance and dramatically reduces write pressure on the append-only store. Individual events are still visible within seconds.

## Open Questions

1. **Machine-learning anomaly detection on operational signals.** Very useful; requires infrastructure to train, serve, and explain. Post-launch. Defer.
2. **Native Grafana dashboards shipped with the platform.** Would accelerate customer onboarding. Probably yes; design the initial set as part of launch work.
3. **Customer-defined custom metrics from within transformations.** A transformation emitting "rows matching condition X" as a metric. Possible through the `platform:core/progress` interface (RFC 5) but needs a clear UX. Defer.
4. **PII in log payloads — automatic scrubbing.** Regex-based scrubbing is error-prone; structural type-based prevention (as we do) is preferred. But error messages from destinations can still leak PII accidentally. Augment with a best-effort automated scrubber on DEBUG logs? Low priority but worth considering.
5. **Audit access by the customer during a breach investigation.** If a customer suspects compromise, they need audit access fast. The API is always available but do we need an expedited export flow? Likely yes; define as part of security RFC.
6. **Cross-tenant aggregate analytics for platform operations.** E.g., "which connector versions have the highest error rates across all tenants?" Valuable for us; customer-privacy-sensitive. Needs explicit anonymization and tenant-consent model. Punt.

## References

- OpenTelemetry specification: https://opentelemetry.io/docs/
- OpenLineage specification: https://openlineage.io/
- Prometheus best practices: https://prometheus.io/docs/practices/
- Google's Trillian (verifiable log): https://github.com/google/trillian
- W3C Trace Context: https://www.w3.org/TR/trace-context/
- NIST SP 800-92 (log management guide).
- Datadog / New Relic / Honeycomb (reference backends; not endorsements).

## Decision

**Accepted pending review.** This completes the Platform tier. RFCs 1-15 specify everything about what the platform is and how it works internally.

The remaining work moves into the Operational tier (multi-tenancy, quotas/billing, deployment topology, security, SDK and extensibility) — RFCs 16 through 20. These are largely specified implicitly by prior RFCs; the operational tier RFCs make the implicit explicit and commit to specific deployment shapes, isolation levels, and extensibility stories.
