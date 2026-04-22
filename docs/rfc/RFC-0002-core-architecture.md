# RFC 0002: Core Architecture and Component Boundaries

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0001 (Platform Vision)

## Summary

This RFC defines the top-level components of the platform, the boundaries between them, and the protocols by which they communicate. It establishes the **control plane / data plane split** as an architectural invariant, enumerates every major service, and specifies which concerns live where.

Downstream RFCs will detail individual components. This RFC's job is to ensure those detailed designs compose into a coherent whole, and to prevent the classic failure mode where each subsystem is designed in isolation and the seams don't line up.

## Motivation

A data platform has many concerns: pipeline definition, scheduling, execution, state management, observability, multi-tenancy, billing, secrets, catalog, destination delivery. Without explicit boundaries, these concerns leak across components and produce a system where changing one thing requires touching five others.

We have two forces pulling in opposite directions:

1. **Operational simplicity** pulls toward a monolith: fewer moving parts, simpler deployment, easier local development.
2. **Product requirements** pull toward separation: BYOC demands that customer data never touch our infrastructure, multi-tenancy demands hard isolation, and differential scaling demands that the hot paths (data movement) scale independently from the cold paths (config edits).

The resolution is a **two-plane architecture** with a small number of well-defined services in each plane, connected by explicit protocols. This is a common pattern (HashiCorp, Snowflake, Confluent Cloud, MongoDB Atlas all use variants) and is well-understood by operators, which matters for our buyer.

## Non-Goals

- This RFC does not specify internal APIs at the method level. That's done per-component.
- This RFC does not specify the wire format for any protocol. RFC 3 covers data interchange; individual component RFCs cover control-plane APIs.
- This RFC does not decide between cloud providers or specific managed services. Those are deployment concerns (RFC 18).
- This RFC does not cover failure modes exhaustively. It defines the components; failure semantics are the responsibility of each component's RFC.

## Architectural Invariants

The following are invariants. Any design that violates them is wrong by definition and requires amending this RFC.

1. **Customer data never traverses the control plane.** Pipeline data flows exclusively within the data plane. The control plane sees metadata (schemas, row counts, cursors, run status) but never row-level data. This is what enables BYOC and what simplifies our compliance posture.

2. **The data plane can run with the control plane unreachable, for bounded time.** If the control plane is down, already-scheduled pipelines continue executing. New pipelines cannot start, but in-flight work survives. This is achieved via Temporal's durability guarantees plus workers caching necessary metadata.

3. **Every durable state transition goes through Temporal.** We do not invent a second durable-state mechanism. If something needs to survive a crash, it is either Temporal workflow state, committed to object storage, or committed to a database through an activity that Temporal can retry.

4. **User-authored code runs only in wasm.** Not in containers, not in subprocesses, not in the host Rust process. This is what keeps the sandboxing story simple and the cross-language story clean. The only exceptions are *first-party* connectors and loaders, which are Rust and trusted.

5. **The control plane is multi-tenant; a data plane is single-tenant.** One customer's data plane never touches another's. This simplifies the isolation model significantly (see RFC 16) at the cost of per-customer data-plane provisioning (handled in RFC 18).

## Top-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CONTROL PLANE                           │
│                      (multi-tenant SaaS)                        │
│                                                                 │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐    │
│   │   API    │  │ Catalog  │  │ Scheduler│  │ Observability│    │
│   │ Gateway  │  │ Service  │  │ Service  │  │   Service    │    │
│   └──────────┘  └──────────┘  └──────────┘  └──────────────┘    │
│                                                                 │
│   ┌──────────┐  ┌──────────┐  ┌──────────────────────────────┐  │
│   │  Auth/   │  │ Billing/ │  │  Temporal (Control Namespace)│  │
│   │  Identity│  │  Metering│  │  - tenant lifecycle workflows│  │
│   └──────────┘  └──────────┘  └──────────────────────────────┘  │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │  Metadata only
                            │  (schemas, cursors, run status,
                            │   pipeline definitions, secrets refs)
                            │
┌───────────────────────────┴─────────────────────────────────────┐
│                        DATA PLANE                               │
│                   (per-tenant deployment)                       │
│                                                                 │
│   ┌──────────────────────────────────────────────────────┐      │
│   │   Temporal (Data Namespace)                          │      │
│   │   - pipeline execution workflows                     │      │
│   └──────────────────────────────────────────────────────┘      │
│          │                                                      │
│   ┌──────┴────────────────┐    ┌─────────────────────┐          │
│   │   Worker Fleet        │    │   Object Storage    │          │
│   │   (Rust processes)    │◄──►│   (staging)         │          │
│   │                       │    │   S3/GCS/Azure      │          │
│   │   ┌───────────────┐   │    └─────────────────────┘          │
│   │   │ Wasm Runtime  │   │                                     │
│   │   │ (wasmtime)    │   │    ┌─────────────────────┐          │
│   │   └───────────────┘   │    │  Secrets Store      │          │
│   │                       │◄──►│  (Vault/cloud KMS)  │          │
│   │   ┌───────────────┐   │    └─────────────────────┘          │
│   │   │ Connector     │   │                                     │
│   │   │ Runtime       │   │    ┌─────────────────────┐          │
│   │   └───────────────┘   │    │  Customer Sources   │          │
│   │                       │◄──►│  (Postgres, SaaS,   │          │
│   │   ┌───────────────┐   │    │   APIs, files)      │          │
│   │   │ Loader        │   │    └─────────────────────┘          │
│   │   │ Runtime       │   │                                     │
│   │   └───────────────┘   │    ┌─────────────────────┐          │
│   │                       │◄──►│  Destinations       │          │
│   └───────────────────────┘    │  (Snowflake,        │          │
│                                │   BigQuery, etc.)   │          │
│                                └─────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

## Control Plane Components

### API Gateway

The single public ingress for all customer-facing interactions: web UI, CLI, SDK, and third-party integrations. Responsibilities:

- TLS termination, request authentication (via Auth Service).
- Request routing to downstream services.
- Rate limiting per tenant.
- API versioning.

Stateless. Horizontally scalable. We will not build anything clever here — it's an off-the-shelf pattern (Envoy / AWS ALB / equivalent behind a thin Rust service for auth enrichment).

### Catalog Service

The system of record for everything describing what a pipeline *is*, as opposed to how it's *running*. Owns:

- Tenant, workspace, project hierarchy.
- Pipeline definitions (source config, destination config, transformation DAG reference, schedule).
- Schema registry (source schemas as discovered, destination schemas as provisioned, evolution history).
- Connection definitions (source credentials references, destination credentials references — actual secrets live in the Secrets Store).
- Lineage graph (derived from pipeline runs, see RFC 15).

Backed by Postgres. This is the "boring CRUD" service and we treat it as such. Detail in RFC 10.

### Scheduler Service

Owns the *decision* of when a pipeline should run. Does **not** run pipelines itself. On a schedule trigger (cron, event, manual), it starts a Temporal workflow in the appropriate data plane namespace and records the run.

The reason Scheduler and Temporal are separate: Temporal executes workflows; it does not own the business semantics of "this pipeline is paused because billing failed" or "this tenant's concurrency limit is 3 and 3 workflows are already running." Scheduler owns those semantics and decides whether to start the workflow. Once started, Temporal is authoritative.

### Observability Service

Aggregates metrics, logs, and run metadata from all data planes. This is the one component that receives *pushes* from data planes (rather than being polled by them), because observability data is high-volume and latency-sensitive.

Responsibilities:

- Metric ingestion (per-pipeline throughput, error rates, latency).
- Log ingestion (structured logs from workers, scoped to tenant).
- Lineage derivation (from run metadata pushed by data planes).
- Alerting rules evaluation.

Detail in RFC 15.

### Auth / Identity Service

Standard SaaS auth: SSO (SAML, OIDC), SCIM provisioning, RBAC, API keys, audit logging. We will not reinvent this; we will use a vendor (WorkOS, Auth0, or equivalent) behind our own thin wrapper.

Scope for this RFC: Auth issues signed tokens that the API Gateway validates. Workers do not directly authenticate users; they authenticate to the control plane using machine identities.

### Billing / Metering Service

Consumes metering events from data planes (GB synced, compute-seconds used) and produces invoices. Separate from Observability because billing data has different retention, correctness, and audit requirements — you cannot be "roughly right" about billing.

Detail deferred to RFC 17.

### Temporal (Control Namespace)

The control plane has its own Temporal namespace for control-plane workflows: tenant provisioning, data plane deployment, billing cycle execution, connector publication workflows. These are distinct from pipeline-execution workflows (which live in data-plane namespaces) and should never be confused.

## Data Plane Components

### Temporal (Data Namespace)

The durable execution substrate for pipeline runs. One namespace per tenant (see RFC 16 on multi-tenancy and RFC 4 on workflow topology for why).

Every pipeline run is a workflow. Every extract/transform/load step is an activity. The workflow holds cursor state, batch references (pointers into object storage), and run metadata. It does **not** hold data.

Deployment options per tier:
- **Our-hosted data plane:** Temporal Cloud per-region shared cluster, tenant-isolated via namespaces.
- **BYOC data plane:** Customer runs their own Temporal cluster (or Temporal Cloud account).

### Worker Fleet

Rust processes that poll Temporal for activity tasks and execute them. A worker is a stateless (modulo caches) process; workers are cattle, not pets. Internally, a worker hosts three distinct runtimes:

- **Wasm Runtime** — embeds wasmtime, loads user-authored wasm components, executes them against the host API (RFC 5). This is where untrusted code runs.
- **Connector Runtime** — the coordination logic that drives a connector's `discover` / `read` / `write` lifecycle. Calls into the Wasm Runtime for user connectors; calls into native Rust code for first-party connectors. Protocol defined in RFC 6.
- **Loader Runtime** — destination-specific code (Snowflake, BigQuery, Iceberg, Postgres, etc.). Always first-party Rust — we do not let users author destination loaders because destination delivery correctness is too load-bearing to sandbox. Protocol in RFC 9.

All three runtimes share the worker's Arrow memory pool, connection pools, and telemetry pipeline. Putting them in the same process is a deliberate choice for zero-copy data flow; splitting them across processes would double the RAM and serialize every hand-off.

### Object Storage (Staging)

S3, GCS, or Azure Blob — whichever the data plane's cloud provides. Holds Arrow IPC files written by extract activities and read by transform/load activities. Lifecycle-managed: staging files are deleted after successful run commit plus a retention window (for debugging and replay).

The staging layer is what makes the pipeline *incrementally* durable. An extract activity writes batches to staging and returns a pointer; Temporal persists the pointer as workflow state. If a later activity fails, we don't re-extract from the source — we resume from the staged data.

### Secrets Store

Source credentials, destination credentials, API tokens. **Not** in the Catalog Service — catalog holds *references* (opaque IDs); the actual secret material lives here. Deployment options:

- Our-hosted: AWS Secrets Manager / GCP Secret Manager / Azure Key Vault, scoped per tenant.
- BYOC: Customer-owned Vault / cloud KMS. We never see the plaintext.

The worker fetches secrets at the start of an activity, holds them in memory only for the duration of the activity, and passes them to connectors via the host API (RFC 5) — never as arguments in workflow state, which would persist them in Temporal history.

Detail in RFC 11.

## Cross-Plane Protocols

Only three protocols cross the plane boundary. Keeping this list short is deliberate — each additional protocol is a failure surface.

### 1. Configuration Pull

Data plane workers pull pipeline definitions, connector bundles, and catalog metadata from the control plane. Pull model (not push) because it survives control-plane transient unavailability: the worker keeps using its cached definition.

- Protocol: HTTPS, authenticated via data-plane machine identity.
- Caching: local disk cache with TTL; workers start from cache if control plane is unreachable.
- Version pinning: every pipeline run is pinned to a specific catalog version, so a definition change mid-run does not affect the running workflow.

### 2. Metadata Push

Data plane pushes run events, metrics, logs, and metering events to the control plane.

- Protocol: gRPC streaming (for logs/metrics) and HTTPS for discrete events.
- Buffering: if the control plane is unreachable, data plane buffers to local disk and retries. Billing events have stronger durability (queued through a dedicated local durable queue).
- No customer row data in any message. Ever. This is enforced by having the metadata-push client live in a separate module with no access to Arrow batch memory.

### 3. Secret Reference Resolution

When the data plane needs to use a secret, it does so against its *own* Secrets Store, not the control plane. The control plane only ever holds references (e.g., `secrets/tenant-42/snowflake-prod`). This keeps the control plane outside the trust boundary for credential material even in the our-hosted case.

## What Lives Where: Quick Reference

| Concern | Control Plane | Data Plane |
|---|---|---|
| Pipeline definitions | ✅ source of truth | cache |
| Schemas | ✅ source of truth | cache |
| Cursors (sync position) | aggregated view | ✅ source of truth (Temporal state) |
| Customer data (rows) | ❌ never | ✅ in memory + staging |
| Secrets | references only | ✅ plaintext (ephemeral) |
| Connector wasm modules | ✅ registry | cached, loaded into runtime |
| Run history | aggregated | ✅ Temporal source of truth |
| Metrics / logs | ✅ aggregated | emits |
| Billing events | ✅ aggregated + invoicing | emits |
| User auth | ✅ | delegates |
| Machine auth | issues tokens | holds tokens |

## Component Boundary Decisions (Contested)

Three decisions are worth calling out because reasonable people disagree.

**Scheduler as separate service vs. embedded in Temporal.** Temporal has schedule primitives. We're choosing to keep the Scheduler separate because pipeline scheduling has business logic (quotas, pause-on-billing-failure, cascading triggers from other pipelines) that belongs in application code, not in workflow-initiation code. Temporal's scheduler remains useful for *internal* periodic work (e.g., "every hour, check for stalled workflows"); we just don't expose it as the customer-facing scheduling primitive.

**Catalog and Schema Registry as one service vs. two.** We're merging them. They share the same consistency requirements, the same backing database, and the same access patterns. Splitting them adds network hops for no benefit. If we later need to split (e.g., if schema registry grows specialized features), that's a refactor, not a day-one decision.

**Worker hosts all three runtimes vs. separate processes.** Covered above. We're consolidating for zero-copy. The risk is that a bug in one runtime takes down the others; we mitigate by using wasm isolation for untrusted code (the actual risk surface) and keeping the other runtimes' code small and audited.

## Deployment Shapes (Preview)

Three deployment shapes are enabled by this architecture, detailed in RFC 18:

1. **Fully hosted.** Control plane and data plane both run on our infrastructure, per-tenant data planes isolated by Kubernetes namespace or equivalent.
2. **BYOC (customer cloud).** Control plane ours; data plane runs in customer's AWS/GCP/Azure account. Customer data never leaves their cloud.
3. **Fully self-hosted.** Enterprise-only. Customer runs both planes. We provide software + support. This is the "big deal" deployment and is expected to be a small number of large customers.

All three shapes use the *same* software — no special builds per deployment mode. This is enforced by the architecture: the control/data split is real in every mode, it's just that in mode 1 both live on our infra.

## Alternatives Considered

**Single-plane architecture.** Simpler operationally. Rejected: kills BYOC, kills the compliance story, forces customer data through our network, eliminates a meaningful differentiator.

**Three-plane architecture (control / execution / data).** Some platforms split execution (workflow engine) from data (workers) from control (config). Rejected: Temporal is the execution plane, workers and Temporal are co-located in the data plane for latency, and splitting them gains nothing here.

**Workers-as-Lambda / serverless data plane.** Run each activity as a serverless function. Rejected: wasm module load cost is non-trivial (tens to hundreds of milliseconds cold), persistent connections to sources/destinations matter for throughput, and debugging serverless at this scale is painful. Long-lived workers are the right shape.

**Push-model configuration (control → data).** Rejected: makes data plane dependent on control-plane availability for *new* workflows to start, and is operationally harder (control plane needs to know every data plane worker). Pull model inverts the dependency.

## Open Questions

1. **Worker pool sizing and autoscaling.** Fixed pool per tenant vs. autoscaling? Probably autoscaling, but the signals (queue depth, CPU, memory pressure) need working out. Defer to an ops RFC.
2. **Temporal namespace strategy at scale.** One namespace per tenant works at 100s of tenants; at 10,000+ tenants we may need to shard namespaces across Temporal clusters. Not a day-one concern but worth flagging. Detail in RFC 4 and RFC 16.
3. **Staging storage lifecycle.** How long do we retain staged Arrow files? Customer-configurable? Tied to plan tier? Resolve in RFC 14.
4. **In-process vs. sidecar for observability agents.** In-process is simpler; sidecar gives stronger isolation guarantees for log data. Lean in-process. Confirm in RFC 15.

## References

- HashiCorp's control/data plane writeups (Consul, Nomad): good prior art for the split.
- Snowflake's cloud services vs. virtual warehouses separation: same idea, different domain.
- Temporal namespace model: https://docs.temporal.io/namespaces
- Confluent Cloud's Kafka-as-service architecture: another control/data-plane reference.

## Decision

**Accepted pending review.** RFC 3 will specify the data interchange format that flows through the data plane, which is the next foundational constraint.
