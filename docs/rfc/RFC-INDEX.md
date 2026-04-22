# RFC Series Index

A Rust + Temporal + Wasm ETL Platform — full architecture specification.

23 RFCs, organized into five tiers. Each RFC is a decision document: it frames a problem, enumerates options with tradeoffs, recommends one, and documents what's explicitly out of scope.

## Foundation Tier

These RFCs constrain everything downstream. Getting them wrong means rewriting 4-23.

- **[RFC 0001: Platform Vision, Wedge, and Non-Goals](RFC-0001-platform-vision.md)** — What we're building, who it's for, what we explicitly won't build. Commits to the Fivetran-displacement wedge.
- **[RFC 0002: Core Architecture and Component Boundaries](RFC-0002-core-architecture.md)** — Control plane / data plane split as architectural invariant. Enumerates every major service and the protocols between them.
- **[RFC 0003: Data Interchange Format and Type System](RFC-0003-data-interchange.md)** — Arrow everywhere; IPC for staging, Parquet for destinations. Platform type system with semantic metadata annotations.

## Execution Tier

How work gets done.

- **[RFC 0004: Temporal Workflow Topology and Durability Model](RFC-0004-temporal-topology.md)** — One canonical workflow shape per run. Activity granularity rules, versioning discipline, commit ordering.
- **[RFC 0005: Wasm Runtime, Sandboxing, and Host API](RFC-0005-wasm-runtime.md)** — Wasmtime + Component Model + WASI 0.2. Capability-based isolation. Per-activity secret handles. AOT-compiled modules.
- **[RFC 0006: Connector Protocol](RFC-0006-connector-protocol.md)** — The WIT interface every connector implements. State ownership, schema events, pull-based streaming model.
- **[RFC 0007: Incremental Sync and Cursor Semantics](RFC-0007-incremental-sync.md)** — At-least-once + PK-based exactly-once at destination. Overlap window pattern. Clock-skew handling.
- **[RFC 0008: CDC Architecture](RFC-0008-cdc-architecture.md)** — Parent-child workflow topology. Replication slot management with orphan reconciliation. Per-source handling (Postgres, MySQL, MongoDB, SQL Server, Oracle).
- **[RFC 0009: Destination Loader Protocol and Idempotency](RFC-0009-destination-loaders.md)** — Rust-native loaders (not wasm). LoadId-based idempotency. Per-destination strategies (Snowflake, BigQuery, Redshift, Postgres, Iceberg, Delta, raw S3).
- **[RFC 0010: Catalog, Schema Registry, and Schema Evolution](RFC-0010-catalog-schema-evolution.md)** — Entity model, versioning discipline, typed schema diffs, four evolution policies.
- **[RFC 0011: Secrets, Connections, and Credential Management](RFC-0011-secrets-management.md)** — Secrets never enter control plane durable state. Per-activity local handles for guests. Three backends (hosted / BYOC / self-hosted) under one trait.

## Platform Tier

Features on top of the execution substrate.

- **[RFC 0012: Transformation Layer and UDF Model](RFC-0012-transformation-layer.md)** — Declarative operators + wasm UDFs. Static schema derivation. Determinism enforced by capability restrictions.
- **[RFC 0013: Pipeline DSL and Configuration Language](RFC-0013-pipeline-dsl.md)** — YAML resources, 1:1 with catalog entities. Environment overlays as the only templating. GitOps-enabling.
- **[RFC 0014: State Storage Architecture](RFC-0014-state-storage.md)** — Complete state catalog. Five durability classes. Commit relationship chains. RTO/RPO commitments.
- **[RFC 0015: Observability, Lineage, and Audit](RFC-0015-observability-lineage-audit.md)** — Three audiences (operators, customers, auditors), three planes, one source of truth. OpenTelemetry everywhere. Hash-chained audit.

## Operational Tier

How we run it commercially.

- **[RFC 0016: Multi-tenancy and Isolation Model](RFC-0016-multi-tenancy.md)** — Eight-tier threat model. Six isolation layers. Type-level tenant identification. Three worker-residency modes.
- **[RFC 0017: Quotas, Billing Metering, and Backpressure](RFC-0017-quotas-billing.md)** — Metering events as system of record. Usage-based billing. Soft/hard/burst quota model. No MAR pricing.
- **[RFC 0018: Deployment Topology](RFC-0018-deployment-topology.md)** — Three modes concretized as Kubernetes deployments. Multi-cloud strategy. Regional data residency. Zero-downtime upgrades.
- **[RFC 0019: Security Model](RFC-0019-security-model.md)** — Consolidated security posture. Authn/authz, encryption primitives, break-glass, vulnerability response, compliance framework.
- **[RFC 0020: SDK and Extensibility](RFC-0020-sdk-extensibility.md)** — Four first-class SDK languages. Connector registry model. Publication gates. Economic model for third-party content.

## Growth Tier (Speculative)

Direction toward Databricks territory. These may change significantly based on post-launch experience.

- **[RFC 0021: Query Engine Integration and SQL Surface](RFC-0021-query-engine.md)** — DataFusion for SQL transformations, in-pipeline analytics, and direct lakehouse queries. Growth-tier; not a launch commitment.
- **[RFC 0022: Lakehouse Storage Format Strategy](RFC-0022-lakehouse-formats.md)** — Iceberg-first, Delta-also. Catalog-neutral. Compaction orchestrated, not provided. Growth-tier.
- **[RFC 0023: Streaming Execution Model](RFC-0023-streaming-execution.md)** — Three options (partner, integrate, build). Most speculative RFC in the series. No commitment.

---

## Key Architectural Invariants

Across all 23 RFCs, these invariants hold:

1. **Customer row data never traverses the control plane.** Data flows exclusively in the data plane.
2. **The data plane can run with the control plane unreachable** for bounded time.
3. **Every durable state transition goes through Temporal** — no second durable-state mechanism.
4. **User-authored code runs only in wasm** — not containers, not subprocesses.
5. **Control plane is multi-tenant; data planes are single-tenant.**
6. **Secrets never enter catalog, Temporal history, or logs.** Ever.
7. **Loaders are first-party Rust, not wasm.** The only exception to "user code in wasm."
8. **Connectors uniformly use wasm path** — first-party and third-party alike.

## Reading Order

For new engineers: read in order (1 → 23). Each RFC builds on prior.

For reviewers focused on specific concerns:

- **Correctness & reliability**: RFCs 3, 4, 7, 8, 9, 10.
- **Security & compliance**: RFCs 5, 11, 14, 15, 16, 19.
- **Product & UX**: RFCs 1, 12, 13, 20.
- **Operations**: RFCs 2, 14, 17, 18.
- **Competitive positioning**: RFCs 1, 21, 22, 23.

## Commitment Levels

Not all RFCs carry equal commitment:

- **RFCs 1-20**: Accepted pending review. These are launch-targeted commitments.
- **RFCs 21-23**: Growth-tier, speculative. Drafted to complete architectural thinking; not launch commitments; may be revised or abandoned based on post-launch signals.

## What Was Deliberately Left Undecided

RFC-level decisions we chose not to make because they're downstream of real experience:

- Exact dollar amounts for pricing tiers (RFC 17).
- Specific compliance certification timelines (RFC 19).
- Post-launch regional expansion cadence (RFC 18).
- Specific vendor selection for audit external anchor (RFC 11, 15).
- Query concurrency and fairness at scale (RFC 21).
- Whether to do streaming at all (RFC 23).

These are not RFC omissions; they're deliberately deferred.
