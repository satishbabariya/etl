# Implementation Roadmap — Complete Platform Build

**Status:** Draft, Pending Review
**Date:** 2026-04-22
**Scope:** Full sequencing of RFC-0001 through RFC-0023 into an executable roadmap for a solo full-time developer, 18–36 month horizon.

## 1. Context & Goal

The project (`/Users/satishbabariya/Desktop/etl`) is a Rust + Temporal + WebAssembly ETL platform defined across 23 RFCs (all dated 2026-04-21). The architecture is specified; no code exists. The goal of this roadmap is a **complete implementation of the full platform** — every launch-tier RFC (1–20) plus at least one Growth-tier RFC (21–23) as demand signals justify.

**Developer profile:** solo, full-time, 18–36 month horizon.

**Deployment trajectory:** hybrid — local dogfood first, then self-hosted for friendly users, then hosted SaaS, with BYOC as part of the SaaS-era roll-out (per RFC-18).

**First pipeline:** Postgres (cursor-incremental → extended to logical-replication CDC) to local Parquet, chosen to exercise RFC-6/7/8's three state flavors on a single connector while deferring warehouse (RFC-9) and lakehouse (RFC-22) complexity.

## 2. Guiding Principles

1. **Each era produces a shippable artifact.** Not "internal code state" — something you or a user can actually run.
2. **Build later eras' invariants into early work; don't build later eras' features early.** `TenantId` is a non-constructible type from day one; every catalog entity carries `tenant_id`; secrets are referenced-not-embedded from day one. These are cheap prophylactics; skipping them forces expensive retrofits.
3. **The RFCs specify the endpoint, not the sequence.** Each era builds a simpler version of each subsystem than the RFC describes, and deepens in later eras.
4. **CDC is not deferred.** Phase I.6 proves RFC-8 early because the architecture is built around CDC; delaying it risks architectural mismatch.
5. **Temporal Cloud from day one** (RFC-18 aligned). Self-hosting Temporal is a second full-time job.
6. **No Growth-tier work until Era IV, and Era IV is optional.** RFCs 21/22/23 are explicitly speculative.

## 3. Four Eras

| Era | Months | Deployment target | Headline milestone |
|---|---|---|---|
| I — Local dogfood | 1–9 | CLI on laptop | "I run CDC pipelines from my Postgres to local Parquet, reliably" |
| II — Self-hosted | 10–18 | Helm chart + docs | "A friend installs the helm chart and onboards a pipeline" |
| III — Hosted SaaS | 19–30 | platform.com multi-tenant | "A paying customer signs up and runs a pipeline" |
| IV — Growth tier | 30+ | Demand-gated | SQL transforms / lakehouse / streaming, in that order, only if demanded |

## 4. Era I — Local Dogfood (months 1–9)

**Goal:** one developer, one laptop, runs reliable Postgres→Parquet pipelines (cursor-incremental + logical-replication CDC) via a CLI, with durable state, catalog history, schema evolution, basic observability, and a WASM connector/transform model matching the RFC protocols exactly.

### Phase I.1 — Skeleton (weeks 1–4)
Rust cargo workspaces (`control-api`, `worker`, `connector-sdk`, `loader-sdk`, `catalog`, `cli`, `common-types`). Arrow wired in. Postgres catalog DB with 4 tables (tenant, connection, pipeline, run) — `tenant_id` column on every row from day one. Temporal Cloud account + no-op `PipelineRunWorkflow`. CLI `platform pipeline run <id>` submits the workflow.

RFCs: 2, 3 (subset), 4 (skeleton), 10 (minimal), 13 (CLI subset), 14 (Class A skeleton).

Demoable: workflow runs to completion, appears in Temporal UI, row in `runs`.
Exit: workflow durable across laptop sleep/restart.

### Phase I.2 — First pipeline, no WASM yet (weeks 5–10)
Rust-native Postgres connector (in-process, not WASM — de-risk protocol before adding sandboxing). Cursor-incremental sync. Rust-native Parquet loader to local `./data/<pipeline>/`. Activities inside `PipelineRunWorkflow`. Arrow RecordBatch transport. Cursor persisted as workflow state + committed to catalog after loader success.

RFCs: 6 (in-process), 7, 9 (idempotent loader trait), 3 (RecordBatch), 4 (activities).

Demoable: real Postgres table → Parquet files → re-run picks up new rows only.
Exit: at-least-once + PK-dedup contract proven; kill worker mid-batch, restart, no data loss/duplication.

### Phase I.3 — WASM runtime + SDK (weeks 11–16)
Wasmtime 25+. WIT interface per RFC-6. Component Model + WASI 0.2. AOT to `.cwasm`. Arrow IPC over linear memory (Tier 1 only). Port Postgres connector into WASM. Fuel-based CPU + memory limits. Host API subset: log, progress, errors, batches, secrets-stub, state/cursor, HTTP. Determinism enforcement for transformations.

RFCs: 5, 6 (over WIT), 20 (Rust SDK v0).

Demoable: same pipeline as I.2 but connector in WASM sandbox; `platform connector build` → `.cwasm`; hot-swap connector version.
Exit: connector cannot exceed limits; invariant violations denied at host-API boundary.

### Phase I.4 — Catalog, DSL, schema evolution (weeks 17–22)
Full catalog entity model (Tenant/Workspace/Connection/Pipeline/Stream/Schema/Run). Schema entities with BLAKE3 fingerprints, append-only versioning. YAML DSL parser (Connection + Pipeline kinds). CLI `platform apply | validate | diff | get`. Schema-diff with RFC-10's 13 typed change kinds. Evolution policies: `propagate_additive` (default), `freeze`, `strict`. Schema-change detection during sync + in-band application.

RFCs: 10 (full), 13 (DSL + CLI), 14 (Class A deepened).

Demoable: `platform apply -f pipeline.yaml`; alter source schema, sync detects + applies per policy.
Exit: catalog recoverable from snapshot; schema history browsable; fingerprint stable across normalized-equivalent schemas.

### Phase I.5 — Transformations + state model (weeks 23–28)
Transformation DAG with declarative operators: select, filter, project, cast, rename, mask, add-column, validate, dedupe, flatten. Static schema derivation per operator. Inline transforms in pipeline YAML. WASM UDF escape hatch (scalar + batch) reusing connector runtime. State-storage architecture formalized (staging `./staging/`, Temporal-backed in-flight, catalog Class A). Dead-letter table (RFC-9 subset). Observability: structured JSON logs, Prometheus metrics (local scrape), OTel trace exporter (console).

RFCs: 12, 14 (consolidated), 15 (operational plane), 9 (dead-letter).

Demoable: pipeline declares `transform: [filter, mask, cast]`; dead-letter rows in rejected table; local Grafana shows live metrics.
Exit: DAG executes ≥1M rows/sec declarative; schema-derivation <100ms for 50-node DAG (RFC-12 targets).

### Phase I.6 — CDC (weeks 29–36)
Postgres logical replication mode (third state flavor in RFC-6 extends existing connector). Replication slot lifecycle (ensure/advance/release). Slot-growth alerting. Parent `CdcPipelineWorkflow` (long-lived) + child `CdcSnapshotWorkflow` (finite). Capture-and-stream (reference LSN → snapshot → stream). CDC metadata columns. Event types (`i/u/d/t/s/c`). Schema-change in-band (DDL events synthesize schema events). Apply-change-stream loader pattern to Parquet (append + logical delete markers, compaction handoff deferred).

RFCs: 8, 6 (CDC flavor), 7 (snapshot+catch-up), 9 (apply-change-stream), 10 (CDC schema handling).

Demoable: snapshot → stream transition; `UPDATE/DELETE/ALTER TABLE` all reflected in Parquet; restart mid-stream resumes cleanly.
Exit: full RFC-8 demo scenarios pass.

**Era I exit criteria:**
- Both pipeline modes proven against real Postgres
- WASM connector SDK documented well enough to write a second connector
- Catalog/DSL/schema evolution fully working single-tenant
- Observability stack functional for self-debugging
- You are using it to replicate a real database you care about

**Era I skipped on purpose:** multi-tenancy beyond TenantId plumbing (II), secrets backend beyond env-var shim (II), auth (II), billing (III), HTTP connectors (II), warehouse destinations (II), self-hosted packaging (II), non-Rust SDKs (II/III), hosted control plane (III), Growth tier (IV).

## 5. Era II — Self-Hosted (months 10–18)

**Goal:** a friendly user downloads a helm chart, runs it in their cluster against their Postgres/Temporal/object-storage, onboards a pipeline in under an hour, no hand-holding.

### Phase II.1 — Multi-tenancy turned real (weeks 37–42)
Postgres row-level-security policies. Temporal namespace-per-tenant. Object-storage per-tenant-prefix. Observability tenant labels. Identity flowing through every request/workflow. Cross-tenant access tests in CI fail loudly. Tenant lifecycle (provision/suspend/terminate) as control-namespace workflows.

RFCs: 16, 14 (per-tenant), 10 (sharding scaffolding).

Exit: adversarial cross-tenant test suite passes.

### Phase II.2 — Secrets, auth, security model (weeks 43–50)
Secrets backend trait (RFC-11): env-var (dev), file-based sealed-secrets (self-hosted default), Vault (enterprise). `SecretRef` entity. `PlaintextSecret` wrapped type zeroed on drop. Per-read audit log (hash-chained, RFC-15 audit plane). OAuth refresh-token flow with cached access tokens (dynamic secrets deferred to III). Local auth: password + TOTP for admin (RFC-19). JWT sessions. RBAC skeleton (Owner/Admin/Operator/Viewer). Scoped API tokens.

RFCs: 11, 19 (subset), 15 (audit plane on).

Exit: no plaintext credentials in catalog; hash-chain verifiable.

### Phase II.3 — More connectors & destinations via SDK (weeks 51–60)
Connector SDK v1: Rust (idiomatic) + TypeScript (via `jco`). Authoring workflow (`platform connector create | test | publish`). Second connector: Stripe (HTTP API — pagination, OAuth, rate-limit, JSON schema discovery). Third: MySQL with binlog CDC (confirms RFC-6 abstracts across source engines). Loaders: Postgres (DB→DB), Snowflake (MERGE-on-commit), BigQuery (Storage Write API). Dead-letter per destination hardened.

RFCs: 20 (SDK Rust+TS), 6 (protocol across connectors), 8 (MySQL CDC), 9 (Snowflake/BigQuery full).

Exit: external tester writes and publishes a connector in under a week using only SDK + docs.

### Phase II.4 — Self-hosted packaging (weeks 61–68)
Helm chart (control plane + worker deployments). Terraform modules for AWS and GCP (VPC, managed Postgres, object storage, IAM). Bootstrap CLI (`platform install`) stands up a running instance in a customer cluster. Operator handbook (install/upgrade/backup/restore/troubleshoot). `platform doctor` diagnostics. Signed container images + release channel. Upgrade procedure tested (N against N-2 CP compat per RFC-18).

RFCs: 18 (self-hosted), 2 (deployment abstraction).

Exit: a second machine (not yours) cold-installs helm chart and runs a pipeline unassisted.

### Phase II.5 — Observability polish + customer dashboards (weeks 69–76)
Customer-facing observability plane: run history, volume trends, schema-change log, dead-letter viewer, health indicator, CDC lag dashboard. Notifications (email/webhook/Slack/Teams). Lineage v0 (stream → pipeline → table, no column-level; OpenLineage feed). SLO dashboards. Logs scrubbed of PII. Basic read-only React web UI for catalog browsing + pipeline status (authoring stays YAML-only).

RFCs: 15 (customer plane, lineage, notifications), 13 (UI read-only).

Exit: self-hosted user debugs their own pipeline failure without asking you.

**Era II exit criteria:**
- Helm chart ships to 3–5 friendly users
- Each writes at least one custom connector
- No plaintext secrets outside activity memory
- One external user running in anger ≥30 days
- ~3 minor versions shipped with release/upgrade loop

**Era II skipped:** BYOC cross-internet hardening (III), billing/quotas (III), SOC2 prep (III), Python/Go SDKs (III), full audit retention (III), marketplace/paid connectors (III+).

## 6. Era III — Hosted SaaS (months 19–30)

**Goal:** platform.com as a real product. Stranger signs up, connects source, picks destination, runs pipeline, pays first invoice.

### Phase III.1 — Control plane as product (weeks 77–86)
SaaS control plane on AWS us-east-1 (launch region). Signup + SSO (SAML/OIDC). Tenant/workspace provisioning as workflows. Invitations + team management. Envoy API gateway with external TLS. Web UI: connection creation, pipeline authoring (form over DSL, YAML canonical), run monitoring. Public API + SDK (same catalog API). Signup → first-pipeline flow target <30min (RFC-18).

RFCs: 2, 18 (hosted), 19 (SSO), 13 (UI authoring).

Exit: cold-start stranger signs up and pipelines data in under an hour.

### Phase III.2 — Billing, quotas, metering (weeks 87–94)
Metering event system (RFC-17): local durable queue → regional Kafka → aggregation service. Event types: BytesExtracted/Loaded, RowsProcessed, ComputeSeconds, StorageBytesHours, EgressBytes, ApiRequests, DestinationWriteOps, CdcSlotHeld, SeatsMonthly. Idempotent by event_id. Hourly streaming aggregation + daily batch + monthly invoice. Stripe invoicing. Quota enforcement (soft/hard/burst). Tier definitions (Trial/Starter/Professional/Enterprise). Cost-observability UI (projected bill, per-pipeline cost, optimization hints).

RFCs: 17, 18 (quota in scheduler), 15 (metering separate).

Exit: first paying customer invoice correct; customer can export raw events and reproduce aggregation.

### Phase III.3 — BYOC mode (weeks 95–102)
Reference Terraform for AWS/GCP/Azure BYOC data plane. CP↔DP protocols internet-hardened (mTLS, metadata-pull, metadata-push per RFC-2). Customer Terraform → DP connects to CP. No inbound CP→DP. Latency tolerance validated (50–200ms). Version-skew matrix (CP N works with DP N/N-1/N-2). BYOC pricing model (RFC-17 differences).

RFCs: 18 (BYOC), 2 (protocols formalized), 17 (BYOC variant).

Exit: one regulated-enterprise design-partner runs BYOC, syncs Postgres→Snowflake in production.

### Phase III.4 — Audit plane, compliance foundations (weeks 103–110)
Audit plane full (7-year retention, daily external anchor, tamper-evident hash chain). Customer audit UI. Staff break-glass procedure per RFC-19. SOC2 Type II controls mapped to implementation. GDPR right-to-forget workflow. Data-region selection at tenant creation. Subprocessor list + DPA templates. `security.txt` + vuln-disclosure program. Trivy, Dependabot, GuardDuty wired in. CMK option for enterprise. Pen-test engagement.

RFCs: 15 (audit full), 19 (compliance full), 14 (retention enforced).

Exit: SOC2 Type II passed; one enterprise customer signs with CMK requirement.

### Phase III.5 — Scale & polish (weeks 111–120)
Multi-region (add eu-central-1, us-west-2, ap-southeast-1). Catalog partitioning (tenant-sharded). Temporal namespace sharding (>10K tenant consideration from RFC-16). Autoscaling rules per RFC-18 (queue-depth >20 for >2min scale up; CPU <30% + queue <5 for >10min scale down). Performance targets validated: RFC-5 <50ms cold start, RFC-12 ≥1M rows/sec declarative, RFC-14 RPO <5min Class A, RFC-18 RTO <4h region-failure enterprise. 10+ connectors in registry. Python + Go SDKs. Integration partnerships (dbt upstream, Terraform provider, Kubernetes operator).

RFCs: 10 (at scale), 16 (at scale), 18 (multi-region), 20 (Python/Go + partnerships).

Exit: ~50–100 paying tenants without manual babysitting; 3–5× cost claim from RFC-1 validated on representative workload.

**Era III exit criteria:**
- 10–20 paying customers
- SOC2 Type II in hand
- BYOC working for ≥2 regulated customers
- First-party connector catalog covers ~80% of ingest volume
- Fivetran cost claim validated

## 7. Era IV — Growth Tier (months 30+, demand-gated)

Three mutually-optional tracks. **Gate each:** ≥5 paying customers asked for it in the last 6 months, else defer.

- **Track Q — Query engine (RFC-21):** 6–9 months. DataFusion embed. Mode 1 (SQL transforms) first — reuses RFC-12 operators as compilation target. Mode 2 (analytical in-pipeline) second. Mode 3 (direct lakehouse query) third. Build if customers consistently ask for SQL authoring.
- **Track L — Lakehouse (RFC-22):** 6–9 months. Iceberg-first (iceberg-rust), Delta-also (delta-rs). Catalog neutrality (REST Catalog, Glue, Polaris). Evolution via RFC-10. Compaction orchestrated, not implemented. Time-travel. Build if enterprises push for "open-lakehouse alternative to Databricks".
- **Track S — Streaming (RFC-23):** ~12 months via Option 2 (integrate Arroyo); 3+ years Option 3 (native). Default = Option 1 (stay micro-batch, partner). Build Option 2 only if Arroyo matures and customers demand sub-second.

**Default sequence if all three demanded:** Q → L → S.

## 8. Cross-Cutting Concerns

### What's not in the plan, and why

1. **No open-source release.** RFCs 1 & 20 leave this open. Explicit decision point at end of Era II or Era III. Punting by default.
2. **No separate platform engineering investment in Era I.** CI/builds/release at the minimum level until Era II has users.
3. **No infra cost optimization early.** Managed Temporal/Postgres/Kafka; revisit at Phase III.5 if costs bite.

### Top risks (ranked)

1. **CDC harder than RFC-8 looks.** Postgres logical replication has toasted columns, replica identity, DDL gaps. *Mitigation:* silent 4-week buffer on Phase I.6; fallback is shipping Era I without CDC, making CDC Phase II.0.
2. **WASM Component Model / WASI 0.2 tooling still stabilizing.** *Mitigation:* pin wasmtime; track wit-bindgen/jco/componentize-py upgrade windows; Python SDK in Era III descopable to "in 6 months".
3. **Solo-founder burnout at 18+ month horizons.** *Mitigation:* each era ends with shippable artifact; Era I ends with "using this for my own data".
4. **Multi-tenancy retrofit in Era II hurts despite prophylactics.** *Mitigation:* adversarial tenant-isolation test suite written first in Phase II.1.
5. **Temporal Cloud pricing at scale.** *Mitigation:* model Temporal costs during Phase III.2 metering work; revisit self-hosting if margins compress.
6. **Warehouse destination complexity (RFC-9's weakest point).** *Mitigation:* Snowflake only in Phase II.3; add others on demand; destination tests against real accounts, not mocks.

### Dependency graph (critical path)

```
RFC-3 (Arrow) → RFC-4 (Temporal) → RFC-6 (connector) → RFC-7 (incremental) → RFC-8 (CDC)
                     ↓                    ↓
                RFC-10 (catalog)     RFC-5 (wasm)
                     ↓                    ↓
                RFC-13 (DSL)         RFC-20 (SDK)
                     ↓                    ↓
                RFC-14 (state) ← RFC-15 (observability)
                     ↓
              [Era I complete]
                     ↓
   RFC-11 (secrets) + RFC-16 (multi-tenancy) + RFC-19 (security)
                     ↓
   RFC-9 (loader breadth) + RFC-20 (SDK breadth) + RFC-18 (self-hosted)
                     ↓
              [Era II complete]
                     ↓
   RFC-17 (billing) + RFC-18 (hosted/BYOC) + RFC-19 (compliance)
                     ↓
              [Era III complete]
                     ↓
   Optional: RFC-21 / RFC-22 / RFC-23 (gated by demand)
```

### Hard gates between eras

- **Era I → II:** both pipeline modes proven on your own real data ≥30 days
- **Era II → III:** ≥3 external users running self-hosted in production ≥60 days
- **Era III → IV:** ≥10 paying customers, SOC2 Type II, Fivetran cost claim validated

## 9. RFC Coverage Summary

Every launch-tier RFC (1–20) is scheduled. Growth-tier RFCs (21–23) are gated on demand.

| RFC | Title | Era | Phase(s) |
|---|---|---|---|
| 0001 | Platform vision | — | Referenced throughout; validated at Era III exit |
| 0002 | Core architecture | I, III | I.1 (structure), III.3 (CP↔DP protocols formalized) |
| 0003 | Data interchange (Arrow) | I | I.1 (subset), I.2 (RecordBatch), deepened across Era I |
| 0004 | Temporal topology | I | I.1 (skeleton), I.2 (activities), I.6 (CDC parent/child) |
| 0005 | Wasm runtime | I | I.3 |
| 0006 | Connector protocol | I | I.2 (in-process), I.3 (WIT), I.6 (CDC flavor) |
| 0007 | Incremental sync | I | I.2 (cursor), I.6 (snapshot+catch-up) |
| 0008 | CDC architecture | I | I.6 |
| 0009 | Destination loaders | I, II | I.2 (Parquet), II.3 (Snowflake/BigQuery/Postgres) |
| 0010 | Catalog & schema evolution | I, II, III | I.1 (minimal), I.4 (full), III.5 (at scale) |
| 0011 | Secrets management | II | II.2 |
| 0012 | Transformation layer | I | I.5 |
| 0013 | Pipeline DSL | I, III | I.1 (CLI subset), I.4 (full YAML), III.1 (UI authoring) |
| 0014 | State storage | I, II, III | I.1 (Class A skeleton), I.5 (consolidated), III.4 (retention) |
| 0015 | Observability / lineage / audit | I, II, III | I.5 (operational), II.2 (audit on), II.5 (customer plane + lineage), III.4 (audit full) |
| 0016 | Multi-tenancy | II, III | II.1 (real), III.5 (at scale) |
| 0017 | Quotas & billing | III | III.2 (hosted), III.3 (BYOC variant) |
| 0018 | Deployment topology | II, III | II.4 (self-hosted), III.1 (hosted), III.3 (BYOC), III.5 (multi-region) |
| 0019 | Security model | II, III | II.2 (subset), III.4 (compliance full) |
| 0020 | SDK & extensibility | I, II, III | I.3 (Rust v0), II.3 (Rust+TS), III.5 (Python+Go+partnerships) |
| 0021 | Query engine | IV | Track Q (demand-gated) |
| 0022 | Lakehouse formats | IV | Track L (demand-gated) |
| 0023 | Streaming | IV | Track S (demand-gated; Option 1 default) |

## 10. Next Step

On approval of this roadmap, invoke the `writing-plans` skill to produce a detailed implementation plan for **Phase I.1 — Skeleton (weeks 1–4)** as the first buildable unit. Subsequent phases get their own plans as they come due.
