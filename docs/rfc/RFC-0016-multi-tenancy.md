# RFC 0016: Multi-tenancy and Isolation Model

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0002 (Core Architecture), RFC 0005 (Wasm Runtime), RFC 0011 (Secrets), RFC 0014 (State Storage), RFC 0015 (Observability)

## Summary

This RFC specifies how multiple tenants share platform infrastructure safely: the tenant-isolation model at every layer (network, compute, storage, state), the trust boundaries between components, the noisy-neighbor mitigation strategy, the failure-isolation properties, and how the three deployment modes (hosted, BYOC, self-hosted) implement the same isolation guarantees with different infrastructure shapes.

Multi-tenancy is where the platform's economic model lives. A competitor running one container per customer per pipeline has per-customer fixed costs that dominate at small customer sizes; our wasm-based model amortizes infrastructure across tenants while keeping isolation strong. Getting this right preserves the unit-economics wedge from RFC 1.

## Motivation

Every prior RFC has implicitly made isolation claims. RFC 2 says "data plane is single-tenant." RFC 5 says "wasm instances are bound to a single tenant." RFC 11 says "tenant isolation is structurally impossible to violate." RFC 14 says "row-level isolation via tenant_id." These are individually true statements that don't add up to a complete isolation model by themselves. This RFC:

1. **Names the threat model explicitly.** What are we defending against? A malicious tenant trying to exfiltrate another's data? A buggy connector crashing and affecting other tenants? A noisy tenant consuming resources? Different threats require different defenses.
2. **Commits to specific isolation guarantees per layer.** Not "we isolate" but "we isolate data at storage level via path scoping, at compute level via per-tenant workers, at network level via VPC boundaries in BYOC mode."
3. **Sets the noisy-neighbor policy.** Tenants share workers in hosted mode; one tenant's bad pipeline must not degrade others. Resource budgets, rate limits, fairness disciplines.
4. **Explains the deployment-mode differences honestly.** Hosted has co-tenant concerns; BYOC eliminates them; self-hosted eliminates them differently. Customers choose based on threat model.

The alternative — isolation as an afterthought — produces either security incidents (co-tenant data leak) or economic collapse (one-tenant-per-worker pattern killing margins).

## Non-Goals

- This RFC does not cover RBAC within a tenant. That's authz in the Auth service; RFC 19 (future security RFC) details.
- This RFC does not cover encryption at rest — that's uniform across state categories (RFC 14) and is a security-tier concern.
- This RFC does not enumerate specific attack mitigations at the code level (SQL injection, XSS, etc.). Those are engineering discipline, not architecture.
- This RFC does not specify billing and quota enforcement at the economic level. Resource enforcement is here; billing mechanics are RFC 17 (future).
- This RFC does not cover legal/contractual isolation (data processing agreements, SCCs, subprocessor lists). Those are compliance artifacts.

## Threat Model

What we defend against, in priority order.

### Tier 1: Malicious tenant

A tenant whose user or workload actively attempts to gain access to another tenant's data or resources. Includes:

- Attempting to enumerate other tenants.
- Attempting to read, modify, or delete data in another tenant's catalog, staging, destination, or secrets.
- Attempting to impersonate another tenant's identity.
- Crafting malicious wasm modules to escape the sandbox.
- Attempting DOS via resource exhaustion.
- Attempting timing / side-channel attacks to exfiltrate other tenants' data.

Our posture: **defend strongly**. Tier 1 attacks succeeding is a business-ending event; resources allocated accordingly.

### Tier 2: Compromised customer credential

A tenant's API key or user credential is stolen. Attacker has legitimate access to tenant A's resources but attempts to pivot to tenant B.

Defense: **the compromised credential gives no cross-tenant access**. Every authenticated action is scoped to the tenant that owns the credential. Enumeration of other tenants returns no information.

### Tier 3: Bug-induced leak

A bug in our code causes data from tenant A to be returned to tenant B's query, surfaced in tenant B's dashboard, or routed to tenant B's destination. Not malicious; accidental.

Defense: **structural barriers beyond code discipline**. Row-level security, path-scoped access, type-level tenant identifiers in our code, defense-in-depth so that a single bug does not produce cross-tenant leakage.

### Tier 4: Noisy neighbor

A tenant's workload consumes disproportionate resources (CPU, memory, I/O, rate limits against shared services) and degrades others' performance. Not malicious; just heavy.

Defense: **quotas, rate limits, fair scheduling, and burstable patterns with bounded impact**.

### Tier 5: Supply chain compromise

A third-party dependency (wasm runtime, Postgres driver, OTel SDK) is compromised. Attacker gains the privileges of that dependency.

Defense: **blast radius limited to the trust zone of the compromised component**. Wasm runtime compromise affects wasm-hosted code (already untrusted). Database driver compromise affects database access. Full platform compromise requires compromising multiple boundaries.

### Tier 6: Our insider threat

A platform engineer accesses customer data without authorization. Covered in RFC 15 (staff access model). Not a primary focus here except to note that isolation mechanisms should not be undermined by convenience features for internal staff.

### Out of scope

- Nation-state APTs targeting our infrastructure at every layer. We design to a commercially reasonable threat level; defending against nation-state targeting requires compliance-tier investments and dedicated security engineering outside this RFC's scope.
- Physical infrastructure compromise. Mitigated by cloud provider; we do not run physical infrastructure.

## Isolation Layers

Isolation is not one thing; it's a stack. We specify each layer and what it protects.

### Layer 1: Identity and Authorization

Every request carries a tenant identity; every operation is authorized against that identity.

- User requests carry a user's tenant (established at authentication).
- Machine-to-machine requests (worker → catalog, worker → secret store) carry a service identity scoped to the tenant(s) that worker serves.
- Cross-tenant operations are impossible: API endpoints refuse requests whose declared tenant doesn't match the caller's authenticated identity.

Enforcement: at the API gateway and again at each service. Double-check so that a compromised API gateway cannot pretend to be tenant A to a downstream service.

### Layer 2: Storage Access

Every durable state access is scoped by tenant.

- **Postgres catalog**: row-level security enabled. Every table has a `tenant_id` column, and a session-level variable is set from the authenticated identity. Postgres RLS enforces that queries only return matching rows.
- **Object storage**: IAM policies restrict each worker's storage credentials to the tenants it serves. Path structure (`/tenants/<tenant_id>/...`) is enforced by the IAM policy, not just by application convention.
- **Secrets backend**: per-tenant paths with IAM-enforced access (RFC 11).
- **Temporal**: namespace-per-tenant. A worker configured for tenant A cannot poll tenant B's namespace.
- **Observability**: tenant-scoped queries at the API layer; backend labels are the enforcement point.

Defense in depth: multiple independent layers enforce the same invariant. A bug in one layer does not produce leakage because other layers catch it.

### Layer 3: Compute Isolation

Where tenant workloads execute.

**Hosted mode, default (shared workers):**

Workers are not dedicated per tenant. A worker process polls a task queue; activities from multiple tenants may be scheduled on the same worker over time. Within a worker, an activity is bound to a single tenant at scheduling; workers do not simultaneously execute activities for multiple tenants (one activity at a time per slot, per RFC 5's store model).

Wasm instance pools are per-tenant (RFC 5 invariant). An instance of connector X that served tenant A is retired before serving tenant B; we do not share instances across tenants ever.

**Hosted mode, dedicated workers (enterprise tier):**

Enterprise customers pay for dedicated worker pools. Workers poll only their tenant's task queue. This eliminates any cross-tenant residency in worker memory.

**BYOC mode:**

Workers run in the customer's cloud account. The customer is the only tenant; cross-tenant isolation at the worker level is not applicable.

**Self-hosted mode:**

Customer chooses. Typically one-tenant-per-deployment; a single customer can run multiple logical tenants inside their deployment if they want to, using the hosted-mode isolation patterns.

### Layer 4: Network Isolation

**Hosted mode:**

- Data plane workers in a shared VPC with per-tenant NetworkPolicies (Kubernetes) or security groups (cloud-native).
- Outbound traffic from workers: connections to tenant sources/destinations are authenticated with tenant-specific credentials; a compromised worker cannot make calls to another tenant's systems because it doesn't have the credentials.
- Inbound traffic: API gateway is the only ingress.

**BYOC mode:**

Customer's VPC. Our control plane communicates with customer's data plane over authenticated HTTPS. No shared network. VPC peering optional for high-bandwidth data plane — control plane communication.

**Self-hosted mode:**

Customer-defined.

### Layer 5: Memory and Process Isolation

Within a worker process:

- Wasm instances have linear memory isolation enforced by the runtime (RFC 5). A guest cannot read host memory or other guests' memory.
- Activity-scoped plaintext (secrets, in-flight batches) is zeroed after activity completion (RFC 11).
- The worker process is a single OS process; if that process is compromised, the attacker gains memory access to everything the worker is currently holding. This is why **instance-level residency matters**: minimize what the worker holds at any instant, especially cross-tenant.

Workers do not run multiple tenants' activities concurrently within the same worker process by default. Configurable per-deployment:

- **Tenant-strict mode**: one tenant per worker process at any time; worker switches tenant by completing all in-flight activities, clearing pool, and re-polling.
- **Tenant-multiplexed mode** (default for hosted): multiple tenants per worker, but per-activity tenant binding; no two activities for different tenants simultaneously in the same slot.
- **Tenant-dedicated mode** (enterprise): worker pool dedicated to one tenant.

### Layer 6: Timing and Side-Channel

We do not claim strong timing isolation. A co-tenant can potentially observe timing variations that reveal information about another tenant's workload (e.g., when activities ran, how large batches were). For customers for whom this threat is real, the answer is enterprise dedicated workers or BYOC — not "wasm solves it" (RFC 5 is explicit about this).

Best-effort mitigations:

- Random jitter on scheduling.
- Constant-time secret comparisons in security-critical code.
- Generally, we do not actively defend against side channels in hosted shared-worker mode.

## Tenant Identity Propagation

The `tenant_id` must travel with every piece of work from ingress to egress.

### Identity type

We define a Rust type:

```rust
#[derive(Clone, Copy, Debug)]
pub struct TenantId(Uuid);

impl TenantId {
    pub fn as_uuid(&self) -> Uuid { self.0 }
    // No public constructor except via authenticated sources.
}
```

Constructing a `TenantId` requires going through authenticated sources (API gateway's verified user context, or a worker's startup config matching its issued identity). Arbitrary code cannot mint a `TenantId` out of thin air; it must receive one.

### Propagation

`TenantId` is included in:

- Every HTTP request context (middleware extracts it from auth and makes it available).
- Every Temporal workflow input and activity input.
- Every log record, metric, trace span.
- Every database query (via session variable for RLS).
- Every object storage operation (via path construction).
- Every secret-backend call.

Components that access tenant-scoped resources accept a `TenantId` parameter. CI lint checks that resource-accessing code takes `TenantId` (not, e.g., `Uuid`) so the type system prevents bypassing the scoping.

### Tenant-agnostic code

Some code is tenant-agnostic (the main HTTP server loop, the Temporal task polling logic). It does not receive a `TenantId`. When such code calls tenant-scoped code, it must obtain and pass a `TenantId` — typically from the request context or the activity input. The split is explicit.

## Noisy Neighbor Mitigation

Resource fairness across tenants sharing infrastructure.

### Resources to bound

- **CPU**: wasm runtime fuel (RFC 5); activity execution time.
- **Memory**: per-activity budget; per-worker aggregate.
- **I/O to shared services**: catalog API rate limits; object storage bandwidth (per tenant per worker); Temporal RPC rate.
- **Outbound HTTP** (via `platform:net/http`): per-tenant rate limits.
- **Concurrency**: number of simultaneous in-flight activities per tenant.

### Enforcement mechanisms

**Quota-based:** per-tenant budgets for per-time-window resources. Example: 10 million HTTP requests per hour per tenant (default). Quota exceeded returns a typed rate-limit error; connector retries with backoff (RFC 6).

**Fair scheduling:** task queues prioritize fairness when multiple tenants compete. Temporal task queues don't have native per-tenant fairness; we implement tenant-aware pollers that weight activity pulling toward under-served tenants. Alternative: separate task queues per tenant class (standard, enterprise), each autoscaled independently.

**Burstable patterns:** bursts above quota are allowed with backoff, up to a hard cap. This is what makes "my monthly sync finishes 4x faster than expected" acceptable, while preventing "one tenant pinned all workers for 3 hours."

**Backpressure propagation:** if a tenant is saturating a downstream (e.g., their Snowflake warehouse is slow), their activities slow down (loaders heartbeat, workflow backs off). We don't retry harder; we retry more patiently, and eventually pause with operator notification.

### Per-tier budgets

Default quotas are generous for most tenants. Quota tiers:

- **Free / trial**: aggressive limits; suitable for evaluation.
- **Standard**: default limits; covers >95% of customer workloads comfortably.
- **Enterprise**: higher or negotiated limits; includes burst budget.

Quotas are declarative in the tenant record. Enforcement reads the current quota at activity setup; no cross-request coordination for quota decisions.

### Quota violations are not security events

A tenant hitting quotas is normal operations, not a security incident. Quota enforcement produces typed errors that pipeline activity handles gracefully. Security events (cross-tenant access attempts) are different — those page.

## Temporal Namespace Architecture

A specific design question deferred from RFC 2 and RFC 4: namespace-per-tenant vs. sharded.

### Default: namespace per tenant

Each tenant has its own Temporal namespace. This gives:

- Hard isolation: workers for tenant A cannot even poll tenant B's namespace.
- Clean operational story: per-tenant retention, rate limiting, and observability.
- Simpler mental model.

### Scale consideration

Temporal Cloud supports many namespaces (thousands practical, tens of thousands stretched). At our target scale (hundreds of tenants at launch, thousands in year two), namespace-per-tenant works.

Beyond ~10,000 active tenants, we may need sharding: multiple tenants per namespace, distinguished by workflow-ID prefix. We defer this decision; it's a migration, not a day-one concern.

### Enterprise dedicated clusters

Enterprise tier customers with strict isolation or compliance requirements can opt for a dedicated Temporal cluster. Their workflows run on infrastructure that hosts no other tenants. Cost premium; available to customers whose threat model justifies it.

## Data Plane Topology

How per-tenant data planes are deployed, per mode.

### Hosted mode

**Default (cost-efficient):** a shared Kubernetes cluster per region hosts all hosted-mode tenants. Each tenant has:

- A Temporal namespace.
- Allocated worker capacity (may be shared workers, may be dedicated based on tier).
- A prefix in the shared object storage bucket.
- A path prefix in the shared secrets backend.
- Rows in the shared catalog database.

Isolation via application-level scoping, IAM, and RLS. Not perfect isolation (a kernel exploit could cross the boundary) but sufficient for commercial-tier threat model.

**Enterprise dedicated (isolation-strict):** each enterprise customer gets its own namespace, dedicated worker pool, dedicated storage buckets, dedicated secret backend paths. Still in our cloud, but with no resource sharing below the cluster level.

### BYOC mode

**Per-tenant cloud account.** Customer deploys a reference architecture in their AWS / GCP / Azure account. We ship:

- Terraform modules for provisioning.
- Helm charts or pure-K8s manifests for data plane components.
- A bootstrap CLI that registers the new data plane with our control plane.

Customer infrastructure:

- Their own Temporal (self-hosted or Temporal Cloud under their account).
- Their own Kubernetes cluster for workers.
- Their own object storage bucket.
- Their own secrets backend (their Vault, their cloud secrets manager).

Our role is purely control-plane: pipeline definitions, scheduling, catalog. No customer data ever touches our infrastructure.

### Self-hosted mode

**Customer runs everything.** Both control plane and data plane on customer infrastructure. We provide software and support; customer operates.

Typically on-prem or in an air-gapped environment where even our control plane can't reach.

## Cross-Tenant Communication (Explicit: None)

There is no mechanism by which one tenant can communicate with another through the platform. Specifically:

- No shared pipelines across tenants.
- No shared connections.
- No shared destination tables.
- No event channels from tenant A to tenant B.
- No notifications routed across tenants.

The only exceptions are administratively mediated:

- **Marketplace-style connector sharing**: a connector published by tenant A for public use can be referenced by tenant B. The shared entity is the connector's wasm artifact; no data flows between tenants. Detail in connector registry RFC (future).
- **Support escalations**: our staff with documented authorization can access a customer's operational view (not data) during support. Audited per RFC 15.

Platforms sometimes add "team collaboration" features that inadvertently create cross-tenant channels. We resist this — collaboration within a tenant (workspaces) is supported; across tenants is not, and such features would be a new RFC with a specific threat-model review.

## Failure Isolation

A subtype of noisy-neighbor: failures in one tenant's workload should not bring down others.

### Pipeline failures

Per-pipeline Temporal workflows fail independently. A crashing connector for tenant A does not affect tenant B's pipelines because:

- Different workflows.
- Different wasm instances (instance-per-tenant rule).
- Different activity attempts (one crash doesn't poison the task queue).

### Worker crashes

A worker process crash (OOM, segfault, panic) affects:

- All activities currently running on that worker (across tenants, if shared).
- Temporal re-schedules those activities on other workers.

Mitigation:

- Workers are small and stateless; crashes are common, quickly replaced.
- Activity-level retries absorb individual crashes.
- An abnormally-crashing worker (crash-looping) is quarantined and the image is rolled back.

Note: a malicious tenant cannot intentionally crash a worker to disrupt others because:

- The wasm sandbox catches errors within the guest and returns them as activity errors.
- Guest resource exhaustion triggers host-side limits, not a process-level OOM.
- Native Rust code (loaders, first-party connectors) is trusted and reviewed.

### Dependency failures

Temporal cluster failure affects all tenants in that cluster's namespace set. Mitigation: enterprise dedicated clusters for customers who need stronger isolation.

Database failures affect all tenants sharing that database. Mitigation: per-region sharding at very large scale; standard database HA for normal operations.

Object storage regional outages affect all tenants in that region. Mitigation: cross-region replication for enterprise DR; no real-time mitigation for standard tier (we depend on cloud provider reliability).

### Cascading failure prevention

- Circuit breakers on downstream calls: if catalog API is slow, workers don't retry forever.
- Queue depth limits: if activity queues grow, scheduler throttles new work.
- Exponential backoff on retries: a flaky source doesn't generate load spikes.

## Capacity and Scaling

How we scale to handle more tenants.

### Horizontal scaling per layer

- **API Gateway**: stateless, scales horizontally.
- **Catalog**: single logical Postgres per region initially; sharded by tenant at very large scale (low-priority for launch).
- **Scheduler**: stateless, coordinates via catalog + Temporal.
- **Workers**: autoscaled based on task queue depth per task queue.
- **Temporal**: scales via its own sharding model.
- **Object storage**: cloud-native scalability.

### Scaling triggers

- **Worker pool grows** when task queue depth > target for > N minutes.
- **Worker pool shrinks** when CPU/memory utilization < target for > M minutes.
- **Catalog read replicas** added when query latency on primary exceeds SLO.
- **Temporal shards** added per Temporal Cloud's operational guidance.

### Per-tenant capacity guarantees

**Standard tier**: best-effort. Quotas prevent abuse; burst allowed.

**Enterprise tier**: reserved capacity. A customer's enterprise contract may guarantee N workers always available, or a specific throughput rate. Implemented via dedicated worker pools.

## Tenant Lifecycle

### Provisioning

A new tenant is created by:

1. Control plane provisions the tenant record (catalog).
2. Control plane creates the tenant's Temporal namespace.
3. Control plane creates the tenant's storage bucket prefix and secrets-backend path.
4. Control plane issues initial tenant admin credentials.
5. User logs in, sets up workspace and connections.

Provisioning is a Temporal workflow in the control namespace (RFC 2) for durability and idempotency.

### Suspension

A tenant can be suspended (payment failure, terms violation):

- All pipelines transition to paused state.
- In-flight activities complete normally (they're already running); no new activities start.
- Stored data remains for a grace period (90 days default).
- Tenant admin can resume with billing / ToS resolved.

Suspension is a policy decision enforced at multiple layers: scheduler refuses to start new runs; API rejects non-admin operations.

### Termination

After prolonged suspension or explicit termination:

- Tenant data is archived to the tenant's designated export location (if configured) or deleted.
- Workspace, connection, pipeline, schema records are deleted.
- Staging bucket contents deleted.
- Secret references deleted; underlying secret material deleted per backend policy.
- Audit log retained for the audit retention period (RFC 15) — 7 years default.
- Temporal namespace is drained and deleted.

Termination is irreversible after the grace period elapses. A Temporal workflow orchestrates termination for durability (multi-step process must complete even across failures).

### Data export before termination

Before termination, tenants have a configurable grace window (default 30 days post-suspension) to export their data. The platform provides:

- Full export of catalog metadata.
- Full export of audit log.
- Snapshot of staging data (current contents).
- Optional replay of CDC streams to a customer-chosen destination.

This is a compliance and customer-trust feature; not every customer will use it, but its availability is table stakes.

## Implementation Discipline

A few CI/testing commitments specific to tenant isolation.

### Tenant-isolation tests

A dedicated test suite attempts cross-tenant access at every layer:

- Catalog queries against tenant A with tenant B's identity; must fail.
- Object storage reads on tenant A's prefix with tenant B's credentials; must fail.
- Secret reads on tenant A's path with tenant B's worker identity; must fail.
- Workflow polls on tenant A's namespace with tenant B's credentials; must fail.
- Impersonation attempts via forged identity claims; must fail.

Run in CI on every merge. Release-blocking.

### Tenant-scoped resource leak tests

Tests that exercise tenant lifecycle: provision → use → terminate, then verify no residual data, credentials, or access:

- No files in the storage bucket with deleted tenant's prefix.
- No rows in catalog tables with deleted tenant's ID.
- No active secrets references.
- No Temporal namespace entries.

Run nightly.

### Type-level enforcement

As noted above, the `TenantId` type has no arbitrary constructor. CI lints enforce that resource-accessing functions take `TenantId`, not raw `Uuid`. This prevents "forgot to scope this query" from making it to code review.

## Deployment Mode Comparison

Summary table of isolation properties per mode:

| Property | Hosted (Default) | Hosted (Enterprise) | BYOC | Self-Hosted |
|---|---|---|---|---|
| Data plane infrastructure | Shared | Dedicated (in our cloud) | Customer cloud | Customer infrastructure |
| Co-tenant workers | Yes (multiplexed) | No | N/A | N/A |
| Co-tenant storage | Shared bucket, prefix-scoped | Separate bucket | Customer bucket | Customer-chosen |
| Co-tenant Temporal | Shared cluster, separate namespace | Dedicated cluster | Customer Temporal | Customer Temporal |
| Co-tenant secrets | Shared backend, path-scoped | Separate paths | Customer backend | Customer-chosen |
| Control plane | Our cloud | Our cloud | Our cloud | Customer infrastructure |
| Customer data in our infrastructure | Yes (scoped) | Yes (dedicated) | No | No |
| Best for | General, cost-sensitive | High-compliance SaaS | Enterprise, data-resident, FSI | Air-gapped, classified, on-prem-only |

## Alternatives Considered

**One-container-per-customer-per-pipeline.** Kubernetes pod per pipeline, hard process isolation. Operational and cost-wise catastrophic at our scale. Rejected: doesn't meet the economics wedge.

**Unikernels per tenant.** Stronger isolation than containers, lighter weight. Too immature and operationally complex. Rejected.

**Running each tenant's data plane as a separate Kubernetes namespace.** Stronger than multi-tenant namespaces; weaker than dedicated clusters. Workable for enterprise tier; over-engineered for default. We use the tier-based approach instead.

**Cryptographic isolation** (all tenant data encrypted with tenant keys that workers briefly use). Adds computational overhead and doesn't address noisy-neighbor. Encryption at rest already happens; at-rest and in-motion encryption with tenant-specific keys is a feature we offer, not the primary isolation mechanism.

**Single-tenant-per-worker at all times.** The strictest option short of BYOC. Rejected as default because worker idle time dominates costs; we multiplex by default for cost efficiency. Available as enterprise dedicated-workers tier.

**No enterprise tier; everyone gets the same isolation.** Simpler. Rejected: high-compliance customers will pay for stronger isolation, and offering it at a premium is how we fund the engineering to build it well.

## Open Questions

1. **Cross-tenant CDN / caching.** We cache compiled wasm artifacts at workers. Cache key includes tenant? Not strictly necessary for public connectors (same artifact across tenants), but worth thinking through. Probably keyed by artifact hash, not tenant.
2. **Tenant migration.** Moving a tenant between regions or from hosted to BYOC. Technically possible but operationally complex (pipelines, staging, Temporal histories). Defer; likely a professional-services engagement rather than self-service.
3. **Shared connector registry across tenants.** First-party connectors are universal. Third-party connectors: how does publisher / subscriber model work across tenants? Registry RFC.
4. **Burst budget accounting.** Enterprise burst allowances need a model (credits? tokens?). Punt to billing RFC.
5. **Compliance-tier certifications.** SOC 2, HIPAA, FedRAMP — each has specific isolation requirements that may constrain the above. Work through certification-specific requirements separately from architecture.

## References

- Google Cloud's multi-tenancy patterns: https://cloud.google.com/architecture/multitenant
- AWS well-architected framework on multi-tenant SaaS: https://aws.amazon.com/saas/
- Temporal namespace documentation: https://docs.temporal.io/namespaces
- NIST SP 800-53 (controls relevant to tenant isolation).
- Snowflake's multi-tenant account model (prior art).
- Postgres Row-Level Security: https://www.postgresql.org/docs/current/ddl-rowsecurity.html

## Decision

**Accepted pending review.** RFC 17 next: Quotas, Billing Metering, and Backpressure — which builds on the quota enforcement sketched here to specify how usage is measured, priced, and billed.
