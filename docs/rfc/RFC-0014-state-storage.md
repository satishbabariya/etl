# RFC 0014: State Storage Architecture

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0002 (Core Architecture), RFC 0004 (Temporal Topology), RFC 0007 (Incremental Sync), RFC 0008 (CDC Architecture), RFC 0010 (Catalog), RFC 0011 (Secrets)

## Summary

This RFC consolidates and specifies the complete state architecture of the platform: every category of persistent state, where it lives, its durability class, its retention policy, its backup/restore story, and how it relates to every other category. Prior RFCs have introduced state requirements in scattered places ("the catalog holds…", "Temporal persists…", "staging is in object storage…"); this RFC pulls them together and makes the implicit explicit.

The goal is that any engineer or operator can answer, for any datum the platform handles, four questions: where is it, how long does it live, what happens if it's lost, and who can see it.

## Motivation

A data platform is, at the end, a state-management system. Where state lives determines:

- **What survives failures.** A workflow state stored only in Temporal survives worker crashes but not Temporal corruption. A cursor stored only in the catalog survives anything but has commit-ordering requirements.
- **What costs what.** Metrics retained for 90 days in Postgres cost an order of magnitude more than the same metrics in object storage. Observability state is high-volume and low-per-record-value; catalog state is low-volume and high-per-record-value. Mixing them up produces either ruinous costs or inadequate retention.
- **What's auditable.** Billing-grade durability ≠ observability-grade durability ≠ scratch-grade. Tax auditors care about different things than SREs.
- **What the restore story is.** If the staging bucket is lost, do pipelines recover? If the Temporal namespace is lost, what's destroyed? Teams need a clear answer, not the handwave of "eventually consistent backups somewhere."

Prior RFCs established individual answers. This RFC is the unified map. It also resolves inconsistencies — a couple of cases where different RFCs implied different things about the same state — explicitly.

## Non-Goals

- This RFC does not choose specific cloud-vendor services by name (though we mention typical choices). Deployment specifics are RFC 18 (future).
- This RFC does not cover the wire protocols for state access. Those are per-component.
- This RFC does not cover encryption keys for state at rest. Encryption-at-rest uses cloud-provider KMS + per-tenant envelope keys, uniformly across all state categories. Covered in the security RFC (future).
- This RFC does not specify the metrics schema, log schema, or audit schema in detail. Those are RFC 15. This RFC specifies where that data lives and how long.
- This RFC does not cover disaster recovery (cross-region failover, recovery-point objectives). That's RFC 18. This RFC specifies what is backed up and what is ephemeral; recovery procedures are deployment.

## Durability Classes

Before cataloging state, we define the durability classes we commit to.

### Class A: System of Record

Loss is a platform incident. Backed up aggressively; restore procedures tested quarterly. Typically stored in Postgres or an equivalent durable, transactional database, plus regularly snapshotted.

Examples: pipeline definitions, schemas, connection metadata (not credentials), billing events, audit logs.

### Class B: Authoritative Execution State

Loss affects in-flight work. Stored in Temporal's backing store (shards persisted to a cloud provider's database). Backed up via Temporal's own mechanisms. Loss of this state does not destroy historical correctness (the catalog records that runs happened), but destroys in-flight workflows.

Examples: workflow state, cursor values mid-run, CDC replication position between commits.

### Class C: Recoverable Staging

Loss is operationally painful but platform-recoverable. Stored in object storage (S3/GCS/Azure Blob). Replicated within a region per cloud provider's guarantees. Cross-region replication is an enterprise-tier feature.

Examples: staging Arrow batches, intermediate transformation outputs, dead-letter rows.

### Class D: Operational Telemetry

Loss is tolerable for individual records. Aggregate loss is visible on dashboards but doesn't affect correctness of data movement. Stored in time-series systems with aggressive retention.

Examples: per-batch metrics, worker-internal logs, per-activity performance traces.

### Class E: Ephemeral Cache

Loss is a cache miss. Stored in memory or fast-repopulable stores (Redis, local disk).

Examples: connector AOT-compile artifact cache, schema fingerprint cache, access-token cache, parsed-config cache.

## The State Map

Every state category the platform maintains, in a single table. Each row: what it is, durability class, storage backend, owner, retention, backup.

### Control plane state

| Name | Class | Backend | Owner | Retention | Backup |
|---|---|---|---|---|---|
| Tenant records | A | Postgres (ctrl) | Catalog | Indefinite | Daily snapshot + PITR |
| Workspace records | A | Postgres (ctrl) | Catalog | Indefinite | Daily snapshot + PITR |
| Pipeline definitions (current + versions) | A | Postgres (ctrl) | Catalog | 90d of old versions | Daily snapshot + PITR |
| Connection metadata (non-secret) | A | Postgres (ctrl) | Catalog | Indefinite | Daily snapshot + PITR |
| Stream configs | A | Postgres (ctrl) | Catalog | Indefinite | Daily snapshot + PITR |
| Schema chains (per stream) | A | Postgres (ctrl) + object storage for large payloads | Catalog | Indefinite | Daily snapshot + PITR |
| Connector registry entries + manifests | A | Postgres (ctrl) | Registry | Indefinite | Daily snapshot |
| Transformation registry entries + manifests | A | Postgres (ctrl) | Registry | Indefinite | Daily snapshot |
| Connector wasm artifacts (AOT .cwasm) | A | Object storage (registry bucket) | Registry | Indefinite | Multi-region replication |
| Transformation wasm artifacts | A | Object storage | Registry | Indefinite | Multi-region replication |
| Run records (head data) | A | Postgres (ctrl) | Catalog | 90d hot / 7y archive | Snapshot + archive |
| Run events (detailed) | A→C | Postgres (ctrl) → object storage | Catalog/Observability | 90d hot / 2y archive | Snapshot + archive |
| Lineage graph (derived) | B | Postgres (ctrl) | Catalog (derivation job) | Rebuildable from run events | Rebuilt, not backed up |
| Billing events | A | Postgres (ctrl, billing schema) | Billing | 7y | Daily snapshot + archive |
| Audit log | A | Append-only store + object storage | Audit | 7y+ | Hash-chained; external anchor (RFC 11) |
| User accounts / SSO mappings | A | Postgres (ctrl) | Auth | Indefinite | Daily snapshot + PITR |
| API keys + machine identities | A | Postgres (ctrl) + dedicated secrets store | Auth + Secrets | Until revoked | Daily snapshot |
| Control-plane Temporal (tenant lifecycle workflows) | B | Temporal cluster | Temporal | Per retention policy (7d typical) | Temporal backup |

### Data plane state (per tenant)

| Name | Class | Backend | Owner | Retention | Backup |
|---|---|---|---|---|---|
| Data-plane Temporal (pipeline workflows) | B | Temporal cluster (tenant namespace) | Temporal | 7d (recent) / 30d (completed) | Temporal backup |
| Workflow state in-flight | B | Temporal event history | Workflow | Lifetime of workflow | Via Temporal |
| Cursors (incremental sync position) | B | Temporal workflow state | Workflow (mid-run) / Catalog (post-commit) | Committed → Class A | Via Temporal + Postgres |
| CDC replication position | B | Temporal workflow state | Workflow | Lifetime of CDC workflow + commit | Via Temporal + Postgres |
| Connector-local state (per-stream KV) | A | Postgres (ctrl) | Catalog (via `platform:state/cursor` host API) | Lifetime of pipeline | Daily snapshot |
| Staging Arrow batches | C | Object storage (staging bucket) | Worker | 48h default (configurable) | Replicated within region |
| Dead-letter rows | A | Object storage (dead-letter bucket) | Loader + Catalog | 30d default | Replicated within region |
| Destination-side staging tables | C | Destination (Snowflake, etc.) | Loader | Cleaned up post-commit | Destination's own backup |
| Destination-side idempotency log | A | Destination | Loader | Per loader policy (typically 30d) | Destination's own backup |
| Schema fingerprint cache (worker) | E | Worker local disk / memory | Worker | Until evicted / restart | None (rebuildable) |
| Connector instance pool | E | Worker memory | Worker | Until retired (RFC 5) | None |
| Compiled wasm artifacts (worker cache) | E | Worker local disk | Worker | LRU | None (re-fetched) |

### Secrets

| Name | Class | Backend | Owner | Retention | Backup |
|---|---|---|---|---|---|
| Secret material (plaintext) | A | Per-mode: AWS Secrets Manager / GCP Secret Manager / Vault / customer-backend | Secrets subsystem | Until revoked + grace window | Per backend's policy |
| SecretRef metadata | A | Postgres (ctrl) | Catalog | Indefinite | Daily snapshot |
| Access tokens (OAuth) | E | Redis (encrypted) | Secret resolver | Short TTL | None (regenerated from refresh token) |
| Refresh tokens | A | Same as secret material | Secrets subsystem | Until revoked | Per backend |
| Secret audit trail | A | Audit log (see above) | Audit | 7y+ | Hash-chained |

### Observability

| Name | Class | Backend | Owner | Retention | Backup |
|---|---|---|---|---|---|
| Structured logs (worker) | D | Log aggregation (e.g., managed Loki / CloudWatch / OpenSearch) | Observability | 30d default | Aggregator's retention |
| Metrics (platform) | D | Time-series DB (e.g., Prometheus / managed equivalent) | Observability | 30d hot / 13mo downsampled | Time-series DB's retention |
| Distributed traces | D | Tracing system (e.g., Tempo / managed equivalent) | Observability | 7d default | Tracing system's retention |
| Pipeline-level dashboards state | E | Dashboard system (e.g., Grafana) | Observability | User-defined | Config backed up; data is view |
| Alert rules | A | Postgres (ctrl) | Observability | Indefinite | Daily snapshot |

### Schedules and queues

| Name | Class | Backend | Owner | Retention | Backup |
|---|---|---|---|---|---|
| Pipeline schedules | A | Postgres (ctrl) | Scheduler | Indefinite | Daily snapshot |
| Scheduled triggers (upcoming) | B | Scheduler service state + Temporal | Scheduler | Until fired | Via Scheduler DB |
| Task queues (Temporal) | B | Temporal | Temporal | Per Temporal policy | Via Temporal |
| Control plane background jobs | B | Temporal (control namespace) | Various services | 7d typical | Via Temporal |

## The Three Storage Systems, Clarified

Prior RFCs have implied a three-way split. Here we name it explicitly.

### Storage system 1: The catalog database

A Postgres instance (per region for our-hosted, per-tenant for BYOC) holding all Class A *metadata* state. High read/write rate for active operations; modest storage footprint.

Characteristics:
- Transactional.
- Horizontally partitioned by tenant for very large deployments; single-database at launch scale.
- Point-in-time recovery (PITR) enabled.
- Daily snapshot to object storage for long-term backup.
- Encryption at rest via provider KMS with per-tenant envelope keys.

What it is not: bulk data storage. Any entity with an unbounded payload (wasm artifacts, archived run events, staged batches) lives elsewhere and is referenced from the catalog.

### Storage system 2: Temporal's backing store

Temporal has its own persistence (Cassandra in classic deployments, Postgres in newer ones, DynamoDB in Temporal Cloud). We treat Temporal as an opaque Class B store: workflows and activities have durable state, we access it through Temporal APIs, we do not direct-write to its backend.

Characteristics:
- Durable per Temporal's guarantees.
- Backed up per Temporal Cloud's policies, or per customer's Temporal operational practices in self-hosted mode.
- Retention governed by Temporal namespace config (7d active, 30d completed as our defaults).
- Cross-region replication depends on deployment tier.

What it is not: long-term state. Workflows complete and fall out of retention. We do not rely on Temporal history for historical queries beyond its retention window; run metadata we care about is copied to the catalog at run completion.

### Storage system 3: Object storage

S3/GCS/Azure Blob, per cloud. Holds Class C (staging, dead-letter), large Class A payloads (wasm artifacts, archived logs, snapshot archives), and anything bulk.

Characteristics:
- Strong per-object consistency, no transactional semantics.
- Cheap; default choice for any data >1MB.
- Region-scoped. Cross-region replication is opt-in and enterprise-tier.
- Lifecycle policies for retention (automatic deletion per class and age).

What it is not: a transactional store. We use it for content-addressed, immutable artifacts and for data whose commit happens elsewhere (staging is committed by a Temporal workflow writing a reference).

Additional storage systems exist for specific categories — the audit store (append-only + external anchor), the secrets backend (AWS Secrets Manager et al.), the time-series observability store — but 95% of platform state fits cleanly into one of the three above.

## Commit Relationships

Several state categories are related by commit ordering. Getting these right is essential for correctness after failures.

### Staging → destination → cursor

Sequence per RFC 4:

1. Worker writes Arrow batch to staging (object storage). Staging is durable at this point.
2. Loader reads staging, writes to destination. Destination commit is the authoritative point of delivery (RFC 9).
3. Worker emits a commit activity: updates cursor in catalog (system of record for cursor-at-rest).

Failure modes:

- Crash after step 1, before step 2: staging exists; Temporal re-triggers the load activity; loader reads same staging; delivers; idempotent at destination by `LoadId`.
- Crash after step 2, before step 3: destination has data; cursor not yet advanced. Next run re-reads source from prior cursor; connector emits overlapping rows; loader dedups at destination by PK; cursor advances on successful commit.
- Crash during step 3: Catalog update is transactional; either cursor is updated (success) or it isn't (retry).

### Temporal workflow state → catalog

Workflow state is authoritative *during* a run. The catalog is authoritative *between* runs.

At run start: workflow loads cursor from catalog.
During run: workflow holds current cursor in its state.
At run commit: workflow writes cursor to catalog (activity).
After run: catalog is the source of truth.

If the workflow is lost (Temporal cluster catastrophic failure between run start and commit): the in-flight run is destroyed. The next run starts from the previously-committed cursor in the catalog. At-most one run is affected by a Temporal failure; historical runs and cursors are preserved in the catalog.

### Dead-letter → run metadata

Dead-letter rows live in object storage. The run record in the catalog references the dead-letter object(s). Deleting dead-letter objects before deleting the run record produces dangling references; we avoid this by:

- Dead-letter lifecycle policy runs after run-record archival.
- Run archival moves the run record to cold storage, then the dead-letter lifecycle job deletes the object.

## Retention Policies

Retention is where state classes meet product decisions. The defaults below; per-plan-tier customization governs actual values.

### Long-retention (7+ years)

- Billing events.
- Audit log.
- Compliance-grade run history for regulated-industry customers.

These exist because auditors (tax, SOC 2, HIPAA, etc.) ask for them. Storage cost is a cost of being in business for regulated customers; we pass it through transparently in pricing tiers.

### Medium-retention (90 days to 2 years)

- Run events detail (90d hot, 2y archived).
- Dead-letter rows (30-90d depending on tier).
- Schema change history (indefinite but compact; kept with the catalog).
- Observability logs and metrics (30d hot, 13mo downsampled).

These support debugging, incident review, and medium-term operational analysis. The hot/archive split keeps Postgres small while preserving access through object-storage-backed archives.

### Short-retention (hours to weeks)

- Staging batches (48h default, extendable to 30d for debug).
- Temporal workflow history after completion (30d).
- Worker caches (indefinite until evicted).
- Access tokens (minutes, via TTL).

### Zero-retention (ephemeral)

- In-flight Arrow batches in worker memory.
- Plaintext secrets (activity-scoped; zeroed on drop).
- Compiled cached configs.

Per-tenant retention overrides are supported via plan-tier configuration. An enterprise tier can extend staging retention to 90d, run event archive to 5y, etc. We do not offer shorter-than-default as a cost-saving option — short retention is also short debugging runway, which is a bad trade.

## Backup and Recovery

We commit to specific RPO (recovery point objective) and RTO (recovery time objective) per state category.

### Backup cadences

- **Postgres catalog**: continuous PITR (point-in-time recovery) for the last 7 days + daily full snapshot retained 90 days. RPO for recent state: < 5 minutes. For older state: last preceding daily snapshot.
- **Temporal backing store**: per Temporal Cloud's own policy (or customer-managed in self-hosted). Typical RPO < 1 hour; RTO depends on Temporal Cloud team's operational response.
- **Object storage**: provider's own durability guarantees (11 9's for S3, for instance). No additional application-level backup for ephemeral tiers; multi-region replication for Class A artifacts (wasm modules, long-term archives) in enterprise tiers.
- **Audit log**: write-through replication + daily external anchor. Effectively zero tolerance for loss; tamper-evidence via hash chain.

### Recovery scenarios

**Loss of catalog database primary.** Fail over to replica (automatic via managed Postgres). RTO minutes; RPO near-zero.

**Loss of entire catalog region.** Restore from latest snapshot in another region. RTO hours; RPO up to 24 hours for non-PITR snapshots. Enterprise tier: streaming replication to secondary region for near-zero RPO.

**Loss of Temporal cluster.** In-flight workflows destroyed. Historical data preserved in catalog. Pipelines with incomplete runs re-run from their last-committed state. RTO: time to stand up new Temporal cluster + redeploy workers; pipelines resume automatically on catalog cursor.

**Loss of staging bucket.** Ongoing pipelines fail; new runs re-extract. No data loss at the destination (already committed). Dead-letter rows from the affected window are unrecoverable; logged as incident. Multi-region replication mitigates this for enterprise tier.

**Loss of observability stack.** Dashboards unavailable; audit continues via its separate pipeline. Recovered by rebuilding the stack; historical metrics from the affected window are lost but not critical.

**Loss of secrets backend.** Catastrophic for pipelines using its secrets. Recovery: restore backend from its provider's backup (AWS Secrets Manager has its own history; Vault has its snapshots). While the backend is down, pipelines pause at activity setup.

**Corruption of audit log.** Detected via hash-chain verification. The external anchor is used to establish the last-good point. Events after corruption are lost for any tenant affected; we disclose this as an incident.

### Restore testing

We commit to **quarterly restore drills**: a non-production clone of the catalog is restored from backup, verified for integrity, and compared to live. Restore drills are required gates for Production-Ready declarations on any new storage system. This is an operational commitment, not an architectural decision, but worth recording here.

## Data Locality and Region

All tenant state is region-scoped. A tenant provisioned in `us-east-1` has its catalog, Temporal namespace, staging bucket, and secrets all in `us-east-1`. Cross-region data movement is explicit, auditable, and limited to:

- Cross-region replication for enterprise-tier disaster recovery (opt-in).
- Customer-initiated data export.
- Support escalations with customer consent.

Regional residency is critical for compliance (GDPR, data sovereignty laws). The state architecture makes it easy: every storage backend is a region-scoped resource; no cross-region writes occur in the hot path.

## Tenant Isolation at the Storage Layer

Tenants share physical infrastructure in hosted mode; isolation is logical. Each state category has an isolation posture:

- **Catalog (Postgres)**: row-level isolation via `tenant_id` column on every table; every query includes `WHERE tenant_id = <current>` enforced by access policy. Aggressive use of row-level security (RLS) where supported.
- **Temporal**: namespace-per-tenant (RFC 2). Namespace is the isolation boundary.
- **Object storage**: prefix-per-tenant (`/tenants/<tenant_id>/`). IAM policies scoped per worker identity.
- **Secrets**: per-tenant paths in backend; strong isolation via IAM.
- **Observability**: tenant label on every metric/log/trace. Queries scoped by tenant label at the query layer.

BYOC mode removes shared-infrastructure concerns entirely (tenant has its own data plane; no co-tenants).

## Storage Cost Accounting

State storage is a meaningful platform cost. We track cost per tenant per storage category for internal economics and customer pricing:

- Postgres row-count + byte-size per tenant (via catalog queries).
- Temporal history-size per namespace.
- Object storage bytes per tenant prefix.
- Time-series cardinality per tenant label.
- Secrets backend API calls per tenant.

These feed the billing system (RFC 17) and internal capacity planning. A tenant whose storage costs us more than they pay us is a flag for the account team.

## State and the Deployment Modes

### Hosted

All storage systems in our cloud accounts. Standard multi-tenant isolation. Customer sees a logical tenant; physical infrastructure is shared.

### BYOC

Control plane state (catalog, registry) in our cloud; data plane state (Temporal, object storage, secrets) in customer cloud. Boundary is strict: no customer row data in our catalog. The split aligns with RFC 2's control/data plane invariant.

### Self-hosted

Everything in customer infrastructure. We ship operational playbooks for catalog backup, Temporal operations, object storage lifecycle, etc. Customer owns the runbook.

Critically, the **state architecture is identical** across modes. Only the operator changes. Code doesn't branch on mode for state access; configuration selects the backend.

## Alternatives Considered

**Use one backing store (e.g., CockroachDB everywhere) for all structured state.** Simpler operationally. Rejected: Temporal and the catalog have very different access patterns (high-concurrency workflow-state reads/writes vs. modest-rate transactional metadata). Forcing them into one store either over-scales the metadata side or under-scales the workflow side. Specialization wins.

**Store workflow cursors directly in the catalog; skip Temporal's durable state for them.** Tempting for transparency. Rejected: the catalog would become a hot path in workflow execution, contending with metadata operations. Temporal's state mechanism is optimized for this; we use it.

**Object storage for archived run events + Postgres for hot run events, vs. keeping all in Postgres.** We chose the hybrid. Rejected the all-Postgres option: run events are high-volume (billions/year at scale) and Postgres storage costs would dominate. Hybrid is standard industry practice.

**Global Redis cluster for all Class E ephemeral state.** Rejected for staging-boundary data (we use local disk on workers). Redis is appropriate for access tokens (needed cross-worker); local disk wins for per-worker caches that don't need sharing.

**Put audit and billing in the same store.** Both are Class A and 7-year retention. Rejected: tamper-evidence requirements differ; billing needs read-mostly for invoice generation while audit is write-mostly; separation simplifies access control.

**Eventually-consistent lineage in a graph database.** Considered in RFC 10. Rejected there; reaffirmed here: adjacency tables in Postgres handle our query patterns fine.

## Open Questions

1. **Temporal backing store choice in self-hosted mode.** Customers may prefer MySQL over Postgres for operational reasons. Ensure our self-hosted docs cover both; no architectural change required.
2. **Staging in destination-local region vs. source-local region.** For cross-region pipelines, which region does staging live in? Source-local minimizes extract egress; destination-local minimizes load egress. Probably destination-local by default; per-pipeline override. Revisit once we have cross-region customer data.
3. **Archive format for run events.** Parquet seems natural but requires a schema definition. JSON is flexible but costly. Go with Parquet, with a versioned schema. Tune.
4. **Retention for diagnostic bundles.** We will occasionally capture diagnostic snapshots (a worker's recent activity records for support debugging). Retention policy for these is TBD; likely 30d with customer consent for anything involving their data.
5. **Cross-region audit anchoring.** External anchor (blockchain tx or equivalent) is published daily; the specific vendor/chain is TBD. Choose for longevity more than cost — an anchor that disappears in 5 years defeats the purpose.
6. **Cost of cross-region replication at scale.** Enterprise tier offers it; marginal cost per customer is non-trivial. Monitor and adjust pricing if the tier becomes unprofitable.

## References

- Temporal's persistence model: https://docs.temporal.io/self-hosted-guide/persistence
- Postgres PITR docs: https://www.postgresql.org/docs/current/continuous-archiving.html
- S3 durability design: https://aws.amazon.com/s3/storage-classes/
- Google Cloud Storage durability: https://cloud.google.com/storage/docs/availability-durability
- Hash-chained audit log patterns: Google's "trillian" is a widely-deployed reference.
- Data residency in SaaS: Snowflake's regional model is prior art for a similar tenant-in-region approach.

## Decision

**Accepted pending review.** RFC 15 next and last in the Platform tier: Observability, Lineage, and Audit — which builds on the state categories specified here to define what the platform makes visible to operators, customers, and auditors.
