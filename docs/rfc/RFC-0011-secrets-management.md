# RFC 0011: Secrets, Connections, and Credential Management

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0002 (Core Architecture), RFC 0005 (Wasm Runtime), RFC 0006 (Connector Protocol), RFC 0010 (Catalog)

## Summary

This RFC specifies how credential material (passwords, OAuth tokens, API keys, private keys, service account JSON) is stored, accessed, rotated, and audited across the platform's three deployment modes. It defines the storage backends (hosted, BYOC, self-hosted), the reference model that keeps secret material out of the catalog and out of workflow history, the runtime access pattern for connectors and loaders, the rotation lifecycle, and the audit requirements.

Secrets are the highest-consequence subsystem: a breach produces customer-data exposure at a scale the rest of the platform cannot. We design defensively, assume every other component can be compromised, and narrow the blast radius of a compromised secret to as close to zero as we can manage.

## Motivation

Every connector and loader requires credentials to reach external systems. At platform scale, we handle:

- Tens of thousands of customer connections.
- Hundreds of credential types (database passwords, OAuth refresh tokens, service account keys, SSH private keys, mTLS client certificates, AWS/GCP/Azure IAM role assumptions, Kerberos tickets).
- Frequent rotation (enterprise customers often mandate 30-90 day rotation).
- Shared credentials (one connection feeding many pipelines).
- Multi-deployment-mode reality: secrets in our-hosted mode, customer-cloud mode (BYOC), and fully self-hosted mode have different trust boundaries.

Getting this wrong in any of several ways produces incidents:

1. **Secret material in logs.** A debug log dumps a config struct containing a plaintext password; the log ships to our observability system and sits there searchable.
2. **Secret material in Temporal history.** Workflow input contains a credential; Temporal history is durable and widely readable; secret is exposed to anyone with workflow-read access.
3. **Secret material in catalog JSON.** Pipeline config includes a credential field; catalog is queried frequently and broadly.
4. **Over-broad access.** Every worker reads every secret "just in case"; one compromised worker compromises every customer.
5. **No rotation.** Credential rotation is operationally painful so it doesn't happen; long-lived secrets accumulate exposure.
6. **No audit.** An incident occurs and no one can answer "which secrets did the attacker see?"

We design to eliminate all six.

## Non-Goals

- This RFC does not cover user authentication to our platform (SSO, OAuth for user login, session management). Those are the Auth service (RFC 2), detailed in a future security RFC.
- This RFC does not cover end-user secrets (e.g., per-row encrypted data). That's destination-side encryption territory, separate concern.
- This RFC does not cover encryption at rest of the catalog / staging storage in general. Those use standard cloud-provider encryption and are covered by deployment RFCs.
- This RFC does not specify the cryptographic primitives in detail — we rely on proven primitives (AEAD, X.509) from well-reviewed libraries. No novel crypto.
- This RFC does not cover customer-data access by our staff (break-glass, customer support). That's an operational/compliance concern, future RFC.

## Core Principles

These are the decisions that constrain every design choice downstream. Violating any of these is a security incident, not a design tradeoff.

1. **Secrets never enter the control plane's durable state.** The catalog, Temporal history, observability logs, billing events — none contain secret material. Ever.
2. **Secrets are referenced, not embedded.** Connections, pipelines, and workflow inputs carry opaque `SecretRef` identifiers. Resolution happens at the point of use in the data plane.
3. **Secrets live in a dedicated store.** The store has a single purpose (hold secrets) and the smallest feasible API surface. Breaking it requires compromising the store itself, not the main platform.
4. **Access is auditable per-read.** Every read of a secret produces an audit event: who asked, for which secret, from which activity, at what time. Audit is non-tamperable.
5. **Secrets are scoped.** A secret belongs to one tenant, accessible only by that tenant's data plane. Cross-tenant access is not merely forbidden; it is structurally impossible.
6. **Rotation is a first-class operation.** The system supports changing a secret's value without pipeline downtime; old values are revoked after a grace window.
7. **Plaintext lives only in memory, and only briefly.** A plaintext secret is loaded into a worker's RAM for the duration of an activity, passed to the guest or native code that needs it, and zeroed immediately after use.
8. **Deployment mode changes the trust boundary, not the protocol.** The same client code reads secrets in our-hosted, BYOC, and self-hosted modes; only the backend differs.

## The Secret Reference Model

### `SecretRef` shape

Every secret in the catalog is represented by a `SecretRef`:

```
SecretRef {
  id: UUID,                    // stable identifier
  tenant_id: UUID,             // owning tenant
  name: string,                // human-readable label
  kind: SecretKind,            // typed (below)
  backend: SecretBackendRef,   // which backend holds the material
  backend_path: string,        // backend-specific address
  created_at, updated_at,
  current_version: u32,        // monotonic version
  rotation_policy: option<RotationPolicy>,
  last_rotated_at: option<Timestamp>,
  allowed_uses: list<AllowedUse>,  // restrict which connections can reference it
  tags: map<string, string>,
}

enum SecretKind {
  password,              // single opaque string
  api_key,               // opaque string
  oauth_refresh_token,   // oauth token with associated access-token cache
  private_key,           // pem-encoded private key
  certificate_chain,     // pem-encoded cert + chain
  service_account_json,  // structured JSON (GCP, etc.)
  aws_role_assumption,   // not a secret per se — an IAM role to assume
  kerberos_keytab,       // binary keytab
  connection_string,     // aggregate containing multiple credential fields
  custom,                // schema defined by the consumer
}

struct AllowedUse {
  scope: UseScope,            // which connection(s) can reference
  granted_at: Timestamp,
  granted_by: UserId,
}

enum UseScope {
  any_in_tenant,                 // default, backward-compatible
  workspace(WorkspaceId),
  connection(ConnectionId),
  pipeline(PipelineId),
}
```

The `SecretRef` lives in the catalog. Everything about the secret *except its value* is here.

### What a Connection stores

A Connection references secrets by logical role:

```
Connection {
  ...
  secret_refs: Map<string, SecretRef::Id>,
  // e.g., {"password": <uuid>, "client_cert": <uuid>}
}
```

The connector's config schema declares which logical secret names it expects. When a user creates a connection, they either supply new secret material (catalog creates a `SecretRef` + writes material to backend) or reference an existing `SecretRef` (reuse).

### What a Workflow input carries

Workflow activity inputs carry `SecretRef::Id` values, never secret material. A typical extract activity input:

```
ExtractActivityInput {
  stream: StreamRef,
  connection_snapshot: ConnectionSnapshot {
    config: json,              // non-secret config
    secret_refs: Map<string, SecretRef::Id>,
    backend_ref: SecretBackendRef,
  },
  ...
}
```

This is what ends up in Temporal history — and it contains no secrets.

## Secret Backends

The storage implementation varies by deployment mode. All backends implement a common Rust trait so client code is identical.

### Backend trait

```rust
pub trait SecretBackend: Send + Sync {
    async fn read(&self, path: &SecretPath, version: Option<u32>)
        -> Result<PlaintextSecret, SecretError>;
    async fn write(&self, path: &SecretPath, value: PlaintextSecret, policy: WritePolicy)
        -> Result<u32, SecretError>;  // returns new version
    async fn revoke(&self, path: &SecretPath, version: u32)
        -> Result<(), SecretError>;
    async fn describe(&self, path: &SecretPath)
        -> Result<SecretMetadata, SecretError>;
    fn capabilities(&self) -> BackendCapabilities;
}

pub struct PlaintextSecret {
    bytes: Vec<u8>,  // zeroed on drop via `zeroize` crate
}
```

`PlaintextSecret` wraps the material in a Rust type whose `Drop` implementation zeroes the memory. Every handling of secret material is expected to go through this type.

### Backend: Hosted (our cloud)

**Store**: AWS Secrets Manager / GCP Secret Manager / Azure Key Vault, per region. One top-level path namespace per tenant: `/tenants/{tenant_id}/secrets/{secret_id}`.

**Encryption**: managed KMS keys, per-tenant (tenant-specific KMS key for envelope encryption).

**Access**: IAM policies restrict access to the worker service identity. The control plane API (which creates secrets) has write-only access to the path pattern; the worker has read access. Separation of duties: the API can't read what it writes.

**Auditing**: cloud-provider-native audit logs (CloudTrail / Cloud Audit Logs), mirrored into our observability pipeline for a unified audit view.

### Backend: BYOC (customer cloud)

**Store**: customer's own Secrets Manager / KMS in their cloud account. Our control plane has no access to it; only the customer's data plane does.

**Credential for the data plane to access customer's secret store**: handled at deployment time via cloud-native service identity (EKS IRSA, GKE Workload Identity, Azure Managed Identity). No long-lived credential for our platform to hold.

**Our role**: we never see the plaintext secret material. The control plane only holds `SecretRef` metadata; the data plane resolves `SecretRef` → plaintext against the customer's own store.

This is the compliance win: a customer can say "your platform cannot exfiltrate our database passwords because it never has access to them." True statement, not a marketing statement.

### Backend: Self-Hosted

**Store**: customer-chosen — HashiCorp Vault, Kubernetes Secrets (with sealed-secrets or equivalent), cloud-native secret stores, or a customer-provided custom backend implementing our trait.

**Credential for data plane**: whatever the customer configures. We document best practices (Vault AppRole, Vault Agent sidecar, Kubernetes service account + Vault auth); we don't enforce one.

**Our role**: software only. No plaintext ever touches our systems.

### Vault as the reference backend

For self-hosted and for the customer-support interoperability story, HashiCorp Vault is the reference. We publish adapters for Vault's KV v2 engine, transit engine (for dynamic secrets), and database secrets engine (for ephemeral database credentials). The adapters are pluggable; customers with existing Vault deployments benefit the most.

## Runtime Access Pattern

This is what happens during a pipeline run.

### Activity setup

1. Activity starts with input containing `connection_snapshot.secret_refs`.
2. Activity invokes `SecretResolver::resolve_all(secret_refs)` — a worker-local service that calls the backend.
3. Backend returns `PlaintextSecret` values, scoped to this activity's lifetime.
4. Activity constructs a per-invocation `ResolvedCredentials` struct.
5. `ResolvedCredentials` is passed to the connector/loader code.
6. When the activity returns, `ResolvedCredentials` is dropped; all contained `PlaintextSecret` values are zeroed.

### Guest code access (wasm connectors)

Wasm connectors cannot directly call the Secret Backend — they access secrets only through the host API `platform:secrets/access` (RFC 5):

```
interface platform:secrets/access {
  get: func(secret-ref: string) -> result<string, secrets-error>
}
```

The "reference" here is not the global `SecretRef::Id`; it's a **per-activity local handle** the host issues at activity setup:

1. Host reads the raw secrets from the backend.
2. Host allocates local handles (e.g., `"primary-password"`, `"oauth-token"`) mapped to the plaintext in host memory.
3. Host passes handles (not UUIDs, not paths) to the guest in the activity context.
4. Guest calls `get("primary-password")` → host returns the plaintext for that activity only.
5. On activity end, all handles are invalidated.

Why local handles rather than the real `SecretRef::Id`: a compromised guest cannot enumerate secrets or request arbitrary ones. It can only resolve what the host granted it.

### Loader access (native Rust)

Loaders run in trusted native code, not wasm. They receive `ResolvedCredentials` directly. Same lifetime rules — drop at activity end, memory zeroed.

### Forbidden patterns

- Putting secrets in workflow-state (Temporal history) — detected by type-level enforcement: `WorkflowState` types do not include `PlaintextSecret`.
- Passing secrets across Temporal activity boundaries — same.
- Logging secrets — structured logging strips known-secret fields. Additionally, our logger refuses `PlaintextSecret` as a log field value (compile-time enforcement via trait bounds).
- Including secrets in error messages — errors carrying credentials are sanitized before serialization.

## OAuth Specifically

OAuth 2.0 flows are common for SaaS connectors (Salesforce, HubSpot, Google Ads, etc.) and have their own quirks.

### The token refresh problem

OAuth gives us two tokens: an access token (short-lived, used for API calls) and a refresh token (long-lived, used to get new access tokens). Access tokens expire mid-sync all the time; we must refresh without failing the sync.

### Design

**Refresh tokens** are stored as `SecretKind::oauth_refresh_token` in the backend. They are long-lived and require protection equivalent to passwords.

**Access tokens** are treated as **derived, cacheable** material. They are not first-class `SecretRef` entities. Instead:

1. A per-tenant OAuth cache (Redis or equivalent, encrypted at rest) stores access tokens by `SecretRef::Id`.
2. At activity setup, the resolver checks the cache for a live access token; if present and not near expiry, it's used.
3. If absent or near expiry, the resolver uses the refresh token to fetch a new access token and updates the cache.
4. Refresh coordination uses a tenant-scoped lock to prevent many workers refreshing simultaneously.

This is specifically **not** in the Secret Backend — access tokens churn on the order of minutes, and treating them as first-class secrets would produce heavy write volume on durable storage.

### Refresh failure

If refresh fails (refresh token revoked by source, provider-side policy change, expired), the connector activity fails with `auth_failed` (RFC 6). The pipeline pauses; operator re-authorizes; refresh token is updated; pipeline resumes.

### OAuth-initiated flows

Creating an OAuth connection involves a browser redirect flow at connection-creation time. The user authorizes our platform; the provider redirects back with an authorization code; we exchange the code for tokens. This happens in the control plane, which does temporarily see the refresh token to persist it. Security notes:

- The exchange is a direct server-to-server call from our control plane to the provider.
- The refresh token is written to the Secret Backend immediately.
- Plaintext is kept in control plane memory only for the duration of the exchange, then zeroed.
- The authorization code is single-use and short-lived; interception at the control plane is mitigated by the narrow time window.

This is the only moment the control plane touches secret material, and it's unavoidable by the nature of OAuth. We scope it narrowly and document it honestly.

## Dynamic Secrets

Some sources support short-lived, generated credentials — Vault's database engine is the canonical example, but AWS STS and cloud-native IAM role assumption produce the same effect.

### Why they matter

A static Postgres password stored for years is a breach waiting to happen. A Vault-generated password that lives 15 minutes eliminates that exposure window.

### Support

For connectors that benefit (databases, especially), we support dynamic-secret backends:

1. `SecretRef` is configured as `kind: dynamic, backend: vault_database`.
2. At activity setup, the resolver calls Vault (or equivalent) to generate a fresh credential.
3. The credential is used for this activity's duration.
4. On activity end, the resolver calls Vault to revoke the credential (best-effort; Vault also enforces TTL-based expiry).

Dynamic secrets are not supported for all source types — some sources (SaaS APIs) don't support them inherently. For those, we use static secrets with rotation.

### Cost consideration

Dynamic secrets require a round-trip to the backend at every activity start. For high-activity-rate pipelines, this is a measurable overhead. We cache within an activity (but not across) and batch requests where possible.

## Rotation

Rotation is the regular replacement of a secret's value. We support three modes.

### Mode 1: Manual rotation

The user (or an external automation) provides the new value via API or UI. The Secret Backend is updated:

1. Write new version to backend.
2. `SecretRef.current_version` is incremented.
3. Old version remains accessible (for grace window) but is no longer the default.
4. After grace window (default 24 hours, configurable), old version is revoked.

### Mode 2: Scheduled rotation (automated)

For secrets whose source supports programmatic rotation (databases, some APIs):

1. Rotation policy defines cadence (30 days, 60 days, etc.).
2. Scheduled workflow: generates new credential at source, writes to backend, runs validation.
3. If validation succeeds: promotes new version, retires old after grace.
4. If validation fails: rolls back, alerts operator.

Rotation is a control-plane workflow (Temporal control namespace, RFC 2). It is long-running and idempotent.

### Mode 3: Dynamic (per-use) secrets

Covered above. Rotation is implicit at every use; no separate rotation workflow.

### Grace windows matter

A rotation without a grace window causes in-flight activities to fail if they were holding the old value. The grace window guarantees already-started activities can complete before the old value is revoked. Default 24 hours is well beyond our longest typical activity.

## Audit

Every secret access produces an audit event. Audit events are non-tamperable (append-only, cryptographically hash-chained) and retained for 7 years by default (longer for compliance tiers).

### Event shape

```
SecretAccessEvent {
  event_id: UUID,
  occurred_at: Timestamp,
  tenant_id, workspace_id,
  secret_ref_id, version,
  actor: Actor {
    kind: system | user,
    identity: ServiceIdentity | UserId,
  },
  context: Context {
    activity_type: string,     // e.g., "extract_stream", "validate_connection"
    pipeline_id: option<UUID>,
    run_id: option<UUID>,
    workflow_id: option<string>,
  },
  outcome: granted | denied,
  denial_reason: option<string>,
}
```

### Collection

Audit events flow through a dedicated collection pipeline, separate from general observability (RFC 15). Reasons:

- Stronger durability guarantees required.
- Tamper-evidence required.
- Different retention policy.
- Compliance queries read audit separately from operational metrics.

### Access to audit

Tenant admins can read their own tenant's audit events. Cross-tenant audit access is limited to platform operators with specific compliance roles; every such access is itself audited (meta-audit).

### Tamper evidence

Audit events are stored with a per-record hash and a chained previous-hash pointer. A daily anchor publishes the chain tip to an immutable external location (e.g., a blockchain transaction, a git-signed tag in a public repository, or equivalent attestation). Altering past events requires forging the hash chain, which is infeasible without forgerly of the anchor.

This is overkill for most needs and reassurance for enterprise-compliance customers; the cost of implementing it correctly once is much less than the cost of failing to have it when an auditor asks.

## Tenant Isolation

A compromised tenant must not be able to read another tenant's secrets. Defense in depth:

1. **Backend path scoping.** Secret paths include `tenant_id`; path structure is enforced server-side by the backend's access policy.
2. **Worker scoping.** In our-hosted mode, worker service identities are per-tenant (or per-shard-of-tenants). A worker can only authenticate with the backend for the tenants it serves.
3. **Activity-level resolution.** Even within a worker, a resolver call is scoped to the activity's tenant (embedded in activity input, verified against the worker's allowed scope).
4. **BYOC mode eliminates the concern** by running each tenant's data plane in the tenant's own cloud account with no cross-access.

## Edge Cases

### Secret written but connection never created

A user starts creating a connection, provides credentials, then abandons the flow. Orphaned `SecretRef`s without referring connections are a leak.

**Mitigation**: `SecretRef` is marked with a grace period at creation. If no connection references it within 72 hours, it is revoked and deleted. Users who return later re-enter credentials.

### Connection deleted with shared secret

A secret shared across 5 connections; one connection is deleted. The secret must remain accessible to the other 4.

**Mitigation**: deletion is by reference-count. `SecretRef` is deleted only when no connection references it. Audit records the reference-count decrement.

### Worker compromised

An attacker gains access to a worker process.

**Blast radius**: secrets for tenants currently being served by the worker, for activities in flight or recent (memory-scraping). Not: other tenants, historical secrets (already zeroed), secrets for inactive pipelines.

**Mitigation**: per-tenant worker isolation (RFC 16); short-lived activity scopes minimizing in-memory secret lifetime; memory zeroing discipline; out-of-process detection via observability alerting on unusual secret-access patterns.

### Backend compromised

The Secret Backend itself is breached.

**Blast radius**: catastrophic. All secrets for all tenants whose material is in this backend are exposed.

**Mitigation**: the backend is the hardened core of the system. Reduced API surface, strong authentication, standard cloud KMS integration, audit, intrusion detection. BYOC mode eliminates this concern for sophisticated customers — their backend is theirs.

### Audit log compromised

Audit events are tampered.

**Mitigation**: hash-chained events with external anchors. Tampering is detectable on integrity check; the anchor provides a trusted reference.

## Client Library

All secret handling in the codebase goes through a single crate: `platform-secrets`. Direct access to backends from other code is forbidden (enforced by module visibility and CI checks).

`platform-secrets` provides:

- `SecretBackend` trait and implementations per deployment mode.
- `SecretResolver`, the worker-local service that caches backend clients and handles per-activity resolution.
- `PlaintextSecret`, the secure wrapper (with `zeroize::Zeroizing` under the hood).
- `SecretRef` types for catalog integration.
- Audit event emission.

This concentration is deliberate. Security review focuses on one crate; updates propagate everywhere; correct patterns are automatic (via type discipline).

## Testing and Validation

### Test discipline

- **No real secrets in test fixtures.** Tests use generated ephemeral credentials or mocked backends.
- **Fuzz tests for log/serialization paths.** Ensure no `PlaintextSecret` ever appears in log output, workflow state, error messages, or catalog writes.
- **Tenant isolation tests.** A worker instance for tenant A cannot read secrets for tenant B, tested via attempted access.
- **Rotation tests.** End-to-end rotation of each backend type; old-version access after grace is denied.
- **Audit completeness tests.** Every access path produces the expected audit event.

### Red-team review

We commit to periodic third-party security review of this subsystem. Not specified here; committed as a practice.

## Alternatives Considered

**Embed secrets directly in workflow inputs (encrypted in workflow state).** Simpler. Rejected: encrypted-at-rest in Temporal is a weaker property than "never stored there at all." A compromised Temporal operator or a bug in history display exposes everything.

**Use a single shared secret store across deployment modes.** Simpler code. Rejected: BYOC's compliance story is built on "we don't have access to your secrets." Sharing breaks this.

**Rely on cloud-provider secret stores alone, no abstraction.** Simpler. Rejected: BYOC and self-hosted modes need different backends; customers with Vault investments want to use Vault. The abstraction is not incidental — it's the BYOC story.

**Make access tokens first-class SecretRefs.** More uniform. Rejected: access tokens churn in minutes; treating them as catalog entities produces heavy write load and clutters the secret audit trail with ephemeral events.

**Give guests direct access to Secret Backend.** Less host-mediation. Rejected: a compromised guest could enumerate secrets and exfiltrate. Host mediation with per-activity handles is the right security posture.

**Skip dynamic secret support at launch.** Simpler. Partially accepted: dynamic secrets are an optional feature per connector; launch set includes Vault database engine as the reference, other sources added post-launch as customer demand warrants.

**Audit events into the same pipeline as operational metrics.** Simpler. Rejected: different retention, different durability, different tamper-evidence requirements. Separate pipelines are worth the cost.

## Open Questions

1. **Key management for tenant-KMS-key bootstrap in our-hosted mode.** Who creates the tenant's KMS key, and when? At tenant provisioning? Defer to deployment RFC.
2. **Secrets UI.** How does a user rotate a secret via the UI? How is the old value destroyed? Product UX, not in this RFC but flagged for coordination.
3. **Secret sharing across workspaces.** The `UseScope::any_in_tenant` is the default; do we need cross-workspace sharing with governance? Defer.
4. **Secret import from external Vaults.** Customer has secrets in their own Vault; they want to reference them without re-entering. Supported in BYOC / self-hosted; in hosted mode this is a federation question. Flag for follow-up.
5. **Break-glass access for platform operators.** For customer support scenarios, do operators ever need access to a tenant's secrets? Strict answer: no. Pragmatic answer: maybe, with very heavy audit. Defer to operational/compliance RFC.
6. **Certificate (mTLS) renewal.** mTLS client certificates have their own renewal flow (CSR + CA signing). Treat as a rotation variant with a specialized workflow. Defer to a per-kind specification.

## References

- HashiCorp Vault documentation: https://developer.hashicorp.com/vault
- AWS Secrets Manager: https://docs.aws.amazon.com/secretsmanager/
- GCP Secret Manager: https://cloud.google.com/secret-manager/docs
- Azure Key Vault: https://learn.microsoft.com/en-us/azure/key-vault/
- OAuth 2.0 RFC 6749 (for refresh-token handling): https://datatracker.ietf.org/doc/html/rfc6749
- The `zeroize` crate for memory scrubbing: https://crates.io/crates/zeroize
- OWASP Cryptographic Storage Cheat Sheet (reference for general principles).
- NIST SP 800-57 (key management guidance).

## Decision

**Accepted pending review.** This completes the Execution tier (RFCs 4-11). The platform's hot path — durable execution, wasm runtime, connector protocol, cursor correctness, CDC, destination delivery, catalog, secrets — is fully specified.

The Platform tier begins with RFC 12 (Transformation Layer and UDF Model), which specifies the user-authored data-transformation stage that sits between extract and load. After that: RFC 13 (Pipeline DSL), RFC 14 (State Storage Architecture), and RFC 15 (Observability, Lineage, Audit) complete the Platform tier.
