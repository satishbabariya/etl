# RFC 0019: Security Model

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0002 (Core Architecture), RFC 0005 (Wasm Runtime), RFC 0011 (Secrets), RFC 0015 (Observability), RFC 0016 (Multi-tenancy), RFC 0018 (Deployment Topology)

## Summary

This RFC consolidates the platform's security posture: the comprehensive threat model (extending RFC 16), authentication and authorization architecture, encryption at rest and in transit, cryptographic primitives, the vulnerability disclosure process, incident response commitments, the compliance-certification path, and the supply-chain security program. It also resolves open questions deferred from prior RFCs (break-glass access, sovereign regions, compliance-tier specifics).

Security is everywhere in prior RFCs but nowhere consolidated. This RFC is the single reference document for "what's our security posture?" — the one we hand to auditors, to enterprise security reviewers, and to our own engineers when a security-relevant decision is needed.

## Motivation

Prior RFCs have made security commitments as they arose:

- RFC 5 (Wasm): sandboxing, capability-based I/O, explicit statement of what wasm does and doesn't protect.
- RFC 11 (Secrets): secret reference model, tenant scoping, audit, memory zeroization.
- RFC 15 (Observability): audit pipeline, tamper-evidence, staff access controls.
- RFC 16 (Multi-tenancy): threat model tiers, isolation layers.

These are individually good; collectively they leave three gaps:

1. **No single document for external reviewers.** Enterprise security teams ask for "your security architecture." Handing them 16 RFCs is not acceptable. This RFC is that document.
2. **Cryptographic commitments are implicit.** We say "encrypted" a lot; we haven't said which primitives, which key management, which rotation policy. Explicit commitments here.
3. **Incident response and vulnerability management are operational, not architectural, but they are promises we make to customers.** Codifying them here turns them from "we should do this" to "we commit to this."

## Non-Goals

- This RFC does not enumerate every threat. It specifies the model; specific threats are addressed by the architecture.
- This RFC does not cover physical security (we don't run physical infrastructure; cloud providers' physical security is inherited).
- This RFC does not re-specify what prior RFCs specified well. It references them.
- This RFC does not cover insider-threat countermeasures at our organization beyond the technical controls already described. Organizational security (background checks, separation of duties in ops, training) is a people program, not architecture.
- This RFC does not specify specific compliance certifications we will pursue (SOC 2, HIPAA, FedRAMP) with timelines. Those are business-tier decisions; this RFC specifies the architecture that makes them feasible.

## Threat Model (Consolidated)

RFC 16 established six tiers. This RFC is the authoritative version and extends them:

### Tier 1: Malicious tenant

Same as RFC 16. **Defend strongly.** This is our highest-priority adversary.

**Capabilities we assume:**

- Full access to their own tenant's API and UI as legitimate user.
- Technical sophistication, including wasm-targeting language expertise.
- Willingness to publish malicious connectors or transformations.
- Ability to craft adversarial inputs.

**Defenses:**

- Multi-layer tenant isolation (RFC 16).
- Wasm sandbox (RFC 5).
- Per-tenant worker scoping for wasm instances (RFC 5, RFC 16).
- Rate limits and quotas (RFC 17).
- Capability-based host API (RFC 5) — a malicious wasm module cannot escape to make arbitrary network or disk access.
- Signed artifacts (RFC 18) — malicious replacement of published modules is prevented.

### Tier 2: Compromised customer credential

Same as RFC 16. **Structural guarantee: zero cross-tenant access from a credential.**

**Additional defenses beyond RFC 16:**

- Short-lived tokens by default (OAuth access tokens: minutes; API keys: scoped, with rotation support).
- Anomaly detection on usage patterns (sudden geographic shifts, traffic spikes) → alerts and optional step-up authentication.
- Customer can revoke credentials instantly; revocation propagates within seconds across all services.

### Tier 3: Bug-induced leak

Same as RFC 16.

**Defenses:**

- Type-level tenant identification (`TenantId` type, RFC 16).
- Row-level security in Postgres.
- IAM policy enforcement at storage layer.
- Audit log visibility — any cross-tenant access attempt is visible for post-hoc review.
- Extensive integration tests for cross-tenant scenarios (RFC 16).

### Tier 4: Noisy neighbor

Same as RFC 16. **Economic and operational concern, not primarily security.**

### Tier 5: Supply chain compromise

RFC 16 named it; this RFC specifies defenses.

**Defenses:**

- Dependency pinning (lockfiles committed, audited).
- Automated vulnerability scanning (RFC 18).
- Reproducible builds where possible.
- Signed artifacts (RFC 18).
- Runtime isolation: a compromised dependency in worker pods has access to the worker's scope, not to other components.
- Principle of least privilege: each service has only the IAM permissions it needs.
- Third-party service dependencies enumerated and monitored.

### Tier 6: Our insider threat

RFC 15 specified staff access; this RFC adds:

**Defenses:**

- Audit everything (RFC 15).
- Principle of least privilege in our organization: engineers don't have production access by default.
- Break-glass procedures are audited in real time and require second approval.
- Separation of duties: the person who approves code changes cannot also approve production deployments.
- Background checks on employees with production access (operational commitment, not architectural).

### Tier 7: Nation-state-level targeting (new)

**Explicit out-of-scope for standard tier.** We do not design to defend against nation-state-level adversaries.

**Compliance-tier enhancement (FedRAMP High, IL5, etc.):** dedicated infrastructure, dedicated staff with clearances, additional operational security practices. These are contract-specific and governed by their respective frameworks.

### Tier 8: Physical compromise of our infrastructure (new)

**Out of scope** (we don't operate physical infrastructure).

**Cloud providers' protections are inherited**: AWS/GCP/Azure physical security, supply chain for hardware, facility access controls.

**For self-hosted customers**: their problem.

## Authentication

### User authentication

**Primary mechanism: SSO via SAML 2.0 and OIDC.** We integrate with customer identity providers (Okta, Azure AD / Entra, Google Workspace, PingIdentity, plus generic SAML/OIDC).

**MFA**: delegated to the identity provider. We respect the IdP's MFA enforcement. We do not implement our own MFA system except for local admin accounts (one per tenant, for initial bootstrap and emergency access).

**Session management:**

- Sessions issued as JWTs, short-lived (1 hour default, configurable).
- Refresh tokens, longer-lived (24 hours default, configurable) stored in httpOnly cookies.
- Absolute session timeout (24 hours default, configurable per tenant).
- Revocation via session store — revoked sessions cannot refresh.

**Local users (non-SSO):**

- Supported for trial and small-team tenants.
- Strong password requirements (NIST 800-63B compliant: length over complexity).
- MFA via TOTP mandatory for admin roles; optional otherwise.
- Breached-password checking (against known-compromised-password databases).

**Enterprise tier requirement**: SSO only. Local users disabled.

### Machine authentication

**Service-to-service:** mutual TLS. Each service has its own certificate; rotation via cert-manager or equivalent.

**Workers to control plane:**

- Workers have a service identity bootstrapped at deployment (cloud-native IAM in hosted/BYOC; pre-shared credential in self-hosted).
- Authentication via signed tokens with short TTL (minutes); refresh via service identity.

**API tokens (programmatic customer access):**

- Generated by customer admins.
- Scoped by role and optionally by workspace.
- Support for revocation and rotation.
- Stored hashed (HMAC-SHA256) with display-once-only tokens.
- Default TTL 365 days, configurable.
- Last-used tracking for cleanup.

### Connector-to-external authentication

(Handled by connectors per RFC 6; summarized here.)

Connectors authenticate to external sources/destinations using credentials from the secrets backend. Common patterns:

- Username + password (basic auth, db passwords).
- OAuth 2.0 refresh/access token pairs.
- Service account keys (GCP, AWS STS).
- Client certificates (mTLS).
- Kerberos (for some on-prem databases).

All credentials live in the secrets backend (RFC 11); connectors request per-activity access.

## Authorization

### Role-based access control (RBAC)

Tenant-scoped roles:

- **Owner**: full access to the tenant, including billing and tenant settings.
- **Admin**: full access within workspaces, but not tenant-level settings.
- **Operator**: can start/pause/resume/edit pipelines; cannot delete connections or destructive changes.
- **Developer**: can create and edit pipelines; cannot modify secrets.
- **Viewer**: read-only access to pipelines, runs, observability.
- **Auditor**: read-only access to audit log; no data access.

Roles are assigned per-workspace (the default scope) or per-tenant (for tenant-wide roles like Owner). Membership is managed by admins; bulk provisioning via SCIM 2.0.

Custom roles with fine-grained permission selection are an enterprise feature.

### Permission checks

Every API endpoint declares required permissions. Checks happen:

- At the API gateway (coarse: "is this user authenticated?").
- At the service (fine: "does this user have permission X on resource Y?").

Permission checks use deny-by-default semantics: a permission not explicitly granted is denied. This includes new resource types — adding a new resource kind without updating the permission model means everyone is denied, which is a safe default.

### Resource-level access

Some resources (pipelines, connections) have per-resource access grants on top of role-based access. A user with "Developer" in workspace X might have "Admin" specifically on Pipeline Y (project ownership). This is implemented via resource ACLs layered on role-based defaults.

### Delegated access

Platforms sometimes need delegated access (e.g., our support staff acting on behalf of a tenant, with that tenant's explicit permission). Implementation:

- Customer admin grants delegation (one-time or time-bounded).
- Support staff act via the delegation; audit shows both the acting staff member and the delegation grant.

## Encryption

### In transit

- **TLS 1.3 required** for all public-facing endpoints. TLS 1.2 accepted for compatibility with older clients, disabled when feasible.
- **mTLS** for service-to-service within our infrastructure and between our control plane and customer data planes (BYOC).
- **Forward secrecy** via ECDHE cipher suites only.
- **Certificate management** via cert-manager (Kubernetes) or cloud-native equivalents; automatic rotation.
- **HSTS** enforced on all public hosts, with preload.

### At rest

**Envelope encryption** is the universal pattern:

- Per-tenant **data encryption keys (DEKs)** encrypt actual data.
- DEKs are encrypted by a **key encryption key (KEK)** in the cloud provider's KMS.
- KEKs are per-tenant, per-region, rotated annually.
- DEK access requires workload identity authorized against the KEK.

**Specifically:**

- **Postgres**: storage-level encryption (cloud-provider-managed with per-tenant KMS keys where provider supports it; otherwise universal but with column-level encryption for sensitive fields).
- **Object storage**: server-side encryption with KMS (SSE-KMS for S3; equivalent for GCP/Azure); per-tenant-prefix keys.
- **Secrets backend**: backend's own encryption (Secrets Manager uses KMS; Vault uses its barrier encryption). Per-tenant KMS key.
- **Temporal**: encryption at the backing store level; additionally, workflow inputs containing sensitive metadata are encrypted at the payload level when necessary.
- **Backups**: encrypted with keys managed by the same KMS as primary storage.

### Key management

- **KMS operations** logged to cloud provider's audit log, mirrored into our audit pipeline.
- **Cross-region** key replication for enterprise-tier DR.
- **Key rotation:**
  - KEKs: annual, automatic.
  - DEKs: on demand (tenant-scoped) or on breach suspicion.
  - Customer-managed keys (CMK): supported in enterprise tier — customer brings their own KMS key, we use it for envelope encryption. Customer can revoke access instantly, rendering their data unreadable.

### Customer-managed encryption keys (CMK)

An enterprise feature. Customer provisions a KMS key in their cloud account; grants our service identity permission to use it; our envelope encryption uses their key instead of ours.

Operational implications:

- Customer revokes our access → we cannot decrypt their data. Pipelines halt; destination data remains (we never had access to destination-side encryption).
- Customer rotates their KMS key → we use the new version going forward; old data decryptable until old version is deleted.

CMK is the strongest customer-controllable data protection in hosted mode. It's the "even if Anthropic is compelled by a court, they can't decrypt our data without our key."

### Cryptographic primitives

Specific primitives we commit to:

- **Symmetric encryption**: AES-256-GCM for general-purpose symmetric; ChaCha20-Poly1305 for performance-constrained contexts.
- **Asymmetric encryption**: RSA-4096 or Ed25519 depending on use case.
- **Hashing**: SHA-256 or BLAKE3 for general-purpose; SHA-512 where required by standards.
- **Password hashing**: Argon2id.
- **Key derivation**: HKDF-SHA256.
- **Signatures**: Ed25519 preferred; RSA-PSS-SHA256 where interoperability requires.
- **Random**: OS-provided CSPRNG (`/dev/urandom` or equivalent); never our own generators.

These are standard primitives from well-reviewed libraries (OpenSSL, BoringSSL, `ring`, libsodium). We do not implement crypto primitives ourselves.

We follow NIST deprecation timelines: MD5, SHA-1, DES, RC4, etc. are not used.

## Data Classification

Different data gets different treatment. We classify:

### Class P1: Customer data (highest)

Row contents from customer sources. Handled per RFC 11 and RFC 5: exists in memory only during activity execution, zeroed on completion, encrypted at rest in staging, never in logs.

### Class P2: Customer metadata

Pipeline configurations, schemas, run statistics. Encrypted at rest; accessible to customer admins and our support with authorization.

### Class P3: Customer identifiable information (PII)

Names, email addresses, user account details, billing information. Encrypted at rest; accessible on need-to-know basis; subject to data-minimization and right-to-be-forgotten obligations.

### Class P4: Our operational data

Metrics, logs, traces (scrubbed of customer data per RFC 15). Less sensitive but not public; access-controlled.

### Class P5: Public data

Documentation, public API specifications, marketing content. No protection beyond normal web security.

Every storage system records the classification of data it holds. Audit events record access per class.

## Vulnerability Management

### Discovery

- **Automated scanning**: dependencies (Dependabot or equivalent), container images (Trivy or equivalent), infrastructure (cloud-native security services — GuardDuty, Security Command Center, Defender).
- **Manual review**: engineering reviews for security-sensitive code paths.
- **Third-party reviews**: annual external penetration test; periodic third-party security reviews of critical subsystems (secrets, wasm runtime).
- **Bug bounty program**: launching 6-12 months post-GA. Reward-tiered by severity.

### Severity classification

- **Critical**: active exploitation possible, cross-tenant impact, RCE, authentication bypass. Response: immediate, full-team.
- **High**: exploitation plausible, significant data exposure possible, privilege escalation. Response: within days.
- **Medium**: theoretical exploitation, limited impact. Response: within weeks.
- **Low**: hardening opportunities, best-practice deviations. Response: backlog.

### Response SLA

- Critical: patch deployed to hosted mode within 72 hours; customer notifications to BYOC/self-hosted within 24 hours with recommended mitigation.
- High: patch within 14 days; notification within 7 days.
- Medium: next minor release.
- Low: routine.

### Coordinated disclosure

Vulnerability disclosure policy:

- Published security contact: `security@platform.com` with PGP key.
- 90-day disclosure window (industry standard) for reported vulnerabilities.
- Credit to reporters who follow coordinated disclosure.
- Published security advisories after fix, with CVE when applicable.

## Incident Response

### Classification

- **Sev 1**: platform breach, cross-tenant data exposure, audit integrity compromised, significant data loss.
- **Sev 2**: significant degradation, limited customer impact, potential security issue under investigation.
- **Sev 3**: isolated incident, specific customer or region affected.
- **Sev 4**: internal issue, no customer impact.

### Response procedures

- Sev 1: pages on-call immediately; incident commander appointed; customers notified within 1 hour of confirmation; continuous updates until resolution.
- Sev 2: on-call response; customers notified within 4 hours if externally visible.
- Sev 3: on-call response; affected customers contacted directly.
- Sev 4: backlog.

### Notification channels

- Status page with subscription (email, RSS, webhooks).
- Direct customer notification for tenant-specific issues.
- Post-incident review published for Sev 1 and Sev 2 with customer-facing detail.

### Forensics and evidence preservation

During a security incident:

- Preserve logs, workflow histories, audit trails.
- Snapshot affected systems for forensic analysis before remediation.
- Coordinate with law enforcement when warranted (customer consent required for customer-data forensics).

## Break-Glass Access (Resolved from RFC 15)

The deferred question: when can our staff access customer data in emergencies?

### Default: no access

Our staff default to no access to Class P1 (customer data) regardless of role. Operational observability, Temporal workflow data, and run metadata (Class P4, partially P2) are accessible with normal authorization.

### Break-glass procedure

Required for Class P1 / P2 access outside customer-authorized support:

1. **Request**: on-call engineer files a break-glass request citing the issue, affected tenant(s), and justification.
2. **Approval**: requires two approvals — one senior engineering lead, one security team member. No self-approval.
3. **Grant**: short-lived credential (4 hours max) scoped to the specific tenant(s) and resources.
4. **Audit**: real-time entry in both our internal audit and the affected tenant's audit log (including our actors' identities and justification).
5. **Customer notification**: tenant admin notified within 24 hours with break-glass record.
6. **Post-incident review**: every break-glass invocation reviewed within a week by security team.

Break-glass is rare and audited heavily. Engineers who invoke it without justification face consequences. The commitment is "you can't silently access customer data — even if you can access it at all, you leave an unmistakable trail."

### Customer-disabled break-glass (enterprise)

Enterprise tier includes an option to disable break-glass entirely for a tenant:

- We cannot access the tenant's Class P1 data under any circumstances (break-glass requires customer approval, not just our internal approval).
- Support incidents involving Class P1 require customer-provided access, typically via customer-operated diagnostic data export.

This is a meaningful reduction in our ability to help with some kinds of incidents; customers who choose it accept the tradeoff.

## Compliance Framework

Architecture supports multiple compliance certifications; pursuit is business-tier decision.

### SOC 2

Target from year one. Our architecture supports SOC 2 Type II:

- Access controls: auth + audit + break-glass model.
- Availability: SLAs + DR.
- Processing integrity: idempotency + correctness RFCs (7, 8, 9).
- Confidentiality: encryption + tenant isolation.
- Privacy: data classification + audit.

### GDPR

Target from year one for EU customers. Architecture supports:

- Data residency (region-scoped state, RFC 14/18).
- Right to be forgotten (tenant termination, RFC 16, and within a tenant, user deletion).
- Data processing agreement (contract).
- Subprocessor transparency (published list).

### HIPAA

Target post-launch for healthcare customers. Architecture supports:

- BAA (business associate agreement) execution.
- Encryption at rest and in transit.
- Audit logs (RFC 15).
- Access controls.
- Customer's own BAA obligations with their sources and destinations are separate.

### FedRAMP

Considered for government customers. Moderate baseline is plausibly achievable with our architecture; High requires dedicated infrastructure (GovCloud) and significant operational investment. Evaluate based on customer demand.

### Other

- **ISO 27001**: ongoing program post-launch.
- **PCI-DSS**: not a target — we don't process payment card data for customers (we use a PCI-compliant payment processor for our own billing).
- **CSA STAR**: achievable as extension of SOC 2.

Compliance is continuous, not a checkbox. Architecture choices that enable compliance make business expansion possible.

## Customer-Facing Security Documentation

We publish:

- **Security whitepaper**: this RFC, translated for non-engineer security reviewers.
- **Subprocessor list**: third parties we use, with purposes. Updated with advance notice.
- **DPA (data processing agreement)**: contract addendum covering GDPR processor obligations.
- **SOC 2 report**: available under NDA to customers.
- **Penetration test summary**: annual, with detail under NDA.
- **Vulnerability disclosure policy**: public, at `/security.txt`.
- **Security advisories**: published for confirmed issues post-fix.

## Alternatives Considered

**Build our own secrets management rather than integrating with cloud KMS / Vault.** Rejected: we benefit from cloud providers' and Vault's hardening; we'd be a smaller security budget fighting a well-resourced attacker.

**Always-on customer-managed keys.** Always require customers to bring their own KMS key. Rejected as default: most customers don't want the operational burden; our default key management is sound. CMK available as enterprise option.

**Fully implement our own authorization engine.** Considered instead of a library or service. Rejected: authz is a well-studied space (OPA / Cedar are the current best-in-class); we use them with custom policies rather than reinventing.

**TLS 1.2 support indefinitely.** Rejected: we plan TLS 1.2 deprecation on the industry timeline. Customers stuck on older clients get a runway.

**Classified-tier compliance at launch.** Would open government-adjacent markets. Rejected as launch scope: requires dedicated infrastructure, specific staff clearances, and significant investment. Post-launch if the market materializes.

**No bug bounty.** Would save operational cost. Rejected: bug bounties are near-table-stakes for security-conscious customers. Launch program 6-12 months post-GA.

## Open Questions

1. **Specific compliance certification timeline.** SOC 2, GDPR from year one; HIPAA within 18 months; FedRAMP evaluated at 24 months. Business decisions; flagged here as architectural-readiness signals.
2. **Bug bounty platform choice.** HackerOne, Bugcrowd, Intigriti, or internally managed? Decide closer to launch.
3. **Post-quantum cryptography readiness.** Algorithms standardized; migration cadence aligned with industry. Monitor; commit when appropriate (likely 2028+).
4. **Confidential computing support.** Enclaves (SGX, AMD SEV) for the most sensitive processing. Future enterprise feature; evaluate demand.
5. **Third-party security reviews of specific subsystems.** Wasm runtime embedding, secrets handling, audit pipeline — annual third-party review for each. Budgeting and scheduling.
6. **Red team engagement.** Beyond pen testing, periodic red-team exercises (full scenarios, not just vuln findings). Post-year-one.

## References

- NIST Cybersecurity Framework: https://www.nist.gov/cyberframework
- NIST SP 800-53 (security controls): https://csrc.nist.gov/publications/detail/sp/800-53/rev-5/final
- NIST SP 800-63B (authentication): https://pages.nist.gov/800-63-3/sp800-63b.html
- OWASP Top 10: https://owasp.org/www-project-top-ten/
- CIS Benchmarks (for Kubernetes, cloud providers): https://www.cisecurity.org/cis-benchmarks
- Cloud Security Alliance Cloud Controls Matrix.
- GDPR regulation text.
- SOC 2 Trust Services Criteria.

## Decision

**Accepted pending review.** RFC 20 next — the last in the Operational tier — covers SDK and Extensibility: how customers and partners extend the platform (connectors, transformations, loaders, integrations), the publishing model for the connector registry, and the economic model for community-contributed content.
