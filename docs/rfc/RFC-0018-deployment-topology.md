# RFC 0018: Deployment Topology

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0002 (Core Architecture), RFC 0014 (State Storage), RFC 0016 (Multi-tenancy), RFC 0017 (Quotas and Billing)

## Summary

This RFC specifies how the platform is deployed: the three deployment modes (hosted, BYOC, self-hosted) concretely realized as infrastructure topologies; the regional strategy and data residency commitments; the Kubernetes-based data plane reference architecture; the control plane topology and scaling; the bootstrap, upgrade, and teardown procedures; the cross-region disaster recovery model; and the exact boundary of what we operate vs. what customers operate in each mode.

Deployment is where architecture meets operations. Prior RFCs have made deployment-relevant commitments in fragments ("runs in customer cloud for BYOC," "region-scoped state," "Kubernetes namespace per tenant"). This RFC assembles those fragments into concrete operational shapes, commits to specific cloud platforms and services at launch, and establishes the procedures that make deployment repeatable.

## Motivation

A platform is only as good as its deployment story. Customers evaluating us will ask:

1. **Where does my data live?** Region, cloud provider, account. Regulated customers need precise answers.
2. **What do I need to operate vs. what do you operate?** The split determines the customer's operational team investment.
3. **How do I get started?** If onboarding takes weeks, we lose to competitors whose onboarding takes hours.
4. **What happens during a regional outage?** Recovery objectives, failover semantics.
5. **How do I upgrade?** Can we deploy new versions without pipeline downtime?
6. **What about on-prem?** Some enterprise customers can't use public cloud at all.

Answering these concretely is what converts architectural commitments into an operable product. Evading them produces a demo-quality platform.

## Non-Goals

- This RFC does not specify every cloud service we use (RDS vs. Aurora vs. Cloud SQL). Service selection is implementation detail; the architecture supports any of several choices per cloud provider.
- This RFC does not cover network vendor selection (load balancers, DDoS protection). Cloud-native defaults suffice for launch.
- This RFC does not cover our internal developer tooling (CI/CD pipelines, staging environments). Those are implementation detail.
- This RFC does not cover customer support portal deployment. Separate operational concern.
- This RFC does not cover a mobile application. We don't have one; not in scope.
- This RFC does not cover specific compliance certifications (SOC 2, HIPAA). Those are addressed per-certification in security RFC work; this RFC specifies the architecture that makes them achievable.

## The Three Deployment Modes (Operationally)

Prior RFCs established these conceptually; here's what they look like on the ground.

### Mode 1: Hosted

**What we operate:** everything.

**What the customer operates:** nothing.

**Customer experience:** sign up, connect source and destination, see data moving. No infrastructure provisioning.

**Our operational footprint:**

- A fleet of Kubernetes clusters across multiple regions, operated by us.
- Control plane services (API gateway, catalog, scheduler, observability, auth, billing, registry) as containerized services.
- Data plane workers as pod deployments, autoscaled per task-queue depth.
- Managed cloud databases (Postgres for catalog; Temporal's backing DB).
- Object storage buckets per region per tenant-prefix.
- Managed secrets backends per region.

**Tenancy:** multi-tenant within each region. Tenants isolated per RFC 16.

**Target customer:** most SaaS customers; low ops overhead; standard compliance.

### Mode 2: BYOC (Bring Your Own Cloud)

**What we operate:** the control plane.

**What the customer operates:** the data plane (in their cloud account).

**Customer experience:** deploy our data plane reference architecture in their AWS / GCP / Azure account via Terraform; register it with our control plane; manage pipelines through our UI.

**Our operational footprint:**

- Control plane as in hosted mode.
- No customer-data-plane infrastructure in our cloud.

**Customer operational footprint:**

- A Kubernetes cluster in their cloud account.
- Our data plane Helm chart installed.
- Their own Temporal (Temporal Cloud in their account, or self-hosted).
- Their own object storage bucket.
- Their own secrets backend (their Vault, their cloud secrets manager).
- A secure connection to our control plane (HTTPS, mutual TLS).

**Tenancy:** single-tenant per customer (by definition; the customer is the only tenant in their data plane).

**Target customer:** regulated enterprises (FSI, healthcare, government-adjacent); teams with strong cloud commitments who want to apply committed spend to data plane infrastructure; customers with strict data-residency requirements where "your cloud is their cloud" is the decisive answer.

### Mode 3: Self-Hosted

**What we operate:** nothing at runtime. We provide software, updates, and support.

**What the customer operates:** control plane and data plane, on their infrastructure.

**Customer experience:** license the software; install per the operational handbook; support contract for escalations.

**Customer operational footprint:**

- Kubernetes cluster(s) — typically on-prem, sometimes in a cloud account they fully control.
- Full control plane stack (every service described above).
- Full data plane stack.
- Their Postgres, their Temporal, their object storage, their secrets backend.
- An optional phone-home mechanism for usage reporting (honor-system per the contract) and anonymous telemetry (opt-in).

**Tenancy:** customer's choice; usually single-tenant but multi-tenant is supported if they want to offer an internal service.

**Target customer:** air-gapped environments (classified, defense, some financial institutions); customers with contractual prohibitions on cloud deployment; customers whose legal jurisdiction requires on-premises operation.

### Mode comparison table

| Property | Hosted | BYOC | Self-Hosted |
|---|---|---|---|
| Control plane location | Our cloud | Our cloud | Customer infrastructure |
| Data plane location | Our cloud | Customer cloud | Customer infrastructure |
| Customer data in our infrastructure | Yes (scoped) | No | No |
| Customer operates Kubernetes | No | Yes (data plane) | Yes (everything) |
| Upgrade cadence | We push; continuous | Customer-pulled; weekly-to-monthly | Customer-pulled; quarterly typical |
| SLA model | Our SLA | Shared (our CP + their DP) | Software warranty only |
| Network between CP and DP | Internal | HTTPS over internet (or VPC peering) | N/A (same environment) |
| Billing model | Usage-based (RFC 17) | Usage-based + their cloud costs | License + support |
| Typical customer | Mid-market, SaaS | Regulated enterprise | Air-gapped, sovereign |

## Cloud Provider Strategy

We are explicitly multi-cloud because our customers are. A Salesforce customer may sit on AWS; their BigQuery destination is GCP; their Snowflake is on whichever. Our infrastructure straddles.

### Launch cloud support

**AWS**: primary. All hosted-mode launch regions. Reference BYOC target.

**GCP**: secondary. Selected hosted-mode regions. BYOC target.

**Azure**: BYOC target only at launch. Hosted Azure regions added post-launch based on demand.

**Why AWS primary:** broader enterprise adoption, richer region coverage, stronger services ecosystem, team familiarity. Not because AWS is technically better.

### Cloud-specific vs. cloud-portable

Where possible, we use cloud-portable technology:

- **Kubernetes** (every cloud has a managed offering).
- **Postgres** (every cloud has a managed offering).
- **Object storage** (universal abstraction with cloud-specific clients).
- **OTel** (vendor-neutral).

Where cloud-specific services win meaningfully, we use them with clean abstractions:

- **Managed Kubernetes**: EKS (AWS), GKE (GCP), AKS (Azure). Operational differences abstracted via a deployment-layer shim.
- **Managed secrets**: AWS Secrets Manager / GCP Secret Manager / Azure Key Vault, all hidden behind the `SecretBackend` trait from RFC 11.
- **Managed load balancing**: cloud-native (ALB, GCLB, Azure Front Door) with identical Envoy config underneath.

### Hosted-mode region strategy

**Launch regions (AWS-primary):**

- `us-east-1` (Virginia): primary, default for US customers.
- `us-west-2` (Oregon): secondary US, for latency-sensitive west-coast customers.
- `eu-central-1` (Frankfurt): EU customers; GDPR residency anchor.
- `ap-southeast-1` (Singapore): APAC customers.

**Post-launch additions, demand-driven:**

- `eu-west-2` (London): UK customers post-Brexit.
- `ap-northeast-1` (Tokyo): Japan.
- `ca-central-1` (Canada): Canadian residency.
- `sa-east-1` (São Paulo): LATAM.

Each region is an independent deployment. Control plane state is region-scoped; tenants choose their region at signup.

### Multi-region for a single tenant

A single tenant lives in one region. Multi-region tenancy (one tenant, workspaces in multiple regions) is an enterprise feature, deferred to post-launch. When we add it, regional tenancy splits along workspace boundaries — not a unified multi-region tenant.

## Control Plane Topology

The control plane is the set of services described in RFC 2: API Gateway, Catalog, Scheduler, Observability, Auth, Billing, Registry, plus the control-namespace Temporal.

### Services as Kubernetes Deployments

Each service runs as a Kubernetes `Deployment`:

- Horizontally scalable, behind a Service.
- Health checks defined for each.
- Rolling updates on deployment.
- Autoscaling via HPA based on CPU/memory/custom metrics.
- Per-service resource requests and limits.

Services talk to each other via Kubernetes Services (internal DNS). External ingress (from users, from data plane workers) goes through a shared Envoy-based gateway with TLS termination and authentication.

### Shared infrastructure per region

- **Postgres primary + replicas.** Managed (RDS multi-AZ in AWS, Cloud SQL HA in GCP). PITR enabled. Read replicas for read-heavy services (observability query layer).
- **Temporal cluster.** Temporal Cloud where available, self-managed where we need control (for sovereign regions). Control namespace and data namespaces per tenant.
- **Redis cluster.** For ephemeral caching (OAuth tokens per RFC 11, rate limit counters per RFC 17).
- **Kafka (or cloud equivalent).** For metering event stream (RFC 17), cross-service event bus (RFC 15).
- **Object storage buckets.** Regional, with lifecycle policies for retention-tier management.
- **Managed secrets backend.** AWS Secrets Manager et al.

### Services we intentionally don't self-host

Unless compliance or cost requires it:

- **We don't run our own SMTP**: email via a transactional email provider.
- **We don't run our own CDN**: Cloudflare / CloudFront / equivalent.
- **We don't run our own observability backends**: managed Loki / Grafana Cloud / equivalent for logs, traces, metrics.
- **We don't run our own auth federation**: an identity-provider-as-a-service for SSO.

Self-hosting these for our own control plane does not differentiate us; it consumes engineering time better spent on product.

### Control plane redundancy

Within a region: standard HA for each service (multi-AZ Kubernetes, Postgres multi-AZ, Redis cluster mode). Single-AZ failures are non-events.

Cross-region: the control plane is not actively replicated. A full regional loss requires operator-initiated recovery in another region (described below). The architecture supports active-active cross-region as a future enhancement but is not launching that way.

## Data Plane Topology (Hosted Mode)

### Kubernetes deployment

A `worker` Deployment per tenant class (standard, enterprise) per task-queue class (default, heavy, external, loader — RFC 4). Workers autoscaled based on Temporal task queue depth via Keda or equivalent.

Workers communicate with:

- Temporal cluster (polling).
- Catalog service (read-mostly; 5-minute cache).
- Object storage (staging reads/writes).
- Secrets backend (activity-scoped reads).
- External customer infrastructure (source and destination).

### Networking

- Worker pods run in a dedicated VPC / network scope.
- Outbound network access is allowed broadly (workers need to reach arbitrary customer sources/destinations), but with egress filtering to prevent SSRF against cloud metadata endpoints.
- Inbound: none; workers only pull, never accept connections.

### Tenant scoping within the data plane

Per RFC 16:

- Standard tier: workers are multi-tenant (multiplex); per-activity binding.
- Enterprise tier: dedicated worker pool per tenant.

Both options are realized as Kubernetes workloads with different deployment specs: multi-tenant pools are shared; dedicated pools carry tenant labels and are selected via node-affinity or sub-pool scheduling.

### Autoscaling parameters

Tuned empirically; starting points:

- **Scale-up trigger**: task queue depth > 20 for > 2 minutes → add pods.
- **Scale-down trigger**: average CPU < 30% for > 10 minutes AND queue depth < 5 for same window → remove pods.
- **Minimum pool size**: per-region per-tier minimum (maintains warm pool for low-latency starts).
- **Maximum pool size**: per-tenant cap (enterprise contract) or region-wide cap (cost control).

## Data Plane Topology (BYOC)

### Reference architecture

We publish a Terraform module (and Helm charts for the Kubernetes layer) that deploys the data plane in a customer's cloud account:

```
customer_cloud/
├── networking/               # VPC, subnets, security groups
├── kubernetes/               # managed K8s cluster
│   ├── namespaces/           # data plane namespace
│   ├── workloads/            # worker deployments
│   └── services/             # internal services, ingress
├── temporal/                 # self-managed Temporal or TC config
├── storage/                  # object storage bucket
├── secrets/                  # AWS Secrets Manager / Vault config
├── iam/                      # role definitions, service accounts
└── control_plane_connection/ # mutual-TLS endpoint to our control plane
```

We provide this for AWS, GCP, and Azure. Modules are versioned; customer upgrades are pulls from our published versions.

### Bootstrap flow

1. Customer signs BYOC contract.
2. We provision a customer-specific "tenant record" and generate deployment credentials (an install token + mTLS certificates).
3. Customer runs Terraform in their account, passing the install token.
4. Terraform creates infrastructure, installs data plane Helm chart, registers data plane with our control plane using the install token.
5. Registration handshake: data plane proves its identity with mTLS; control plane records the data plane as active for this tenant.
6. Customer begins pipeline configuration via our UI/CLI.

Bootstrap is self-service but supported by our team — the first BYOC deployment for a customer is usually assisted.

### Control plane ↔ data plane communication

- **Metadata pull** (data plane → control plane): workers fetch pipeline definitions, connector manifests, catalog state. Over HTTPS, mTLS-authenticated.
- **Metadata push** (data plane → control plane): run events, metrics, logs, billing events. Over gRPC streaming or HTTPS with batching.
- **No control plane → data plane inbound.** The data plane is the client in every interaction. Control plane never initiates a connection to the customer's cloud.

This matters for security: customers' perimeter defenses don't need to allow inbound from us.

### Latency considerations

Control plane round-trip is typically 50-200ms depending on distance. This is fine for non-hot-path operations (pipeline definition loads, metric pushes). For hot-path operations (workflow execution), everything is in-region to the data plane (Temporal, storage).

A worker calls the catalog API to load a pipeline definition once per run (cached), so the cross-region call is amortized.

### Upgrades

Customer pulls new data plane versions on their schedule. We commit to:

- Data plane N works against control plane N, N+1, and N+2.
- Control plane N works against data plane N, N-1, and N-2.
- Customers are not forced to upgrade on our cadence; they upgrade when they choose within the support window.
- Security fixes generate notifications with recommended upgrade timeframes.

## Data Plane Topology (Self-Hosted)

### Deployment package

For self-hosted, we ship:

- A Helm chart covering control plane + data plane.
- Terraform modules for common infrastructure (Postgres, Kubernetes, etc.) — optional; customer may have existing infra.
- An operator's handbook: setup, upgrade, backup, monitoring, troubleshooting.
- A configuration validator that pre-checks a deployment plan before execution.

### Bootstrap flow

1. Customer licenses the software.
2. We provide the deployment package and installation guide.
3. Customer provisions prerequisites (Kubernetes cluster, Postgres, Temporal, secrets backend, object storage).
4. Customer applies our Helm chart.
5. Customer runs the bootstrap CLI to initialize the first admin user and initial configuration.
6. Customer brings pipelines online.

Assisted onboarding for enterprise contracts typically includes our professional services for the initial deployment.

### Upgrade and patch model

- Quarterly minor releases (new features).
- Monthly patch releases (bug fixes, security).
- Ad-hoc security patches (critical only).
- Release notes, upgrade guides, tested rollback procedures per release.

Self-hosted customers lag our hosted deployment by design — we battle-test on hosted first. Self-hosted customers' patch commitments from us are milder than hosted SLAs.

### What's the same as hosted

Identical code paths. Configuration selects which backend to use, not which feature set is available. Self-hosted customers are not on a different product.

### What's different

- No phone-home for usage data (unless customer opts in for support).
- No managed services — everything the customer runs themselves.
- No usage-based billing; license-based per contract.
- Support is ticket-based, not chat.

## Regional Strategy and Data Residency

Regions are the primary data-residency unit. A tenant in `eu-central-1` has all their data in `eu-central-1` — control plane metadata, Temporal namespace, workers, staging, secrets, everything.

### Sovereign regions

Beyond standard cloud regions, some customers need sovereign cloud (AWS GovCloud, Azure Government, regional on-prem for national-security contexts). Launch posture:

- Sovereign US cloud (GovCloud, Azure Gov): supported BYOC. Hosted-mode sovereign at launch is limited; we evaluate per-contract.
- EU sovereign (Gaia-X and similar initiatives): supported BYOC.
- National clouds (China, Russia, specific regional players): not supported at launch; post-launch evaluation.

### Data transfer limitations

We commit that within the hosted-mode platform:

- Customer row data never leaves the tenant's chosen region.
- Metadata (pipeline names, schemas, run status) never leaves the tenant's chosen region.
- Secrets (including references) never leave the tenant's chosen region.
- Audit events live in the tenant's region; copied to a long-term archive in the same region.

The one exception: support-tier escalations may involve our staff (globally distributed) accessing a tenant's operational observability. This is audited (RFC 15) and described transparently in our subprocessor list.

### Regional outage handling

If `us-east-1` goes down:

- `us-east-1` tenants' pipelines stop running.
- Their data is safe in replicated cloud infrastructure (S3 is multi-AZ durable).
- Recovery proceeds as AWS recovers the region; no action on our side required.

For faster recovery, enterprise tier offers cross-region replication:

- Control plane Postgres has a warm replica in a secondary region.
- Object storage has cross-region replication.
- Temporal state backed up cross-region.
- Failover is operator-initiated (we trigger it); RTO 4-8 hours typical.

Standard tier does not include cross-region replication; customers accept regional-outage risk.

## Upgrade Procedures (Hosted)

We upgrade the hosted platform continuously. Principles:

### Zero-downtime upgrades

- Control plane services: rolling updates via Kubernetes Deployments. One pod at a time; health checks determine readiness before the next pod.
- Data plane workers: same. New workers start with new code; old workers drain gracefully (finish current activity, stop polling, exit). Temporal handles the transition — activities can complete on either version.
- Database schema migrations: forward-only; compatible with N-1 code (RFC 10). Migrations run, then code deploys.
- Temporal workflows in flight: continue using their pinned code version (RFC 4's versioning discipline). New workflows use new code.

### Deployment frequency

- Target multiple deploys per day for services that don't affect in-flight work.
- Slower cadence for services with migration impact (catalog, secrets) — typically weekly.
- Progressive rollout: deploy to a canary region first, wait for signal, then roll to the full fleet. Canary catches regressions before they affect most tenants.

### Upgrade observability

Every deploy produces:

- A deploy event in our audit log.
- Metrics showing the deploy's impact (error-rate change, latency change).
- Automatic rollback triggers if error rates spike beyond thresholds.

We over-invest in deploy observability because a bad deploy is our highest-impact internal incident.

### Database migrations

- Pre-deploy: migration runs (nullable column added, etc.).
- Deploy: new code deploys; coexists with pre-migration code briefly.
- Post-deploy: backfill if needed; further migrations (non-null, index changes) in subsequent steps.

Risky migrations (large table rewrites) are deferred off-peak. The migration system has dry-run and rollback support.

## Disaster Recovery

### RTO and RPO commitments

| Scenario | Hosted Standard | Hosted Enterprise | BYOC | Self-Hosted |
|---|---|---|---|---|
| Single pod / worker failure | Seconds | Seconds | Customer | Customer |
| Single AZ failure | Minutes; automatic | Minutes; automatic | Customer | Customer |
| Single region failure (data loss) | Hours (snapshot restore); RPO up to 24h | Hours; RPO minutes (cross-region replication) | Customer | Customer |
| Single region failure (no data loss) | Recovery as AWS recovers; RTO best-effort | 4-8h failover to secondary | Customer | Customer |
| Cloud provider catastrophic failure | Not in scope | Not in scope | Depends on customer DR | Depends on customer DR |

"Customer" entries mean the customer's DR posture determines recovery; our platform's architecture supports their DR model but we don't execute it.

### DR testing

- **Quarterly**: restore-from-backup drill in a non-production environment. Validates catalog restore, Temporal recovery, observability rebuild.
- **Semi-annually (enterprise tier)**: simulated regional failover for cross-region-replicated tenants.

### Backup verification

Backups are not trusted until tested. We verify daily that the last Postgres snapshot can be restored to a temp instance. We verify weekly that object storage lifecycle policies produce expected archive artifacts.

## Bootstrap and Teardown (Control Plane)

Our own control plane:

### New region bootstrap

1. Provision infrastructure via Terraform (networking, Kubernetes, managed services).
2. Deploy control plane Helm chart.
3. Initialize databases (run migrations from scratch).
4. Configure DNS.
5. Run smoke tests.
6. Register region in the multi-region control plane registry.
7. Open region for tenant signups.

Bootstrapping a new region is a one-week project. Automation covers 90% of it; the remaining 10% is cloud-vendor paperwork (GovCloud access, regional quota increases, etc.).

### Control plane teardown

Rare; usually a region deprecation:

1. Stop new tenant signups in the region.
2. Offer migration to affected tenants (tooling-assisted).
3. Migrate or terminate tenants over a long horizon (6-12 months).
4. Once empty, decommission infrastructure.

## Customer Onboarding Flow

The end-to-end experience for a new hosted-mode customer:

1. **Signup**: email, company name, region selection, plan tier. Tenant created (control plane Temporal workflow); workspace created; admin user provisioned.
2. **Invite teammates** (optional).
3. **Create first connection**: source system details, credentials. Catalog stores SecretRef; backend stores material.
4. **Discover streams**: connector runs `describe` + `discover`; schemas populate the catalog.
5. **Create destination connection**: similar flow.
6. **Create first pipeline**: DSL or UI. Streams selected, schedule set, evolution policy chosen.
7. **First run**: pipeline executes; user sees data appearing in destination.

Target: under 30 minutes from signup to first data landing for a straightforward case (Postgres → Snowflake).

Customers who fail to reach "first data landing" in their first session are the largest churn risk; onboarding is continually instrumented and iterated.

## Supply Chain

Deployment of something we built requires trusting what we built.

### Build integrity

- All first-party code builds reproducibly via our CI.
- Build artifacts (container images, wasm binaries, Helm charts) are signed.
- Signatures are verified at deployment time (Kubernetes admission controller for images; client-side verification for downloadable artifacts).

### Dependency management

- Cargo.lock / package-lock.json / go.sum committed and audited.
- Automated vulnerability scanning (Dependabot / Snyk / equivalent).
- Critical vulnerabilities trigger emergency patches within a defined SLA per severity.

### Third-party services

- SaaS dependencies (email, DNS, CDN, observability) are enumerated in a subprocessor list per customer contract.
- New subprocessor additions trigger customer notification per their contract.

## Alternatives Considered

**Single-cloud (AWS only) at launch.** Simpler. Rejected: losing GCP and Azure customers at launch costs us meaningful deals. BYOC on all three is the launch commitment; hosted on AWS primary with GCP secondary.

**Active-active multi-region from day one.** Stronger resilience. Rejected: meaningful engineering investment for a problem most customers don't have at launch scale. Added post-launch when demand and scale justify.

**Serverless data plane.** Workers as Lambda / Cloud Run functions. Rejected per RFC 5: wasm module cold-start via serverless function cold-start compounds unfavorably. Long-lived workers are the right shape.

**Hosted-only launch, BYOC later.** Faster time-to-market. Rejected: the BYOC story is structural (RFC 2's control-data split enables it for free), and enterprise sales motion benefits from it from day one.

**One Kubernetes cluster across all regions.** Operationally simpler. Rejected: regional data residency requires regional clusters, and cluster blast radius from any failure is limited by keeping clusters separate.

**Our own Temporal deployment** for all scenarios. Would give us tighter control. Rejected where Temporal Cloud meets our needs: operating Temporal well is its own engineering competency. Use their product unless we have a specific reason not to.

**Customer-operated upgrade model for hosted.** "You push; we don't" — more control for customers. Rejected: undermines the hosted value proposition (we operate it). Hosted is continuous-deployment; BYOC and self-hosted are customer-controlled cadences.

## Open Questions

1. **Regional expansion cadence.** Which region next? Market-driven; not an architecture decision.
2. **Edge deployment for low-latency connectors.** Some customers want ingestion close to source (e.g., globally distributed OLTP databases). Future work; evaluate with customer demand.
3. **Air-gap refresh model for self-hosted.** Self-hosted customers in air-gapped environments cannot pull updates from the internet. Mechanism for delivering updates (USB? portal with download? customer-operated mirror?) TBD.
4. **Tenant region migration.** A hosted tenant wants to move from `us-east-1` to `eu-central-1`. Complex: pipelines, schemas, credentials, Temporal state. Currently a professional-services engagement; self-service someday.
5. **Cloud-provider feature divergence.** AWS Secrets Manager vs. GCP Secret Manager vs. Azure Key Vault — each has slightly different features. Abstraction smoothes most of this; some features are cloud-specific. Policy: prefer portable features.
6. **Customer-operated hosted-like deployments.** A customer wants hosted-style managed operation but on their cloud account. Somewhere between hosted and BYOC. Possible as a premium managed-service offering; not launching.

## References

- AWS Well-Architected Framework: https://aws.amazon.com/architecture/well-architected/
- GCP architecture framework: https://cloud.google.com/architecture/framework
- Azure Well-Architected Framework: https://learn.microsoft.com/en-us/azure/well-architected/
- Kubernetes deployment patterns: https://kubernetes.io/docs/concepts/workloads/
- Helm documentation: https://helm.sh/docs/
- Terraform module registry patterns: https://developer.hashicorp.com/terraform/language/modules
- Temporal Cloud documentation: https://docs.temporal.io/cloud
- GovCloud (AWS), Azure Government, GCP sovereign offerings (comparative reference).

## Decision

**Accepted pending review.** RFC 19 next: Security Model — which consolidates security concerns (authn, authz, encryption, threat model extensions) into one document, referencing the security-relevant commitments from RFCs 11, 15, and 16.
