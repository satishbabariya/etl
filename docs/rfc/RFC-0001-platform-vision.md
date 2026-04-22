# RFC 0001: Platform Vision, Wedge, and Non-Goals

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** None (this is the foundational RFC)

## Summary

This RFC defines what we are building, who it is for, and — equally importantly — what we are explicitly *not* building. It establishes the wedge we will use to enter a market dominated by Fivetran (data ingestion) and Databricks (lakehouse compute), and commits us to a sequencing strategy: win ingestion first, grow into transformation and compute second.

Every subsequent RFC derives its constraints from this document. If a later design decision conflicts with this RFC, either the design is wrong or this RFC must be amended explicitly.

## Motivation

The managed data platform market is large, growing, and structurally inefficient. Fivetran charges per monthly active row (MAR) at margins that imply their connector runtime is expensive to operate. Databricks charges per DBU at margins that imply their compute layer is expensive to operate for the median workload. Both incumbents built on stacks (JVM, Spark) that made sense in 2015 but no longer represent the performance frontier.

We believe the combination of **Rust** (for orchestration and I/O), **Temporal** (for durable workflow execution), and **WebAssembly** (for sandboxed user code) enables a platform with structurally better unit economics than either incumbent. Specifically:

1. Rust's performance and memory efficiency means we can run connectors and transformations on ~3–5x less compute than JVM-based equivalents for I/O-bound workloads, and ~5–10x less for compute-bound workloads under ~1TB.
2. Temporal provides durable, long-running, retry-tolerant workflow execution out of the box — functionality that Fivetran built in-house and Databricks assembles from multiple systems. We avoid that engineering cost.
3. Wasm enables user-supplied code (connectors, transformations) to run inside our sandbox without us reviewing it, without container overhead, and in any source language. This is a capability neither incumbent offers cleanly.

The thesis is not that Rust, Temporal, or Wasm is individually novel — each is used in production at scale elsewhere. The thesis is that their combination, applied specifically to the ETL/ELT problem, produces a platform whose unit economics allow pricing 3–5x below Fivetran and 2–3x below Databricks for the target workload range, while offering a differentiated developer experience.

## Wedge: Ingestion First

We enter the market as a Fivetran competitor, not a Databricks competitor. This is deliberate.

**Why ingestion first.** Ingestion is a connector-breadth problem more than a compute problem. Connector moats compound: every new connector we ship is permanent surface area a competitor must replicate. Compute moats do not compound the same way — a faster query engine can be matched by the next faster query engine. Starting with ingestion means each month of work increases our defensibility; starting with compute means each month of work is spent catching up to Spark's last decade of optimizer investment.

**Why not Databricks first.** Competing with Databricks on day one means competing with Spark, Photon, Delta Lake, Unity Catalog, MLflow, and a notebook experience that took ten years to build. Our stack is well-suited to *eventually* offer a superior experience for sub-terabyte workloads, but "eventually" is not a viable go-to-market. We defer this to the Growth tier (RFCs 21-23).

**Why not streaming-first.** The streaming data platform market (Confluent, Materialize, Arroyo, RisingWave) is crowded, technically demanding, and sells to a narrower buyer. Temporal is not a stream processor and we will not pretend it is. We address streaming as a later expansion (RFC 23), not as our entry point.

**The specific wedge.** We will win ingestion by being:

- **Dramatically cheaper** at volume (target: 3–5x lower cost per GB synced vs. Fivetran at 100GB+/month).
- **More extensible** (users can author connectors in any wasm-targeting language without our review).
- **More reliable** under failure (Temporal's durability guarantees are stronger than Fivetran's custom orchestrator).
- **Self-hostable** from day one (BYOC deployment model — see RFC 18).

We will not initially compete on connector breadth. Fivetran has 500+ connectors and an army building more. We will ship the top 30 connectors by revenue concentration (Salesforce, HubSpot, Stripe, Postgres CDC, MySQL CDC, Google Ads, Facebook Ads, Shopify, NetSuite, Workday, etc.) and rely on the wasm SDK to let the community and customers fill the long tail.

## Target Customer

The primary target is **mid-market to enterprise data teams** spending $100K–$2M/year on Fivetran and feeling the MAR-based pricing as a constraint on what data they bring into their warehouse. These customers:

- Have a dedicated data engineering function (3–20 engineers).
- Run Snowflake, BigQuery, Redshift, Databricks, or an Iceberg-based lakehouse as their destination.
- Have hit the "we can't afford to sync this" conversation internally at least once.
- Are technical enough to run a self-hosted data plane if it meaningfully reduces cost.

The secondary target is **platform engineering teams building internal data products** who need to embed ingestion into their own product offering and cannot get acceptable economics from Fivetran's per-MAR model.

We explicitly **do not** target:

- SMBs with <$20K/year ETL spend. They are better served by simpler tools (Airbyte Cloud, Stitch) and our feature set is overkill.
- Analytics-engineer-led teams whose primary need is dbt orchestration. That is a different product.
- Teams whose primary workload is streaming (sub-second latency). Temporal is the wrong core for that.

## Non-Goals

The following are explicitly out of scope for at least the first 18 months:

1. **Notebook experience.** We are not building a Databricks-style notebook environment. Customers use their existing tools (Hex, Deepnote, Jupyter, or direct warehouse query).
2. **ML platform.** No MLflow equivalent, no feature store, no model serving. These are separate products and the market is well-served.
3. **Sub-second streaming.** Our latency floor is micro-batch (tens of seconds to minutes). Users needing true streaming should use Kafka + a stream processor.
4. **Visual pipeline builder.** We are a code-and-config-first platform. A GUI may come later but is not a wedge.
5. **BI / visualization layer.** We feed destinations; we do not replace Tableau, Looker, or Metabase.
6. **Warehouse replacement.** We write to warehouses; we do not aspire to be one. (This is revisited in RFC 22 for the lakehouse direction, but even there we are a compute/orchestration layer over open formats, not a proprietary warehouse.)
7. **Reverse ETL.** Writing from warehouse back to SaaS tools is a legitimate market (Census, Hightouch) but is not our wedge and we will not do it poorly as a feature.
8. **Low-code / citizen-developer tooling.** The buyer is a data engineer. We optimize for that buyer.

Items on this list can be reconsidered via RFC amendment, but the default answer to "should we add X?" where X is on this list, is **no**.

## High-Level Architecture (to be detailed in RFC 2)

To make this vision concrete enough to reason about, we commit to the following top-level shape, to be refined in RFC 2:

- A **control plane** (multi-tenant SaaS) hosting pipeline definitions, the catalog, scheduling, observability, and billing.
- A **data plane** (customer-deployable or our-hosted) running the Rust worker processes that execute Temporal activities. This is where connector wasm modules run and where data actually moves.
- **Temporal** as the durable orchestrator, deployed either as Temporal Cloud or self-hosted depending on tier.
- **Object storage** (S3 / GCS / Azure Blob) as the staging layer between extract and load.
- **Wasm components** (using the Component Model / WASI 0.2) as the unit of user-extensible code.

The control plane / data plane split is non-negotiable because it is what enables BYOC (bring-your-own-cloud) deployment, which is a core part of our cost and compliance story.

## Business Model (summary)

Pricing will be **compute-and-volume based**, not per-MAR. Specifically: a base platform fee plus usage priced on GB synced and compute-seconds consumed. This aligns our margin with our actual costs and avoids the perverse incentives of MAR pricing (which penalizes customers for syncing update-heavy tables).

Detailed pricing is out of scope for this RFC. What matters here is the commitment: **we will not adopt MAR pricing**, because doing so would sacrifice the primary economic wedge.

## Success Criteria

We will consider this RFC's vision validated when:

1. We can demonstrate a Postgres-to-Snowflake sync pipeline running at >=3x lower cost per GB than the equivalent Fivetran configuration on a benchmark workload.
2. A third-party developer can author, compile, and deploy a custom connector as a wasm component in under 4 hours of work.
3. The platform has sustained >=99.9% sync success rate across 1000+ consecutive sync runs in a production-representative test.

These criteria are the bar for declaring the foundational bet sound. They are not product-launch criteria; they are thesis-validation criteria, and should be achievable in a prototype phase.

## Alternatives Considered

**Start with Databricks-style compute wedge.** Rejected for reasons above: competing with Spark on day one is a losing position.

**Start with streaming wedge.** Rejected: crowded market, Temporal is wrong core, narrower buyer.

**Build on JVM instead of Rust.** Rejected: we would lose the unit-economics wedge. The JVM is fine for functionality but not for cost structure.

**Build on Kubernetes-native workflow engine (Argo, Flyte) instead of Temporal.** Rejected: Temporal's durability semantics, signal/query primitives, and workflow-as-code model are meaningfully better for the specific problem of long-running, retry-heavy, state-machine-shaped work. Argo and Flyte are optimized for DAG-shaped ML training workloads, not for "sync this API for 6 hours and survive three worker crashes."

**Use containers (Docker) instead of wasm for user code.** Rejected: per-invocation container startup is too slow for operator-granularity execution, container sandboxing is weaker than wasm, and the cross-language story is worse (wasm is a compile target, containers are an OS abstraction). Containers remain appropriate for connector *development* environments but not for the production execution boundary.

## Open Questions

1. **BYOC vs. pure SaaS sequencing.** Do we launch SaaS-first and add BYOC later, or BYOC-first? BYOC-first is harder to operate but is a sharper differentiator. Resolve in RFC 18.
2. **Open source strategy.** Do we open-source the worker / runtime to build ecosystem? There are strong arguments both ways. Resolve in a dedicated RFC before launch.
3. **Connector licensing.** If users author connectors as wasm modules, what's the license model for community-contributed connectors? Defer until we have a registry design.
4. **Geographic scope at launch.** Single-region or multi-region on day one? Affects RFC 18 and RFC 19.

## References

- Temporal architecture: https://docs.temporal.io/temporal
- Wasm Component Model: https://component-model.bytecodealliance.org/
- Apache Arrow (the data interchange format we will almost certainly adopt — see RFC 3): https://arrow.apache.org/
- Fivetran's MAR pricing (for context on what we are displacing): publicly documented in their pricing pages.
- Comparable prior art: Airbyte (OSS ingestion, Python/JVM-based), Meltano (Singer-based), Estuary (streaming-focused).

## Decision

**Accepted pending review.** This RFC establishes the north star. RFC 2 will translate this vision into a component architecture.
