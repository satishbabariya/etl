# RFC 0017: Quotas, Billing Metering, and Backpressure

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0001 (Platform Vision), RFC 0009 (Destination Loaders), RFC 0014 (State Storage), RFC 0015 (Observability), RFC 0016 (Multi-tenancy)

## Summary

This RFC specifies how usage is measured, priced, billed, and enforced. It defines the metering events the platform emits, the billing model's relationship to those events, the quota system that caps usage per tier, the backpressure mechanisms that engage when limits are approached, and the invoicing, dispute-resolution, and cost-attribution stories.

Billing is not a platform afterthought; it's what funds the platform. Getting it right makes our unit-economics wedge (RFC 1) real and defensible. Getting it wrong produces either customer-perceived unfairness (billing surprises) or company-perceived losses (undercharged usage).

## Motivation

Prior RFCs have committed to the pricing model in general terms (RFC 1: "compute-and-volume based, not per-MAR"). This RFC operationalizes that commitment. Concretely:

1. **Metering events need to exist and be durable.** If a customer asks "where did this $50K charge come from?" we must be able to show them, line-item, exactly what was done. Billing events must be at-least-once captured with no dropped records; reconciliation is built in, not bolted on.
2. **Quotas need enforcement that matches what we bill for.** Billing for GB-synced while quotas are per-row produces confusion. Metering and quotas must share units.
3. **Backpressure has to be nuanced.** "Over quota" shouldn't immediately break pipelines; it should degrade gracefully, alert, and allow burst with boundaries.
4. **Cost attribution matters for customer trust.** Customers want to know why their bill looks the way it does — which pipelines cost what, which destinations are expensive, where they could save. Cost observability is part of the product.
5. **MAR pricing is explicitly rejected.** RFC 1 committed to this. This RFC operationalizes a replacement.

## Non-Goals

- This RFC does not specify actual dollar amounts. Pricing is a business decision that evolves; the metering infrastructure supports whatever the business sets.
- This RFC does not cover plan management (tier upgrades, downgrades, seats, user counts). Those are in the Auth/Billing UI product.
- This RFC does not cover tax handling, invoicing provider integration (Stripe, NetSuite), or payment processing. Operational.
- This RFC does not cover contracts, ESAs, or enterprise-specific terms. Business.
- This RFC does not cover cost optimization features (automatic batch-size tuning, pipeline scheduling optimization). Future product work.

## Design Principles

**Metering events are system-of-record.** The event stream is the source of truth for what happened; invoices are derived. Disputes are resolved by re-aggregating the event stream.

**Metering is separate from observability.** Observability events are high-volume, drop-tolerant, short-retention. Metering events are lower-volume, zero-tolerance-for-loss, long-retention. Mixing them produces either bankrupt observability or loose billing.

**Bill for resources, not abstractions.** GB transferred is a resource. Compute-seconds is a resource. "Monthly active rows" is an abstraction that doesn't correspond to cost on our side. We bill for what we pay for, plus margin, not for things that feel like they should cost more.

**Quota boundaries match billing boundaries.** A customer's tier caps what they can consume per billing period. Exceeding triggers notification; sustained exceedance triggers enforcement; enforcement degrades gracefully before it fails loudly.

**Transparency by default.** Customers can see their consumption in real time, not just at month-end. A projected bill is always visible. Line-item drill-down is always available.

**Idempotency is non-negotiable.** Metering events must be safe to re-emit (Temporal retries). Billing aggregation must be safe to re-run (daily reconciliation). Duplicate events produce duplicate-but-deduplicated aggregations.

## Metering Events

Every billable action produces an event. Events are emitted through a dedicated pipeline (separate from observability per RFC 15).

### Event shape

```
MeteringEvent {
  event_id: UUID (time-ordered),
  emitted_at: Timestamp,
  tenant_id: UUID,
  workspace_id: UUID,
  pipeline_id: Option<UUID>,
  run_id: Option<UUID>,
  component: EventComponent,
  metric: BillableMetric,
  quantity: f64,
  unit: Unit,
  dimensions: Map<string, string>,  // for attribution, not pricing
  source_event_ref: Option<UUID>,   // for reconciliation
}

enum BillableMetric {
  BytesExtracted,
  BytesLoaded,
  RowsProcessed,
  ComputeSeconds,
  WasmComputeSeconds,
  StorageBytesHours,
  EgressBytes,
  ApiRequests,
  DestinationWriteOperations,
  CdcSlotHeld,     // hourly, charged only on idle/over-committed slots
  SeatsMonthly,    // per month of active users
}

enum Unit {
  Bytes,
  Rows,
  Seconds,
  ByteHours,
  Requests,
  Hours,
  UserMonths,
}
```

### Emission points

Every billable action has a defined emission point. Specifically:

- **Extraction**: connector emits `BytesExtracted` and `RowsProcessed` at each batch boundary (RFC 6). Worker activity aggregates into per-activity totals.
- **Load**: loader emits `BytesLoaded` and `RowsProcessed` at batch boundary (RFC 9).
- **Compute**: worker emits `ComputeSeconds` and `WasmComputeSeconds` when activities complete, measured per RFC 5's instance accounting.
- **Storage**: a daily job computes staging bucket usage per tenant and emits `StorageBytesHours`.
- **Egress**: network egress from our infrastructure to customer destinations emits `EgressBytes` (only in hosted mode — BYOC and self-hosted have no egress charge from us).
- **API**: control-plane API requests from customer emit `ApiRequests` (rate-limited, usually not billed directly in standard tier).
- **Destination writes**: loaders emit `DestinationWriteOperations` counting destination-side operations when the destination reports them.
- **CDC slots**: an hourly job emits `CdcSlotHeld` for slots that are held on source systems.
- **Seats**: a daily job emits `SeatsMonthly` based on active-user count.

### Durability

Metering events:

- Are written locally to a durable queue (per-worker, per-node) before being considered emitted. Loss of a worker mid-activity does not lose events already locally queued.
- Are forwarded to the billing service via a reliable queue (Kafka, or cloud-native equivalent). Cross-region replication per RFC 14's enterprise tier.
- Are aggregated daily into per-tenant usage records; the raw events remain for 7 years (retention per RFC 14).
- On disagreement between aggregates and raw events, raw events win.

Dropped metering events are a correctness bug of the highest severity — they mean we're under-billing or miscounting. Observability metering drop alerts are Sev 1.

### Idempotency

Event re-emission is safe because events have stable UUIDs derived from source operation:

- An activity's `ComputeSeconds` event has `event_id = hash(pipeline_id, run_id, activity_id, "compute")`.
- A batch's `BytesLoaded` event has `event_id = hash(pipeline_id, run_id, stream, batch_sequence, "load_bytes")`.

The billing aggregation service deduplicates by event_id before summing. Retries produce no extra billing.

## Billing Aggregation

Daily, per-tenant, per-workspace, per-billable-metric.

### Aggregation pipeline

1. **Hourly**: streaming aggregation of events into per-tenant rolling counts, visible to the live billing view.
2. **Daily**: batch aggregation of the previous day's events into canonical daily summaries. This is the billing system's source of truth.
3. **Monthly**: invoice generation from daily summaries, plus tier-specific credits, overages, and negotiated rates.

Daily aggregation is idempotent: re-running for a past day produces the same summary (subject to late-arriving events; see below).

### Late-arriving events

Events occasionally arrive late: a worker that was network-isolated for a few hours catches up and emits events with old timestamps. Policy:

- Events emitted within 24 hours of the billing day are included in that day's summary.
- Events emitted 1-7 days late are included in a "late arrivals" pool and trigger re-aggregation of affected days; re-aggregation produces a corrected daily summary; invoice adjustments propagate if the month is still open.
- Events emitted >7 days late are counted in the current day (rather than historically attributing them) and surfaced as an anomaly for investigation.

### Reconciliation

A daily reconciliation job:

- Recomputes the prior day's aggregate from raw events.
- Compares to the stored daily summary.
- On disagreement, flags the day for re-aggregation and alerts.

Reconciliation catches:

- Events arriving after daily aggregation.
- Aggregation bugs.
- Storage corruption.

## Quota System

Quotas bound usage per tier per billing period. They are not the same as rate limits (rate limits are short-window; quotas are period-window).

### Quota entities

Every tenant record includes a `QuotaConfig`:

```
QuotaConfig {
  plan: PlanTier,
  period: BillingPeriod,  // monthly typically
  limits: Map<BillableMetric, QuotaLimit>,
  overrides: Vec<QuotaOverride>,
}

QuotaLimit {
  metric: BillableMetric,
  soft_cap: Option<f64>,   // warning at this level
  hard_cap: Option<f64>,   // enforcement at this level
  burst_budget: Option<f64>,  // allowable overage, replenishing
  burst_replenish_rate: Option<f64>,  // per hour
}

enum PlanTier {
  Trial,
  Starter,
  Professional,
  Enterprise(ContractId),
}
```

Standard tiers have known quota structures; enterprise tiers reference their contract for specific terms.

### Quota enforcement

At metering event generation, the worker checks current consumption vs. hard cap:

- **Below soft cap**: normal operation.
- **Soft cap reached**: notification fires (once per tenant per period); operations continue.
- **Hard cap reached**: burst budget checked. If burst budget available, consume and continue; burst replenishes at the configured rate.
- **Hard cap + burst exhausted**: enforcement engages. New activities for metrics subject to the exhausted quota are rejected at scheduling (workflow doesn't start) or paused in flight.

Enforcement is per-metric. Running out of `BytesExtracted` quota does not affect `ComputeSeconds` accounting.

### Enforcement semantics

**Rejection at scheduling**: the scheduler's quota-check sees the tenant is over; new runs don't start. Pipeline enters `quota_exceeded` state. User notified.

**In-flight pause**: an activity running when quota exhausts completes the current batch (so we don't leave partial work), then checks quota before next batch. If still exhausted, activity returns with a typed error; workflow transitions to paused.

**Recovery**: when the next billing period starts, quota resets; paused pipelines automatically resume. User can also purchase an overage package for immediate resolution.

### Quota vs. billing

Quotas cap what is billed; they don't reduce the bill below what was used. Usage within quota is charged per tier; usage via burst is charged at a burst rate (potentially higher); usage via paid overage is charged at a contracted overage rate.

Customers on enterprise tier often have "uncapped" quotas with billing based on actual usage. Quota enforcement there is advisory (alerts only).

## Backpressure

Backpressure is distinct from quota: it's when something is temporarily slower than ideal.

### Sources of backpressure

- **Destination slow**: loader reports slow; workflow backs off on next batch.
- **Temporal task queue depth high**: new runs defer, already-running continue.
- **Worker pool saturated**: autoscaling adds more; meanwhile, tasks wait.
- **Source rate-limited**: connector reports 429; retries with longer delay.
- **Internal service backlog**: catalog writes slow; scheduler retries.

### Propagation

Backpressure propagates up through activities' natural retry mechanics (RFC 4) — an activity that hits backpressure doesn't complete; its retry-after signal causes Temporal to delay its next attempt. Pipelines naturally slow down without explicit coordination.

### Never drop data to catch up

A backpressured pipeline slows or pauses; it does not drop events, skip batches, or fabricate completion. This is covered in RFC 8 for CDC specifically; here we make it platform-wide: data correctness always wins over throughput.

### Backpressure visibility

Customers see backpressure via:

- Pipeline health indicators (slowed, paused).
- Per-pipeline metrics (throughput trend lines showing decline).
- Typed alerts (pipeline backpressured for > N minutes).

Operators see it via our operational observability stack (RFC 15).

## Cost Observability for Customers

This is the "know why your bill is what it is" feature.

### Per-pipeline cost attribution

Every metering event carries dimensions including `pipeline_id`. Daily aggregates roll up per pipeline. Customers see:

- Cost per pipeline, per day.
- Trend over the billing period.
- Breakdown by billable metric within a pipeline.

### Projected bill

Based on period-to-date consumption and remaining days, a projection is computed and displayed:

```
Monthly projection: $1,847 (actual so far: $1,102; extrapolated remainder: $745)
  Top pipelines: orders-to-snowflake ($623), users-cdc-to-bq ($487)
  Closest quota: BytesLoaded (65% of limit, on track for 94% by month end)
```

### Cost optimization hints

The platform surfaces suggestions:

- "Pipeline X is loading the same row 50 times per batch on average. Consider enabling batch-dedup (transformation) to reduce destination-side MERGE cost."
- "Pipeline Y is extracting 200 columns but only loading 20. Consider a project transformation to reduce extract bandwidth."
- "Pipeline Z has slot lag > 2 hours for the last 3 days. Consider increasing worker allocation."

These are educated suggestions, not rules. The platform is not an optimizer; it's an observer with recommendations.

### Third-party cost pass-through

Destination costs (Snowflake credits, BigQuery bytes-scanned) are reported by loaders when available, displayed as informational ("your Snowflake warehouse ran for 12 minutes during load; at your rate, that's approximately $X"). We do not bill for destination costs; we surface them.

## Tier Design

While not specifying dollar amounts, we specify structure.

### Trial

- Capped at small usage (gigabytes per month, handful of pipelines, single workspace).
- No enterprise features (dedicated workers, compliance reports, priority support).
- Conversion target: become paying in 30 days.

### Starter

- Modest quotas, billed usage-based.
- Full feature access except enterprise-specific (dedicated workers, BYOC, custom compliance).
- Standard support.

### Professional

- Higher quotas, burst budgets.
- BYOC option available.
- Scheduled support hours.

### Enterprise

- Contracted terms: committed usage, discounts, SLA.
- Dedicated workers option.
- Compliance tier options (HIPAA, FedRAMP, custom).
- 24/7 support.
- Dedicated customer success.

Enterprise contracts are bespoke; the metering system remains the same, but invoice generation uses contract-specific rates.

## BYOC Billing Differences

BYOC customers use their own cloud infrastructure for data plane. Consequences:

- **Their cloud bill** covers compute, storage, egress.
- **Our bill** covers control plane services, catalog, support, engineering investment, software license (effectively).

BYOC metering emits most of the same events (pipeline runs, compute time), but different metrics feed our bill vs. customer's cloud bill:

- Our bill: `ControlPlaneActivities`, `CatalogOperations`, `SeatsMonthly`.
- Customer's cloud: everything data plane (compute, storage, egress).

BYOC is often more cost-effective for large customers because their cloud commits amortize across workloads, and they pay only our control-plane fee.

## Self-Hosted Billing

Self-hosted customers run everything. Our bill is a software license (typically annual) plus optional support. No real-time metering against us; the platform emits metering events to the customer's own billing system if they have one, but we don't receive them.

License compliance is honor-system at the feature level, monitored via periodic check-ins (monthly usage reports submitted to us, per contract).

## Billing Event Processing

### Pipeline

```
Worker/Component emits MeteringEvent
    │
    ▼
Local durable queue (per-node)
    │
    ▼
Regional billing-event queue (Kafka or equivalent)
    │
    ▼
Billing aggregation service
    │
    ├─▶ Real-time usage counters (Redis or equivalent)
    ├─▶ Daily summary job (Postgres billing schema)
    └─▶ Raw event archive (object storage, 7y retention)
    │
    ▼
Monthly invoice generation
    │
    ▼
Payment processor integration (Stripe, etc.)
```

### Failure modes

- **Local queue full**: worker backpressures; activity retries. Events are not dropped.
- **Regional queue unavailable**: local queue buffers up to 24h; beyond that, worker alerts (Sev 1). Events are not dropped until the buffer is at capacity and operator decision is required.
- **Aggregation service down**: raw events accumulate; when service recovers, re-aggregation catches up.
- **Daily job fails**: retried; reconciliation flags discrepancies.
- **Invoice generation fails**: retries; human operator intervenes if persistent.

### Audit trail

Every metering event, every aggregation run, every invoice generation produces an entry in the billing audit trail. This is separate from the general audit log (RFC 15) because it has different consumers (finance, auditors) and different retention (forever for tax purposes in many jurisdictions).

## Dispute Resolution

A customer says "this invoice is wrong."

### Process

1. Customer submits dispute through the billing API or support.
2. Support pulls the daily summaries for the disputed period; customer sees per-pipeline, per-day breakdown.
3. If summaries disagree with customer's expectations, support re-aggregates from raw events (a self-service customer tool also allows this).
4. If re-aggregation matches summaries, summaries are correct; discussion is about usage, not billing.
5. If re-aggregation disagrees, we have a bug or data corruption; invoice adjusted, RCA performed.

### Customer self-service

The billing API exposes:

- Full raw event export for the customer's tenant and period.
- Aggregation tool that runs the same aggregation we do, so customers can verify.

This is table-stakes transparency. Customers who can't audit their bill don't trust it.

## Quotas and Enterprise Contracts

Enterprise contracts may specify:

- **Committed usage**: minimum monthly consumption (customer pays this amount even if they use less).
- **Tier rates**: negotiated per-unit prices.
- **Overage rates**: what happens beyond committed usage.
- **True-ups**: annual reconciliation of committed vs. actual.
- **SLAs**: performance guarantees with credits for breach.

These map onto the metering system as:

- Committed usage = minimum billing amount applied monthly.
- Tier rates = tenant-specific rate schedule used in invoice generation.
- Overage rates = secondary rate schedule applied after committed usage is exceeded.
- True-ups = annual reconciliation against contract terms.
- SLAs = tracked via observability (RFC 15); breach generates service credits via the billing system.

The architecture accommodates arbitrary enterprise contracts without modification — just data in the tenant record and the billing rules engine.

## Alternatives Considered

**Monthly-Active-Rows (MAR) pricing.** Fivetran's model. Rejected in RFC 1 for economic-wedge reasons; reaffirmed here because it has perverse incentives (customers avoid update-heavy tables even when they need them). Usage-based pricing aligns incentives better.

**Free tier with infinite usage.** Good for adoption, catastrophic for unit economics. Rejected: we have a trial tier with caps, not a free tier.

**Event sampling for metering.** Would reduce metering event volume. Rejected: sampling produces probabilistic billing, which customers correctly distrust. Every billable action emits an event.

**Billing only at month-end from aggregated metrics.** Simpler infrastructure. Rejected: without per-event records, dispute resolution is impossible. We maintain raw events for the full 7-year audit window.

**Unified event stream (observability + metering).** Operational simplicity. Rejected in RFC 15 and reaffirmed here: different durability, retention, and tolerance requirements. Separate pipelines are worth the cost.

**Credits-based billing ("you have 100 compute credits this month").** Common in some SaaS, arguably friendlier UX. Rejected: real resources don't come in credit units, and translation layers add customer confusion. Usage-based is more directly explainable.

**Real-time quota enforcement via distributed coordinator.** A single source of truth for current tenant usage, hit on every billable action. Rejected as default: at our scale, worker-local consumption tracking with eventual consistency is sufficient for quota enforcement (a small amount of burst is acceptable and actually desirable). Enterprise tier may warrant stricter enforcement; implementable as an extension.

## Open Questions

1. **Multi-currency pricing.** Enterprise customers in non-USD regions want local currency. Infrastructure supports it; business decision about when to offer.
2. **Usage-based discounts (volume pricing).** Large customers expect lower per-unit rates. Implement as tier rates on enterprise contracts; standard tiers have published rates.
3. **Pre-paid credits option.** Some customers want to commit a budget upfront and draw against it. Implementable via the contract mechanism; low priority for launch.
4. **Sub-tenant billing (cost allocation to workspaces).** Large customers with many workspaces want per-workspace cost reports for chargeback. We have per-workspace dimensions on events; reporting tooling needed.
5. **Billing for data transformations specifically.** Currently, transformations consume compute-seconds, which are billed. Should we bill specifically for transformation runs (more granular) or just include them in compute? Probably the latter for now.
6. **CDC slot charges.** A slot held on a customer's source costs the customer in source-side resources (storage, I/O). Should we charge for held slots as an incentive to not abandon them? Likely yes at enterprise tier; punt for standard.

## References

- Stripe's usage-based billing patterns (reference for SaaS metering): publicly documented.
- Lago (open-source usage-based billing): https://www.getlago.com/ — prior art for the architecture we want.
- AWS's cost allocation tags: prior art for per-dimension cost attribution.
- Temporal Cloud's own billing model (they use this architecture internally).

## Decision

**Accepted pending review.** RFC 18 next: Deployment Topology — which pulls together the three deployment modes and specifies the operational topology, regional strategy, and bootstrap/teardown mechanics that support the metering and tenancy models here.
