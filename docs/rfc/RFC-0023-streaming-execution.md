# RFC 0023: Streaming Execution Model

- **Status:** Draft (Growth Tier — highly speculative; may change dramatically or be abandoned)
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0001 (Platform Vision), RFC 0004 (Temporal Topology), RFC 0008 (CDC Architecture), RFC 0012 (Transformation Layer), RFC 0021 (Query Engine), RFC 0022 (Lakehouse Formats)

## Summary

This RFC sketches what it would mean for the platform to do **real streaming** — sub-second latency event processing with windowed operations, event-time semantics, watermarks, and out-of-order handling. It considers three possibilities: **(1) never** (stay micro-batch and partner with stream processors), **(2) adopt an existing stream processor** (Arroyo, RisingWave, Materialize, Flink) as a pluggable component, or **(3) build streaming on our substrate** using a combination of Temporal, wasm, and potentially DataFusion's streaming primitives. Each option has very different implications; this RFC names them, lays out the tradeoffs, and commits to a decision posture that will likely evolve as the business matures.

This is the **most speculative RFC in the series**. Parts of it will be proven wrong within a year of launch. The value is in laying out the option space explicitly, not in committing to a specific path.

## Motivation

Streaming is the adjacent market we have repeatedly deferred. RFC 1 said: "we're not launching streaming; Temporal is wrong for sub-second event processing." RFC 12 said: "transformations are batch-local; cross-batch state is stream processing, which is out of scope." This has been the right stance — every previous RFC is designed for batch and micro-batch, and adding streaming-native capability is a significant architectural effort.

But streaming keeps resurfacing because customers keep asking for it:

1. **CDC pipelines want lower latency.** Our CDC architecture (RFC 8) is micro-batch; typical latency is seconds to minutes. Customers with "real-time dashboards" or "immediate downstream reaction" use cases want sub-second.
2. **Event-driven architectures are common.** Many customers already have Kafka / Kinesis / Pub/Sub. They want to do things with those streams — enrich, aggregate, route — before landing.
3. **Analytics on fresh data is valuable.** "Show me sales in the last 30 minutes" requires either a streaming platform or a very fast batch platform. The streaming answer has inherent latency advantages.
4. **The alternative (partner with stream processors) has friction.** "Use us for ingestion, Arroyo for streaming, Snowflake for warehouse" is three vendors and three operational models. Customers prefer fewer pieces.

Whether we address these is the question this RFC exists to answer.

## Non-Goals

- This RFC does not commit to building streaming. It analyzes whether and how we would.
- This RFC does not promise any timeline for streaming support. Post-launch experience drives that decision.
- This RFC does not exhaustively compare every stream processor. It names a handful representative of the space.
- This RFC does not cover "streaming SQL" semantics exhaustively. If we commit to streaming, a dedicated RFC handles that.
- This RFC does not pretend to resolve open streaming research problems (end-to-end exactly-once across asynchronous systems, late-arriving event handling at extreme scale). Those are research topics that inform but don't constrain our scope.

## Design Principles (Tentative)

**If we do streaming, it is additive to batch, not a replacement.** Our batch story is strong; customers who need batch will continue to use batch. Streaming is additional capability for customers whose workloads warrant it.

**If we do streaming, it is honest about latency bounds.** We don't promise "sub-100ms end-to-end" without meaning it. Real streaming is 100ms–seconds; our batch is seconds–minutes. The gap matters.

**If we don't do streaming, we integrate cleanly with those who do.** A "use us for ingestion, Arroyo for processing" story should be smooth, not janky.

**We do not pretend Temporal solves streaming.** Temporal is not a stream processor. Our Temporal-based workflow engine is excellent for durable, long-running, state-machine-shaped work. It is wrong for event-per-event sub-second processing. If we do streaming, we either add a different substrate or we pick a specific constrained subset where Temporal is still viable.

**Stream processing correctness is non-negotiable.** Streaming systems with approximate correctness look fine until real customer workloads hit them and silent errors accumulate. If we do streaming, we commit to rigorous correctness semantics (event time, watermarks, exactly-once within bounded scope).

## The Three Options

### Option 1: Stay Micro-Batch; Partner with Streamers

**What it looks like:**

- We keep our current micro-batch model indefinitely.
- Customers who need streaming use Arroyo, RisingWave, Materialize, Flink, or similar alongside us.
- We provide tight integration: our connectors can feed their streams; their outputs can be our sources.

**Pros:**

- Zero new architectural investment.
- We stay focused on the ingestion wedge (RFC 1).
- Streaming is genuinely hard; partnering avoids that difficulty.
- Customers already using stream processors don't need to migrate.

**Cons:**

- Customers asking for "everything in one platform" are underserved.
- We cede the streaming+ingestion revenue to partners.
- Our positioning stays "batch-focused ingestion tool" rather than "full data platform."

**When this is the right choice:**

- If post-launch customer feedback says streaming is a nice-to-have, not a deal-breaker.
- If our engineering bandwidth is better spent on connector breadth and lakehouse depth.
- If a clear partner emerges that we can genuinely integrate deeply with.

### Option 2: Integrate an Existing Stream Processor

**What it looks like:**

- We embed or tightly integrate a production-ready stream processor as a pluggable component in our data plane.
- From the customer's perspective, streaming feels native: DSL extends, UI shows streaming pipelines, observability and billing are uniform.
- Under the hood, a specific stream processor (e.g., Arroyo) is doing the streaming work.

**Candidates:**

- **Arroyo** (Rust-based, Arrow-native, actively developed, relatively young). Architecturally closest to our stack. Seriously worth considering.
- **RisingWave** (Rust-based, SQL-first, streaming database). Mature; pluggability is lesser; potentially heavyweight.
- **Materialize** (Rust/Timely Dataflow-based, SQL-first, incremental view maintenance). Elegant; business model is direct product competition.
- **Flink** (JVM, mature, ecosystem leader). Operationally heavy; JVM is a step back from our Rust stack.

**Pros:**

- Faster time-to-market than building from scratch.
- Proven technology; we inherit years of streaming correctness work.
- If we choose well (Arroyo being the most aligned), minimal stack incompatibility.

**Cons:**

- Dependency on a third party's roadmap and business viability.
- Integration is non-trivial (shared observability, shared security, shared catalog).
- Licensing and commercial considerations with open-source vendors.
- Customer experience fragments if the partner engine's UX diverges from ours.

**When this is the right choice:**

- If post-launch customer feedback says streaming is important, but our engineering investment in the core platform is still the priority.
- If Arroyo (or similar) reaches production maturity with a commercial model we can work with.
- If the integration can be deep enough to feel unified.

### Option 3: Build on Our Substrate

**What it looks like:**

- We add streaming-native execution to our data plane workers.
- Temporal remains the orchestrator for workflow-shaped work (CDC lifecycle, scheduling, provisioning), but streaming jobs run as long-lived processes in dedicated streaming workers.
- DataFusion's streaming primitives (if RFC 21 plays out well) provide stream-SQL.
- Wasm remains the UDF model (extended to streaming contexts).
- Custom work: watermarks, event-time semantics, windowed operations, out-of-order handling, end-to-end exactly-once.

**Pros:**

- Unified stack; no third-party dependency.
- Maximal architectural coherence.
- Strongest product positioning: "one platform for batch and streaming."

**Cons:**

- Substantial engineering investment (multi-year, likely).
- Real risk of getting it wrong (streaming is hard; we'd be new to it).
- Distracts from the ingestion wedge.
- Competing against deeply-invested competitors (Flink community has been doing this for years; we'd be late to features they've solved).

**When this is the right choice:**

- If post-launch market signals are very strong and consistent: "streaming is the reason we pick a data platform."
- If the company has grown to the scale where a dedicated streaming engineering team is justifiable.
- If the available stream processors aren't good enough partners.

## Likely Decision Trajectory

The honest answer about what happens:

**Year 1 post-launch**: Option 1. Focus on ingestion wedge. Partner loosely with stream processors where customers need them.

**Year 2-3**: Decision point based on signals. If strong streaming demand + Arroyo (or similar) is production-grade + partnership viable → Option 2. If not → continue Option 1.

**Year 3-5**: If business is large and streaming is a strategic front for company expansion → Option 3 is on the table. Prior to that it's premature.

We may never reach Option 3. That's fine if the business is succeeding without it.

## Architectural Sketch for Option 2 (If Adopted)

For completeness: what Option 2 would look like in practice.

### Embedding / integration model

Not literally embedding (running Arroyo's engine inside our workers) but **tight operational integration**:

- Streaming jobs deployed as separate pods alongside pipeline workers.
- Stream processor instance per tenant (in hosted mode); co-located with their data plane.
- Shared observability: their metrics flow into our observability pipeline.
- Shared catalog: stream pipelines are catalog entities (new kind `StreamingPipeline`) alongside batch pipelines.
- Shared DSL (RFC 13): YAML resources can declare streaming pipelines.

### Stream source and sink

- **Sources**: Kafka, Kinesis, Pub/Sub, as well as our CDC connectors' output (Mode 1 usage of CDC extracts).
- **Sinks**: Iceberg / Delta (RFC 22), Kafka, our warehouse loaders (RFC 9).

### Stream processing surface

- SQL for stream transformations (the stream processor's SQL dialect plus ours where they diverge).
- UDFs using our wasm model.
- Windowed operations, joins, aggregations, state.

### Boundaries

Clearly documented to customers:

- **Use the batch path for**: scheduled pipelines, bulk historical loads, analytical transformations, anything where seconds of latency is acceptable.
- **Use the streaming path for**: sub-second latency, event-time-sensitive logic, continuous aggregation.
- **They can coexist**: a single customer can have both, referencing shared connections and destinations.

## Architectural Sketch for Option 3 (If Adopted, and Highly Speculative)

Not planning for this seriously; sketching to complete the thinking.

### Streaming workers

A new class of worker processes, separate from pipeline workers and query workers:

- Long-lived (hours to months).
- Stateful (in-memory state for windows, joins, aggregations).
- Persistence via RocksDB-style embedded state stores.
- Checkpoints to object storage (Flink-style or Samza-style).

### Orchestration

Temporal orchestrates streaming jobs' lifecycle (start, stop, rescale, checkpoint coordination) but not their per-event execution. A streaming job is a Temporal workflow whose activity is "run this streaming executor until told to stop"; the executor itself is a long-lived Rust process.

### Event-time semantics

- **Event time**: extracted from each event by the source (CDC timestamp, Kafka header, etc.).
- **Watermarks**: periodic advance signals that "no more events older than time X should arrive."
- **Out-of-order handling**: events arriving after their watermark either dropped, routed to a late-arrival handler, or included with cost.

These are well-understood primitives; implementing them correctly is the job.

### Windows

- Tumbling, sliding, session, global windows.
- Trigger semantics: on watermark, on event count, on event time.
- State per window materialized in local state store.

### Exactly-once semantics

Within the streaming job: state store transactions + checkpoint coordination. Across external systems: harder. We can offer "exactly-once within our system" and "at-least-once end-to-end" honestly; true end-to-end exactly-once requires destination cooperation and is usually not achievable.

### The scope of what we'd have to build

Roughly: 3-5 years of engineering, one dedicated team of 5-10 senior engineers, with real risk of getting it wrong and real risk of getting beaten by existing stream processors who have a 10-year head start.

This is why Option 3 is unlikely.

## Partial Solutions (Short of Full Streaming)

Even without committing to any of the three options, some streaming-adjacent capabilities are cheap wins post-launch:

### Reduced micro-batch latency

Our current micro-batch can target seconds rather than minutes:

- CDC pipelines (RFC 8) with short commit cadences (every 1-5 seconds).
- Streaming-write-friendly destinations (BigQuery Storage Write, Iceberg with short snapshots).
- Short-buffered staging.

This gives us "near-real-time" latency (single-digit seconds) without being true streaming. Many customers asking for "real-time" are actually happy with this.

### Webhook triggers

Instead of polling sources, some sources push to us (webhooks, HTTP callbacks). Trivial latency reduction for event-driven sources. Cheap to build; narrow applicability.

### Streaming destinations with batch sources

Our loaders support streaming writes (BigQuery Storage Write, Kafka sinks). This makes our output appear streaming even when our input is batch. Partial solution but genuinely useful for fan-out to downstream real-time systems.

### Continuous pipelines (pseudo-streaming)

A pipeline that runs every 30 seconds, rather than scheduled at hours, looks "real-time enough" for many use cases. We optimize for this case in RFC 4's workflow topology. Practical without formal streaming.

## Integration with Prior RFCs (If We Stream)

### Connector protocol (RFC 6)

Connectors supporting streaming emit events as they arrive, not batch-by-batch. The current `read` activity pattern needs an alternative: persistent subscribe semantics where the connector is a long-lived source of events.

This is a meaningful protocol extension — a new mode `streaming` alongside `full_refresh`, `incremental_cursor`, `incremental_cdc`.

### CDC architecture (RFC 8)

CDC fits streaming naturally. A streaming CDC connector emits events as they occur in the source log, not in micro-batches. This is simpler in concept but requires the streaming runtime to deliver.

### Catalog (RFC 10)

Streaming pipelines are catalog entities. Schema evolution in streaming is subtler — events with different schemas interleave; the catalog must handle this.

### Destination loaders (RFC 9)

Streaming writes: every event a separate operation, vs. batch loaders that commit per-batch. Some destinations support both (BigQuery Storage Write); others don't. Loader protocol extends with a streaming variant.

### Observability (RFC 15)

Streaming needs streaming-specific observability: watermark lag, state store size, checkpoint cadence. Extends RFC 15's metric set.

### Multi-tenancy (RFC 16)

Streaming workers are long-lived and stateful. Per-tenant isolation is harder than stateless worker pools. Likely: tenant-dedicated streaming workers, not shared.

## Customer Experience Considerations

If we do streaming (Option 2 or 3):

### The batch/streaming choice

Customers need clear guidance. The UI and docs have to answer "when do I use batch vs. streaming" clearly:

- Latency sensitivity.
- Cost (streaming is usually more expensive per event).
- State requirements.
- Operational complexity.

We don't want customers picking streaming because it "sounds better" when batch would serve them fine.

### Cost model

Streaming is usage-based too, but the units differ: events per second, state-store GB-hours, checkpoint storage. Billing (RFC 17) extends.

### Debugging

Streaming debugging is famously hard. We need tools:

- Replay events from a past point.
- Inspect state store contents.
- Trace specific events through the pipeline.

These need engineering.

## Alternatives Considered (Meta)

**Commit to streaming from day one.** Rejected in RFC 1. Reaffirmed: we would lose the ingestion wedge.

**Promise streaming eventually.** Tempting marketing. Rejected: we don't make roadmap commitments we can't confidently honor.

**Say "we don't do streaming" definitively.** Rejected: that forecloses Option 2/3 prematurely. Customers appreciate honesty, including honest "we're evaluating."

**Build Flink-inspired streaming.** Most complete, most expensive. Rejected as anything other than very-long-term option.

**Build a narrow streaming layer (CDC-only low-latency).** An Option-3-lite. Conceivable: extend CDC pipelines to sub-second latency by skipping staging for some destinations. Smaller investment; may be worth it if customers push for CDC-specific streaming. Flagged as potential scope limiter if Option 3 is ever pursued.

## Open Questions (All of Them)

1. **What do post-launch customers actually ask for?** The whole RFC hinges on this. Specific unmet needs will redirect our thinking.
2. **Is Arroyo (or an equivalent) production-grade and commercially partnerable?** Affects Option 2 viability.
3. **Does Databricks' streaming story (Delta Live Tables, Structured Streaming) dominate so much that competing is futile?** Affects whether streaming investment has positive ROI at all.
4. **What's the revenue opportunity?** Streaming licenses are often higher-priced but also a smaller market. Numbers matter.
5. **Do lakehouse formats (RFC 22) enable us to punt on streaming longer?** Near-real-time writes to Iceberg/Delta + near-real-time queries via RFC 21 may satisfy "streaming-adjacent" needs without committing to true streaming.
6. **What does Temporal's evolution offer?** Temporal is adding more capabilities each year. Some future Temporal feature may reduce the streaming gap.
7. **Organizational readiness.** Streaming engineering is a distinct skill. Do we have or can we hire the team? Implicit gate.
8. **Compliance of streaming systems.** Per-event audit, state-store retention, privacy rights for in-flight data — all harder than in batch. Worth thinking through.

## Growth-Tier Caveats

Even more emphasized than RFCs 21 and 22:

- This RFC is the most likely to change on first contact with reality.
- Writing it is useful as option-space enumeration; committing to it is not useful.
- Specific vendor dynamics (Arroyo's commercial future, Databricks' streaming features, new entrants) will reshape the decision space.
- Revisit annually.

Commitment level: **"we have thought about whether and how to add streaming; we have not decided to."**

## References

- Arroyo: https://www.arroyo.dev/
- RisingWave: https://www.risingwave.com/
- Materialize: https://materialize.com/
- Apache Flink: https://flink.apache.org/
- Timely Dataflow (Materialize's foundation): https://github.com/TimelyDataflow/timely-dataflow
- Google's Dataflow Model paper (event time, watermarks): https://research.google/pubs/the-dataflow-model/
- Apache Beam programming model (reference for streaming semantics): https://beam.apache.org/documentation/programming-guide/
- Databricks Structured Streaming: https://docs.databricks.com/structured-streaming/
- Kafka Streams: reference; JVM-centric.
- Samza (incrementally-durable state, Kafka Streams predecessor).

## Decision

**Draft, growth-tier, no commitment.** This RFC's purpose is to complete the option space. Post-launch experience and market signals guide any future action.

This also completes the 23-RFC series.
