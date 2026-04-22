# RFC 0022: Lakehouse Storage Format Strategy

- **Status:** Draft (Growth Tier — speculative; may change significantly)
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0001 (Platform Vision), RFC 0009 (Destination Loaders), RFC 0021 (Query Engine)

## Summary

This RFC specifies our strategy for open lakehouse formats: **Apache Iceberg**, **Delta Lake**, and (if it matures) **Apache Hudi**. It addresses whether to support one, two, or all three; how our loaders (RFC 9) and query engine (RFC 21) integrate with them; our stance on catalog standards (REST Catalog, AWS Glue, Unity Catalog, Polaris, Nessie); the compaction, maintenance, and governance story; and our positioning vs. customers who want to exit proprietary warehouse lock-in.

This is a **Growth-tier RFC**. Like RFC 21, it is drafted to complete architectural thinking. It is more confidently predictive than RFC 21 because the industry direction on open lakehouse formats is clearer than on SQL engines — but specific vendor dynamics (Databricks/Tabular, Snowflake/Polaris, Apache governance) can still shift significantly.

## Motivation

The lakehouse format wars are real, public, and ongoing. Why it matters to us:

1. **Customers want to exit warehouse lock-in.** Large customers with multi-year Snowflake or BigQuery investments are actively exploring open formats. "Write to Iceberg; query from wherever" is a real strategy, and platforms that facilitate it have tailwinds.
2. **Open formats are where data-platform competition is going.** Databricks' acquisition of Tabular, Snowflake's release of Polaris catalog, AWS's Iceberg-first S3 Tables — everyone important is staking positions. Our stance matters.
3. **The format choice constrains everything else.** A pipeline landing data in Iceberg is fundamentally different from one landing in Delta. Time-travel semantics, schema evolution rules, row-level deletes, catalog protocols — all diverge.
4. **Our query engine (RFC 21) and loaders (RFC 9) need to speak these formats well.** "We support Iceberg" at the marketing level vs. "our Iceberg support is production-grade including V3 features" at the engineering level is a significant investment gap.

Getting this right means we're a credible open-lakehouse platform. Getting it wrong means we ship half-integrations that work for demos and fail in production.

## Non-Goals

- This RFC does not propose building our own lakehouse format. We commit to open standards.
- This RFC does not propose a catalog product competing with Unity Catalog, Polaris, AWS Glue, Nessie, or the REST Catalog standard. We integrate with them.
- This RFC does not cover data governance features (column masking, row-level security at the catalog layer, access controls beyond standard catalog capabilities). Those are catalog responsibilities; we respect them.
- This RFC does not specify a specific compaction scheduling product. We orchestrate compaction; we don't vendor it.
- This RFC does not commit to launching comprehensive lakehouse support at general availability. It is speculative planning for the Growth tier.

## Design Principles

**Iceberg-first, Delta-also, Hudi-maybe.** The industry is coalescing around Iceberg; Delta is important because of Databricks; Hudi is niche but not negligible. Our investment reflects this.

**Open catalogs, not vendor lock-in.** We integrate with whatever catalog the customer uses. We do not try to make customers move to "our catalog." Neutral positioning is the product.

**Full read and write support for first-class formats.** Our loaders write Iceberg/Delta tables; our query engine reads them. Half-support (read-only, or write-without-read) is not enough to be credible.

**Compaction is orchestrated, not provided.** We trigger and monitor compaction; we don't run a compaction service competing with dedicated tools (Databricks, AWS Glue, Dremio, proprietary offerings).

**Time-travel as a first-class feature.** Both Iceberg and Delta support time travel. Our platform exposes this: "query the table as of yesterday" is a natural interaction.

**No proprietary extensions.** We write and read standard format. Customers can swap our platform out and keep their data — the open-format promise is real.

## Format Comparison

Brief tactical comparison, current as of early 2026.

### Apache Iceberg

**Status**: de facto industry standard. Apache Software Foundation governance. Active community; major vendor investments (Snowflake's Polaris catalog is Iceberg-based; AWS S3 Tables; Databricks' Tabular acquisition).

**Strengths**:

- Clean specification, versioned (V1, V2, V3).
- Robust catalog protocol (REST Catalog) with multiple implementations.
- Excellent partition evolution (change partitioning without rewriting data).
- Strong V2 features: row-level deletes (merge-on-read), upsert semantics.
- Growing V3 features: variant types for semi-structured data, geospatial types.
- Widely supported: Spark, Trino, Flink, DuckDB, DataFusion, Snowflake, BigQuery (in preview), many more.

**Weaknesses**:

- Complex for simple use cases; catalog setup is a barrier.
- Compaction is essential but not automatic without additional tooling.
- V2 deletes produce read-time overhead.

**Our support tier**: First-class. Full read/write. All major V2 features. V3 features as they stabilize.

### Delta Lake

**Status**: Databricks-originated, Linux Foundation governance (Delta Lake project). Strong momentum specifically due to Databricks' platform adoption. Databricks' UniForm feature emits both Delta and Iceberg metadata from the same files, which is an interesting convergence signal.

**Strengths**:

- Simpler mental model than Iceberg (transaction log is straightforward).
- Excellent streaming-write support.
- Mature tooling within the Databricks ecosystem.
- Strong compaction via Databricks' Photon/autooptimize.
- Time travel and schema evolution are mature.

**Weaknesses**:

- Historical "Delta is Databricks-flavored" perception, now somewhat reduced.
- Catalog story less standardized than Iceberg REST Catalog (Unity Catalog is the flagship but proprietary).
- Partition evolution less sophisticated than Iceberg.

**Our support tier**: First-class. Full read/write for Delta 3+ (including `delta-rs` production use).

### Apache Hudi

**Status**: Apache project; used by some large engineering organizations (Uber originated; others adopted). Less industry momentum than Iceberg / Delta. More complex to operate.

**Strengths**:

- Strong streaming/upsert story.
- Record-level indexing.
- Copy-on-write + merge-on-read modes offer flexibility.

**Weaknesses**:

- Smaller community; fewer integrations.
- Operational complexity higher.
- Convergence pressure toward Iceberg for many previous Hudi use cases.

**Our support tier**: Second-tier. Read support via compatible libraries. Write support deferred; evaluate based on customer demand.

### Summary Decision

We commit to **Iceberg and Delta as first-class formats** at launch of lakehouse support. Hudi gets read-only or not-at-all depending on customer demand.

## Integration with Loaders (RFC 9)

### Iceberg loader

Already sketched in RFC 9 as part of the launch loader set. Extensions and specifics:

**Library**: `iceberg-rust` (the Apache implementation), production-ready at this point. Contributed to by multiple vendors.

**Write path**:

1. Loader receives batches from staging.
2. Writes Parquet data files to the table's data path.
3. Appends to the table's manifest (metadata).
4. Commits the snapshot atomically through the catalog (REST Catalog, Glue, etc.).
5. Optimistic concurrency: on conflict with a concurrent writer, retry.

**Format version**: V2 default (row-level deletes supported). V3 opt-in as V3 features stabilize.

**Partition strategy**: specified per-pipeline. Supports Iceberg's hidden partitioning, partition evolution, and explicit partition columns.

**Merge-on-read vs. copy-on-write**: configurable per pipeline. Default merge-on-read for CDC pipelines (efficient writes); copy-on-write for analytical pipelines (simpler reads).

### Delta loader

**Library**: `delta-rs` (the Rust implementation), production-ready.

**Write path**:

1. Loader receives batches.
2. Writes Parquet data files to the table path.
3. Appends transaction log entry (`_delta_log/<version>.json`).
4. Concurrent writer conflict resolution via log log-tail comparison.

**Format version**: Delta 3.0+ protocol.

**Schema evolution**: automatic additive evolution; explicit manual steps for breaking changes (per RFC 10).

**Deletion vectors**: Delta's answer to merge-on-read. Supported.

### UniForm considerations

Databricks' UniForm emits Delta transaction log + Iceberg manifest from the same data files. A customer using UniForm tables gets both reads — the customer's Snowflake can query via Iceberg, their Databricks via Delta, same data.

Our loader supports writing UniForm tables (a configuration option on the Delta loader). This gives customers maximum optionality.

### Schema evolution

Both formats support rich schema evolution. Our loaders apply RFC 10's evolution policies against lakehouse schemas:

- Additive changes: `ALTER TABLE ADD COLUMN` (or equivalent metadata update).
- Widening: `ALTER TABLE ... TYPE ...` when supported.
- Removing / narrowing / breaking: per RFC 10 policy; requires operator action.

Lakehouse formats' schema evolution is generally more flexible than warehouse schemas, so additive changes are near-trivial.

### Time travel

Loaders expose table snapshots:

- Every loader commit creates a new snapshot (Iceberg) or version (Delta).
- Our catalog tracks our run-to-snapshot mapping: "run X produced snapshot Y."
- Customers can reference snapshots by run ID or by time.

The snapshot retention policy is per-table, configurable. Default: 7 days of snapshots retained; older snapshots expired. Enterprise: longer retention, vendor-compatible.

## Integration with Query Engine (RFC 21)

### Reading lakehouse tables via DataFusion

DataFusion has Iceberg reader support via `iceberg-rust`. Delta reader support via `delta-rs`. We wire these in as `TableProvider`s.

Queries against lakehouse tables:

```sql
SELECT * FROM my_catalog.warehouse.orders WHERE order_date = '2024-01-15'
```

Execution:

1. DataFusion parses / plans.
2. Catalog (REST Catalog, Glue, etc.) provides table metadata: schema, partition spec, snapshot.
3. Partition pruning: `order_date = '2024-01-15'` prunes to a specific partition's files.
4. Predicate pushdown into Parquet reads.
5. Data scanned; query completes.

### Read performance

Partition pruning + Parquet column pruning + page-level predicate pushdown gets us to reasonable query speed. For our workload (Mode 3 from RFC 21 — one-off analytical queries), this is sufficient. For interactive dashboards, customers should use a dedicated engine.

### Time-travel queries

```sql
SELECT * FROM my_catalog.warehouse.orders
FOR TIMESTAMP AS OF '2024-01-14 00:00:00'
```

Or:

```sql
SELECT * FROM my_catalog.warehouse.orders
FOR VERSION AS OF 1234
```

Support depends on format and catalog; Iceberg's snapshot model handles this natively. Delta's version-based approach also works.

### Write-through-query (deferred)

A `INSERT INTO ... SELECT ...` pattern where a query writes to another lakehouse table. Implementable via our pipeline model (scheduled SQL transformation with lakehouse destination). Possible future surface; not core.

## Catalog Strategy

The most strategically important part of this RFC.

### Why catalogs matter

An Iceberg or Delta table is a pile of Parquet files plus metadata. The catalog is what tells you *which* files are the current table. Without a catalog agreement, you can't reliably read or write.

Four catalog standards matter:

**REST Catalog (Iceberg)**: open specification, multiple implementations. The default we recommend.

**AWS Glue**: ubiquitous in AWS shops. Used by many customers as their existing metadata store.

**Unity Catalog**: Databricks' proprietary catalog, partially open-sourced. Integral to Databricks customers.

**Polaris**: Snowflake's open-source catalog (Iceberg REST Catalog-compatible). Newer; adoption growing.

**Nessie**: Git-like versioned catalog from Dremio. Niche but valuable use cases.

### Our catalog position

We are **catalog-neutral and catalog-integrating**:

- Customers bring their own catalog; we connect to it.
- We support REST Catalog, Glue, Unity Catalog (read), Polaris, Nessie.
- We don't build a proprietary catalog.
- We don't force customers to migrate catalogs.

This is strategic: by being neutral, we're a credible partner for customers regardless of their catalog investment. If we picked a side, we'd alienate customers on the other side.

### Unity Catalog specifics

Unity Catalog is Databricks-controlled. Integration: our loader writes data; customer's Databricks layer manages Unity Catalog entries. We don't write directly to Unity Catalog (that would require deep Databricks integration we don't currently have).

This is an asymmetry: for Iceberg REST Catalog we can write catalog entries directly; for Unity Catalog we can read but writes happen via Databricks' own mechanisms.

### Polaris specifics

Snowflake's Polaris is a REST Catalog-compatible implementation. Our Iceberg loader writes to it as it would to any REST Catalog. Customers who Snowflake-plus-Polaris get our Iceberg-landing for free.

### Catalog bootstrapping

Customers new to lakehouses often don't have a catalog set up. Our documentation and CLI include bootstrap helpers:

```bash
platform-cli lakehouse bootstrap \
  --format iceberg \
  --catalog rest \
  --backend s3://my-bucket/warehouse/ \
  --catalog-endpoint http://my-catalog-server:8080
```

We don't operate the catalog for them; we help them stand one up.

## Compaction and Maintenance

Lakehouse tables accumulate small files from streaming / CDC writes. Compaction (rewriting many small files into fewer large ones) is essential for read performance.

### Our position

We orchestrate compaction; we don't implement our own compaction engine.

**Orchestration** means:

- Scheduling compaction runs per table.
- Triggering compaction via the customer's chosen tool (Iceberg compaction via Spark / Trino / Iceberg's native REST procedures; Delta via `delta-rs` or Databricks).
- Monitoring compaction progress.
- Alerting on overdue compaction.

**We do not provide**:

- A compaction worker pool running on our infrastructure in hosted mode. Compaction is heavy work; running it on our side changes the cost structure significantly.
- A proprietary compaction algorithm. The tools that exist are good enough.

### Compaction workflow

Pipeline config includes a `maintenance` section:

```yaml
kind: Pipeline
spec:
  destination:
    connection: dst-iceberg
    table: orders
  maintenance:
    compact:
      schedule: daily
      trigger_via: rest_catalog_procedure
      target_file_size: 256MB
```

Our Scheduler service triggers the compaction procedure at the configured cadence. The procedure runs on the customer's chosen engine (typically a Databricks job, AWS Glue job, Snowflake job, or customer-operated Spark/Trino). Our role is scheduling and observability.

### Manifest compaction (Iceberg-specific)

Iceberg has its own concept of manifest files that also accumulate and benefit from compaction. Handled the same way: orchestrated, triggered via Iceberg's REST Catalog procedures.

### Snapshot expiration

Old snapshots accumulate metadata. We schedule snapshot expiration at the per-pipeline retention level:

```yaml
maintenance:
  expire_snapshots:
    retain_for: 30_days
    retain_minimum_count: 10
```

## Cost Observability

Lakehouse costs are subtle. We surface them.

### Storage costs

Per-table storage usage, per-snapshot retention cost. Customers see: "your `orders` table has 2.3 TB of current data + 0.8 TB of retained snapshots for time travel."

### Query costs

For Mode 3 queries (RFC 21), cost per query attributed to:

- Bytes scanned (object storage egress if cross-region, compute resources to decode).
- Compute seconds.

### Compaction costs

Compaction runs on customer infrastructure typically; cost surfaces in their cloud bill. We report triggering and result: "compacted 1247 files → 12 files; 45 GB rewritten."

## Positioning

How we talk about this to customers.

### "Open lakehouse, not proprietary warehouse"

Customers investing in open formats can adopt our platform without lock-in. Their data lives in their cloud, in open formats, with open catalogs. They can migrate away from us by disconnecting; their data remains usable.

This is the differentiator vs. Databricks ("data in Delta is open, but proprietary catalog and compute") and vs. Snowflake ("proprietary formats; Iceberg support is a recent addition").

### "Ingestion and optional query; not a full data platform"

We're honest about scope. We don't replace Databricks for complex analytics or ML. For customers who want the full Databricks value, we integrate with them — their lakehouse is still our landing zone.

### "Portable across clouds"

Lakehouse formats + object storage + our platform = the same architecture in AWS, GCP, or Azure. Customers who migrate clouds can take us with them.

## Enterprise Features

Specific capabilities for the enterprise tier:

### Governance catalogs

Integration with Alation, Collibra, DataHub for enterprise data governance. Our lineage (RFC 15) flows into these tools.

### Data contracts

A pipeline can enforce a "data contract" against its destination: specific columns with specific types must exist, with specific constraints. Lakehouse schema evolution is checked against the contract; breaking changes fail the pipeline.

### Column-level access

Respected when the catalog supports it (Unity Catalog, Polaris have column-level access). Our query engine honors the catalog's access decisions.

### Audit integration

Catalog-level access events (who read what) flow into our audit pipeline (RFC 15) when the catalog exposes them.

## Alternatives Considered

**Support only Iceberg.** Simplest. Rejected: Delta is strategically important for Databricks customers; excluding it is a real-world loss.

**Support only Delta.** Smaller surface area. Rejected: Iceberg's momentum makes it the primary direction; we'd be betting against the majority.

**Build our own open lakehouse format.** Comical. Rejected trivially.

**Build our own catalog.** Rejected for neutrality reasons. Customers don't want another catalog; they want us to integrate with theirs.

**Full compaction runtime ownership.** Making our platform a one-stop lakehouse solution. Rejected: changes cost structure significantly; we'd be competing with Databricks on their turf. Orchestration + customer engine is the right level.

**Don't do lakehouse at all; stay in ingestion.** Safe. Rejected: lakehouse is the open-format direction; staying warehouse-focused means we can't address the customers exiting warehouses. Growth-tier investment warranted.

**Deep Databricks integration (become a Databricks partner).** Warm relationship with Databricks is valuable. Rejected as strategy: we'd be Databricks-complementary, not competitive with Databricks. Neutral is better.

## Open Questions

1. **Apache Polaris's trajectory.** Will Polaris become the standard REST Catalog implementation, or will alternatives win? Affects our catalog integration priorities.
2. **Unity Catalog's openness.** Databricks has open-sourced parts of Unity Catalog. How far does that go? Affects write-integration feasibility.
3. **Iceberg V3 timing.** V3 includes variant types (for semi-structured data) and geospatial types. Production readiness TBD; major ecosystem adoption cadence matters.
4. **Hudi support.** Read-only via available libraries is cheap; write support is expensive. Decide based on customer demand year two.
5. **Compaction cost absorption.** Some customers want managed compaction as a premium feature. Could be an enterprise offering; changes cost model. Revisit.
6. **Streaming writes to lakehouse.** Real-time CDC into Iceberg/Delta with low latency. Both formats support streaming writes, but latency considerations are non-trivial. Interlinked with RFC 23.
7. **Cross-format conversion.** Help customers convert Delta tables to Iceberg (or vice versa). UniForm handles simultaneous emission; one-time conversion is a different feature. Potentially useful; potentially niche.
8. **Catalog federation.** A customer with multiple catalogs (Glue for some tables, Unity for others) wants a unified view. Pursuing federation is a large investment; more likely to partner with dedicated catalog products.

## Growth-Tier Caveats

Like RFC 21, this is speculative:

- **Vendor dynamics can shift the landscape.** A major Apache governance event, a major acquisition, a standards convergence — any of these changes the strategy.
- **Customer demand shape.** Early signals suggest strong lakehouse interest; post-launch data will either confirm or update.
- **Ecosystem maturity.** `iceberg-rust` and `delta-rs` have improved rapidly. If their trajectory continues, more aggressive lakehouse positioning is viable. If they stall, we wait.

Commitment level: **"if the open lakehouse direction materializes as expected, we have a plan."**

## References

- Apache Iceberg specification: https://iceberg.apache.org/spec/
- Delta Lake protocol: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
- Apache Hudi documentation: https://hudi.apache.org/docs/overview
- Iceberg REST Catalog specification: https://github.com/apache/iceberg/tree/main/open-api
- iceberg-rust: https://github.com/apache/iceberg-rust
- delta-rs: https://github.com/delta-io/delta-rs
- Snowflake Polaris: https://www.snowflake.com/en/data-cloud/polaris/
- Databricks Unity Catalog: https://www.databricks.com/product/unity-catalog
- AWS Glue Data Catalog: https://docs.aws.amazon.com/glue/latest/dg/components-overview.html
- Project Nessie: https://projectnessie.org/
- Databricks UniForm: https://docs.databricks.com/aws/en/delta/uniform
- Tabular acquisition (Databricks, 2024): public announcement.

## Decision

**Draft, growth-tier — not accepted for launch commitment.** RFC 23 next: Streaming Execution Model — the last RFC in the series and the most speculative, covering if/when we go beyond micro-batch and what a real streaming platform would look like on our substrate.
