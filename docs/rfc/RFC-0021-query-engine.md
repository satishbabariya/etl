# RFC 0021: Query Engine Integration (DataFusion) and SQL Surface

- **Status:** Draft (Growth Tier — speculative; may change significantly)
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0001 (Platform Vision), RFC 0003 (Data Interchange), RFC 0009 (Destination Loaders), RFC 0012 (Transformation Layer), RFC 0022 (Lakehouse Formats — drafted next)

## Summary

This RFC sketches how we introduce a SQL surface to the platform by embedding **Apache DataFusion** as the query engine. It covers three distinct uses of the query engine: **SQL transformations** (alternative to the DAG-of-operators from RFC 12), **in-pipeline analytical operations** (joins, aggregations across multiple streams), and eventually **direct query access** to staged / lakehouse data without requiring a destination warehouse. It defines the scope of SQL we support, the integration with existing transformation and staging infrastructure, the performance and correctness expectations, and the boundaries beyond which customers should still use a real warehouse.

This is a **Growth-tier RFC**. It is drafted to complete architectural thinking and surface decisions we would need to make when the Databricks direction becomes a priority. It is not a launch commitment. Some details here may be wrong in ways we only discover after the ingestion wedge plays out.

## Motivation

RFC 1 committed to an ingestion-first wedge, explicitly deferring Databricks-style compute. RFC 12 provided a DAG-of-operators transformation model that covers the 80% of pre-destination transformation needs. This RFC asks: what about the other 20%, and what about workloads that don't want a destination warehouse at all?

Three concrete user needs motivate a query engine:

1. **Customers who want SQL transformations.** Data engineers write SQL fluently; RFC 12's YAML DSL plus UDFs is powerful but foreign to them. "Let me write `SELECT col_a, UPPER(col_b) FROM stream WHERE created_at > '2024-01-01'`" is what they actually want.
2. **Customers who want joins, aggregations, and analytical operations before the destination.** RFC 12 explicitly rejected cross-batch aggregation and general joins. Some workloads genuinely need them pre-destination: enrichment with large lookup tables, deduplication across extraction batches, pre-aggregation to reduce destination-side cost.
3. **Customers who want to skip the destination entirely.** The most interesting case. They use us for ingestion and transformation, and query the lakehouse (RFC 22) directly with SQL — no Snowflake, no BigQuery, no warehouse at all. This is competing with Databricks directly.

A query engine in the data plane enables all three. The trick is doing it without building another Databricks (a decade-plus project) or becoming a generic query engine (undifferentiated commodity).

## Non-Goals

- This RFC does not propose competing with dedicated query engines (Snowflake, BigQuery, Databricks Photon) on complex analytics. Their optimizers, distributed execution, and caching represent decades of investment.
- This RFC does not propose serving user-facing analytical queries (BI tools, dashboards). That is low-latency, high-concurrency territory with different requirements.
- This RFC does not commit to launching SQL transformations in the first year. It's speculative planning for when the pieces come together.
- This RFC does not redefine the RFC 12 transformation model. DAG operators remain; SQL transformations are an additional authoring surface that compiles to the same underlying execution.
- This RFC does not specify a full SQL-92 compliance commitment. A constrained SQL subset covers our use cases; full compliance is a tar pit.

## Design Principles

**DataFusion, not our own engine.** Building a query engine is a decade of work. DataFusion is production-grade, Rust-native, Arrow-first, actively maintained, and used by several successful products (InfluxDB 3, Polars-adjacent tooling, Apache Comet, dozens of startup companies). Adopting it gives us years of leverage.

**SQL as an authoring surface, not a new execution model.** SQL transformations compile to the same underlying operator representation as RFC 12 DAG transformations. The execution engine is unified; only the authoring UX differs.

**Constrained SQL dialect.** We support a well-defined subset (SELECT/WHERE/GROUP BY/basic JOIN/common functions). We do not support the long tail of SQL features — recursive CTEs, complex window functions, stored procedures, imperative control flow. Users who need those use a real warehouse.

**Schema-derivable at validation time.** Every supported SQL construct must have a static output-schema derivation (RFC 12 requirement). This constrains what we can support (non-deterministic schema-changing SQL is out) but keeps the platform's schema-evolution story intact.

**Same determinism rules as RFC 12.** SQL transformations are deterministic: no `NOW()`, no `RANDOM()`, no `CURRENT_USER` except via host-provided virtual columns. Same Temporal-retry-safety story.

**Bounded by the cost model.** The query engine runs in data plane workers (RFC 2); workloads must fit within per-activity resource limits. We are not building an always-on distributed query cluster. SQL queries that would require terabytes of shuffle are rejected at planning time.

## What DataFusion Brings

### Core capabilities

- **Columnar execution engine** over Arrow RecordBatches, matching our data format (RFC 3) natively.
- **Catalyst-style query optimizer**: logical plan → optimized logical plan → physical plan. Cost-based optimization for simple cases.
- **Standard SQL parser**: sqlparser-rs, covering a large portion of PostgreSQL dialect.
- **Pluggable table providers**: Parquet, CSV, JSON, Arrow, and custom providers we write.
- **Pluggable function registry**: users and we can register custom scalar / aggregate / window functions.
- **Streaming execution**: partial (supports some streaming operators); sufficient for our micro-batch model.

### What we don't inherit

DataFusion is an engine, not a product. Building on it, we still need to:

- Define our specific SQL dialect (features allowed / disallowed).
- Integrate with our connector protocol for source reads.
- Integrate with our loader protocol for sink writes.
- Add our authorization, tenancy, and observability layers.
- Provide query plan visibility (EXPLAIN) in our UI.
- Integrate with our catalog and schema model (RFC 10).

Adoption saves years; it doesn't hand us a product.

## Three Modes of Use

### Mode 1: SQL Transformations

Alternative authoring surface for the transformation layer (RFC 12). A transformation package can be authored in SQL:

```sql
-- transformation.sql in a TransformationPackage
SELECT
  id,
  UPPER(email) AS email_normalized,
  MD5(ssn) AS ssn_hash,
  created_at,
  CASE
    WHEN plan_tier IN ('gold', 'platinum') THEN TRUE
    ELSE FALSE
  END AS is_premium
FROM source.users
WHERE status != 'deleted';
```

Equivalent to a DAG of `filter` + `project` + `mask` + `add_column` operators, but more compact for users who prefer SQL.

**Validation:**

- DataFusion parses the SQL and produces a logical plan.
- Our layer validates: are all referenced tables registered? Are only deterministic functions used? Is the output schema statically derivable?
- Output schema is derived from the plan and registered in the catalog (RFC 10).

**Execution:**

- DataFusion's physical plan becomes a sequence of operators that consume input batches and produce output batches.
- These operators run in the transformation activity (RFC 4), on worker machines, in the same way DAG transformations do.

**Schema evolution:**

- SQL references columns by name. A column dropped from source breaks a SQL transformation referencing it; pipeline pauses for operator review.
- Additive source changes (new columns) propagate transparently: `SELECT *` includes them; explicit column lists ignore them per RFC 10 evolution policy.

### Mode 2: In-Pipeline Analytical Operations

SQL transformations with capabilities that DAG operators deliberately don't support:

**Joins across streams:**

```sql
SELECT
  orders.order_id,
  orders.amount,
  customers.region,
  customers.segment
FROM source.orders
JOIN source.customers ON orders.customer_id = customers.id
WHERE orders.created_at > DATE '2024-01-01';
```

The `orders` and `customers` streams are separately extracted, staged, and joined at the SQL transformation stage. Small tables can be broadcast-joined; larger joins require both sides sorted on the join key (or hashed).

**Cross-batch aggregations:**

```sql
SELECT
  region,
  plan_tier,
  COUNT(*) AS user_count,
  AVG(revenue) AS avg_revenue
FROM source.users
GROUP BY region, plan_tier;
```

Unlike RFC 12's batch-local `aggregate`, this aggregates across all batches in a run. The execution materializes intermediate state (hash table); finalizes at run boundary.

**Window functions (constrained):**

```sql
SELECT
  user_id,
  event_time,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) AS event_seq
FROM source.events;
```

Supported window functions: `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, simple aggregates over windows. Not supported: complex analytical window functions requiring global state across a full run.

**Limits on in-pipeline analytical operations:**

- Total memory per activity caps aggregate state. A `GROUP BY` producing millions of groups may spill to disk; tens-of-millions of groups exceeds budget and fails.
- Joins require one side to fit in memory OR both sides pre-sorted. Full sort-merge join across billions of rows is possible but slow.
- Users receive explicit warnings when their SQL is expensive: "This query will process ~500 GB; consider using a destination query engine."

### Mode 3: Direct Query Access (Lakehouse Only)

The most ambitious mode, requiring RFC 22's lakehouse integration. Customers write SQL against their lakehouse directly through our platform:

```sql
-- Via our query API or CLI
SELECT
  DATE_TRUNC('day', order_date) AS day,
  SUM(amount) AS total_revenue
FROM my_iceberg_catalog.warehouse.orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY day
ORDER BY day;
```

**What this is:**

- Customer has ingested data into their Iceberg or Delta tables via our pipelines.
- They issue SQL queries through our API or CLI.
- The query executes on our data plane compute.
- Results stream back to the caller.

**What this is not:**

- A dashboard backend (too slow for interactive dashboards).
- A general-purpose data warehouse replacement (missing caching, concurrency, result materialization for interactive queries).
- A competitor to Snowflake/BigQuery on complex workloads.

**What it is useful for:**

- One-off analytical queries without setting up a warehouse.
- ETL-adjacent computation: "run this query nightly, put results in another table."
- "Cheap" queries against lakehouse data that would otherwise require spinning up a Databricks SQL Warehouse or an Athena query.

### When customers should use a real warehouse instead

We surface this explicitly: the platform recommends a dedicated warehouse when:

- Queries are interactive (sub-second response expected).
- Concurrency is high (hundreds of simultaneous users).
- Data volume is huge (tens of TB+ for single queries).
- Complex optimization (query planning decisions across dozens of joins) matters.

We are not trying to replace Snowflake. We are replacing Snowflake *for workloads where Snowflake is overkill*.

## Query Execution Infrastructure

### Where queries run

SQL transformations (Mode 1) and in-pipeline analytical operations (Mode 2) run in **pipeline transformation activities** (RFC 4). Same worker pool, same resource limits, same observability.

Direct query access (Mode 3) runs in **dedicated query workers**. These are a separate worker pool:

- Scaled independently from pipeline workers (query load has different patterns).
- Longer-running activities (queries can take minutes).
- Different resource profile (more memory, potentially more CPU).
- Per-tenant isolation (RFC 16 applies).

Query workers use Temporal task queues specific to query work (`query-default`, `query-heavy`), extending RFC 4's task queue model.

### Execution model

For each query:

1. **Parse**: DataFusion's SQL parser produces an AST.
2. **Validate**: our layer checks for disallowed constructs, references, determinism.
3. **Plan**: DataFusion's optimizer produces a physical plan. Our layer can intervene (e.g., reject plans that would exceed resource budget).
4. **Execute**: physical plan runs over input batches. Results stream to output.
5. **Complete**: output is written to staging (for Mode 1/2) or returned to the caller (for Mode 3).

Temporal wraps this: a query execution is an activity, heartbeated, retryable on transient failure. Long queries use iterator activities (RFC 6 pattern).

### Integration with connectors

DataFusion's table providers are the integration point. We provide:

- **StagingTableProvider**: reads staging batches from object storage (Mode 1/2; the transformation activity's input).
- **LakehouseTableProvider**: reads Iceberg / Delta tables (Mode 3; RFC 22 for format details).
- **LookupTableProvider**: reads small reference data (analogous to RFC 12's `enrich` operator).
- **LiveConnectorTableProvider** (speculative): wraps a running connector's output as a queryable table — for real-time "query my ingestion stream" use cases. Probably yes later.

Table providers implement a DataFusion-defined trait; we write them on top of the existing connector and storage infrastructure.

### Query plan visibility

Customers see their queries' execution plans:

- `EXPLAIN SELECT ...` returns the logical and physical plans (text or JSON).
- In the UI, a graphical representation of the plan.
- Per-operator metrics (rows processed, time, memory) after execution.

This matters because SQL users expect query plans. Warehouse users expect this as table stakes.

### Caching

**Query result caching** (for Mode 3): short-lived cache keyed by query text + data-version. If the same query hits within N minutes and underlying tables haven't changed, return cached results.

- Not a primary performance mechanism (queries dominated by data scan, not by cache lookup).
- Helpful for UI polling patterns ("refresh my dashboard every 30 seconds").
- Cache invalidation: tied to lakehouse table version (Iceberg snapshot ID, Delta version). If the table has new snapshots, cache is invalid.

## SQL Dialect Scope

What we support and what we don't.

### Supported SELECT features

- Projection: column lists, `*`, expressions, aliases.
- Filtering: `WHERE` with all standard predicates (`=`, `!=`, `<`, `>`, `IN`, `LIKE`, `IS NULL`, `BETWEEN`, `AND`/`OR`/`NOT`).
- Grouping: `GROUP BY`, `HAVING`.
- Sorting: `ORDER BY`, `LIMIT`, `OFFSET`.
- Joins: `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS`, `LATERAL` (limited).
- Set operations: `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`.
- Common table expressions (CTEs): non-recursive only.
- Scalar subqueries and `IN (SELECT ...)`.
- Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, aggregate functions over windows.

### Supported functions

A curated function library:

- Arithmetic: standard operators and functions.
- String: `UPPER`, `LOWER`, `TRIM`, `SUBSTRING`, `CONCAT`, `REPLACE`, `REGEXP_MATCHES`, `SPLIT_PART`, etc.
- Date/time: `DATE_TRUNC`, `EXTRACT`, date arithmetic, formatting. **Not**: `NOW()`, `CURRENT_TIMESTAMP` — use host-provided virtual columns.
- Type conversion: `CAST`, explicit type functions.
- JSON: basic accessors (`->`, `->>`, `jsonb_path_query` subset).
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `ARRAY_AGG`, `STRING_AGG`, `PERCENTILE_CONT` / `PERCENTILE_DISC`, `APPROX_DISTINCT`.

Custom functions:

- User-defined scalar and aggregate functions registered via wasm UDFs (RFC 12's UDF model, extended to SQL-registerable).

### Explicitly not supported

- **Recursive CTEs**: unbounded computation; hard to reason about resource usage.
- **Stored procedures**: outside our model.
- **Triggers**: outside our model.
- **DDL statements** (`CREATE TABLE`, `ALTER TABLE`): catalog manages schema; DDL at the SQL layer would bypass it.
- **DML statements** (`INSERT`, `UPDATE`, `DELETE`): destinations are updated through loaders (RFC 9), not through SQL.
- **Transactions** (`BEGIN`, `COMMIT`): transformation activities are transactional at the run level; SQL-level transactions don't exist here.
- **`NOW()` and friends**: non-deterministic; breaks retry-safety and schema-derivation.
- **Non-deterministic order-dependent functions** without explicit ordering.

### Dialect anchor: PostgreSQL

When there's a choice between SQL dialects, we anchor on PostgreSQL's syntax and function names. Rationale: most widely understood, DataFusion already largely PostgreSQL-compatible, familiar to most data engineers.

We do not claim PostgreSQL compatibility; we claim "PostgreSQL-flavored."

## Resource Limits and Cost Control

SQL queries can be expensive. We enforce limits.

### Per-query limits

Declared in the transformation package or query invocation:

- **Maximum memory**: default 2 GB; cap 16 GB for enterprise tier.
- **Maximum wall time**: default 5 minutes; cap 1 hour for long-running analytics.
- **Maximum rows scanned**: default 1 billion; cap per tier.
- **Maximum rows output**: default 100 million; cap per tier.

Queries that exceed limits fail with a clear error ("your query would process ~500GB — consider adding a WHERE filter on `created_at`").

### Cost estimation

DataFusion's optimizer produces cost estimates. We expose these:

- Dry-run: `EXPLAIN ANALYZE` without execution returns estimated rows/bytes scanned.
- Before-execution warnings: "this query is estimated to scan 50 GB; proceed?"
- Post-execution actuals: "query scanned 47 GB; took 4m 12s."

This is the "know what you're about to pay for" feature that keeps SQL users from submitting expensive queries by accident.

### Partition pruning

For lakehouse tables with partition columns, DataFusion prunes partitions based on `WHERE` clauses. This is the single biggest performance win — a query filtering on `order_date = '2024-01-15'` scans one partition, not the whole table.

We ensure partition pruning works by:

- Exposing partition metadata from lakehouse tables to DataFusion.
- Checking query plans for pruning opportunities and warning when predicates don't prune.
- Recommending partition choices in the UI when tables are designed.

### Predicate pushdown

For connector-sourced tables, we push predicates to the connector where possible:

- `WHERE created_at > '2024-01-01'` becomes an incremental cursor filter rather than a post-extract filter.
- Only pushable for deterministic, connector-supported predicates.

This extends the optimization story: SQL transformations benefit from connector-level filtering automatically.

## Integration with Existing RFCs

### Transformation layer (RFC 12)

SQL transformations are a new authoring surface; they compile to the same underlying execution. A `TransformationPackage` can contain:

- Declarative operators (RFC 12), or
- Wasm UDFs (RFC 12), or
- SQL (this RFC).

At the catalog level, they're all `TransformationPackage` entities with different `authoring_style`. The compiled representation is uniform: a sequence of physical operators that DataFusion executes.

### Pipeline DSL (RFC 13)

YAML references SQL transformations the same way it references DAG transformations:

```yaml
kind: Transformation
metadata:
  name: clean-users
spec:
  sql: |
    SELECT
      id,
      UPPER(email) AS email,
      ...
    FROM source.users
    WHERE status != 'deleted'
```

Or inline in a pipeline:

```yaml
kind: Pipeline
spec:
  transformation:
    inline:
      sql: "SELECT ..."
```

### Catalog (RFC 10)

SQL transformations are catalog entities. The SQL text, its derived output schema, and its lineage (which source tables/columns it reads) are all stored.

Schema evolution: when a source schema changes, SQL transformations are re-analyzed:

- Additive changes: transformation continues (if using `SELECT *`) or ignores (if using explicit column list).
- Breaking changes: transformation is marked invalid; pipeline pauses per RFC 10 policy.

### Destination loaders (RFC 9)

SQL transformations produce batches that loaders consume identically to batches from DAG transformations. No loader changes needed.

### Observability (RFC 15)

SQL execution produces detailed metrics per operator (following DataFusion's metrics API):

- `sql_operator_rows_processed_total` (by operator type, by stage).
- `sql_operator_duration_seconds`.
- `sql_operator_memory_bytes`.
- `sql_query_duration_seconds`.
- `sql_query_rows_returned_total`.

These surface in the pipeline dashboard alongside DAG transformation metrics.

## Development Experience

### Query authoring UX

In the pipeline configuration UI:

- SQL editor with syntax highlighting.
- Auto-completion for table names, column names, functions.
- Real-time schema display: the output schema updates as the user types.
- EXPLAIN on demand.
- Preview: run against a sample of recent extracted data.

These matter for adoption; data engineers expect SQL tooling to feel like Snowflake or BigQuery's editor.

### Preview mode

Analogous to RFC 12's preview mode:

- Extract a sample of rows (default 1000).
- Run the SQL against the sample.
- Display results.

No production impact; fast feedback loop.

### Debugging

When a SQL transformation fails on a specific batch:

- The batch is preserved (per RFC 14 staging retention).
- `EXPLAIN ANALYZE` with the batch shows where in the query plan the failure occurred.
- Row-level errors (e.g., division by zero on one row) are attributed to specific rows, reported as dead-letter entries per RFC 9's mechanism.

## Alternatives Considered

**Build our own SQL engine.** Rejected: decade-plus engineering investment. DataFusion exists and is good.

**Use Polars instead of DataFusion.** Polars is also Arrow-native and Rust-based. Has some advantages (ergonomic API for programmatic use). Rejected for this role: Polars is a DataFrame library; DataFusion is a query engine. We want query-engine capabilities (parser, optimizer, pluggable providers). Polars is an internal implementation option for specific operators if we need it.

**Embed DuckDB.** DuckDB is excellent for analytical SQL. Rejected primarily because it is C++, not Rust; mixing significantly complicates our build and debugging story. DataFusion is 90% of DuckDB's capability for our use case in native Rust.

**Don't offer SQL; require SDK / DSL authoring for transformations.** Rejected: we lose a large segment of data engineers who prefer SQL. SQL is the most widely known data manipulation language; excluding it cedes ground unnecessarily.

**Full SQL compatibility (PostgreSQL / Snowflake compatible).** Rejected: infinite tail of compatibility bugs. A well-defined subset with clear docs is better than imperfect compatibility.

**Dedicated long-running query cluster.** Rejected at launch: adds a new operational axis. Query workers are a pool, not a cluster; queries are activities, not long-lived connections. If the Mode 3 direct-query use case grows large enough, revisit.

**Two execution engines (DataFusion for SQL, our own for DAG operators).** Rejected: maintenance burden and semantic divergence. Unified execution (everything is a physical plan that DataFusion can run) is simpler.

## Open Questions

1. **Query concurrency and fairness.** Mode 3 direct queries compete for worker capacity. Per-tenant query concurrency limits, priority tiers, queuing strategy — all TBD at scale. Real-world operation will teach us.
2. **Result caching for direct queries.** Sketched above; details (cache size, eviction, invalidation granularity) need real workload data.
3. **Federated queries.** A query that spans multiple lakehouse tables is natural; a query that spans lakehouse + a live Postgres source via a connector is interesting but potentially catastrophic (slow source brings down query). Probably allow with warnings and timeouts; details TBD.
4. **Materialized views.** "Run this SQL periodically and save results to a new Iceberg table" is the obvious next step after Mode 3. Really just a scheduled SQL-transformation-to-lakehouse-destination pipeline, nicely packaged.
5. **SQL standard library evolution.** DataFusion's function library grows; which upstream additions do we adopt automatically vs. gate? Likely a curated subset with explicit review.
6. **Streaming SQL.** DataFusion has some streaming support. When we eventually do streaming (RFC 23), what role does SQL play? Probably "SQL over windowed streams" as a future surface, but deeply interlinked with RFC 23's decisions.
7. **Cost surprises.** A query that cost-estimates to 1 GB but actually scans 500 GB is a bad experience. Our cost estimates need to be conservative (lean higher) to prevent surprises. Post-execution reconciliation important.
8. **Authorization at the column level.** A tenant might want to let user A query table T but not see column X. Column-level authorization is a real feature request in data platforms. Requires integration with our auth model (RFC 19); potential but significant project.

## Growth-Tier Caveats

As noted in the header: this RFC is speculative. Specific areas most likely to change:

- **Priority ordering of the three modes.** Mode 1 (SQL transformations) is clearly useful and relatively cheap to build. Mode 2 and Mode 3 depend on the lakehouse direction (RFC 22) playing out. If lakehouse doesn't become a priority, Mode 3 effectively doesn't ship.
- **DataFusion's evolution.** Fast-moving upstream project. Some design decisions here may become trivial or impossible as DataFusion changes. We track closely.
- **Customer demand shape.** Post-launch, we'll learn what customers actually want. Some of this RFC will be confirmed; some will be revised; some will be abandoned.

The commitment level of this RFC is **"we've thought about this carefully and can build it when we choose to,"** not **"we've committed to building this on any timeline."**

## References

- Apache DataFusion: https://datafusion.apache.org/
- DataFusion architecture overview: https://arrow.apache.org/datafusion/contributor-guide/architecture.html
- sqlparser-rs: https://github.com/sqlparser-rs/sqlparser-rs
- PostgreSQL SQL reference (dialect anchor): https://www.postgresql.org/docs/current/sql.html
- Apache Comet (Spark acceleration using DataFusion): https://github.com/apache/datafusion-comet
- InfluxDB 3's use of DataFusion: prior art for embedded DataFusion in production.
- Polars (alternative engine, considered-and-rejected for this role): https://pola.rs/
- DuckDB (alternative engine, considered-and-rejected): https://duckdb.org/

## Decision

**Draft, growth-tier — not accepted for launch commitment.** RFC 22 next: Lakehouse Storage Format — which is tightly interlinked with Mode 3 of this RFC and drives the strategic positioning for customers who want to avoid warehouse lock-in.
