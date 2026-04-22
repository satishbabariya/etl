# RFC 0004: Temporal Workflow Topology and Durability Model

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0002 (Core Architecture), RFC 0003 (Data Interchange)

## Summary

This RFC specifies how we use Temporal: the hierarchy of workflows, the boundary between workflow code and activity code, the signal/query surface, the retry and compensation model, and the rules for versioning long-running workflows. It turns Temporal from "an orchestrator we depend on" into "a set of concrete patterns every component follows."

This RFC is prescriptive. Temporal is flexible enough to support many workflow topologies, and an ungoverned codebase will produce several of them — making debugging and operations miserable. We commit to a single dominant pattern for pipeline execution and a small number of auxiliary patterns for everything else.

## Motivation

Temporal's power comes from its primitives composing freely. That same freedom means teams routinely produce workflow topologies that are locally reasonable but collectively chaotic: some pipelines use one workflow with many activities, others use parent/child hierarchies three deep, others use signals where queries would suffice. The result is that operators can't answer "what's this pipeline doing right now?" without reading custom code for each one.

We avoid this by committing to:

1. **One canonical pipeline workflow shape.** Every pipeline run looks the same from the outside. Operators and tooling only need to learn one pattern.
2. **Explicit rules for when child workflows are justified.** The default is "one workflow per run." Splitting into children must earn its complexity.
3. **A specified durability model.** What state is durable where, how it's recovered, what "crash-safe" actually means at each boundary.
4. **A versioning strategy that survives long-running workflows.** A sync that started yesterday must still be correct after a deploy.

Temporal's own best-practices guidance covers individual primitives well. What's missing — and what this RFC provides — is the platform-specific composition of those primitives for our workload.

## Non-Goals

- This RFC does not specify the connector protocol, CDC workflow, or loader protocol. Those are RFCs 6, 8, and 9 respectively. This RFC covers the *shape* of workflows; those RFCs fill in the per-component details.
- This RFC does not cover Temporal operational concerns (cluster sizing, shard count, retention). Those are deployment concerns in RFC 18.
- This RFC does not re-justify the choice of Temporal. That's settled in RFC 1 and RFC 2.
- This RFC does not cover streaming execution. The topology here is designed for batch and micro-batch; streaming (if we do it) will use a different topology and gets its own RFC.

## Namespace and Task Queue Structure

Each data plane has a single Temporal namespace per tenant (as stated in RFC 2). Within that namespace, we use task queues to segment work by resource profile:

- **`pipeline-default`** — the main task queue. Standard-size workers poll this.
- **`pipeline-heavy`** — for activities requiring large memory or long execution (bulk historical syncs, large CDC snapshots). Separately-sized workers poll this.
- **`pipeline-external`** — for activities that make outbound API calls and should be rate-limited per tenant (SaaS connectors). Worker pool here is capped and uses cooperative rate limiting.
- **`pipeline-loader`** — for destination-delivery activities. Often different resource profile than extract.

A task queue is not a trust boundary or a pricing boundary; it's a scheduling and sizing tool. All queues share the same wasm runtime, host API, and security model.

The reason to split task queues this early: it lets us autoscale worker pools independently, which matters for cost control. An account doing a one-time historical backfill shouldn't starve its own incremental syncs.

## The Canonical Pipeline Workflow

Every scheduled pipeline run is one instance of a single workflow type: `PipelineRunWorkflow`. This is the dominant pattern. Exceptions require explicit justification.

### Workflow inputs

The workflow is started with:

- `pipeline_id` — reference to the pipeline definition in the catalog.
- `pipeline_version` — pinned catalog version for this run. All definitions read later in the workflow use this version.
- `run_id` — caller-assigned run identifier (also serves as the Temporal workflow ID, suffixed with a run index).
- `trigger` — one of: `scheduled`, `manual`, `backfill(range)`, `cdc_resume(position)`, `cascade_from(upstream_run_id)`.
- `config_overrides` — optional per-run overrides (e.g., manual backfill with a specific start cursor).

### Workflow state

The workflow holds, in its execution state:

- Pipeline plan: the resolved DAG of extract/transform/load stages for this run.
- Per-stream cursor state: where each stream is in its incremental sync.
- Staging references: pointers to Arrow IPC files in object storage produced by each stage.
- Schema state: the current schema for each stream, updated as schema changes are observed.
- Retry/backoff context: counters and next-attempt times for stages that have failed.
- Lifecycle flags: paused, cancelling, completed.

**What it does not hold:** row data, secrets, or anything large enough to bloat workflow history. Workflow history size is a correctness and cost concern — we target <1MB of history per typical run and hard-cap at 50MB before forcing a continue-as-new.

### Workflow structure

```
PipelineRunWorkflow(inputs):
  1. load_plan(pipeline_id, pipeline_version)           [activity]
  2. for each extract stage in plan:
       extract_stream(stream_config, cursor_state)      [activity]
         → staging ref + new cursor + schema delta
     record schema deltas, persist cursors
  3. for each transform stage in plan:
       transform_batch(input_refs, transform_wasm_ref)  [activity]
         → staging ref
  4. for each load stage in plan:
       load_to_destination(input_refs, dest_config)     [activity]
         → load receipt
  5. commit_run(run_id, cursors, schema_state)          [activity]
  6. emit_metadata(run summary)                         [activity]
```

Notes on this shape:

- **Extract, transform, load are sequential stages**, not pipelined. Micro-batch execution model: a batch of data is produced by extract, persisted to staging, then consumed by transform, persisted to staging, then consumed by load. We accept the latency cost of this staging in exchange for crash recovery that resumes from the last staged boundary.
- **Parallelism is within a stage**, not across stages. Multiple streams extract in parallel (Temporal's `execute_activity` futures), but the transform stage does not start until the extract stage commits.
- **Each activity is idempotent.** Extract activities are idempotent on (stream, cursor range). Transform activities are idempotent on input staging refs. Load activities are idempotent on load receipt IDs (RFC 9 covers this in detail).
- **The workflow writes cursors to workflow state, not to the catalog.** The catalog is updated by `commit_run` at the end as an activity. This is critical for crash recovery: if the workflow crashes after extract but before load, Temporal resumes with the extracted data staged and the cursor in workflow state, and we don't re-extract.

### Why not one workflow per stage

It's tempting to model extract, transform, and load as three separate workflows chained by signals. We don't, because:

- Cursor commit must be atomic with destination commit (you cannot advance the cursor before the destination has the data). Putting them in separate workflows makes atomicity harder.
- Cross-stage retry semantics are clearer in one workflow: if load fails, we know exactly which staged data to retry against.
- Observability is simpler: one workflow per run means one timeline to look at.
- Child workflows have overhead (separate history, separate IDs) that adds up at scale.

The cost is that very long pipelines risk hitting history size limits. Mitigation: `continue-as-new` at stage boundaries for unusually long runs (detail below).

## Activity Design Rules

Activities are where real work happens. They are Rust functions that the worker invokes after polling a task queue. Several rules govern how activities are written.

### Activity granularity

An activity should be:

- **Small enough to complete in under 1 hour** under normal conditions. Activities that routinely exceed this should be broken up.
- **Large enough to do meaningful work** — rule of thumb, at least 10 seconds of wall time in the common case. An activity that completes in 10ms has overhead exceeding its value.
- **Bounded in memory.** Activities hold Arrow batches in memory while processing; the per-activity memory budget is declared in the activity config and enforced by the worker.

For extracts that take longer than 1 hour (large historical backfills), the activity is an **iterator activity** that returns after processing a bounded amount of work (say, 100M rows or 30 minutes, whichever hits first), reporting progress. The workflow loops, invoking the activity repeatedly with advancing cursors until the stream is caught up.

### Heartbeats

Any activity running longer than 30 seconds **must** heartbeat at least every 30 seconds. The heartbeat payload contains progress information (rows processed, current cursor position). This has two effects:

- Temporal detects hung activities and fails them, freeing the worker slot.
- Progress information is visible to queries, so operators can see real-time progress.

Activities that cannot heartbeat (e.g., a synchronous library call that blocks for minutes) are wrapped in a thread that heartbeats independently, or refactored to expose progress.

### Activity idempotency

Every activity must be **safely re-executable** on the same input. This is non-negotiable because Temporal will retry activities on worker crashes. Activity implementations achieve idempotency by:

- **Extract activities:** writing staged data to deterministic paths derived from (stream, cursor range, attempt-independent run ID). Re-execution overwrites; consumers read whichever file exists.
- **Transform activities:** deterministic output paths from input paths + transform version. Transforms themselves must be deterministic (RFC 5 enforces this for wasm transforms; first-party transforms are audited).
- **Load activities:** destination-side idempotency. MERGE patterns keyed on primary key + load attempt ID. Full detail in RFC 9.
- **Commit activity:** the final commit uses a conditional update keyed on run ID so a retried commit is a no-op.

An activity that cannot be made idempotent is a design error and must be redesigned before merge.

### Activity return values

Activity return values go into workflow history and therefore cost money and time. Rules:

- **Never return data.** Return pointers (object storage URLs, row counts, cursor values). Activities that produce data write to staging and return the reference.
- **Return structured results.** Every activity returns a typed struct with explicit success/partial/failure variants, not a bare value that the workflow has to interpret.
- **Cap return size at 64KB.** A return value pushing this limit is a design smell.

### Activity retry policy

Activities have explicit retry policies, not defaults. The policy for each activity type is declared in code and reviewed. Categories:

- **Transient-friendly** (network, 5xx from source API): initial interval 1s, exponential backoff, max interval 60s, max attempts 10, retry on all errors.
- **Rate-limit-friendly** (429 from source API): initial interval 30s, backoff factor 2, max interval 15min, max attempts 20, retry only on rate-limit errors.
- **Long-tail retries** (historical backfill stages): initial interval 10s, max interval 5min, max attempts 100, retry on all errors.
- **No-retry** (schema discovery, commit): retry budget 1-3 attempts; repeated failure should escalate, not churn.

Retry policies are set in the workflow code invoking the activity, not in activity metadata, because the same activity implementation may have different retry semantics in different contexts.

### Non-retriable errors

Some errors should not be retried regardless of policy: invalid credentials (404 on auth endpoint), schema mismatch with destination, quota exceeded, explicit "stop" responses from a source. Activities distinguish these by throwing typed errors that the retry policy recognizes as non-retriable. The workflow catches these, records the error, and transitions the pipeline to a "failed pending operator attention" state.

## Signals and Queries

Signals and queries are the interaction points between the workflow and the outside world while it's running.

### Signals (inbound changes)

A pipeline run workflow accepts these signals:

- `cancel(reason)` — graceful cancellation. Workflow finishes its current activity, commits whatever is safely committable, and exits with status `cancelled`.
- `pause()` / `resume()` — for long-running runs (backfills), the operator can pause between stage iterations. The workflow checks the pause flag between activities.
- `adjust_concurrency(new_limit)` — change per-run parallelism mid-flight (for handling noisy-neighbor issues).
- `schema_acknowledge(decision)` — when schema drift requires human intervention (RFC 10), the workflow blocks on a signal providing the decision.
- `external_event(key, value)` — a generic escape hatch for integrations that inject data into the workflow (e.g., "upstream pipeline finished, I am your cascade trigger").

Signals are persistent: if the workflow is not running when a signal is sent, it's queued and delivered when the workflow is next active.

### Queries (outbound status)

A pipeline run workflow answers these queries:

- `status()` — high-level status (`running`, `paused`, `awaiting_schema_decision`, `completed`, `failed`, `cancelled`).
- `progress()` — per-stream progress: rows processed, current cursor, estimated completion (when computable).
- `current_stage()` — which stage is running now and what it's doing.
- `recent_errors()` — the last N errors observed, even if they were retried successfully. Useful for "this pipeline works but something is noisy."
- `plan()` — the resolved DAG for this run.

Queries must be fast (single-digit milliseconds) and side-effect-free. They read workflow state only; they do not invoke activities.

## Child Workflows: When Justified

Our default is one workflow per run, no children. Child workflows are introduced only when one of the following is true:

1. **The child has independent lifecycle.** A CDC sync is a long-lived workflow (potentially weeks); a snapshot taken during CDC init is a child because the snapshot is finite and should not bloat the long-lived parent's history. Detail in RFC 8.
2. **The child represents a cascading trigger.** Pipeline A completing triggers Pipeline B; B is a new workflow (not a child, not an activity) started by A via Temporal's `start_child_workflow` or a signal to the scheduler. We use a signal, not a child workflow, because B is a peer run, not a subordinate — the scheduler owns that relationship.
3. **The child is a fan-out with many instances.** Rare in our workload. Example: a pipeline that processes 10,000 shards in parallel might model each shard as a child. We would reach for activity fan-out first and only escalate to children if activity retry semantics were insufficient.

Justification 1 is the only common case. Justifications 2 and 3 are escape hatches, not patterns to reach for.

## `continue-as-new` Policy

Temporal history has a practical limit (Temporal Cloud's is ~50K events; we set our soft cap lower to leave headroom). Long-running workflows exceed this. `continue-as-new` restarts the workflow with fresh history, carrying over summary state.

Rules:

- **Standard pipeline runs should never trigger `continue-as-new`.** A normal run is short enough (minutes to hours). If it's approaching history limits, something is wrong (too many activity retries, too-fine-grained activities).
- **CDC workflows use `continue-as-new` on a timed cadence** (e.g., every 6 hours or every 10M rows, whichever first). RFC 8 specifies the exact policy.
- **Backfill workflows use `continue-as-new` at stage boundaries** when the backfill is projected to exceed 24 hours.

The payload carried across `continue-as-new` is the minimum state needed to resume: cursors, schema, run metadata, pending-signal state. It is explicitly not "the whole plan" — the plan is re-resolved from the catalog on each continuation.

## Workflow Versioning

Long-running workflows (CDC, long backfills) outlive deploys. The workflow code that started the run may not exist by the time the run completes. This is the #1 operational footgun of Temporal. We address it with four commitments:

### 1. Workflow code is explicitly versioned at decision points

We use Temporal's `patched()` / `GetVersion()` primitive at every point where workflow logic has changed since a prior version. A typical pattern:

```
// Old behavior: retry 5 times.
// New behavior: retry 10 times with different backoff.
if workflow::patched("pipeline-retry-v2") {
    run_with_new_retry_policy()
} else {
    run_with_old_retry_policy()
}
```

Workflows in flight when the new code deploys continue on the old path. New workflow instances take the new path. Old paths are removed only after all in-flight workflows using them have terminated.

### 2. Workflow inputs and outputs are versioned types

All types at the workflow boundary (inputs, signal payloads, query responses, activity args/returns) are versioned via a `schema_version` field. Workers handle multiple versions; the current deploy writes the newest version. This gives us forward/backward compatibility windows on the order of weeks.

### 3. Activity implementations are free to change; workflow code is not

Activity code runs fresh every invocation — the latest deployed version always runs. Activities can be refactored freely. Workflow code, by contrast, is effectively a finite state machine replayed from history; changing it without `patched()` causes non-determinism errors. This is the single most important discipline, and it is enforced by CI: workflow-layer files have a linting rule that disallows certain operations (direct I/O, randomness, time access without `workflow::now()`), and any change to workflow-layer code requires a `patched()` gate unless reviewed as cosmetic.

### 4. Deprecation is explicit and tracked

When a `patched()` gate is introduced, it's tagged with a removal criterion (e.g., "remove after all CDC workflows deployed before 2026-06-01 have continued-as-new at least once"). A quarterly cleanup pass removes satisfied gates. This keeps workflow code from accumulating patched branches indefinitely.

## Durability Model

This is the explicit model of what survives what.

| Failure | What survives | What is lost | Recovery mechanism |
|---|---|---|---|
| Worker process crash | Workflow state, staged data | In-flight activity attempt | Temporal re-schedules activity on another worker |
| Worker machine loss | Workflow state, staged data | Any activity running on that worker | Same as above |
| Data plane control plane brief unavailability | Workflow state, staged data, in-flight activities | Ability to start *new* workflows | Workflows already running continue; new workflows queue at API Gateway |
| Temporal cluster transient failure (<few minutes) | All workflow state | Nothing durable | Temporal resumes on recovery; workers re-poll |
| Temporal cluster extended failure | Workflow state (in storage) | Progress during outage | When cluster recovers, workflows resume from last event |
| Object storage outage (staging) | Workflow state | Ability to write/read staged data | Workflow retries activities; if staging is unavailable for >retry budget, workflow pauses and alerts |
| Source database outage | Workflow state, already-staged data | New extracts until source returns | Activity retry policy; eventual alert escalation |
| Destination outage | Workflow state, all staged data | Ability to deliver to destination | Activity retry policy; staged data held until destination recovers |
| Catalog service outage | Workflow state | Ability to load new pipeline definitions | In-flight workflows use their pinned version (already loaded); no impact. New workflows queue. |

What *cannot* survive:

- Simultaneous loss of all worker nodes *and* the staging bucket. This is a multi-service catastrophe; we depend on the cloud provider's regional durability guarantees and do not engineer for cross-region disaster at the worker level (that's a deployment-tier concern in RFC 18).
- Corruption of Temporal state (history, visibility store). We treat this as a recover-from-backup scenario; it has not happened at Temporal Cloud scale in public record and we accept the residual risk.

## Commit Semantics and the Commit Boundary

The pivotal moment in every pipeline run is **commit**: advancing cursors, committing destination data, and recording run metadata. Commit is where "this run really happened" becomes true.

We commit in this order:

1. **Destination data committed.** The load activity ensures data is durably in the destination before returning success. For destinations that support transactional load (Postgres, Snowflake with explicit transactions), we use transactions. For destinations that don't (BigQuery Storage Write commits on `finalize_write_stream`, S3 completes on object visibility), we use the destination's own commit primitive.
2. **Cursors written to catalog.** The `commit_run` activity writes new cursor positions to the catalog.
3. **Run marked complete.** Final activity updates run status and emits billing/metering events.

Between step 1 and step 2, we have a window where the destination has the data but the catalog thinks the sync didn't advance. If the workflow crashes here, we re-run and the destination gets the same data again — which is fine because load activities are idempotent (RFC 9). The cursor advances on the retry's commit.

Between step 2 and step 3, a crash leaves cursors advanced but run status not finalized. On recovery, the workflow sees commit succeeded and proceeds to step 3.

**We do not implement a two-phase commit across destination and catalog.** This would require distributed transaction semantics the destination doesn't support. Instead, we rely on idempotent loads + cursor-after-destination ordering, which provides at-least-once delivery with eventual cursor consistency. Exactly-once is achievable per-row via destination-side deduplication on primary key.

## Cascade Triggers and Pipeline Dependencies

A common pattern: Pipeline B depends on Pipeline A. B should run only after A completes successfully.

We model this via the **Scheduler Service** (RFC 2), not via direct workflow-to-workflow signaling. When A completes, its final activity emits a `pipeline_completed` event to the Scheduler. The Scheduler evaluates dependent pipelines and starts B if its dependencies are satisfied.

We chose this over direct parent/child workflows because:

- Dependencies can be many-to-many (C depends on A and B both).
- Dependency evaluation involves catalog state (pipeline enablement, quota, schedule windows) that belongs in the Scheduler.
- It keeps pipeline workflows independent — each is a standalone unit, not a node in a cross-workflow graph.

The downside is a small window where A has completed but B hasn't started. For ETL, this is entirely acceptable; for use cases requiring tighter coupling (streaming-adjacent), we'd need a different design — out of scope here.

## Observability Integration

Workflows emit events to the Observability Service via a dedicated activity invoked at key transitions (run started, stage started, stage completed, run committed). These events:

- Are idempotent (the event is keyed by run ID + transition name).
- Go through an activity (not a direct side effect) so they're retry-safe and survive observability-service hiccups.
- Carry no row data, only metadata (row counts, byte counts, error counts, durations).

Detailed metrics (per-activity latency, per-stream throughput) are emitted by the worker, not by the workflow — the worker is the right layer for dense metric emission. The workflow emits coarse-grained "something happened" events; the worker emits fine-grained "here's how well it's going" metrics. Detail in RFC 15.

## Testing

Workflow testing discipline:

- **Unit test workflows against a mock activity interface.** The Temporal SDK's test framework supports this; we use it for every workflow.
- **Replay tests against recorded histories.** Every significant workflow version change ships with a replay test against histories from the prior version, verifying that old histories still replay without non-determinism errors. This catches the most common versioning footguns.
- **Activity tests are normal Rust unit tests.** Activities are just Rust functions; no Temporal-specific testing needed beyond mocking the Temporal client.
- **End-to-end tests run a local Temporal server** (Temporalite or `temporal server start-dev`) and exercise real workflow execution against in-memory sources and destinations.

## Alternatives Considered

**Multiple workflow types per pipeline stage (extract workflow, transform workflow, load workflow, chained by signals).** Rejected as default. Considered for specific cases (CDC parent + snapshot child; see RFC 8).

**Saga pattern with compensating activities.** Rejected. Sagas are about rolling back committed work; our commit boundary is single-point (the final commit activity), so we don't need compensation — we need idempotent retries, which we have.

**Direct workflow-to-workflow dependencies instead of Scheduler-mediated.** Rejected for reasons in the cascade section.

**No task queue segmentation.** Rejected; autoscaling is significantly harder with a single queue.

**Custom workflow engine (not Temporal).** Rejected in RFC 1. Re-raised briefly here because some teams will argue Temporal is overkill for simple pipelines. The reply: even simple pipelines benefit from durability, retry, and observability. Building those ourselves is work we should not repeat.

## Open Questions

1. **Exact soft cap for workflow history size.** Temporal Cloud's hard limit is 50K events. Our soft cap should leave a 2-3x safety margin. Proposed: 15K events triggers a warning, 25K events triggers `continue-as-new`. Confirm with operational data once available.
2. **Heartbeat granularity cost.** Every heartbeat is a Temporal RPC. At very high activity concurrency, heartbeat RPC volume matters. Tune per task queue; revisit in operational RFC.
3. **Rate-limiting design for `pipeline-external` queue.** Per-tenant rate limits are required; whether they're enforced by the worker (cooperative) or by a gateway is an open question. Lean cooperative for simplicity.
4. **Query cost at scale.** Customer dashboards that poll `status()` and `progress()` queries across hundreds of pipelines could generate significant query load. Consider caching layer in the Observability Service.

## References

- Temporal versioning guide: https://docs.temporal.io/workflows#workflow-versioning
- Temporal task queue documentation: https://docs.temporal.io/workers#task-queue
- Temporal activity best practices: https://docs.temporal.io/activities
- Prior art on idempotency patterns for data pipelines: Stripe's idempotency key design is a well-known reference.

## Decision

**Accepted pending review.** RFC 5 next: the Wasm runtime, sandboxing model, and host API — which defines what user-authored code can and cannot do inside our workers.
