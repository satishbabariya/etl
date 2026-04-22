# RFC 0005: Wasm Runtime, Sandboxing, and Host API

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0002 (Core Architecture), RFC 0003 (Data Interchange), RFC 0004 (Temporal Topology)

## Summary

This RFC specifies the WebAssembly runtime that hosts user-authored code inside workers: which runtime implementation we use, how modules are loaded and isolated, the exact host API surface exposed to guest modules, resource limits, determinism guarantees, and the lifecycle of a wasm execution within a Temporal activity.

This RFC is the contract between the platform and everyone who will ever write code that runs on it. Changing this contract after launch is expensive — guest modules will pin to host API versions, and breaking them means breaking customers. We invest carefully here.

## Motivation

Wasm's role in this platform is load-bearing. It is:

1. **The extensibility mechanism.** Users author connectors and transformations as wasm components. This is the developer-facing surface, and it's a primary differentiator against Fivetran (whose connector authorship requires writing Java and going through their review) and Airbyte (whose connector runtime is subprocess-based and has per-invocation overhead).
2. **The isolation boundary.** Untrusted user code runs in wasm; the host Rust process is trusted. This is what lets us execute customer-authored transformations in a multi-tenant data plane without container-per-invocation cost.
3. **The cross-language story.** Any language that compiles to wasm can author code for our platform. That means Rust, TinyGo, AssemblyScript, and — via component adapters — C, C++, Zig, and increasingly JavaScript and Python.

The three decisions that matter most and are worth designing deliberately:

- **Which runtime.** Wasmtime is the obvious choice and we adopt it, but with specific reasons and configuration.
- **Which ABI layer.** Raw wasm, WASI preview 1, WASI 0.2 + Component Model, or something custom. This choice determines how much machinery we own.
- **The host API surface.** What capabilities do guests get? Every capability is forever.

Get these right and we have a polished extensibility story that competitors can't match without years of work. Get them wrong and we spend the next five years with a painful API we can't change.

## Non-Goals

- This RFC does not cover the connector protocol or the transformation protocol at the interface level. Those are RFC 6 and RFC 12 respectively. This RFC specifies the *mechanism* by which guest code runs and talks to the host; the *specific interfaces* for connectors and transformations are defined in those RFCs and consume this RFC's host API.
- This RFC does not cover the connector registry, packaging, or distribution. That's a later RFC (provisionally "Connector registry and packaging").
- This RFC does not benchmark specific wasm runtimes against each other. We adopt wasmtime based on ecosystem consolidation; detailed benchmarking against Wasmer / WasmEdge / WAVM is out of scope because none of them offer advantages that justify the ecosystem cost.
- This RFC does not cover non-wasm sandboxing options (containers, gVisor, V8 isolates). Those were rejected in RFC 1 and RFC 2.

## Runtime: Wasmtime

We embed **wasmtime** (the Bytecode Alliance implementation) in every worker process. Specific version commitments:

- **Wasmtime 25+** at project start, with a policy of tracking the current long-term-support branch. Wasmtime's release cadence is monthly; we pin on a supported LTS branch and update on a quarterly cadence unless a security advisory forces faster movement.
- **Cranelift** as the compiler backend (wasmtime's default). We do not adopt the single-pass "Winch" compiler — it's faster to compile but produces slower code, and our guest modules run repeatedly enough that JIT-compile cost amortizes quickly.
- **AOT compilation** for guest modules. Modules are compiled to wasmtime's `.cwasm` format once at publish time (in the connector registry) and loaded as pre-compiled artifacts by workers. This eliminates per-invocation JIT cost entirely.

### Why wasmtime, specifically

The wasm runtime space consolidated in 2024–2025. Wasmtime has:

- The reference implementation of the Component Model and WASI 0.2.
- Production deployment at Fastly, Shopify, Cloudflare-adjacent projects, and most of the serverless-wasm ecosystem.
- A Rust-native API that composes cleanly with our Rust worker code.
- Mature epoch-based interruption (how we implement execution deadlines) and fuel-based metering (how we implement CPU accounting).
- A security track record: multiple years in production, CVEs handled professionally, regular third-party audits.

Wasmer and WasmEdge are competent runtimes with narrower deployment footprints. We do not pick a fight with the ecosystem majority for no meaningful gain.

## ABI: Component Model + WASI 0.2

Guest modules are **wasm components**, not core modules. This is a deliberate choice with significant implications.

### Why components

The Component Model (standardized as WASI Preview 2 / "WASI 0.2") provides:

- **Structured types across the host-guest boundary.** Records, variants, lists, options, results — not just i32s and pointers. This is the single biggest authorial ergonomics win. Authors write `fn read(cursor: Cursor) -> Result<Batch, ReadError>` in their language of choice, not `extern "C" fn read(cursor_ptr: i32, cursor_len: i32, out_ptr: i32) -> i32`.
- **Interface typing via WIT.** Interfaces are declared in a canonical IDL (WIT — WebAssembly Interface Types) that has bindings generators for every serious target language. We publish `.wit` files; authors import them and get idiomatic bindings in their language.
- **Composition.** Components can import and export interfaces from other components. We don't need this on day one but we might on day three, and locking into core modules closes that door.
- **Linear memory isolation per component instance.** Each instance has its own memory; there is no global shared memory across instances. This is a security property we want.

### What we give up

The Component Model is younger than core wasm. Consequences:

- Language support is uneven. Rust and JavaScript/TypeScript (via jco) are first-class. Go via TinyGo is solid. Python via componentize-py works but is still maturing. C/C++ via wasi-sdk works. Languages without WASI 0.2 toolchain support (Java, C#) are effectively excluded from authoring — they'll compile, but the ergonomics are poor.
- Runtime overhead is marginally higher than raw core wasm for small function calls. This cost is negligible for our call pattern (batch-granularity calls on Arrow data, not row-granularity).
- Some wasm tooling (debuggers, profilers) still targets core modules primarily. The ecosystem is catching up quickly.

We accept these tradeoffs because authorial ergonomics is a first-class concern for our adoption story. A connector developer who has to hand-write FFI glue will not finish their connector.

### Version policy

We commit to WASI 0.2 at launch. When WASI 0.3 ships (async in the Component Model, currently under development), we evaluate adoption. The version of WASI a component targets is part of its manifest; workers support multiple WASI versions concurrently during migration windows.

## Execution Lifecycle

A wasm component's lifecycle inside a worker follows strict rules. This section is the operational spec.

### Load

Modules are loaded by **manifest reference**, not by inline bytes in workflow state. When an activity needs to invoke a user connector or transformation:

1. The activity receives a `ModuleRef` (catalog-assigned identifier + version).
2. The worker's **module cache** checks for a loaded `Instance` for this `ModuleRef`. If present, reuse it (subject to freshness and concurrency rules below).
3. If absent, the worker fetches the AOT-compiled artifact from the connector registry (cached on local disk after first fetch).
4. The worker instantiates the component with a new `Store`, passing host-provided imports.
5. The instance enters the active pool.

AOT compilation makes step 3 fast (tens of milliseconds for typical connectors) and step 4 near-instant. Cold-start for a never-before-seen module is dominated by disk fetch.

### Instance pool and lifetime

Instances are **not** one-per-invocation. A loaded instance may service many batch invocations in sequence. This matters for performance: initializing a guest's internal state (parsed config, HTTP client pools inside the guest, etc.) is amortized across invocations.

Rules:

- Each `Store` in wasmtime is single-threaded. Parallelism is achieved by instantiating N stores for the same module and round-robining invocations.
- An instance is **bound to a single tenant and a single pipeline** for its lifetime. We do not share instances across tenants, ever. This is a security invariant: one tenant's configured-state on the guest must never leak to another tenant.
- Instances are retired after (whichever hits first): 1 hour wall time, 10,000 invocations, 256 MB peak linear memory, or an explicit worker-initiated retirement (e.g., policy rotation, module upgrade).
- Retirement is graceful: outstanding invocations complete, then the instance's resources are freed.

### Invocation

An activity invokes a guest function like this:

1. Obtain an instance from the pool (or instantiate).
2. Set invocation limits: epoch deadline (for timeout), fuel budget (for CPU), memory high-water mark (for OOM detection).
3. Pass arguments: for data-carrying calls, transfer an Arrow `RecordBatch` reference (see "Data transfer" below).
4. Call the component function.
5. Collect results, including outbound host calls that occurred during execution (telemetry events, etc.).
6. Return instance to pool.

Total overhead for a well-tuned invocation on a cached instance: target <200 µs for control-path calls, <2 ms plus Arrow serialization for data-path calls. Performance targets in RFC 3 assume these numbers.

### Data transfer across the boundary

This is the most performance-critical aspect of the entire RFC.

The naive approach — serialize the RecordBatch to Arrow IPC bytes, copy into guest linear memory, have the guest parse it — works and gives us cross-language support, but the copy dominates at large batch sizes. We use a tiered strategy:

**Tier 1: Arrow IPC over linear memory (default, cross-language).** The host serializes the RecordBatch to Arrow IPC (streaming format, single batch) and writes into a guest-allocated buffer. The guest uses its language's Arrow library to read. This works for every language with an Arrow binding. Overhead is a memory copy, not a parse — IPC is designed for zero-copy access, and the only real cost is moving bytes into the sandbox's linear memory.

**Tier 2: Shared memory via `memory-sharing` extension (Rust guests, optional).** For performance-critical Rust connectors and transformations, we support a wasmtime-specific (but standards-track) shared-memory extension where the guest can read host-owned Arrow buffers without copy. This requires careful lifetime management (the host must keep the buffers alive for the duration of the call and ensure no writes during read). Available only for trusted tier or internal first-party code, because bugs in this path can cause memory-safety violations that defeat the sandbox. We do not enable this for general user-authored guests.

**Tier 3: Streaming interface for very large batches.** For batches that exceed a threshold (default 64 MB), the host exposes a streaming API: the guest pulls records/slices from a host-owned iterator rather than receiving the whole batch at once. This is used primarily by loader transforms and large-batch transformations. It avoids blowing up linear memory when the default batch size is unusually large.

Tier 1 is the default and covers the vast majority of cases. Tiers 2 and 3 are performance escapes for specific situations.

## Resource Limits and Accounting

Every wasm invocation operates within declared limits. Limits are enforced, not advisory.

### CPU

**Fuel-based metering.** Wasmtime's fuel mechanism increments a counter per unit of wasm work; when the counter exceeds a budget, execution traps. Budgets are per-invocation, declared in the connector/transformation manifest, and capped by platform policy.

Default budget: 30 seconds of wall-clock-equivalent CPU per invocation, with a platform hard cap of 5 minutes. An invocation exceeding its budget results in a typed error the host returns to the activity, which is retriable or non-retriable per activity policy (RFC 4).

### Memory

**Linear memory cap per instance.** Enforced by wasmtime's memory limits. Default 256 MB, configurable up to a platform cap of 4 GB. An instance that attempts to grow beyond its cap traps.

We also enforce a **worker-level aggregate memory cap** across all instances: the worker will not instantiate additional guests if total wasm-committed memory would exceed 60% of worker RAM. This prevents a pipeline with many instances from starving the host runtime.

### Wall time

**Epoch-based interruption.** Wasmtime increments an epoch counter on a host-controlled schedule; the guest yields on every epoch check. We set epoch ticks at 1 ms intervals, so wall-time cutoffs are accurate to ~1 ms. Default wall-time cap is 60 seconds per invocation; hard cap is 15 minutes.

Wall time can exceed CPU time when the guest is blocked on host imports (e.g., an HTTP fetch). Both are capped independently.

### I/O

- **Host-mediated HTTP:** per-invocation rate caps, per-tenant rate caps. See Host API below.
- **No direct filesystem access** for user-authored code. File-like access is mediated through specific host APIs (e.g., streaming batch input).
- **No direct network sockets.** All network goes through host APIs that understand connections, timeouts, retries, and tenant rate limits.

### What happens on limit violation

Every limit, when violated, results in a typed error returned to the host activity. The activity decides whether to retry (per RFC 4 retry policy), and the error is recorded in run metadata with the specific limit that was hit. This is important for debuggability: "your connector timed out" is less useful than "your connector consumed 30s of CPU fuel on batch #47."

## Determinism Guarantees

User-authored transformations must be **deterministic** — same input produces same output. This is a platform invariant required by RFC 4 (activity idempotency). We enforce it by denying access to non-deterministic host APIs in transformation contexts.

### What is denied in transformation context

- **Wall clock time.** Guests cannot read the current time directly. If they need a time stamp, the host passes it as an input derived from the batch or run context.
- **Random number generation.** No access to randomness. Guests that need randomness for e.g., sampling must seed from a host-provided deterministic seed.
- **Network I/O.** Transformations cannot make outbound network calls. (Connectors can, because connectors are the I/O layer; transformations are pure data functions.)
- **Environment variables, process info.** Not accessible.
- **Thread IDs, process IDs.** Not accessible.

### What is allowed in transformation context

- Arithmetic, collections, strings, everything in the wasm instruction set and the guest language's standard library that doesn't hit the above.
- Logging through the host `log` API. Log statements do not affect output determinism because log output is not part of the batch data — it's a side channel for observability.
- **Lookup tables and reference data** provided by the host as part of the invocation input. (Not by guests doing their own fetches.)

### Connector context differs

Connectors are, by their nature, non-deterministic — they read from external systems. The above denials apply only in the transformation context. Connector context permits time, randomness, and network I/O through the host API. The host API is the same set of interfaces; we enable or disable specific capabilities at instance creation based on the context.

### Enforcement

We do not solely rely on guest-side discipline. The host-side capability object passed to the instance at creation has explicit permission bits. A transformation instance gets a capability object with `network=false`, `time=false`, `random=false`. When the guest attempts to call a denied interface, the host returns a permission-denied error rather than executing the call. This is checked by the host, not by the guest.

## Host API

This is the contract. Everything a guest can do, it does through this API.

All interfaces are defined in WIT. We publish the `.wit` files versioned as part of the platform SDK. The following sections describe the interfaces by role, with WIT sketches.

### Core: logging, error reporting, progress

Every guest has access to:

```
interface platform:core/log {
  record log-record {
    level: level,
    message: string,
    fields: list<tuple<string, string>>,
  }
  enum level { trace, debug, info, warn, error }
  emit: func(record: log-record)
}

interface platform:core/progress {
  report: func(units-processed: u64, cursor-hint: option<string>)
}

interface platform:core/errors {
  record structured-error {
    kind: error-kind,
    message: string,
    retriable: bool,
    details: list<tuple<string, string>>,
  }
  variant error-kind {
    transient,
    rate-limited(option<u32>),  // retry-after seconds
    auth-failed,
    not-found,
    bad-request,
    server-error,
    schema-mismatch,
    user-error,
  }
}
```

These interfaces are universal — connectors, transformations, and future roles all use them.

### Data: Arrow batch I/O

```
interface platform:data/batches {
  resource batch-reader {
    read-next: func() -> result<option<batch>, read-error>
  }
  resource batch-writer {
    write: func(b: batch) -> result<_, write-error>
    finish: func() -> result<batch-ref, write-error>
  }
  record batch {
    schema-fingerprint: string,
    ipc-bytes: list<u8>,   // Arrow IPC streaming single-batch
  }
  record batch-ref {
    uri: string,
    schema-fingerprint: string,
    row-count: u64,
    byte-count: u64,
  }
}
```

The `batch` record carries Arrow IPC bytes across the boundary. This is the Tier 1 transfer path (see above). Tier 2 (shared memory) uses a different interface reserved for trusted code. Tier 3 (streaming) uses the `batch-reader` resource for pulling slices rather than whole batches.

### Capabilities for connectors only

Connectors get additional interfaces. These are *imported* by the guest, and the host provides them only when the invocation context is a connector.

```
interface platform:net/http {
  record request {
    method: method,
    url: string,
    headers: list<tuple<string, string>>,
    body: option<list<u8>>,
    timeout-ms: option<u32>,
  }
  record response {
    status: u16,
    headers: list<tuple<string, string>>,
    body: list<u8>,
  }
  variant method { get, post, put, patch, delete, head, options }
  send: func(req: request) -> result<response, http-error>
}

interface platform:secrets/access {
  // Reference is opaque; guest cannot enumerate, only request-by-ref.
  get: func(secret-ref: string) -> result<string, secrets-error>
}

interface platform:state/cursor {
  // Small-state KV scoped to (tenant, pipeline, stream).
  // Used for connector-local state that doesn't fit in Temporal workflow state.
  get: func(key: string) -> option<list<u8>>
  put: func(key: string, value: list<u8>) -> result<_, state-error>
  delete: func(key: string) -> result<_, state-error>
}

interface platform:time/clock {
  now-utc-micros: func() -> u64
}

interface platform:crypto/random {
  fill: func(len: u32) -> list<u8>
}
```

Notes:

- `platform:net/http` is the only way guests reach external networks. All requests flow through the host, which applies per-tenant rate limiting (RFC 16), connection pooling, DNS resolution with a denylist (no internal IPs), and per-request logging.
- `platform:secrets/access` takes an opaque reference, not a secret name. References are allocated by the host when activity is set up; the guest cannot request secrets it hasn't been granted.
- `platform:state/cursor` is for connector-local state that is too large or too frequently-updated to live in Temporal workflow state (which is visible to the workflow anyway). It is backed by the catalog's state store, scoped per-stream. Hard-capped at 1 MB per stream.

### Capabilities for transformations only

Transformations get only the core and data interfaces. No network, no secrets, no state, no time, no random. Pure functions over batches.

```
// Transformations import only:
//   platform:core/log, platform:core/progress, platform:core/errors
//   platform:data/batches
// and export the transformation interface defined in RFC 12.
```

This is the determinism enforcement mechanism: transformations literally cannot call non-deterministic things because the host doesn't link those interfaces into the instance.

### Capabilities we are explicitly *not* providing

The negative space matters. We do not provide:

- **Filesystem access.** WASI filesystem is not linked. No temp files, no directories. If a guest needs scratch space, it uses linear memory.
- **Sockets / raw TCP / UDP.** Only HTTP, through the host.
- **Spawning threads / tasks.** Guests are single-threaded within a Store. Parallelism is at the store level, controlled by the host.
- **Subprocess execution.** No.
- **Reading host environment variables.** Explicit configuration is passed in through the activity input.
- **Access to other guests.** Instances are isolated. No cross-instance communication inside the worker.

We will get requests for several of these ("let me cache to /tmp"; "let me spawn a worker thread"). The answer is no unless the feature is added via RFC amendment after careful review.

## Versioning the Host API

The host API will evolve. Guest modules published against version N must keep working when the host upgrades to version N+1.

### Versioning scheme

Each interface has a semver version: `platform:net/http@0.2.1`. Guests declare which versions they import. The host supports multiple versions concurrently during migration windows.

### Compatibility rules

- **Adding interfaces:** never a breaking change. New interfaces are optional imports.
- **Adding fields to records with defaults:** non-breaking within a major version. We use WIT's upcoming optional fields / default values semantics. (Today this requires emitting a new interface version.)
- **Removing or renaming:** always a breaking change. Requires new major version.
- **Changing semantics without changing signature:** forbidden. If behavior changes, the interface changes.

### Deprecation policy

An interface version is deprecated with at least 12 months notice before removal. Guests pinned to deprecated versions receive warnings at deploy time and periodic nags in run metadata, but they keep working until the deprecation deadline.

### Practical deploy story

Workers load a guest module's manifest, which declares the versions it imports. The worker links each declared version from its supported set. If a module declares a version the worker does not support, instantiation fails with a clear error. Operators update worker deployments; guest authors update their modules at their own pace within the support window.

## Security Considerations

### Sandbox guarantees we rely on

- **Memory isolation.** Wasmtime enforces linear memory boundaries. A guest cannot read or write outside its own memory.
- **Capability-based I/O.** Guests have no ambient authority. Every effect goes through a capability we chose to grant.
- **Deterministic wasm execution.** Wasmtime's execution is well-specified; guest behavior is a function of input + host responses + internal state.

### Sandbox guarantees we do *not* rely on

- **Side-channel isolation.** Guests can probably measure timing side channels. We do not consider the wasm sandbox to protect against a hostile co-tenant learning secrets through timing. Multi-tenant isolation at that level is achieved by tenant-separated workers (RFC 16), not by wasm alone.
- **Spectre/Meltdown class attacks.** Wasmtime applies reasonable mitigations; we do not claim the sandbox is bulletproof against CPU speculation attacks. High-security workloads should request single-tenant workers.
- **Denial-of-service via pathological input.** Resource limits bound the damage but cannot prevent a guest from exhausting its budget on bad input. Activity retry + error reporting handles this at the platform level.

### Guest authentication and publication trust

Guest modules are signed at publish time by the author's platform identity. Workers verify signatures before loading. Details of the signing and registry model are a later RFC.

### Supply chain

Every module declares its source language toolchain, wasm compiler, and any bundled dependencies in its manifest. We do not scan wasm bytecode for malicious patterns (a known-hard problem); we rely on signatures, sandbox enforcement, and resource limits. For first-party modules, we apply standard supply-chain practices (lockfiles, reproducible builds, audit).

## Testing and Development Experience

Good developer experience here is not a nicety; it is the actual product for connector authors.

### Local development

We ship a **wasm host SDK** in Rust that embeds wasmtime with the same host API the production workers use. Authors develop against this SDK locally:

- Run their module against a local mock source/destination.
- Observe logs and metrics locally.
- Exercise error and retry paths.

The local SDK is the same code as the worker's wasm runtime, stripped of platform integrations. This means "works locally, breaks in production" is structurally unlikely.

### Testing support

Authors get a test harness for each role:

- **Connector test harness.** Feeds a module recorded HTTP responses and asserts on produced batches.
- **Transformation test harness.** Feeds a module input batches and asserts on output batches.

### Language SDK ergonomics

For the launch languages (Rust, TypeScript, Go, Python), we ship language-specific SDKs that wrap the generated WIT bindings with idiomatic APIs. Authors do not write to raw WIT bindings — those are an implementation detail underneath the language SDK.

## Performance Budget

Targets, reprising and refining RFC 3's performance section:

- **Cold-start instantiation (AOT-compiled module, cached on disk):** <50 ms P99.
- **Warm invocation overhead (control path, no data):** <200 µs P99.
- **Warm invocation overhead (data path, 16 MB batch, Tier 1 transfer):** <8 ms for boundary work (excluding guest's actual computation).
- **Worker aggregate guest memory:** up to 60% of worker RAM.
- **Module cache hit rate:** >95% in steady state.

If these targets cannot be met, the wasm story loses much of its competitive edge over container-per-invocation approaches. They are not aspirational — they are achievable today with wasmtime and careful host-side design.

## Alternatives Considered

**Use WASI Preview 1 instead of the Component Model.** Mature, broader language support. Rejected because Preview 1's ABI is primitive (everything is i32 pointers into memory) and the authorial ergonomics are poor. The Component Model is the future of wasm on the server and adopting it early is worth the tooling immaturity cost.

**Use core wasm with custom ABI.** We'd own every binding-generation decision. Rejected: we would spend years reinventing WIT poorly. WIT exists, works, and has momentum.

**Use Wasmer instead of wasmtime.** Credible runtime. Rejected: ecosystem gravity is with wasmtime, Bytecode Alliance drives the standards, and Wasmer's distinguishing features (wasm-native package manager) are not relevant to our use case.

**Embed V8 for JavaScript guest code.** Tempting because JS has broad reach. Rejected: adding a second runtime doubles the sandboxing and security surface. Authors who want JavaScript can target wasm via jco (JavaScript component tooling), which produces wasm components. Our runtime stays wasm-only.

**Run each invocation in a fresh instance (no pool).** Simpler isolation story; no cross-invocation state risk. Rejected: instantiation cost (even with AOT) is not free, and connectors benefit significantly from reusing HTTP client pools and parsed config across invocations. Our pooling + per-instance tenant binding preserves isolation.

**Use containers instead of wasm for user code.** Revisited from RFC 1. Rejected again: container-per-invocation is 100-1000x slower to start than wasm instantiation, container sandbox is weaker, and the cross-language story via language-specific containers is messier than wasm's compile-to-target model.

## Open Questions

1. **Shared-memory tier availability.** Tier 2 (shared memory, no copy) is flagged as trusted-only. Is there a clean path to offer this to user code under stronger attestation (e.g., signed modules from verified publishers)? Defer; revisit after measuring Tier 1 overhead in production.
2. **Async in guest code.** WASI 0.3 will standardize async in the Component Model. Some guests (high-throughput HTTP connectors) would benefit from guest-side concurrency. Adopt when WASI 0.3 ships and major languages support it.
3. **Hot-patching running instances.** If a module ships a security fix, we want running instances of the old version to be retired quickly. Policy TBD — likely "drain within 15 minutes of advisory, force-retire within 1 hour."
4. **Cross-instance state for stateful connectors.** `platform:state/cursor` is per-stream KV. Some connectors want more (e.g., a schema cache shared across streams of the same connection). Consider a second state interface for connection-scoped state. Defer to connector-registry RFC.
5. **GPU access.** Some transformations (vector embeddings, ML inference) would benefit. Wasm has no standard GPU story yet; WebGPU on the server is early. Not on the roadmap; may revisit when adjacent work matures.

## References

- Wasmtime: https://wasmtime.dev/
- WASI 0.2 / Component Model: https://component-model.bytecodealliance.org/
- WIT language reference: https://component-model.bytecodealliance.org/design/wit.html
- jco (JavaScript component tooling): https://github.com/bytecodealliance/jco
- componentize-py: https://github.com/bytecodealliance/componentize-py
- TinyGo wasi support: https://tinygo.org/docs/guides/webassembly/wasi/
- Fastly's production use of wasmtime: Compute@Edge architecture writeups.
- Shopify's production use of wasmtime: Shop Functions architecture writeups.

## Decision

**Accepted pending review.** RFC 6 next: the Connector Protocol — the WIT interface that connectors implement on top of the host API defined here.
