# RFC 0013: Pipeline DSL and Configuration Language

- **Status:** Draft
- **Author:** TBD
- **Created:** 2026-04-21
- **Supersedes:** None
- **Related:** RFC 0006 (Connector Protocol), RFC 0009 (Destination Loaders), RFC 0010 (Catalog), RFC 0012 (Transformation Layer)

## Summary

This RFC specifies the user-facing configuration language: the YAML-based DSL for defining pipelines, connections, and transformations; the templating, referencing, and environment-overlay mechanisms; the relationship between the DSL and the underlying catalog API; and the tooling (CLI, validation, IDE integration) that makes the DSL usable in practice.

The DSL is not a new idea. Kubernetes YAML, Terraform HCL, dbt YAML, GitHub Actions — pattern well-established. Our innovation is not in inventing a format; it's in making the format align precisely with the architectural primitives of prior RFCs, so configuration reads like a description of what the platform actually does, not a separate layer requiring translation.

## Motivation

A data platform is used through its configuration surface more than any other touchpoint. Every pipeline change, every connector setup, every transformation edit goes through this interface. Done well, it becomes the thing teams version-control, review in PRs, and diff when debugging. Done poorly, it becomes a fossil — teams set it up once via a GUI and never touch it again, and the platform's power is inaccessible.

The specific failure modes we avoid:

1. **GUI-only configuration.** The UI is good for discovery; the source of truth is text. Without text-based config, GitOps workflows are impossible, code review is impossible, and the platform is operationally brittle.
2. **A DSL that doesn't match the mental model.** Fivetran's UI talks about "connectors" and "destinations"; their internal model has the same shape. A DSL where `pipeline.yaml` mentions concepts the UI calls different names is cognitive overhead that pays no rent.
3. **A DSL with too much magic.** DSLs that "figure out" what the user meant via heuristics work until they don't, and then they fail mysteriously. Explicit is better than clever.
4. **A DSL that duplicates imperative code.** YAML that fights its format by embedding Jinja loops, conditional blocks, and dynamic references becomes unreadable. If users need loops and conditionals, they should use a programming language to generate YAML, not embed a template engine.
5. **A DSL without a real API behind it.** DSL processing that happens server-side-only means we can't offer local validation, CI checks, or IDE support. The DSL must be a thin serialization layer over a real API.

We commit to: YAML source of truth, 1:1 correspondence with catalog entities, explicit references (no magic resolution), minimal templating (environment overlays only), and a CLI/validator that runs locally.

## Non-Goals

- This RFC does not specify the visual pipeline builder UI. That's product.
- This RFC does not specify the REST/gRPC API shape in detail. The API surface is implicit in the catalog model (RFC 10) and the DSL serializes to it; exact endpoint design is an implementation detail.
- This RFC does not mandate GitOps. We enable it; customers choose whether to adopt it.
- This RFC does not cover secret values in DSL (never present — references only, per RFC 11). This is summarized but not re-specified.

## Design Principles

**YAML, not our-own-DSL.** YAML is boring, widely supported, diffable, and has ecosystem tooling. We do not invent a syntax. HCL has merits (interpolation, cleaner multi-block files) but it adds a dependency and learning curve without proportional benefit.

**1:1 with catalog entities.** A `Pipeline` resource in YAML is a `Pipeline` entity in the catalog. The YAML field names match the catalog field names. Users reading the catalog schema (RFC 10) learn the DSL for free.

**Explicit references with `type:name` syntax.** No magic: if a pipeline refers to a connection, the reference is spelled out. We adopt `kind` + `name` the way Kubernetes does, not the way Terraform does (which infers types from variable paths).

**Environment overlays, not full templating.** We support overriding specific fields per environment (dev/staging/prod) via a well-defined overlay mechanism. We do not support if-conditions, for-loops, or function calls inside YAML. Customers who need those generate YAML programmatically with a real language.

**Validation before submission.** The CLI validates locally: schema-correct, references resolve, types match. No round-trip to our API is needed for basic validation. This is what makes the DSL usable in CI.

**Forward-compatible.** Adding fields to a resource kind is non-breaking. Removing or renaming is breaking and gets a major version bump of the resource kind. Old DSL files work against new platform versions within a deprecation window.

## Resource Model

Every DSL document declares one or more **resources**. A resource has:

- `apiVersion`: the resource-kind version. Starts at `platform/v1`.
- `kind`: one of the declared resource kinds (see below).
- `metadata`: identity and organizational metadata.
- `spec`: the resource's configuration.

This mirrors Kubernetes because the pattern works. It gives us easy multi-resource files, clean validation, and predictable tooling.

### Resource kinds (launch set)

- `Connection` — a reusable connection to an external system (RFC 10).
- `Pipeline` — a configured pipeline (RFC 10).
- `Transformation` — a transformation package reference for use in pipelines (RFC 12).
- `Secret` — a secret *reference* (not material) with access scoping (RFC 11).
- `Schedule` — a reusable schedule that pipelines reference.
- `Workspace` — workspace-level configuration.

Not resources:

- `Tenant` — managed by the platform, not by user config.
- `Run` — a result of execution, not a configured thing.
- `Schema` — derived from connector discovery, not user-declared.

### Resource file layout

Multiple resources per file, separated by YAML document markers:

```yaml
apiVersion: platform/v1
kind: Connection
metadata:
  name: prod-postgres
  workspace: analytics
spec:
  connector: platform/postgres@^2
  config:
    host: db.example.com
    port: 5432
    database: analytics_prod
  secrets:
    password: prod-postgres-password   # Secret name, resolved by ref
---
apiVersion: platform/v1
kind: Pipeline
metadata:
  name: orders-to-snowflake
  workspace: analytics
spec:
  source:
    connection: prod-postgres
    streams:
      - name: orders
        mode: incremental_cdc
      - name: customers
        mode: incremental_cursor
        cursor_field: updated_at
  destination:
    connection: prod-snowflake
    schema: RAW
  schedule:
    interval: 15m
  evolution_policy: propagate_additive
```

### Cross-file references

Resources reference each other by `kind + name` (scoped to workspace unless explicitly cross-workspace). File layout is a user choice: one resource per file, multiple per file, or whatever organizational shape the team prefers. The CLI processes a directory tree, picks up every YAML file, and treats the set as one configuration unit.

## Reference Resolution

How one resource points at another.

### Name-based references

Fields that take references are typed. In the `Pipeline.spec.source.connection` field, the value is the `metadata.name` of a `Connection` resource:

```yaml
source:
  connection: prod-postgres   # refers to Connection/prod-postgres
```

Reference resolution rules:

1. Same-workspace resolution is implicit. If the referring resource is in workspace `analytics` and the referenced `Connection` is also in `analytics`, the name alone suffices.
2. Cross-workspace references use `workspace/name` syntax: `connection: platform-shared/prod-postgres`. Requires permission (RFC 10's `UseScope` governs).
3. References are typed: a field declared as `ref: Connection` cannot point at a `Schedule`. The DSL schema (see below) enforces this.

### Secret references

Secret references are always by name, never by UUID in the DSL:

```yaml
spec:
  secrets:
    password: prod-postgres-password
```

The CLI, at validation time, resolves the name to a `SecretRef::Id` if the user has a live platform session. In offline validation (no session), unresolvable secret names produce warnings, not errors.

### Version constraints on connectors and transformations

Connectors and transformations are referenced with semver constraints:

```yaml
connector: platform/postgres@^2.0.0     # any 2.x >= 2.0.0
# or
connector: platform/postgres@2.3.1      # exact version
# or
connector: platform/postgres@latest      # discouraged; resolved at validation time
```

`latest` is explicitly discouraged (and the CLI warns) because it makes pipeline behavior depend on when you apply the config, not on the content of the config. Reproducibility requires pinned versions.

### What references do not do

- **No expressions.** A reference is a literal name, not an expression that evaluates to a name. No `${workspace}-postgres` kind of interpolation in reference fields.
- **No implicit types.** You cannot reference a `Connection` by just `prod-postgres` if context could mean something else. The field's declared type dictates kind; if ambiguous, the DSL schema disallows the field.

## Templating via Overlays (Minimal)

We provide one and only one templating mechanism: **environment overlays**. Everything else is a programmatic-generation concern, not a DSL concern.

### The overlay model

A base YAML file defines the full resource. An overlay file defines differences per environment:

```
config/
  base/
    connections.yaml
    pipelines.yaml
  overlays/
    dev/
      patches.yaml
    staging/
      patches.yaml
    prod/
      patches.yaml
```

Applying environment `prod` means: read `base/*.yaml`, apply `overlays/prod/patches.yaml`, produce the final resource set.

### Patch syntax

Patches are standard JSON Patch (RFC 6902) or strategic merge patch (Kubernetes-compatible). We default to strategic merge:

```yaml
# overlays/prod/patches.yaml
apiVersion: platform/v1
kind: Connection
metadata:
  name: prod-postgres
spec:
  config:
    host: db-prod.example.com   # override host
  secrets:
    password: prod-postgres-password-prod
---
apiVersion: platform/v1
kind: Pipeline
metadata:
  name: orders-to-snowflake
spec:
  schedule:
    interval: 5m   # prod runs more frequently than dev
```

The patch identifies the base resource by `kind + metadata.name` and replaces/merges fields. Strategic merge understands lists-with-merge-keys (e.g., streams by name).

### What overlays don't do

- **No environment-variable injection.** `${VAR}` syntax is not supported. Environment differences go in overlays, not in variable expansion.
- **No conditional blocks.** `if env == prod` does not exist. Either the field is in the overlay or it isn't.
- **No loops.** `for each table in [...]` does not exist. If you need N similar pipelines, generate them programmatically with a real language (Python, Go, etc.) and commit the generated YAML.

This feels restrictive and is deliberately so. Embedded templating languages produce YAML that can't be validated statically, can't be diffed meaningfully, and can't be comprehended without running the template engine. We trade convenience for readability.

### When overlays are enough

Most environment differences:

- Different connection hosts / credentials / database names.
- Different schedule cadences (dev hourly, prod 5-minute).
- Different evolution policies (dev propagate-all, prod strict).
- Different workspace targets.

These are all field-level overrides. Overlays handle them cleanly.

### When overlays aren't enough

"Generate 50 pipelines, one per customer, each with the same structure but different source tables." This is programmatic generation. Write a Python/Go/Rust script that emits YAML; commit the generated YAML; apply it.

We will provide example generators and a Rust library for YAML generation, but we will not add loop constructs to the DSL.

## The DSL Schema

Every resource kind has a schema, published alongside the DSL. Schemas are JSON Schema for compatibility with IDE tooling, plus platform-specific extensions for typed references.

### Schema location

Schemas are published at versioned URLs: `https://schemas.platform.com/v1/pipeline.json`, etc. IDE plugins and the CLI both fetch these.

### Schema content

Per resource kind, the schema specifies:

- Required and optional fields in `spec`.
- Types for every field (including typed references).
- Enumerated values where applicable.
- Default values.
- Deprecated fields (marked in schema).

Example (sketch for `Pipeline`):

```jsonschema
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Pipeline",
  "type": "object",
  "required": ["apiVersion", "kind", "metadata", "spec"],
  "properties": {
    "apiVersion": { "const": "platform/v1" },
    "kind": { "const": "Pipeline" },
    "metadata": { "$ref": "#/definitions/metadata" },
    "spec": {
      "type": "object",
      "required": ["source", "destination"],
      "properties": {
        "source": {
          "type": "object",
          "required": ["connection", "streams"],
          "properties": {
            "connection": { "type": "string", "x-platform-ref": "Connection" },
            "streams": {
              "type": "array",
              "items": { "$ref": "#/definitions/stream" }
            }
          }
        },
        // ...
      }
    }
  }
}
```

The `x-platform-ref` extension marks reference fields so IDE plugins provide autocomplete and the CLI validates resolution.

### Schema evolution

- Adding an optional field: non-breaking. Updates publish `platform/v1` schema.
- Adding a required field: breaking. New version `platform/v2`; `platform/v1` remains supported for the deprecation window.
- Changing field semantics: breaking; new version.
- Removing a field: breaking; new version.

A configuration file declares its target version via `apiVersion`. The CLI validates against the declared version. We support reading `platform/v1` for at least 24 months after `platform/v2` ships.

## CLI and Local Validation

The CLI (`platform-cli`, or whatever the final name is) is how users interact with their configuration.

### Core commands

- `platform validate <path>` — validates YAML against schemas, resolves references, checks for common errors. No network required unless verifying against remote catalog state.
- `platform diff <path>` — shows what would change if applied: additions, removals, modifications. Compares local YAML to catalog's current state.
- `platform apply <path>` — submits configuration to the platform. Creates / updates / deletes to match YAML.
- `platform get <kind> [name]` — retrieves resources from the platform as YAML.
- `platform export <workspace>` — dumps all resources in a workspace to YAML (for onboarding existing pipelines to GitOps).
- `platform preview <pipeline> [--sample-rows N]` — runs a transformation preview (RFC 12) against a small sample.
- `platform run <pipeline>` — triggers an immediate run of a pipeline.

### Validation layers

The CLI validates in layers, fastest first:

1. **Syntax.** YAML parses; resource documents conform to basic structure.
2. **Schema.** Each resource validates against its JSON Schema.
3. **Reference resolution.** All references point to resources that exist (either locally declared or known remotely).
4. **Semantic.** Per-resource-kind semantic rules (e.g., "CDC mode requires a PK declared on the stream").
5. **Compatibility.** (Optional, requires platform session.) The referenced connector version exists; the destination is reachable; etc.

Each layer can be run independently. CI uses layers 1-4 (no platform access needed); full `platform apply` runs 5 on the server.

### Validation error messages

Error messages are load-bearing. Bad errors produce support tickets; good errors produce self-service fixes. Discipline:

- Include the file and line.
- Name the exact field.
- Explain *why* it's wrong, not just that it's wrong.
- Suggest a fix when deterministic.

Example:

```
error: pipelines/orders.yaml:23
  Pipeline.spec.source.streams[0].mode = "incremental_cursor"
  but no cursor_field was specified.
  Either add `cursor_field: <field-name>` under this stream,
  or change `mode` to one of: full_refresh, incremental_cdc.
```

Not:

```
error: validation failed.
```

### Diff output

`platform diff` shows changes in a form close to `kubectl diff`: a unified diff of the YAML representation of each resource. Changed fields are highlighted. Resource-level additions and removals are marked clearly.

Breaking-change warnings are emitted prominently: changing a pipeline's `source.connection` is a breaking change that may require resnapshot; the diff flags it and requires an explicit `--confirm-breaking` flag to apply.

## IDE Integration

We ship a Language Server Protocol (LSP) server for the DSL. IDEs (VS Code, IntelliJ, Neovim, Emacs) with LSP support get:

- Autocomplete for field names, enum values, and references.
- Inline validation against schemas.
- Hover documentation on every field.
- Reference resolution: "go to definition" on a connection name.
- Rename refactoring: renaming a connection updates all pipelines referencing it.

This is not decoration. A platform whose configuration language feels like editing JSON in Notepad is a platform no one reaches for. IDE experience is part of the product.

A VS Code extension is the first-class reference implementation. Other editors get LSP configuration examples.

## Relationship to API

The DSL serializes to a catalog API; nothing in the DSL is privileged over direct API calls. Specifically:

- `platform apply` is an API-client operation: for each resource in the YAML, call the appropriate catalog API endpoint.
- The same resources can be created/modified via REST/gRPC directly (for programmatic integrations).
- Catalog state is the source of truth; YAML is just a serialization.

This matters for GitOps: third-party tools (Argo CD, Flux, etc.) can apply our resources because our resources are just HTTP calls to a documented API. We don't reinvent the controller pattern; we fit into what Kubernetes-adjacent ecosystems already do.

### API-only fields

Some fields exist in the catalog but not in the DSL:

- Internal IDs (`id`, version numbers).
- Derived state (`status`, run history).
- Computed timestamps (`created_at`, `updated_at`).

These are populated by the platform and returned from the API; users don't write them.

### DSL-only shortcuts

Some DSL constructs are conveniences that expand to multiple API operations. Example: a `Pipeline` with an inline transformation block creates a `TransformationPackage` and a `Pipeline` referencing it. The YAML is compact; the catalog has two entities. `platform export` re-serializes this as the compact form when possible.

## GitOps Integration

We enable, not mandate, GitOps.

### The GitOps loop

1. Team stores configuration YAML in a git repository.
2. Pull requests change the YAML. CI runs `platform validate` + `platform diff --against=prod`.
3. Merge triggers `platform apply --environment=prod` from CI.
4. Platform state converges to YAML state.

Supported by:

- **Idempotent apply.** Applying the same YAML twice produces no changes the second time.
- **Deterministic diffing.** `platform diff` output is stable across repeated runs with the same input.
- **Breaking-change guards.** As noted, breaking changes require explicit confirmation. In CI, a special `--confirm-breaking` flag must be set, typically via a PR label or annotation.

### What we don't do

- **No built-in git polling.** We don't run a controller that watches your git repo. Argo CD and Flux already do this; we integrate with them rather than reinvent them.
- **No auto-apply from git.** The customer's CI decides when to apply. We provide the tools; the workflow is theirs.

## Drift and Reconciliation

When YAML and catalog state disagree.

### Detection

`platform diff` shows drift. Common sources:

- Someone changed a resource via the UI after it was applied from YAML.
- A `Connection` was edited via the API directly (credential rotation).
- Catalog auto-populated fields that overlays don't override.

### Resolution policies

Per-resource, configurable in metadata:

- `drift: reject` — `apply` fails if the catalog state differs from what YAML-last-applied produced. Forces reconciliation before apply.
- `drift: overwrite` (default) — `apply` overwrites the catalog state with YAML state.
- `drift: merge` — specific fields preserved on the catalog side (annotated via `x-drift-preserve`), others overwritten.

For GitOps teams, `drift: reject` is preferred. Ad-hoc teams default to `overwrite`.

### "Reconcile from platform" workflow

`platform export` dumps current catalog state to YAML. Teams migrating from UI-based configuration to GitOps use this to bootstrap their repository from existing resources.

## Inline vs. Referenced Transformations

A pipeline can have transformations inline in its YAML or reference a separate `Transformation` resource.

### Inline (for simple cases)

```yaml
kind: Pipeline
# ...
spec:
  transformation:
    inline:
      - operator: filter
        predicate: "status != 'deleted'"
      - operator: mask
        fields:
          email: { strategy: hash(sha256) }
```

Inline transformations are convenient for pipeline-specific logic. They are stored in the catalog as an anonymous `TransformationPackage` scoped to the pipeline.

### Referenced (for shared logic)

```yaml
kind: Transformation
metadata:
  name: pii-masking
spec:
  operators:
    - operator: mask
      fields:
        email: { strategy: hash(sha256) }
        phone: { strategy: redact(keep_last=4) }
---
kind: Pipeline
# ...
spec:
  transformation:
    ref: pii-masking
    config:
      # parameter overrides for pii-masking if it's parameterized
```

Referenced transformations are reusable across pipelines, versioned independently, and sharable across workspaces (subject to permissions).

The DSL supports both cleanly; inline is the starter pattern, referenced is the maturation pattern.

## Validation of Configuration Correctness

Beyond schema validation, the CLI runs semantic checks:

- A stream declared as `incremental_cursor` must have `cursor_field`.
- A stream declared as `incremental_cdc` must be on a connector that supports CDC (checked against the connector's `describe` output).
- A pipeline's transformation's input schema requirements must be compatible with its source's declared output schema (where statically derivable).
- Destination schema constraints (primary key declared, partition key specified for partitioned destinations) are satisfied.
- Scheduling constraints (can't have both `interval` and `cron`, can't have `trigger_from_pipeline` pointing at itself).

These checks run during `validate` and `apply`. They do not require platform connectivity for the static parts; the connector-capability checks require a recent catalog cache (fetched lazily).

## Examples

The RFC would be incomplete without showing what real configurations look like. Three sketches:

### Minimal: Postgres to Snowflake, full refresh

```yaml
apiVersion: platform/v1
kind: Connection
metadata:
  name: src-pg
spec:
  connector: platform/postgres@^2
  config: { host: db.example.com, database: app }
  secrets: { password: src-pg-password }
---
apiVersion: platform/v1
kind: Connection
metadata:
  name: dst-snowflake
spec:
  connector: platform/snowflake@^1
  config: { account: acme.us-east-1, database: RAW, warehouse: LOADER_WH }
  secrets: { password: dst-snowflake-password }
---
apiVersion: platform/v1
kind: Pipeline
metadata:
  name: app-to-warehouse
spec:
  source:
    connection: src-pg
    streams:
      - { name: users, mode: full_refresh }
      - { name: orders, mode: full_refresh }
  destination:
    connection: dst-snowflake
    schema: APP_RAW
  schedule:
    interval: 1h
```

### Realistic: CDC with transformation

```yaml
apiVersion: platform/v1
kind: Transformation
metadata:
  name: pii-scrub
spec:
  operators:
    - operator: mask
      fields:
        email: { strategy: hash(sha256, salt: "pii-scrub-v1") }
        phone: { strategy: redact(keep_last=4) }
        ssn: { strategy: remove }
---
apiVersion: platform/v1
kind: Pipeline
metadata:
  name: users-cdc-to-bq
spec:
  source:
    connection: src-pg
    streams:
      - name: users
        mode: incremental_cdc
        primary_key: [id]
  transformation:
    ref: pii-scrub
  destination:
    connection: dst-bigquery
    dataset: app_cdc
  evolution_policy: propagate_additive
```

### With overlays (environment-specific)

```yaml
# overlays/prod/patches.yaml
apiVersion: platform/v1
kind: Connection
metadata: { name: src-pg }
spec:
  config: { host: db-prod.internal, database: app_prod }
  secrets: { password: src-pg-password-prod }
---
apiVersion: platform/v1
kind: Pipeline
metadata: { name: users-cdc-to-bq }
spec:
  schedule:
    interval: 5m
  evolution_policy: strict
```

Apply to prod: `platform apply ./config --env prod`.

## Alternatives Considered

**Adopt HCL (Terraform's language) instead of YAML.** Cleaner syntax for references, builtin interpolation, better multi-block files. Rejected: adopting HCL brings the Terraform provider pattern and its operational model with it. Users expect `terraform plan/apply`; we'd need to either be a Terraform provider (good, and we should provide one as a supplementary interface) or reinvent Terraform's ecosystem. YAML + CLI is lower cognitive load.

**Adopt Python or TypeScript as the primary config language.** Pulumi-style. Powerful, but every real config becomes a small program. Hard to review, hard to validate statically, hard to explain in docs. Rejected. (We will likely provide a Python library for YAML generation, which is different.)

**Adopt Kubernetes CRDs and run as a Kubernetes controller.** Users already familiar with kubectl could manage pipelines with it. Rejected as the primary model: not every customer runs Kubernetes, and tying pipeline configuration to a K8s cluster's lifecycle creates unnecessary coupling. A Kubernetes operator for our API (as a supplementary integration) is a good idea and we'll probably build one post-launch.

**Embed full Jinja2 templating.** Familiar to many users (Airflow, Ansible). Rejected for reasons articulated above: templated YAML is static-analysis-hostile and produces bugs we can't catch at CI time.

**Support Kustomize-style layering without patches.** Kustomize uses bases and overlays via directory structure, not file-level patches. We considered fully adopting the Kustomize model but our overlay needs are simpler (single-level, not nested). A subset of Kustomize's patch semantics (strategic merge) is what we need.

**One file per resource vs. multi-resource files.** We allow both; we don't mandate either. Some teams prefer one-file-per-resource for clarity; others prefer grouping related resources.

**Environment variables in YAML via `$ENV_VAR` substitution.** Rejected: breaks local validation (variables aren't present when validating in isolation), encourages secret-value interpolation (bad), and conflicts with overlays. Overlays do this job cleanly; environment variables should not appear in the config at all.

## Open Questions

1. **JSON as an alternate surface.** Some teams prefer JSON for tooling reasons. The catalog API is JSON; we could accept JSON-formatted resources directly. Low-cost to add. Punt.
2. **Anchor / alias support.** YAML's native `&anchor` / `*alias` syntax lets users avoid repetition within a file. We lean toward supporting it (it's YAML-native; nothing to build) but will document best practices conservatively.
3. **Schema fetching in air-gapped environments.** On-prem customers may not have internet access during validation. We ship schemas as a versioned package that can be vendored.
4. **Permission-scoped validation.** The CLI should warn if a user attempts to write to workspaces/resources they don't have permission for. Requires platform session; low priority but a real UX win.
5. **DSL version migration tool.** When `platform/v2` ships, a `platform migrate` command that converts `v1` YAML to `v2` helps upgrades. Design after we have experience with the first breaking change.
6. **Multi-tenant DSL.** A single YAML file declaring resources across multiple tenants (for consultants / platform teams managing many customer tenants). Possible with tenant-scoped metadata; needs thinking through.

## References

- Kubernetes YAML (primary reference for the resource model).
- Kustomize (reference for overlays): https://kustomize.io/
- dbt project configuration (prior art for data-platform config DSL).
- Airflow DAG configuration (the anti-pattern of Python-as-config we're avoiding as primary surface).
- Terraform HCL (considered-and-rejected alternative): https://www.terraform.io/language
- JSON Patch RFC 6902, JSON Merge Patch RFC 7396.
- LSP specification: https://microsoft.github.io/language-server-protocol/

## Decision

**Accepted pending review.** RFC 14 next: State Storage Architecture — which pulls together where every kind of state lives (workflow state, staging, catalog, metrics, audit), cleaning up the implicit answers scattered across prior RFCs.
