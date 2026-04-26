mod auditlog;
mod auth;
mod auth_client;
mod dsl;
mod secret;
mod status;
mod tenant;
mod terminate;

use anyhow::Context;
use catalog::{Catalog, NewRun};
use clap::{Parser, Subcommand};
use common_types::connection_config::ConnectionConfig;
use common_types::ids::{PipelineId, RunId};
use common_types::pipeline_spec::{PipelineSpec, SourceSpec};
use std::time::Duration;
use temporalio_client::WorkflowStartOptions;
use worker::temporal::{make_client, TemporalConfig};
use worker::workflows::{PipelineRunInput, PipelineRunWorkflow};

#[derive(Parser)]
#[command(name = "platform", version, about = "ETL platform CLI")]
struct Cli {
    /// Override tenant for this call (admin only).
    #[arg(long, global = true)]
    tenant: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Pipeline {
        #[command(subcommand)]
        cmd: PipelineCmd,
    },
    Connector {
        #[command(subcommand)]
        cmd: ConnectorCmd,
    },
    /// Apply YAML resources (Connection, Pipeline) to the catalog.
    Apply {
        #[arg(short, long)]
        file: String,
    },
    /// Print a catalog resource as YAML.
    Get {
        kind: String,
        name: String,
    },
    /// Parse YAML and resolve references without writing to the catalog.
    Validate {
        #[arg(short, long)]
        file: String,
    },
    /// Show what would change if this file were applied.
    Diff {
        #[arg(short, long)]
        file: String,
    },
    /// Workflow-level operations (Temporal).
    Workflow {
        #[command(subcommand)]
        cmd: WorkflowCmd,
    },
    /// Tenant lifecycle (admin operations).
    Tenant {
        #[command(subcommand)]
        cmd: TenantCmd,
    },
    /// Secret reference management (RFC-11).
    Secret {
        #[command(subcommand)]
        cmd: SecretCmd,
    },
    /// Authentication: login, whoami, create-principal (RFC-12).
    Auth {
        #[command(subcommand)]
        cmd: AuthCmd,
    },
}

#[derive(Subcommand)]
enum AuthCmd {
    /// Verify password against the issuer and cache the JWT pair.
    Login {
        name: String,
        #[arg(long)]
        password: String,
    },
    /// Print the cached principal's id, tenant, and role.
    Whoami,
    /// Manually refresh the cached access token.
    Refresh,
    /// Invalidate the cached refresh token and clear the cache.
    Logout,
    /// Admin: create a principal in the named tenant.
    CreatePrincipal {
        #[arg(long)]
        tenant: String,
        name: String,
        #[arg(long)]
        password: String,
        #[arg(long, default_value = "operator")]
        role: String,
    },
}

#[derive(Subcommand)]
enum SecretCmd {
    /// Register a catalog SecretRef pointing at (backend, key).
    Create {
        name: String,
        #[arg(long, default_value = "file")]
        backend: String,
        #[arg(long)]
        key: Option<String>,
    },
    /// Write a plaintext value into the file backend.
    Put {
        name: String,
        value: String,
        /// Also register a catalog SecretRef row (backend=file, key=name).
        #[arg(long)]
        register: bool,
    },
    /// List registered secrets (no plaintexts).
    List,
    /// Delete a SecretRef catalog row by name.
    Delete { name: String },
}

#[derive(Subcommand)]
enum TenantCmd {
    /// Create a tenant: catalog row + Temporal namespace.
    Create { name: String },
    /// List all tenants.
    List,
    /// Flip tenants.status to 'suspended' (blocks pipeline runs).
    Suspend { name: String },
    /// Flip tenants.status back to 'active'.
    Resume { name: String },
    /// Cascade-delete catalog rows + remove ./data/<tenant_id>/.
    Terminate { name: String },
}

#[derive(Subcommand)]
enum WorkflowCmd {
    /// Terminate a running workflow and mark its run Failed.
    Terminate {
        workflow_id: String,
        #[arg(short, long)]
        reason: Option<String>,
    },
}

#[derive(Subcommand)]
enum PipelineCmd {
    Run {
        id: String,
    },
    /// Print the current status of a pipeline as JSON.
    Status {
        id: String,
    },
}

#[derive(Subcommand)]
enum ConnectorCmd {
    /// Compile a guest Rust crate to a precompiled .cwasm artifact.
    Build {
        path: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, default_value = "./connectors")]
        out: String,
        /// Runtime to precompile for: 'source' (default) or 'scalar'.
        #[arg(long, default_value = "source")]
        kind: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let tenant_override = cli.tenant.clone();
    match cli.cmd {
        Cmd::Pipeline { cmd: PipelineCmd::Run { id } } => {
            pipeline_run(id, tenant_override.as_deref()).await
        }
        Cmd::Pipeline { cmd: PipelineCmd::Status { id } } => status::run(id).await,
        Cmd::Connector {
            cmd: ConnectorCmd::Build { path, name, version, out, kind },
        } => connector_build(path, name, version, out, kind).await,
        Cmd::Apply { file } => apply_cmd(file, tenant_override.as_deref()).await,
        Cmd::Get { kind, name } => get_cmd(kind, name, tenant_override.as_deref()).await,
        Cmd::Validate { file } => validate_cmd(file).await,
        Cmd::Diff { file } => diff_cmd(file, tenant_override.as_deref()).await,
        Cmd::Workflow { cmd: WorkflowCmd::Terminate { workflow_id, reason } } => {
            terminate::terminate(workflow_id, reason).await
        }
        Cmd::Tenant { cmd } => {
            // Tenant subcommands need Admin role except `list`. Revocation
            // check happens inside each tenant::* function via the catalog
            // it already opens.
            let p = auth::current_principal()?;
            match &cmd {
                TenantCmd::List => auth::require_role(&p, common_types::auth::Action::Read)?,
                _ => auth::require_role(&p, common_types::auth::Action::Admin)?,
            }
            match cmd {
                TenantCmd::Create { name } => tenant::create(name).await,
                TenantCmd::List => tenant::list().await,
                TenantCmd::Suspend { name } => tenant::suspend(name).await,
                TenantCmd::Resume { name } => tenant::resume(name).await,
                TenantCmd::Terminate { name } => tenant::terminate(name).await,
            }
        }
        Cmd::Secret { cmd } => match cmd {
            SecretCmd::Create { name, backend, key } => {
                let key = key.unwrap_or_else(|| name.clone());
                secret::create(name, backend, key).await
            }
            SecretCmd::Put { name, value, register } => {
                secret::put(name, value, register).await
            }
            SecretCmd::List => secret::list().await,
            SecretCmd::Delete { name } => secret::delete(name).await,
        },
        Cmd::Auth { cmd } => match cmd {
            AuthCmd::Login { name, password } => auth::login(name, password).await,
            AuthCmd::Whoami => auth::whoami().await,
            AuthCmd::Refresh => auth::refresh_now().await,
            AuthCmd::Logout => auth::logout().await,
            AuthCmd::CreatePrincipal { tenant, name, password, role } => {
                auth::create_principal(tenant, name, password, role).await
            }
        },
    }
}

async fn apply_cmd(file: String, tenant_override: Option<&str>) -> anyhow::Result<()> {
    let path = std::path::PathBuf::from(&file);
    let files = dsl::load_path(&path)?;

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    catalog.migrate().await?;

    auth::ensure_bypass_tenant(&catalog).await?;
    let p = auth::current_principal()?;
    auth::assert_not_revoked(&catalog, &p).await?;
    auth::require_role(&p, common_types::auth::Action::Write)?;
    let ctx = auth::resolve_context(&catalog, tenant_override).await?;
    let report = dsl::apply(&catalog, ctx.tenant_id, &files, &p).await?;

    println!(
        "applied:\n  connections: {} created, {} updated, {} unchanged\n  pipelines:   {} created, {} updated, {} unchanged",
        report.connections_created,
        report.connections_updated,
        report.connections_unchanged,
        report.pipelines_created,
        report.pipelines_updated,
        report.pipelines_unchanged,
    );
    Ok(())
}

async fn validate_cmd(file: String) -> anyhow::Result<()> {
    let path = std::path::PathBuf::from(&file);
    let files = dsl::load_path(&path)?;
    let mut conn_names = std::collections::HashSet::new();
    let mut pipes = Vec::new();
    for f in &files {
        for env in &f.resources {
            match env.kind {
                common_types::dsl::ResourceKind::Connection => {
                    conn_names.insert(env.metadata.name.clone());
                }
                common_types::dsl::ResourceKind::Pipeline => {
                    let spec: common_types::dsl::PipelineDslSpec =
                        serde_json::from_value(env.spec.clone())?;
                    pipes.push((env.metadata.name.clone(), spec));
                }
            }
        }
    }
    for (name, spec) in &pipes {
        if !conn_names.contains(&spec.source_connection) {
            anyhow::bail!(
                "pipeline '{name}' references connection '{}' which is not declared",
                spec.source_connection
            );
        }
    }
    println!(
        "validated {} file(s): {} connection(s), {} pipeline(s)",
        files.len(),
        conn_names.len(),
        pipes.len()
    );
    Ok(())
}

async fn get_cmd(kind: String, name: String, tenant_override: Option<&str>) -> anyhow::Result<()> {
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    catalog.migrate().await?;
    auth::ensure_bypass_tenant(&catalog).await?;
    let p = auth::current_principal()?;
    auth::assert_not_revoked(&catalog, &p).await?;
    auth::require_role(&p, common_types::auth::Action::Read)?;
    let ctx = auth::resolve_context(&catalog, tenant_override).await?;
    let tenant_id = ctx.tenant_id;

    match kind.as_str() {
        "connection" => {
            let row: Option<(uuid::Uuid, String, String, serde_json::Value)> = sqlx::query_as(
                "SELECT connection_id, name, connector_ref, config \
                 FROM connections WHERE tenant_id = $1 AND name = $2",
            )
            .bind(tenant_id.as_uuid())
            .bind(&name)
            .fetch_optional(catalog.pool())
            .await?;
            let (_id, name, connector_ref, config) =
                row.with_context(|| format!("connection '{}' not found", name))?;
            let env = serde_yaml::to_string(&common_types::dsl::ResourceEnvelope {
                api_version: "platform.etl/v0".into(),
                kind: common_types::dsl::ResourceKind::Connection,
                metadata: common_types::dsl::Metadata {
                    name,
                    workspace: Some("default".into()),
                    labels: Default::default(),
                },
                spec: serde_json::json!({
                    "connector_ref": connector_ref,
                    "config": config,
                }),
            })?;
            print!("{env}");
        }
        "pipeline" => {
            let row: Option<(uuid::Uuid, String, uuid::Uuid, serde_json::Value)> = sqlx::query_as(
                "SELECT pipeline_id, name, source_conn_id, spec \
                 FROM pipelines WHERE tenant_id = $1 AND name = $2",
            )
            .bind(tenant_id.as_uuid())
            .bind(&name)
            .fetch_optional(catalog.pool())
            .await?;
            let (_id, pname, src, spec) =
                row.with_context(|| format!("pipeline '{}' not found", name))?;
            let src_name: String =
                sqlx::query_scalar("SELECT name FROM connections WHERE connection_id = $1")
                    .bind(src)
                    .fetch_one(catalog.pool())
                    .await?;
            let env = serde_yaml::to_string(&common_types::dsl::ResourceEnvelope {
                api_version: "platform.etl/v0".into(),
                kind: common_types::dsl::ResourceKind::Pipeline,
                metadata: common_types::dsl::Metadata {
                    name: pname,
                    workspace: Some("default".into()),
                    labels: Default::default(),
                },
                spec: serde_json::json!({
                    "source_connection": src_name,
                    "source": spec.get("source").cloned().unwrap_or(serde_json::json!({})),
                    "destination": spec.get("destination").cloned().unwrap_or(serde_json::json!({})),
                    "batch_size": spec.get("batch_size").cloned().unwrap_or(serde_json::json!(100)),
                    "evolution_policy": spec.get("evolution_policy").cloned().unwrap_or(serde_json::json!("propagate_additive")),
                }),
            })?;
            print!("{env}");
        }
        other => anyhow::bail!("unknown kind: {other} (expected 'connection' or 'pipeline')"),
    }
    Ok(())
}

async fn diff_cmd(file: String, tenant_override: Option<&str>) -> anyhow::Result<()> {
    let path = std::path::PathBuf::from(&file);
    let files = dsl::load_path(&path)?;
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    catalog.migrate().await?;
    auth::ensure_bypass_tenant(&catalog).await?;
    let p = auth::current_principal()?;
    auth::assert_not_revoked(&catalog, &p).await?;
    auth::require_role(&p, common_types::auth::Action::Read)?;
    let ctx = auth::resolve_context(&catalog, tenant_override).await?;
    let rows = dsl::diff(&catalog, ctx.tenant_id, &files).await?;
    for row in rows {
        match row {
            dsl::DiffRow::Create { kind, name } => println!("+ {kind:?}/{name}"),
            dsl::DiffRow::Update { kind, name, fields } => {
                println!("~ {kind:?}/{name} ({})", fields.join(", "))
            }
            dsl::DiffRow::Unchanged { kind, name } => println!("= {kind:?}/{name}"),
        }
    }
    Ok(())
}

// ensure_dev_tenant deleted in Phase II.2.b — replaced by
// auth::resolve_context + auth::ensure_bypass_tenant.

async fn connector_build(
    path: String,
    name: Option<String>,
    version: Option<String>,
    out: String,
    kind: String,
) -> anyhow::Result<()> {
    use std::path::PathBuf;
    use std::process::Command as StdCommand;

    let crate_dir = PathBuf::from(&path);
    let cargo_toml = crate_dir.join("Cargo.toml");
    if !cargo_toml.exists() {
        anyhow::bail!("no Cargo.toml at {}", cargo_toml.display());
    }

    let toml_text = std::fs::read_to_string(&cargo_toml)?;
    let pkg_name = name.unwrap_or_else(|| {
        read_toml_value(&toml_text, "name").unwrap_or_else(|| "connector".into())
    });
    let pkg_version = version.unwrap_or_else(|| {
        read_toml_value(&toml_text, "version").unwrap_or_else(|| "0.1.0".into())
    });

    let status = StdCommand::new("cargo")
        .current_dir(&crate_dir)
        .args(["build", "--release"])
        .status()?;
    if !status.success() {
        anyhow::bail!("guest build failed");
    }

    let wasm_name = format!("{}.wasm", pkg_name.replace('-', "_"));
    let wasm_path = crate_dir
        .join("target")
        .join("wasm32-wasip2")
        .join("release")
        .join(&wasm_name);
    if !wasm_path.exists() {
        anyhow::bail!(
            "expected artifact not found at {} — check the crate's [lib] crate-type and package name",
            wasm_path.display()
        );
    }

    let out_dir = PathBuf::from(&out);
    let target_name = format!("{}@{}", pkg_name, pkg_version);

    let out_path = match kind.as_str() {
        "source" => {
            let rt = worker::wasm_runtime::WasmSourceRuntime::new(&out_dir)?;
            let p = rt.artifact_path(&target_name);
            rt.precompile_to(&wasm_path, &p)?;
            p
        }
        "scalar" => {
            let rt = worker::wasm_runtime::WasmScalarRuntime::new(&out_dir)?;
            let p = rt.artifact_path(&target_name);
            rt.precompile_to(&wasm_path, &p)?;
            p
        }
        other => anyhow::bail!("unknown --kind: '{other}' (expected 'source' or 'scalar')"),
    };

    println!("built {} ({})", out_path.display(), kind);
    Ok(())
}

fn read_toml_value(text: &str, key: &str) -> Option<String> {
    let mut in_package = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_package = trimmed == "[package]";
            continue;
        }
        if !in_package {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix(&format!("{} = \"", key)) {
            if let Some(end) = rest.find('"') {
                return Some(rest[..end].to_string());
            }
        }
    }
    None
}

async fn pipeline_run(id_str: String, tenant_override: Option<&str>) -> anyhow::Result<()> {
    let pipeline_id = parse_pipeline_id(&id_str)
        .with_context(|| format!("parsing pipeline id '{}'", id_str))?;

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    catalog.migrate().await?;

    auth::ensure_bypass_tenant(&catalog).await?;
    let p = auth::current_principal()?;
    auth::assert_not_revoked(&catalog, &p).await?;
    auth::require_role(&p, common_types::auth::Action::Run)?;
    let _auth_ctx = auth::resolve_context(&catalog, tenant_override).await?;

    let pipeline = catalog
        .get_pipeline_admin(pipeline_id)
        .await?
        .with_context(|| format!("pipeline {} not found", pipeline_id))?;
    let ctx = catalog::TenantContext::new(pipeline.tenant_id);

    let tenant_row = catalog
        .get_tenant(pipeline.tenant_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tenant {} not found", pipeline.tenant_id))?;
    if tenant_row.status == "suspended" {
        anyhow::bail!(
            "tenant '{}' is suspended — use 'platform tenant resume {}' to re-enable",
            tenant_row.name,
            tenant_row.name,
        );
    }

    let spec: PipelineSpec = serde_json::from_value(pipeline.spec.clone())
        .context("pipelines.spec did not deserialize as PipelineSpec")?;

    let source_conn_row = catalog
        .get_connection(ctx, pipeline.source_conn_id)
        .await?
        .with_context(|| format!("source connection {} not found", pipeline.source_conn_id))?;
    // Phase II.2.b: pass the unresolved ConnectionConfig through; the
    // worker activity resolves at activity start so plaintext lifetime
    // is the activity body only.
    let source_connection: ConnectionConfig =
        serde_json::from_value(source_conn_row.config.clone())
            .context("source connections.config did not deserialize as ConnectionConfig")?;

    let connector_ref = source_conn_row.connector_ref.clone();

    let stream_name = match &spec.source {
        SourceSpec::Postgres(p) => p.table.clone(),
        SourceSpec::Wasm(_) => {
            let bare = connector_ref
                .strip_prefix("wasm:")
                .unwrap_or(&connector_ref);
            let name = bare.split('@').next().unwrap_or(bare);
            name.to_string()
        }
    };

    let (cursor_column, cursor_kind, pk_columns) = match &spec.source {
        SourceSpec::Postgres(p) => (
            p.cursor_column.clone(),
            p.cursor_kind,
            p.pk_columns.clone(),
        ),
        SourceSpec::Wasm(_) => (
            "_row_index".to_string(),
            common_types::cursor::CursorKind::Int64,
            vec![],
        ),
    };

    // Pull evolution_policy out of the pipelines.spec JSONB; fall back to
    // PropagateAdditive if absent (Phase I.2 catalog rows won't have it).
    let evolution_policy = pipeline
        .spec
        .get("evolution_policy")
        .and_then(|v| serde_json::from_value::<common_types::evolution::EvolutionPolicy>(v.clone()).ok())
        .unwrap_or_default();

    let initial_cursor = catalog
        .get_stream_state(ctx, pipeline_id, &stream_name)
        .await?
        .and_then(|s| s.cursor);

    let run_id = RunId::new();
    let workflow_id = format!("run-{}", run_id.as_uuid());

    catalog
        .create_run(NewRun {
            run_id,
            tenant_id: pipeline.tenant_id,
            pipeline_id,
            trigger: "manual".into(),
            temporal_workflow_id: Some(workflow_id.clone()),
        })
        .await?;

    let cfg = TemporalConfig::from_env()?;
    // Idempotent: register the namespace if it doesn't exist yet. Lets
    // tests + admin paths that bypass `platform tenant create` still work.
    let _ = tenant::ensure_temporal_namespace(&pipeline.tenant_id).await;
    // Fall back to the default namespace if the worker hasn't been
    // restarted to pick up the new tenant namespace yet — Phase II.4
    // will hot-reconfigure. For now, default polls everything as a backstop.
    let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".into());
    tracing::info!(%namespace, tenant = %pipeline.tenant_id, "starting workflow");
    let client = worker::temporal::make_client_for_namespace(&cfg, &namespace).await?;

    let opts = WorkflowStartOptions::new(cfg.task_queue.clone(), workflow_id.clone())
        .execution_timeout(Duration::from_secs(3600))
        .run_timeout(Duration::from_secs(3600))
        .task_timeout(Duration::from_secs(60))
        .build();

    // Phase I.6: route to CdcPipelineWorkflow if sync_mode is Cdc.
    let is_cdc = matches!(
        &spec.source,
        common_types::pipeline_spec::SourceSpec::Postgres(p)
            if p.sync_mode == common_types::pipeline_spec::SyncMode::Cdc
    );
    if is_cdc {
        let cdc_input = worker::workflows::CdcPipelineInput {
            run_id: run_id.as_uuid(),
            pipeline_id: pipeline_id.as_uuid(),
            tenant_id: pipeline.tenant_id.as_uuid(),
            spec: spec.clone(),
            source_conn: source_connection.clone(),
            max_windows: std::env::var("ETL_CDC_MAX_WINDOWS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
        };
        client
            .start_workflow(worker::workflows::CdcPipelineWorkflow::run, cdc_input, opts)
            .await
            .context("starting CdcPipelineWorkflow")?;
        println!("started CDC workflow {}", workflow_id);
        println!("run id: {}", run_id);
        return Ok(());
    }

    let input = PipelineRunInput {
        run_id: run_id.as_uuid(),
        pipeline_id: pipeline_id.as_uuid(),
        spec,
        source_connection,
        initial_cursor,
        stream_name,
        connector_ref,
        evolution_policy,
        cursor_column,
        cursor_kind,
        pk_columns,
        tenant_id: pipeline.tenant_id.as_uuid(),
    };

    client
        .start_workflow(PipelineRunWorkflow::run, input, opts)
        .await
        .context("starting PipelineRunWorkflow")?;

    println!("started workflow {}", workflow_id);
    println!("run id: {}", run_id);
    Ok(())
}

fn parse_pipeline_id(s: &str) -> anyhow::Result<PipelineId> {
    if let Ok(p) = s.parse::<PipelineId>() {
        return Ok(p);
    }
    let u = uuid::Uuid::parse_str(s)?;
    Ok(PipelineId::from_uuid_unchecked(u))
}
