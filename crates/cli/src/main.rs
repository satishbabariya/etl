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
}

#[derive(Subcommand)]
enum PipelineCmd {
    Run {
        id: String,
    },
}

#[derive(Subcommand)]
enum ConnectorCmd {
    /// Compile a guest Rust crate to a precompiled .cwasm artifact.
    Build {
        /// Path to the guest crate (must contain Cargo.toml with [lib] crate-type = ["cdylib"]).
        path: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, default_value = "./connectors")]
        out: String,
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
    match cli.cmd {
        Cmd::Pipeline { cmd: PipelineCmd::Run { id } } => pipeline_run(id).await,
        Cmd::Connector {
            cmd: ConnectorCmd::Build { path, name, version, out },
        } => connector_build(path, name, version, out).await,
    }
}

async fn connector_build(
    path: String,
    name: Option<String>,
    version: Option<String>,
    out: String,
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
    let rt = worker::wasm_runtime::WasmSourceRuntime::new(&out_dir)?;
    let target_name = format!("{}@{}", pkg_name, pkg_version);
    let out_path = rt.artifact_path(&target_name);
    rt.precompile_to(&wasm_path, &out_path)?;

    println!("built {}", out_path.display());
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

async fn pipeline_run(id_str: String) -> anyhow::Result<()> {
    let pipeline_id = parse_pipeline_id(&id_str)
        .with_context(|| format!("parsing pipeline id '{}'", id_str))?;

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    catalog.migrate().await?;

    let pipeline = catalog
        .get_pipeline(pipeline_id)
        .await?
        .with_context(|| format!("pipeline {} not found", pipeline_id))?;

    let spec: PipelineSpec = serde_json::from_value(pipeline.spec.clone())
        .context("pipelines.spec did not deserialize as PipelineSpec")?;

    let source_conn_row = catalog
        .get_connection(pipeline.source_conn_id)
        .await?
        .with_context(|| format!("source connection {} not found", pipeline.source_conn_id))?;
    let source_connection: ConnectionConfig =
        serde_json::from_value(source_conn_row.config.clone())
            .context("source connections.config did not deserialize as ConnectionConfig")?;

    let connector_ref = source_conn_row.connector_ref.clone();

    let stream_name = match &spec.source {
        SourceSpec::Postgres(p) => p.table.clone(),
        SourceSpec::Wasm(_) => {
            // Phase I.3: derive stream name from the connector's name
            // (strip the "wasm:" prefix and the "@version" suffix).
            let bare = connector_ref
                .strip_prefix("wasm:")
                .unwrap_or(&connector_ref);
            let name = bare.split('@').next().unwrap_or(bare);
            name.to_string()
        }
    };

    let initial_cursor = catalog
        .get_stream_state(pipeline_id, &stream_name)
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
    let client = make_client(&cfg).await?;

    let opts = WorkflowStartOptions::new(cfg.task_queue.clone(), workflow_id.clone())
        .execution_timeout(Duration::from_secs(3600))
        .run_timeout(Duration::from_secs(3600))
        .task_timeout(Duration::from_secs(60))
        .build();

    let input = PipelineRunInput {
        run_id: run_id.as_uuid(),
        pipeline_id: pipeline_id.as_uuid(),
        spec,
        source_connection,
        initial_cursor,
        stream_name,
        connector_ref,
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
