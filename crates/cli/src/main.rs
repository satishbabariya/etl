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
}

#[derive(Subcommand)]
enum PipelineCmd {
    Run {
        id: String,
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
    }
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

    let stream_name = match &spec.source {
        SourceSpec::Postgres(p) => p.table.clone(),
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
