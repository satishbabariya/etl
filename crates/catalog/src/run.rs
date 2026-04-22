use chrono::{DateTime, Utc};
use common_types::ids::{PipelineId, RunId, TenantId};
use sqlx::PgPool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Queued => "queued",
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "queued" => RunStatus::Queued,
            "running" => RunStatus::Running,
            "completed" => RunStatus::Completed,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Run {
    pub run_id: RunId,
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub status: RunStatus,
    pub trigger: String,
    pub temporal_workflow_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

pub struct NewRun {
    /// Caller supplies the id. Required so callers can correlate the row
    /// with identifiers passed to external systems (e.g. Temporal workflow
    /// input) before the INSERT commits.
    pub run_id: RunId,
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub trigger: String,
    pub temporal_workflow_id: Option<String>,
}

pub async fn create(pool: &PgPool, new: NewRun) -> sqlx::Result<RunId> {
    sqlx::query(
        "INSERT INTO runs (run_id, tenant_id, pipeline_id, status, trigger, temporal_workflow_id) \
         VALUES ($1, $2, $3, 'queued', $4, $5)",
    )
    .bind(new.run_id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(new.pipeline_id.as_uuid())
    .bind(&new.trigger)
    .bind(new.temporal_workflow_id)
    .execute(pool)
    .await?;
    Ok(new.run_id)
}

pub async fn mark_running(pool: &PgPool, id: RunId) -> sqlx::Result<()> {
    sqlx::query("UPDATE runs SET status = 'running' WHERE run_id = $1")
        .bind(id.as_uuid())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn mark_completed(pool: &PgPool, id: RunId) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE runs SET status = 'completed', completed_at = NOW() WHERE run_id = $1",
    )
    .bind(id.as_uuid())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_failed(pool: &PgPool, id: RunId, err: &str) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE runs SET status = 'failed', completed_at = NOW(), error = $2 WHERE run_id = $1",
    )
    .bind(id.as_uuid())
    .bind(err)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get(pool: &PgPool, id: RunId) -> sqlx::Result<Option<Run>> {
    let row: Option<(
        uuid::Uuid,
        uuid::Uuid,
        uuid::Uuid,
        String,
        String,
        Option<String>,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT run_id, tenant_id, pipeline_id, status, trigger, temporal_workflow_id, \
                started_at, completed_at, error \
         FROM runs WHERE run_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(rid, tid, pid, status, trigger, wf, started_at, completed_at, error)| Run {
        run_id: RunId::from_uuid_unchecked(rid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        status: RunStatus::parse(&status).expect("DB check constraint enforces valid values"),
        trigger,
        temporal_workflow_id: wf,
        started_at,
        completed_at,
        error,
    }))
}
