//! Catalog: persistent metadata store (RFC-10).
//!
//! Phase I.1 scope: 4 tables (tenants, connections, pipelines, runs),
//! every row tenant-scoped, async CRUD via sqlx. Subsequent phases add
//! workspaces, streams, schemas, transformations, audit.

mod db;
pub mod connection;
pub mod pipeline;
pub mod run;
pub mod schema;
pub mod stream;
pub mod stream_state;
pub mod tenant;
pub mod workspace;

pub use connection::{Connection, NewConnection};
pub use pipeline::{NewPipeline, Pipeline};
pub use run::{NewRun, Run, RunStatus};
pub use tenant::Tenant;

use common_types::ids::{ConnectionId, PipelineId, RunId, SchemaId, StreamId, TenantId, WorkspaceId};
use sqlx::PgPool;

/// Wrapper that exposes catalog operations as methods.
#[derive(Clone)]
pub struct Catalog {
    pool: PgPool,
}

impl Catalog {
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = db::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), sqlx::migrate::MigrateError> {
        db::migrate(&self.pool).await
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // Tenants
    pub async fn create_tenant(&self, name: &str) -> sqlx::Result<TenantId> {
        tenant::create(&self.pool, name).await
    }
    pub async fn get_tenant(&self, id: TenantId) -> sqlx::Result<Option<Tenant>> {
        tenant::get(&self.pool, id).await
    }

    // Connections
    pub async fn create_connection(&self, new: NewConnection) -> sqlx::Result<ConnectionId> {
        connection::create(&self.pool, new).await
    }
    pub async fn get_connection(&self, id: ConnectionId) -> sqlx::Result<Option<Connection>> {
        connection::get(&self.pool, id).await
    }

    // Pipelines
    pub async fn create_pipeline(&self, new: NewPipeline) -> sqlx::Result<PipelineId> {
        pipeline::create(&self.pool, new).await
    }
    pub async fn get_pipeline(&self, id: PipelineId) -> sqlx::Result<Option<Pipeline>> {
        pipeline::get(&self.pool, id).await
    }

    // Runs
    pub async fn create_run(&self, new: NewRun) -> sqlx::Result<RunId> {
        run::create(&self.pool, new).await
    }
    pub async fn mark_run_running(&self, id: RunId) -> sqlx::Result<()> {
        run::mark_running(&self.pool, id).await
    }
    pub async fn mark_run_completed(&self, id: RunId) -> sqlx::Result<()> {
        run::mark_completed(&self.pool, id).await
    }
    pub async fn mark_run_failed(&self, id: RunId, err: &str) -> sqlx::Result<()> {
        run::mark_failed(&self.pool, id, err).await
    }
    pub async fn get_run(&self, id: RunId) -> sqlx::Result<Option<Run>> {
        run::get(&self.pool, id).await
    }

    // Stream state
    pub async fn get_stream_state(
        &self,
        pipeline_id: PipelineId,
        stream_name: &str,
    ) -> sqlx::Result<Option<stream_state::StreamState>> {
        stream_state::get(&self.pool, pipeline_id, stream_name).await
    }

    pub async fn upsert_stream_state(
        &self,
        pipeline_id: PipelineId,
        stream_name: &str,
        cursor: Option<common_types::cursor::CursorValue>,
        last_run_id: Option<RunId>,
    ) -> sqlx::Result<()> {
        stream_state::upsert(&self.pool, pipeline_id, stream_name, cursor, last_run_id).await
    }

    // Workspaces
    pub async fn ensure_default_workspace(
        &self,
        tenant_id: TenantId,
    ) -> sqlx::Result<WorkspaceId> {
        workspace::ensure_default(&self.pool, tenant_id).await
    }
    pub async fn get_workspace_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> sqlx::Result<Option<workspace::Workspace>> {
        workspace::get_by_name(&self.pool, tenant_id, name).await
    }

    // Streams
    pub async fn ensure_stream(&self, new: stream::NewStream) -> sqlx::Result<StreamId> {
        stream::ensure(&self.pool, new).await
    }
    pub async fn get_stream_by_name(
        &self,
        pipeline_id: PipelineId,
        name: &str,
    ) -> sqlx::Result<Option<stream::Stream>> {
        stream::get_by_name(&self.pool, pipeline_id, name).await
    }
    pub async fn set_stream_current_schema(
        &self,
        stream_id: StreamId,
        schema_id: SchemaId,
    ) -> sqlx::Result<()> {
        stream::set_current_schema(&self.pool, stream_id, schema_id).await
    }

    // Schemas
    pub async fn insert_schema(&self, new: schema::NewSchema) -> sqlx::Result<SchemaId> {
        schema::insert(&self.pool, new).await
    }
    pub async fn get_latest_schema(
        &self,
        stream_id: StreamId,
    ) -> sqlx::Result<Option<schema::Schema>> {
        schema::get_latest(&self.pool, stream_id).await
    }

    /// Truncates every table. Intended for test cleanup only.
    #[doc(hidden)]
    pub async fn truncate_all_for_tests(&self) -> sqlx::Result<()> {
        sqlx::query(
            "TRUNCATE cdc_slots, runs, stream_state, schemas, streams, pipelines, connections, workspaces, tenants CASCADE",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
