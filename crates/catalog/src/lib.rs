//! Catalog: persistent metadata store (RFC-10).
//!
//! Phase I.1 scope: 4 tables (tenants, connections, pipelines, runs),
//! every row tenant-scoped, async CRUD via sqlx. Subsequent phases add
//! workspaces, streams, schemas, transformations, audit.

mod db;
pub mod cdc;
pub mod connection;
pub mod pipeline;
pub mod run;
pub mod schema;
pub mod secret;
pub mod stream;
pub mod stream_state;
pub mod tenant;
pub mod workspace;

pub use common_types::ids::TenantContext;
pub use connection::{Connection, NewConnection};
pub use pipeline::{NewPipeline, Pipeline};
pub use run::{NewRun, Run, RunStatus};
pub use secret::NewSecret;
pub mod principal;
pub use principal::NewPrincipal;
pub mod refresh;
pub mod revoke;
pub use refresh::NewRefreshToken;
pub use tenant::Tenant;

use common_types::ids::{
    ConnectionId, PipelineId, RunId, SchemaId, SecretId, StreamId, TenantId, WorkspaceId,
};
use sqlx::PgPool;

/// Wrapper that exposes catalog operations as methods.
#[derive(Clone)]
pub struct Catalog {
    pool: PgPool,
}

impl Catalog {
    /// Connect with full privileges (etl superuser). Use for migrations
    /// and admin tenant CRUD only.
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = db::connect(url).await?;
        Ok(Self { pool })
    }

    /// Connect as the non-superuser etl_app role. RLS is enforced.
    pub async fn connect_app(url: &str) -> Result<Self, sqlx::Error> {
        let pool = db::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), sqlx::migrate::MigrateError> {
        db::migrate(&self.pool).await
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Open a transaction that issues `SET LOCAL app.tenant_id = '<uuid>'`
    /// (or empty string for admin mode). Every public method below uses
    /// this to scope its query under RLS.
    async fn begin_with_tenant(
        &self,
        ctx: Option<TenantContext>,
    ) -> sqlx::Result<sqlx::Transaction<'_, sqlx::Postgres>> {
        let mut tx = self.pool.begin().await?;
        let lit = ctx
            .map(|c| format!("'{}'", c.tenant_id.as_uuid()))
            .unwrap_or_else(|| "''".to_string());
        sqlx::query(&format!("SET LOCAL app.tenant_id = {lit}"))
            .execute(&mut *tx)
            .await?;
        if let Some(c) = ctx {
            if let Some(pid) = c.principal_id {
                sqlx::query(&format!(
                    "SET LOCAL app.principal_id = '{}'",
                    pid.as_uuid()
                ))
                .execute(&mut *tx)
                .await?;
            }
            if let Some(role) = c.role {
                let token = match role {
                    common_types::auth::Role::Admin => "admin",
                    common_types::auth::Role::Operator => "operator",
                    common_types::auth::Role::Viewer => "viewer",
                };
                sqlx::query(&format!("SET LOCAL app.role = '{token}'"))
                    .execute(&mut *tx)
                    .await?;
            }
        }
        Ok(tx)
    }

    // Tenants — admin-mode (NULL app.tenant_id)
    pub async fn create_tenant(&self, name: &str) -> sqlx::Result<TenantId> {
        let mut tx = self.begin_with_tenant(None).await?;
        let id = tenant::create(&mut tx, name).await?;
        tx.commit().await?;
        Ok(id)
    }
    pub async fn get_tenant(&self, id: TenantId) -> sqlx::Result<Option<Tenant>> {
        let mut tx = self.begin_with_tenant(None).await?;
        let r = tenant::get(&mut tx, id).await?;
        tx.commit().await?;
        Ok(r)
    }
    pub async fn get_tenant_by_name(&self, name: &str) -> sqlx::Result<Option<Tenant>> {
        let mut tx = self.begin_with_tenant(None).await?;
        let r = tenant::get_by_name(&mut tx, name).await?;
        tx.commit().await?;
        Ok(r)
    }
    pub async fn list_tenants(&self) -> sqlx::Result<Vec<Tenant>> {
        let mut tx = self.begin_with_tenant(None).await?;
        let r = tenant::list(&mut tx).await?;
        tx.commit().await?;
        Ok(r)
    }
    pub async fn delete_tenant(&self, id: TenantId) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(None).await?;
        tenant::delete(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn tenant_set_status(
        &self,
        tenant_id: TenantId,
        status: &str,
    ) -> sqlx::Result<u64> {
        if status != "active" && status != "suspended" {
            return Err(sqlx::Error::Protocol(format!(
                "invalid status '{status}' (expected active|suspended)"
            )));
        }
        let r = sqlx::query("UPDATE tenants SET status = $1 WHERE tenant_id = $2")
            .bind(status)
            .bind(tenant_id.as_uuid())
            .execute(self.pool())
            .await?;
        Ok(r.rows_affected())
    }

    // Connections
    pub async fn create_connection(&self, new: NewConnection) -> sqlx::Result<ConnectionId> {
        let ctx = TenantContext::new(new.tenant_id);
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let id = connection::create(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }
    pub async fn get_connection(
        &self,
        ctx: TenantContext,
        id: ConnectionId,
    ) -> sqlx::Result<Option<Connection>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = connection::get(&mut tx, id).await?;
        tx.commit().await?;
        Ok(r)
    }

    // Pipelines
    pub async fn create_pipeline(&self, new: NewPipeline) -> sqlx::Result<PipelineId> {
        let ctx = TenantContext::new(new.tenant_id);
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let id = pipeline::create(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }
    pub async fn get_pipeline(
        &self,
        ctx: TenantContext,
        id: PipelineId,
    ) -> sqlx::Result<Option<Pipeline>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = pipeline::get(&mut tx, id).await?;
        tx.commit().await?;
        Ok(r)
    }

    /// Admin-mode lookup: bypasses RLS to fetch a pipeline regardless
    /// of tenant. Used by the superuser-mode CLI when the caller
    /// doesn't yet know the tenant.
    pub async fn get_pipeline_admin(
        &self,
        id: PipelineId,
    ) -> sqlx::Result<Option<Pipeline>> {
        let mut tx = self.begin_with_tenant(None).await?;
        let r = pipeline::get(&mut tx, id).await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn get_connection_admin(
        &self,
        id: ConnectionId,
    ) -> sqlx::Result<Option<Connection>> {
        let mut tx = self.begin_with_tenant(None).await?;
        let r = connection::get(&mut tx, id).await?;
        tx.commit().await?;
        Ok(r)
    }

    // Runs
    pub async fn create_run(&self, new: NewRun) -> sqlx::Result<RunId> {
        let ctx = TenantContext::new(new.tenant_id);
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let id = run::create(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }
    pub async fn mark_run_running(&self, ctx: TenantContext, id: RunId) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        run::mark_running(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn mark_run_completed(&self, ctx: TenantContext, id: RunId) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        run::mark_completed(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn mark_run_failed(
        &self,
        ctx: TenantContext,
        id: RunId,
        err: &str,
    ) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        run::mark_failed(&mut tx, id, err).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn get_run(&self, ctx: TenantContext, id: RunId) -> sqlx::Result<Option<Run>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = run::get(&mut tx, id).await?;
        tx.commit().await?;
        Ok(r)
    }

    // Stream state
    pub async fn get_stream_state(
        &self,
        ctx: TenantContext,
        pipeline_id: PipelineId,
        stream_name: &str,
    ) -> sqlx::Result<Option<stream_state::StreamState>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = stream_state::get(&mut tx, pipeline_id, stream_name).await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn upsert_stream_state(
        &self,
        ctx: TenantContext,
        pipeline_id: PipelineId,
        stream_name: &str,
        cursor: Option<common_types::cursor::CursorValue>,
        last_run_id: Option<RunId>,
    ) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        stream_state::upsert(&mut tx, ctx.tenant_id, pipeline_id, stream_name, cursor, last_run_id)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    // Workspaces
    pub async fn ensure_default_workspace(
        &self,
        ctx: TenantContext,
    ) -> sqlx::Result<WorkspaceId> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = workspace::ensure_default(&mut tx, ctx.tenant_id).await?;
        tx.commit().await?;
        Ok(r)
    }
    pub async fn get_workspace_by_name(
        &self,
        ctx: TenantContext,
        name: &str,
    ) -> sqlx::Result<Option<workspace::Workspace>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = workspace::get_by_name(&mut tx, ctx.tenant_id, name).await?;
        tx.commit().await?;
        Ok(r)
    }

    // Streams
    pub async fn ensure_stream(
        &self,
        new: stream::NewStream,
    ) -> sqlx::Result<StreamId> {
        let ctx = TenantContext::new(new.tenant_id);
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = stream::ensure(&mut tx, new).await?;
        tx.commit().await?;
        Ok(r)
    }
    pub async fn get_stream_by_name(
        &self,
        ctx: TenantContext,
        pipeline_id: PipelineId,
        name: &str,
    ) -> sqlx::Result<Option<stream::Stream>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = stream::get_by_name(&mut tx, pipeline_id, name).await?;
        tx.commit().await?;
        Ok(r)
    }
    pub async fn set_stream_current_schema(
        &self,
        ctx: TenantContext,
        stream_id: StreamId,
        schema_id: SchemaId,
    ) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        stream::set_current_schema(&mut tx, stream_id, schema_id).await?;
        tx.commit().await?;
        Ok(())
    }

    // Schemas
    pub async fn insert_schema(&self, new: schema::NewSchema) -> sqlx::Result<SchemaId> {
        let ctx = TenantContext::new(new.tenant_id);
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let id = schema::insert(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }
    pub async fn get_latest_schema(
        &self,
        ctx: TenantContext,
        stream_id: StreamId,
    ) -> sqlx::Result<Option<schema::Schema>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = schema::get_latest(&mut tx, stream_id).await?;
        tx.commit().await?;
        Ok(r)
    }

    // CDC slots
    pub async fn cdc_upsert(&self, ctx: TenantContext, slot: &cdc::CdcSlot) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        cdc::upsert(&mut tx, slot).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn cdc_get(
        &self,
        ctx: TenantContext,
        pipeline_id: PipelineId,
    ) -> sqlx::Result<Option<cdc::CdcSlot>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = cdc::get(&mut tx, pipeline_id).await?;
        tx.commit().await?;
        Ok(r)
    }
    pub async fn cdc_update_confirmed_flush(
        &self,
        ctx: TenantContext,
        pipeline_id: PipelineId,
        lsn: &str,
    ) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        cdc::update_confirmed_flush(&mut tx, pipeline_id, lsn).await?;
        tx.commit().await?;
        Ok(())
    }

    // Secrets
    pub async fn secret_create(
        &self,
        ctx: TenantContext,
        new: NewSecret,
    ) -> sqlx::Result<SecretId> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let id = secret::create(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }
    pub async fn secret_get_by_name(
        &self,
        ctx: TenantContext,
        name: &str,
    ) -> sqlx::Result<Option<secret::Secret>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = secret::get_by_name(&mut tx, name).await?;
        tx.commit().await?;
        Ok(r)
    }
    pub async fn secret_list(
        &self,
        ctx: TenantContext,
    ) -> sqlx::Result<Vec<secret::Secret>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = secret::list(&mut tx).await?;
        tx.commit().await?;
        Ok(r)
    }
    pub async fn secret_delete(
        &self,
        ctx: TenantContext,
        id: SecretId,
    ) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        secret::delete(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }

    // Principals
    pub async fn principal_create(
        &self,
        ctx: TenantContext,
        new: NewPrincipal,
    ) -> sqlx::Result<common_types::ids::PrincipalId> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let id = principal::create(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Lookup is intentionally unscoped — a JWT login looks up by name
    /// across all tenants and the principal's tenant_id is read from the
    /// returned row. RLS isn't engaged here (admin path).
    pub async fn principal_get_by_name(
        &self,
        name: &str,
    ) -> sqlx::Result<Option<(principal::Principal, String)>> {
        let mut conn = self.pool.acquire().await?;
        principal::get_by_name(&mut conn, name).await
    }

    /// Used by the issuer's refresh endpoint: re-load a principal by id
    /// to recover the role for a new access token.
    pub async fn principal_get_by_id(
        &self,
        id: common_types::ids::PrincipalId,
    ) -> sqlx::Result<Option<(principal::Principal, String)>> {
        let mut conn = self.pool.acquire().await?;
        principal::get_by_id(&mut conn, id).await
    }

    // Refresh tokens
    pub async fn refresh_create(
        &self,
        ctx: TenantContext,
        new: NewRefreshToken,
    ) -> sqlx::Result<uuid::Uuid> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let id = refresh::create(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn refresh_get(
        &self,
        token_id: uuid::Uuid,
    ) -> sqlx::Result<Option<refresh::RefreshTokenRow>> {
        let mut conn = self.pool.acquire().await?;
        refresh::get(&mut conn, token_id).await
    }

    pub async fn refresh_delete(
        &self,
        token_id: uuid::Uuid,
    ) -> sqlx::Result<u64> {
        let mut conn = self.pool.acquire().await?;
        refresh::delete(&mut conn, token_id).await
    }

    // Revoked tokens
    pub async fn revoke_insert(
        &self,
        ctx: TenantContext,
        jti: uuid::Uuid,
        exp: chrono::DateTime<chrono::Utc>,
    ) -> sqlx::Result<()> {
        let tid = ctx.tenant_id;
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        revoke::insert(&mut tx, jti, tid, exp).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn revoke_is_revoked(
        &self,
        jti: uuid::Uuid,
    ) -> sqlx::Result<bool> {
        let mut conn = self.pool.acquire().await?;
        revoke::is_revoked(&mut conn, jti).await
    }

    // Audit
    pub async fn audit_write(&self, row: &audit::AuditRow) -> Result<i64, audit::ChainError> {
        audit::chain::write_event(&self.pool, row).await
    }

    pub async fn audit_verify_chain(
        &self,
        tenant_id: Option<common_types::ids::TenantId>,
    ) -> Result<audit::verify::VerifyResult, audit::ChainError> {
        audit::verify::verify_chain(&self.pool, tenant_id).await
    }

    pub async fn audit_record_checkpoint(
        &self,
        tenant_id: Option<common_types::ids::TenantId>,
        cp: audit::chain::Checkpoint,
    ) -> Result<(), audit::ChainError> {
        audit::chain::record_checkpoint(&self.pool, tenant_id, cp).await
    }

    pub async fn audit_get_checkpoint(
        &self,
        tenant_id: Option<common_types::ids::TenantId>,
    ) -> Result<Option<audit::chain::Checkpoint>, audit::ChainError> {
        audit::chain::get_checkpoint(&self.pool, tenant_id).await
    }

    pub async fn audit_prune_before(
        &self,
        tenant_id: Option<common_types::ids::TenantId>,
        older_than_audit_id: i64,
    ) -> Result<u64, audit::ChainError> {
        audit::chain::prune_before(&self.pool, tenant_id, older_than_audit_id).await
    }

    pub async fn audit_verify_and_checkpoint(
        &self,
        tenant_id: Option<common_types::ids::TenantId>,
    ) -> Result<audit::verify::VerifyResult, audit::ChainError> {
        audit::verify::verify_and_checkpoint(&self.pool, tenant_id).await
    }

    pub async fn audit_tail(
        &self,
        tenant_id: common_types::ids::TenantId,
        limit: i64,
    ) -> sqlx::Result<
        Vec<(
            i64,
            String,
            Option<uuid::Uuid>,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
            serde_json::Value,
        )>,
    > {
        sqlx::query_as(
            "SELECT audit_id, action, principal_id, target, occurred_at, payload \
             FROM audit_log WHERE tenant_id = $1 \
             ORDER BY audit_id DESC LIMIT $2",
        )
        .bind(tenant_id.as_uuid())
        .bind(limit)
        .fetch_all(self.pool())
        .await
    }

    /// Truncates every table. Intended for test cleanup only — admin only.
    #[doc(hidden)]
    pub async fn truncate_all_for_tests(&self) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(None).await?;
        sqlx::query(
            "TRUNCATE audit_verified_chain, audit_log, revoked_tokens, refresh_tokens, principals, secrets, cdc_slots, runs, stream_state, schemas, streams, pipelines, connections, workspaces, tenants CASCADE",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
