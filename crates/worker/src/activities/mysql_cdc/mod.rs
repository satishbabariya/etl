pub mod inputs;

use anyhow::{anyhow, Context};
use catalog::Catalog;
use inputs::*;
use mysql_async::prelude::*;
use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::connectors::mysql::cdc::{position::GtidSet, schema, snapshot, stream};
use crate::connectors::mysql::cdc::schema::InfoSchemaColumn;
use crate::loaders::cdc_parquet::CdcParquetLoader;

#[derive(Clone)]
pub struct MysqlCdcActivities {
    pub catalog: Arc<Catalog>,
    pub secrets: Arc<crate::secrets::auditing::AuditingSecrets>,
}

fn into_activity_err(e: anyhow::Error) -> ActivityError {
    tracing::error!(
        error = %e,
        chain = ?e.chain().collect::<Vec<_>>(),
        "mysql_cdc activity error"
    );
    e.into()
}

async fn resolve_url(
    secrets: &crate::secrets::auditing::AuditingSecrets,
    conn: &common_types::connection_config::ConnectionConfig,
    tenant_id: uuid::Uuid,
    principal_id: uuid::Uuid,
    jti: uuid::Uuid,
) -> anyhow::Result<String> {
    let resolve_ctx = crate::secrets::auditing::ResolveContext {
        tenant_id: common_types::ids::TenantId::from_uuid_unchecked(tenant_id),
        principal_id: (!principal_id.is_nil())
            .then(|| common_types::ids::PrincipalId::from_uuid_unchecked(principal_id)),
        jti: (!jti.is_nil()).then_some(jti),
    };
    let resolved =
        crate::secrets::resolve_connection_audited(secrets, conn, resolve_ctx).await?;
    Ok(resolved.expect_url().to_string())
}

#[activities]
impl MysqlCdcActivities {
    #[activity]
    pub async fn verify_mysql_config(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: VerifyMysqlConfigInput,
    ) -> Result<(), ActivityError> {
        tracing::info!(
            schema = %input.schema, table = %input.table,
            "mysql_cdc: verify_mysql_config entering"
        );
        let url = resolve_url(
            &self.secrets,
            &input.source_conn,
            input.tenant_id,
            input.principal_id,
            input.jti,
        )
        .await
        .map_err(into_activity_err)?;
        let pool = mysql_async::Pool::new(url.as_str());
        let mut conn = pool
            .get_conn()
            .await
            .context("mysql connect")
            .map_err(into_activity_err)?;

        let gtid_mode: Option<(String, String)> = conn
            .query_first("SHOW GLOBAL VARIABLES LIKE 'gtid_mode'")
            .await
            .context("query gtid_mode")
            .map_err(into_activity_err)?;
        let binlog_format: Option<(String, String)> = conn
            .query_first("SHOW GLOBAL VARIABLES LIKE 'binlog_format'")
            .await
            .context("query binlog_format")
            .map_err(into_activity_err)?;

        let gtid_mode = gtid_mode.map(|t| t.1).unwrap_or_default();
        let binlog_format = binlog_format.map(|t| t.1).unwrap_or_default();

        if !gtid_mode.eq_ignore_ascii_case("ON") {
            return Err(into_activity_err(anyhow!(
                "MySQL gtid_mode must be ON (got '{gtid_mode}')"
            )));
        }
        if !binlog_format.eq_ignore_ascii_case("ROW") {
            return Err(into_activity_err(anyhow!(
                "MySQL binlog_format must be ROW (got '{binlog_format}')"
            )));
        }
        let exists: Option<(i64,)> = conn
            .exec_first(
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = ? AND table_name = ?",
                (&input.schema, &input.table),
            )
            .await
            .context("table exists check")
            .map_err(into_activity_err)?;
        if exists.is_none() {
            return Err(into_activity_err(anyhow!(
                "table {}.{} not found",
                input.schema,
                input.table
            )));
        }
        drop(conn);
        pool.disconnect().await.ok();
        Ok(())
    }

    #[activity]
    pub async fn capture_start_gtid(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: CaptureStartGtidInput,
    ) -> Result<CaptureStartGtidOutput, ActivityError> {
        tracing::info!(
            pipeline_id = %input.pipeline_id,
            "mysql_cdc: capture_start_gtid entering"
        );
        let url = resolve_url(
            &self.secrets,
            &input.source_conn,
            input.tenant_id,
            input.principal_id,
            input.jti,
        )
        .await
        .map_err(into_activity_err)?;
        let pool = mysql_async::Pool::new(url.as_str());
        let mut conn = pool
            .get_conn()
            .await
            .context("mysql connect")
            .map_err(into_activity_err)?;
        let row: Option<(String,)> = conn
            .query_first("SELECT @@GLOBAL.gtid_executed")
            .await
            .context("read gtid_executed")
            .map_err(into_activity_err)?;
        let gtid_str = row.map(|t| t.0).unwrap_or_default();
        // Validate it parses; fail fast on junk.
        GtidSet::parse(&gtid_str)
            .context("parse gtid_executed")
            .map_err(into_activity_err)?;
        drop(conn);
        pool.disconnect().await.ok();
        Ok(CaptureStartGtidOutput {
            gtid_set: gtid_str,
        })
    }

    #[activity]
    pub async fn discover_mysql_schema(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DiscoverMysqlSchemaInput,
    ) -> Result<DiscoverMysqlSchemaOutput, ActivityError> {
        tracing::info!(
            schema = %input.schema, table = %input.table,
            "mysql_cdc: discover_mysql_schema entering"
        );
        let url = resolve_url(
            &self.secrets,
            &input.source_conn,
            input.tenant_id,
            input.principal_id,
            input.jti,
        )
        .await
        .map_err(into_activity_err)?;
        let pool = mysql_async::Pool::new(url.as_str());
        let cols = schema::discover_columns(&pool, &input.schema, &input.table)
            .await
            .map_err(into_activity_err)?;
        // Validate the type map upfront — better to fail at discovery
        // than at first row event.
        let _check = schema::schema_from_columns(&cols).map_err(into_activity_err)?;
        pool.disconnect().await.ok();
        let schema_json = serde_json::to_string(&cols)
            .context("serialize InfoSchemaColumn list")
            .map_err(into_activity_err)?;
        Ok(DiscoverMysqlSchemaOutput { schema_json })
    }

    #[activity]
    pub async fn read_window(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MysqlReadWindowInput,
    ) -> Result<MysqlReadWindowOutput, ActivityError> {
        tracing::info!(
            batch_seq = input.batch_seq,
            schema = %input.schema, table = %input.table,
            start_gtid = %input.start_gtid,
            "mysql_cdc: read_window entering"
        );
        let url = resolve_url(
            &self.secrets,
            &input.source_conn,
            input.tenant_id,
            input.principal_id,
            input.jti,
        )
        .await
        .map_err(into_activity_err)?;
        let cols: Vec<InfoSchemaColumn> = serde_json::from_str(&input.schema_json)
            .context("parse schema_json")
            .map_err(into_activity_err)?;
        let arrow_schema = schema::schema_from_columns(&cols).map_err(into_activity_err)?;
        let arrow_schema = std::sync::Arc::new(arrow_schema);
        let start = GtidSet::parse(&input.start_gtid).map_err(into_activity_err)?;
        let out = stream::read_window(
            &url,
            input.server_id,
            &input.schema,
            &input.table,
            &start,
            input.max_events as usize,
            arrow_schema.clone(),
            input.heartbeat_secs,
            5, // idle timeout: break the loop after 5s with no events
        )
        .await
        .map_err(into_activity_err)?;

        if let Some(batch) = out.batch.as_ref() {
            CdcParquetLoader
                .write(
                    &input.destination,
                    input.tenant_id,
                    input.pipeline_id,
                    input.run_id,
                    input.batch_seq,
                    batch,
                )
                .await
                .map_err(into_activity_err)?;
        }
        Ok(MysqlReadWindowOutput {
            rows: out.rows as u32,
            new_gtid: out.new_gtid.format(),
        })
    }

    #[activity]
    pub async fn mysql_snapshot_chunk(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MysqlSnapshotChunkInput,
    ) -> Result<MysqlSnapshotChunkOutput, ActivityError> {
        tracing::info!(
            batch_seq = input.batch_seq,
            schema = %input.schema, table = %input.table,
            pk_column = %input.pk_column, last_pk = ?input.last_pk,
            "mysql_cdc: snapshot_chunk entering"
        );
        let url = resolve_url(
            &self.secrets,
            &input.source_conn,
            input.tenant_id,
            input.principal_id,
            input.jti,
        )
        .await
        .map_err(into_activity_err)?;
        let cols: Vec<InfoSchemaColumn> = serde_json::from_str(&input.schema_json)
            .context("parse schema_json")
            .map_err(into_activity_err)?;
        // Validate pk_column existence + integer type before opening tx.
        let pk_meta = cols
            .iter()
            .find(|c| c.column_name == input.pk_column)
            .ok_or_else(|| {
                into_activity_err(anyhow!(
                    "pk_column '{}' not found in table {}.{}",
                    input.pk_column,
                    input.schema,
                    input.table
                ))
            })?;
        let pk_dt = schema::map_mysql_type(&pk_meta.data_type)
            .map_err(into_activity_err)?;
        if !matches!(
            pk_dt,
            arrow::datatypes::DataType::Int32 | arrow::datatypes::DataType::Int64
        ) {
            return Err(into_activity_err(anyhow!(
                "snapshot only supports integer pk columns in v1; '{}' is {:?}",
                input.pk_column,
                pk_dt
            )));
        }
        let arrow_schema = schema::schema_from_columns(&cols).map_err(into_activity_err)?;
        let arrow_schema = std::sync::Arc::new(arrow_schema);
        let chunk = snapshot::read_chunk(
            &url,
            &input.schema,
            &input.table,
            &input.pk_column,
            input.last_pk,
            input.batch_size as usize,
            arrow_schema,
            &input.captured_gtid,
        )
        .await
        .map_err(into_activity_err)?;
        if let Some(batch) = chunk.batch.as_ref() {
            CdcParquetLoader
                .write(
                    &input.destination,
                    input.tenant_id,
                    input.pipeline_id,
                    input.run_id,
                    input.batch_seq,
                    batch,
                )
                .await
                .map_err(into_activity_err)?;
        }
        // Per-chunk: persist snapshot state for crash-resume.
        let snap_state = catalog::cdc_snapshot::CdcSnapshotState {
            pipeline_id: common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id),
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
            last_pk: chunk.last_pk,
            completed: false,
            captured_position: input.captured_gtid.clone(),
        };
        let snap_ctx = common_types::ids::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        self.catalog
            .cdc_snapshot_upsert(snap_ctx, &snap_state)
            .await
            .map_err(|e| into_activity_err(anyhow::anyhow!(e)))?;
        Ok(MysqlSnapshotChunkOutput {
            rows: chunk.rows as u32,
            last_pk: chunk.last_pk,
            is_final: chunk.is_final,
        })
    }

    #[activity]
    pub async fn mysql_snapshot_state_get(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MysqlSnapshotStateGetInput,
    ) -> Result<MysqlSnapshotStateGetOutput, ActivityError> {
        let ctx = common_types::ids::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        let pid = common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id);
        let state = self
            .catalog
            .cdc_snapshot_get(ctx, pid)
            .await
            .map_err(|e| into_activity_err(anyhow::anyhow!(e)))?;
        Ok(MysqlSnapshotStateGetOutput {
            last_pk: state.as_ref().and_then(|s| s.last_pk),
            completed: state.as_ref().map(|s| s.completed).unwrap_or(false),
            captured_gtid: state.map(|s| s.captured_position).unwrap_or_default(),
        })
    }

    #[activity]
    pub async fn mysql_snapshot_mark_completed(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MysqlSnapshotMarkCompletedInput,
    ) -> Result<(), ActivityError> {
        let ctx = common_types::ids::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        let pid = common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id);
        self.catalog
            .cdc_snapshot_mark_completed(ctx, pid)
            .await
            .map_err(|e| into_activity_err(anyhow::anyhow!(e)))?;
        Ok(())
    }
}
