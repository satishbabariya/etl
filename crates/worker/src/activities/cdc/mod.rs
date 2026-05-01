pub mod inputs;

use arrow::datatypes::DataType;
use catalog::{cdc::{CdcSlot, SlotState}, Catalog};
use common_types::ids::{PipelineId, TenantId};
use inputs::*;
use metrics;
use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::connectors::postgres::cdc::{slot, snapshot, stream};
use crate::loaders::cdc_parquet::CdcParquetLoader;

#[derive(Clone)]
pub struct CdcActivities {
    pub catalog: Arc<Catalog>,
    pub secrets: Arc<crate::secrets::auditing::AuditingSecrets>,
}

fn retryable(e: anyhow::Error) -> ActivityError {
    tracing::error!(error = %e, chain = ?e.chain().collect::<Vec<_>>(), "cdc activity returning retryable error");
    e.into()
}

#[activities]
impl CdcActivities {
    #[activity]
    pub async fn ensure_slot(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: EnsureSlotInput,
    ) -> Result<EnsureSlotOutput, ActivityError> {
        tracing::info!(pipeline_id = %input.pipeline_id, "cdc: ensure_slot entering");
        let slot_name = format!("etl_{}", input.pipeline_id.as_simple());
        let pub_name = format!("etl_{}_pub", input.pipeline_id.as_simple());
        let qualified = format!("{}.{}", input.schema, input.table);
        let resolve_ctx = crate::secrets::auditing::ResolveContext {
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
            principal_id: (!input.principal_id.is_nil())
                .then(|| common_types::ids::PrincipalId::from_uuid_unchecked(input.principal_id)),
            jti: (!input.jti.is_nil()).then_some(input.jti),
        };
        let resolved = crate::secrets::resolve_connection_audited(
            self.secrets.as_ref(),
            &input.source_conn,
            resolve_ctx,
        )
        .await
        .map_err(retryable)?;
        let url = resolved.expect_url();
        slot::ensure_publication(url, &pub_name, &qualified)
            .await
            .map_err(retryable)?;
        let r = slot::ensure_slot(url, &slot_name)
            .await
            .map_err(retryable)?;
        let pid = PipelineId::from_uuid_unchecked(input.pipeline_id);
        let tid = TenantId::from_uuid_unchecked(input.tenant_id);
        let ctx = common_types::ids::TenantContext::new(tid);
        self.catalog
            .cdc_upsert(
                ctx,
                &CdcSlot {
                    pipeline_id: pid,
                    tenant_id: tid,
                    slot_name: r.slot_name.clone(),
                    publication_name: pub_name.clone(),
                    consistent_point: r.consistent_point.clone(),
                    confirmed_flush: None,
                    state: SlotState::Active,
                },
            )
            .await
            .map_err(|e| retryable(anyhow::anyhow!(e)))?;
        Ok(EnsureSlotOutput {
            slot_name: r.slot_name,
            publication_name: pub_name,
            consistent_point: r.consistent_point,
            created: r.created,
        })
    }

    #[activity]
    pub async fn snapshot_chunk(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: SnapshotChunkInput,
    ) -> Result<SnapshotChunkOutput, ActivityError> {
        tracing::info!(last_pk = ?input.last_pk, batch_seq = input.batch_seq, "cdc: snapshot_chunk entering");
        // MVP: pk column rendered as text alongside data columns (SELECT *).
        // We don't know the column list until we query; for first pass we
        // just use the pk as the only "data" column and let the actual
        // snapshot query return whatever it returns. rows_to_cdc_batch
        // iterates the schema we pass in; we pass a schema listing just
        // the pk_col and rely on the Parquet loader preserving _cdc.*.
        let cdc_schema =
            snapshot::cdc_schema_for(&[(input.pk_col.as_str(), DataType::Utf8)]);
        let resolve_ctx = crate::secrets::auditing::ResolveContext {
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
            principal_id: (!input.principal_id.is_nil())
                .then(|| common_types::ids::PrincipalId::from_uuid_unchecked(input.principal_id)),
            jti: (!input.jti.is_nil()).then_some(input.jti),
        };
        let resolved = crate::secrets::resolve_connection_audited(
            self.secrets.as_ref(),
            &input.source_conn,
            resolve_ctx,
        )
        .await
        .map_err(retryable)?;
        let chunk = snapshot::read_chunk(
            resolved.expect_url(),
            &input.schema,
            &input.table,
            &input.pk_col,
            input.last_pk,
            input.batch_size,
            &input.consistent_point,
            cdc_schema,
        )
        .await
        .map_err(retryable)?;
        metrics::counter!(
            crate::metrics::CDC_EVENTS,
            "op" => "s",
            "tenant_id" => input.tenant_id.to_string(),
        )
            .increment(chunk.batch.num_rows() as u64);
        if chunk.batch.num_rows() > 0 {
            CdcParquetLoader
                .write(
                    &input.destination,
                    input.tenant_id,
                    input.pipeline_id,
                    input.run_id,
                    input.batch_seq,
                    &chunk.batch,
                )
                .await
                .map_err(retryable)?;
        }
        Ok(SnapshotChunkOutput {
            rows: chunk.batch.num_rows(),
            is_final: chunk.is_final,
            last_pk: chunk.last_pk,
        })
    }

    #[activity]
    pub async fn read_window(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ReadWindowInput,
    ) -> Result<ReadWindowOutput, ActivityError> {
        tracing::info!(slot = %input.slot_name, batch_seq = input.batch_seq, "cdc: read_window entering");
        let resolve_ctx = crate::secrets::auditing::ResolveContext {
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
            principal_id: (!input.principal_id.is_nil())
                .then(|| common_types::ids::PrincipalId::from_uuid_unchecked(input.principal_id)),
            jti: (!input.jti.is_nil()).then_some(input.jti),
        };
        let resolved = crate::secrets::resolve_connection_audited(
            self.secrets.as_ref(),
            &input.source_conn,
            resolve_ctx,
        )
        .await
        .map_err(retryable)?;
        let out = stream::read_window(
            resolved.expect_url(),
            &input.slot_name,
            &input.publication_name,
            input.start_lsn.as_deref(),
            input.max_events,
            Default::default(),
        )
        .await
        .map_err(retryable)?;
        if out.is_empty {
            return Ok(ReadWindowOutput { rows: 0, new_lsn: None });
        }
        let rel_id_opt = out
            .relations
            .values()
            .find(|r| format!("{}.{}", r.namespace, r.name) == input.table_rel_name)
            .map(|r| r.rel_id);
        let rel_id = match rel_id_opt {
            Some(r) => r,
            // No Relation message seen yet (can happen if the slot only emitted
            // Begin/Commit with no data rows for our table this window).
            None => return Ok(ReadWindowOutput { rows: 0, new_lsn: None }),
        };
        let rel = out.relations.get(&rel_id).unwrap();
        let cols: Vec<(&str, DataType)> = rel
            .columns
            .iter()
            .map(|c| {
                (
                    c.name.as_str(),
                    crate::connectors::postgres::cdc::types::pg_oid_to_arrow_type(c.type_oid),
                )
            })
            .collect();
        let schema = snapshot::cdc_schema_for(&cols);
        let batch =
            stream::events_to_batch(&out.events, &out.relations, rel_id, schema)
                .map_err(retryable)?;
        let rows = batch.num_rows();
        for ev in &out.events {
            let op = match ev {
                crate::connectors::postgres::cdc::CdcEvent::Insert { .. } => "i",
                crate::connectors::postgres::cdc::CdcEvent::Update { .. } => "u",
                crate::connectors::postgres::cdc::CdcEvent::Delete { .. } => "d",
                _ => continue,
            };
            metrics::counter!(
                crate::metrics::CDC_EVENTS,
                "op" => op,
                "tenant_id" => input.tenant_id.to_string(),
            )
            .increment(1);
        }
        if rows > 0 {
            CdcParquetLoader
                .write(
                    &input.destination,
                    input.tenant_id,
                    input.pipeline_id,
                    input.run_id,
                    input.batch_seq,
                    &batch,
                )
                .await
                .map_err(retryable)?;
        }
        let new_lsn = out.new_position.map(common_types::cursor::lsn_to_string);
        if let Some(lsn) = &new_lsn {
            let pid = PipelineId::from_uuid_unchecked(input.pipeline_id);
            let tid = TenantId::from_uuid_unchecked(input.tenant_id);
            let ctx = common_types::ids::TenantContext::new(tid);
            self.catalog
                .cdc_update_confirmed_flush(ctx, pid, lsn)
                .await
                .map_err(|e| retryable(anyhow::anyhow!(e)))?;
        }
        Ok(ReadWindowOutput { rows, new_lsn })
    }

    #[activity]
    pub async fn release_slot(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ReleaseSlotInput,
    ) -> Result<(), ActivityError> {
        if let Ok(resolved) =
            crate::secrets::resolve_connection(self.secrets.as_ref(), &input.source_conn).await
        {
            let _ = slot::release_slot(resolved.expect_url(), &input.slot_name).await;
        }
        Ok(())
    }
}
