//! Periodic poll of Postgres `pg_replication_slots.confirmed_flush_lsn`
//! lag, emitted as a gauge per slot. Lives as a long-running tokio
//! task on the worker; not invoked per-activity.

use catalog::Catalog;
use std::sync::Arc;
use std::time::Duration;

pub fn spawn_slot_lag_poller<F>(
    catalog: Arc<Catalog>,
    source_url_resolver: F,
    interval: Duration,
) where
    F: Fn(uuid::Uuid) -> Option<String> + Send + Sync + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            // Admin-mode query — bypass RLS so we see every tenant's slot.
            // We connect via etl_app pool but issue SET LOCAL app.tenant_id = ''
            // (admin) inside a tx.
            let mut tx = match catalog.pool().begin().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(error = %e, "slot-lag poller: begin tx failed");
                    continue;
                }
            };
            if let Err(e) = sqlx::query("SET LOCAL app.tenant_id = ''")
                .execute(&mut *tx)
                .await
            {
                tracing::warn!(error = %e, "slot-lag poller: SET LOCAL failed");
                continue;
            }
            let rows = match sqlx::query_as::<_, (uuid::Uuid, String, uuid::Uuid)>(
                "SELECT pipeline_id, slot_name, tenant_id FROM cdc_slots WHERE state = 'active'",
            )
            .fetch_all(&mut *tx)
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(error = %e, "slot-lag poller: catalog query failed");
                    continue;
                }
            };
            let _ = tx.commit().await;
            for (pipeline_id, slot_name, tenant_id) in rows {
                let Some(url) = source_url_resolver(pipeline_id) else { continue };
                match crate::connectors::postgres::cdc::slot::slot_lag_bytes(&url, &slot_name).await
                {
                    Ok(lag) => {
                        metrics::gauge!(
                            crate::metrics::CDC_SLOT_LAG_BYTES,
                            "pipeline_id" => pipeline_id.to_string(),
                            "tenant_id" => tenant_id.to_string(),
                        )
                        .set(lag as f64);
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, slot = %slot_name, "slot-lag poll failed");
                    }
                }
            }
        }
    });
}
