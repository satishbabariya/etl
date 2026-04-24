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
            let rows = match sqlx::query_as::<_, (uuid::Uuid, String)>(
                "SELECT pipeline_id, slot_name FROM cdc_slots WHERE state = 'active'",
            )
            .fetch_all(catalog.pool())
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(error = %e, "slot-lag poller: catalog query failed");
                    continue;
                }
            };
            for (pipeline_id, slot_name) in rows {
                let Some(url) = source_url_resolver(pipeline_id) else { continue };
                match crate::connectors::postgres::cdc::slot::slot_lag_bytes(&url, &slot_name).await
                {
                    Ok(lag) => {
                        metrics::gauge!(
                            crate::metrics::CDC_SLOT_LAG_BYTES,
                            "pipeline_id" => pipeline_id.to_string(),
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
