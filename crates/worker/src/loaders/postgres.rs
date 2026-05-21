//! Postgres destination loader (RFC-9). MVP: insert-only or
//! `ON CONFLICT DO UPDATE`, per-call transaction, idempotency log.
//!
//! Scope cuts (see plan): no CDC op-aware DELETE, no mid-run schema
//! evolution, no soft delete, no dead-letter routing.

use anyhow::{Context, bail};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId, LoadResult};

pub struct PostgresLoader;

#[async_trait]
impl DestinationLoader for PostgresLoader {
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()> {
        let _spec = postgres_spec(dest)?;
        // Connectivity check arrives in Task 4.
        Ok(())
    }

    async fn load(
        &self,
        dest: &DestinationSpec,
        _load_id: LoadId,
        _batch: RecordBatch,
    ) -> anyhow::Result<LoadResult> {
        let _spec = postgres_spec(dest)?;
        bail!("PostgresLoader::load not yet implemented");
    }
}

fn postgres_spec(dest: &DestinationSpec) -> anyhow::Result<&PostgresDestinationSpec> {
    match dest {
        DestinationSpec::Postgres(s) => Ok(s),
        other => bail!("PostgresLoader received non-postgres destination: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};

    #[tokio::test]
    async fn validate_rejects_non_postgres_spec() {
        let loader = PostgresLoader;
        let spec = DestinationSpec::LocalParquet(
            common_types::pipeline_spec::LocalParquetSpec { base_path: "/tmp".into() },
        );
        let err = loader.validate(&spec).await.unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("postgres"));
    }
}
