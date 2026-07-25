//! Snowflake destination loader (RFC-9, phase 2-5a MVP).
//!
//! Idempotency: `_ETL_LOADED_BATCHES` log table, checked before writing;
//! data DML + log INSERT execute in one multi-statement transaction
//! (BEGIN; ...; COMMIT). Same LoadId retried ⇒ short-circuit no-op
//! (Temporal serializes retries, so check-then-write has no concurrent
//! writer). Append (`pk_columns: []`) uses INSERT; upsert uses
//! MERGE-from-VALUES. Deferred (see 2-5a design memo): stage+COPY fast
//! path, CDC apply, schema evolution, multi-table routing, VARIANT.

pub mod client;
pub mod jwt;
pub mod sql;

use anyhow::{bail, Context};
use arrow::record_batch::RecordBatch;
use common_types::pipeline_spec::{DestinationSpec, SnowflakeDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId, LoadResult};

pub struct SnowflakeLoader;

fn spec_of(dest: &DestinationSpec) -> anyhow::Result<&SnowflakeDestinationSpec> {
    match dest {
        DestinationSpec::Snowflake(s) => Ok(s),
        other => bail!("SnowflakeLoader invoked with non-snowflake destination: {other:?}"),
    }
}

#[async_trait::async_trait]
impl DestinationLoader for SnowflakeLoader {
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()> {
        let spec = spec_of(dest)?;
        let pem = std::env::var(&spec.private_key_env).with_context(|| {
            format!("env var {} (private_key_env) not set", spec.private_key_env)
        })?;
        jwt::KeyPairAuth::from_pem(&spec.account, &spec.user, &pem)?;
        sql::qualified(&spec.database, &spec.schema, &spec.table)?;
        Ok(())
    }

    async fn load(
        &self,
        dest: &DestinationSpec,
        load_id: LoadId,
        batch: RecordBatch,
    ) -> anyhow::Result<LoadResult> {
        let spec = spec_of(dest)?;
        let path = format!(
            "snowflake://{}/{}/{}/{}",
            spec.account, spec.database, spec.schema, spec.table
        );
        if batch.num_rows() == 0 {
            return Ok(LoadResult {
                rows_loaded: 0,
                bytes_written: 0,
                path,
            });
        }
        let client = client::SnowflakeClient::from_spec(spec)?;
        let (db, sch) = (&spec.database, &spec.schema);

        client
            .execute(&sql::ensure_log_table_ddl(db, sch)?)
            .await
            .context("ensuring idempotency log table")?;
        client
            .execute(&sql::ensure_target_table_ddl(
                db,
                sch,
                &spec.table,
                batch.schema().as_ref(),
            )?)
            .await
            .context("ensuring target table")?;

        let check = client
            .execute(&sql::check_loaded_sql(db, sch, &load_id)?)
            .await
            .context("checking idempotency log")?;
        if client::rowset_len(&check) > 0 {
            tracing::info!(
                target: "loader.snowflake",
                batch_seq = load_id.batch_seq,
                "batch already loaded; idempotent no-op"
            );
            return Ok(LoadResult {
                rows_loaded: 0,
                bytes_written: 0,
                path,
            });
        }

        let dml = if spec.pk_columns.is_empty() {
            sql::append_insert_sql(db, sch, &spec.table, &batch)?
        } else {
            sql::merge_upsert_sql(db, sch, &spec.table, &batch, &spec.pk_columns)?
        };
        let stmts = vec![
            "BEGIN".to_string(),
            dml,
            sql::log_insert_sql(db, sch, &load_id, batch.num_rows())?,
            "COMMIT".to_string(),
        ];
        client
            .execute_multi(&stmts)
            .await
            .context("transactional load")?;

        Ok(LoadResult {
            rows_loaded: batch.num_rows(),
            bytes_written: batch.get_array_memory_size() as u64,
            path,
        })
    }
}

#[cfg(test)]
mod loader_tests {
    use super::*;
    use loader_sdk::DestinationLoader;

    #[tokio::test]
    async fn validate_rejects_missing_env() {
        let spec = common_types::pipeline_spec::DestinationSpec::Snowflake(
            common_types::pipeline_spec::SnowflakeDestinationSpec {
                account: "a".into(),
                user: "u".into(),
                private_key_env: "DEFINITELY_NOT_SET_VAR_2_5A".into(),
                warehouse: "w".into(),
                database: "d".into(),
                schema: "s".into(),
                table: "t".into(),
                role: None,
                pk_columns: vec![],
            },
        );
        let err = SnowflakeLoader
            .validate(&spec)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("DEFINITELY_NOT_SET_VAR_2_5A"), "got {err}");
    }

    #[tokio::test]
    async fn validate_rejects_wrong_variant() {
        let spec = common_types::pipeline_spec::DestinationSpec::LocalParquet(
            common_types::pipeline_spec::LocalParquetSpec {
                base_path: "/tmp".into(),
            },
        );
        assert!(SnowflakeLoader.validate(&spec).await.is_err());
    }
}
