//! Snowflake destination loader (RFC-9, phase 2-5a MVP).
//!
//! Idempotency: `_ETL_LOADED_BATCHES` log table, checked before writing;
//! data DML + log INSERT execute in one multi-statement transaction
//! (BEGIN; ...; COMMIT). Same LoadId retried ⇒ short-circuit no-op
//! (Temporal serializes retries, so check-then-write has no concurrent
//! writer). Append (`pk_columns: []`) uses INSERT; upsert uses
//! MERGE-from-VALUES. A batch carrying `_cdc.op` is applied by key
//! (2-5b, see `cdc`): DELETE for keys whose last change is a delete, MERGE
//! for the rest. `LoadId.stream_name`, when set, names the batch's
//! target table inside spec.schema (2-5c, as the PG loader's 2-4c). A
//! CDC `c` event evolves the destination first (2-5d, see `evolution`).
//! Deferred: stage+COPY fast path, VARIANT.

pub mod cdc;
pub mod client;
pub mod evolution;
pub mod jwt;
pub mod sql;

use anyhow::{bail, Context};
use arrow::record_batch::RecordBatch;
use common_types::pipeline_spec::{DestinationSpec, SnowflakeDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId, LoadResult};

pub struct SnowflakeLoader;

/// The batch's target table: `stream_name` when the connector set one
/// (multi-table pipelines), else `spec.table`. Empty only for
/// single-table pipelines, so their target is unchanged.
fn target_table<'a>(
    spec: &'a SnowflakeDestinationSpec,
    stream_name: &'a str,
) -> anyhow::Result<&'a str> {
    let table = if stream_name.is_empty() {
        spec.table.as_str()
    } else {
        stream_name
    };
    sql::quote_ident(table).context("target table")?;
    Ok(table)
}

fn spec_of(dest: &DestinationSpec) -> anyhow::Result<&SnowflakeDestinationSpec> {
    match dest {
        DestinationSpec::Snowflake(s) => Ok(s),
        other => bail!(
            "SnowflakeLoader invoked with non-snowflake destination: {}",
            other.kind()
        ),
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
        let table = target_table(spec, &load_id.stream_name)?;
        let path = format!(
            "snowflake://{}/{}/{}/{}",
            spec.account, spec.database, spec.schema, table
        );
        if batch.num_rows() == 0 {
            return Ok(LoadResult {
                rows_loaded: 0,
                bytes_written: 0,
                path,
            });
        }
        // Planned before any request so a bad CDC batch fails fast.
        let cdc_plan = crate::loaders::postgres::is_cdc_batch(batch.schema().as_ref())
            .then(|| cdc::plan(&batch, &spec.pk_columns))
            .transpose()?;
        let target_schema = match &cdc_plan {
            Some(p) => p.upserts.schema(),
            None => batch.schema(),
        };
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
                table,
                target_schema.as_ref(),
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

        if cdc_plan.as_ref().is_some_and(|p| p.schema_change) {
            // Snowflake DDL commits implicitly, so it can't join the data
            // transaction: apply it first. evolution_ddl errors on a
            // destructive change before anything runs; ADD COLUMN IF NOT
            // EXISTS is safe to repeat on retry.
            let cols = client
                .execute(&evolution::dest_columns_sql(db, sch, table)?)
                .await
                .context("reading destination columns")?;
            let cols = evolution::parse_dest_columns(&cols)?;
            for stmt in evolution::evolution_ddl(db, sch, table, &cols, target_schema.as_ref())? {
                client
                    .execute(&stmt)
                    .await
                    .context("applying schema evolution")?;
            }
        }

        let (dml, rows_loaded) = match &cdc_plan {
            None if spec.pk_columns.is_empty() => (
                vec![sql::append_insert_sql(db, sch, table, &batch)?],
                batch.num_rows(),
            ),
            None => (
                vec![sql::merge_upsert_sql(
                    db,
                    sch,
                    table,
                    &batch,
                    &spec.pk_columns,
                )?],
                batch.num_rows(),
            ),
            // Delete and upsert keys are disjoint, so their order is free.
            Some(p) => {
                let mut dml = Vec::new();
                if p.deletes.num_rows() > 0 {
                    dml.push(sql::delete_keys_sql(
                        db,
                        sch,
                        table,
                        &p.deletes,
                        &spec.pk_columns,
                    )?);
                }
                if p.upserts.num_rows() > 0 {
                    dml.push(sql::merge_upsert_sql(
                        db,
                        sch,
                        table,
                        &p.upserts,
                        &spec.pk_columns,
                    )?);
                }
                (dml, p.applied)
            }
        };
        let mut stmts = vec!["BEGIN".to_string()];
        stmts.extend(dml);
        stmts.push(sql::log_insert_sql(db, sch, &load_id, rows_loaded)?);
        stmts.push("COMMIT".to_string());
        client
            .execute_multi(&stmts)
            .await
            .context("transactional load")?;

        Ok(LoadResult {
            rows_loaded,
            bytes_written: batch.get_array_memory_size() as u64,
            path,
        })
    }
}

#[cfg(test)]
mod loader_tests {
    use super::*;
    use loader_sdk::DestinationLoader;

    fn sf_spec(table: &str) -> SnowflakeDestinationSpec {
        SnowflakeDestinationSpec {
            account: "a".into(),
            user: "u".into(),
            private_key_env: "K".into(),
            warehouse: "w".into(),
            database: "d".into(),
            schema: "s".into(),
            table: table.into(),
            role: None,
            pk_columns: vec![],
        }
    }

    #[test]
    fn target_table_prefers_stream_name_and_falls_back_to_spec() {
        let spec = sf_spec("fallback");
        assert_eq!(target_table(&spec, "public.users").unwrap(), "public.users");
        assert_eq!(target_table(&spec, "").unwrap(), "fallback");
        assert!(target_table(&spec, "evil\"name").is_err());
        assert!(target_table(&spec, "nul\0").is_err());
        assert!(target_table(&sf_spec(""), "").is_err());
    }

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

    #[tokio::test]
    async fn wrong_variant_error_does_not_leak_spec() {
        let spec = common_types::pipeline_spec::DestinationSpec::Postgres(
            common_types::pipeline_spec::PostgresDestinationSpec {
                connection_url: "postgres://etl:hunter2@db.internal/etl".into(),
                schema: "public".into(),
                table: "t".into(),
                pk_columns: vec![],
            },
        );
        let err = SnowflakeLoader.validate(&spec).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(!msg.contains("hunter2"), "got {msg}");
        assert!(msg.contains("postgres"), "got {msg}");
    }
}
