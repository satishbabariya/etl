//! Rust-native Postgres source connector (Phase I.2).
pub mod discover;
pub mod read;

use async_trait::async_trait;
use connector_sdk::{ReadOutcome, SourceConnector};

pub struct PostgresConnector;

#[async_trait]
impl SourceConnector for PostgresConnector {
    async fn discover(
        &self,
        conn: &common_types::connection_config::ConnectionConfig,
        source: &common_types::pipeline_spec::SourceSpec,
    ) -> anyhow::Result<arrow::datatypes::SchemaRef> {
        match source {
            common_types::pipeline_spec::SourceSpec::Postgres(pg) => {
                discover::run(conn, pg).await
            }
            common_types::pipeline_spec::SourceSpec::Wasm(_) => {
                anyhow::bail!("PostgresConnector received a SourceSpec::Wasm — dispatcher bug")
            }
        }
    }

    async fn read_batch(
        &self,
        conn: &common_types::connection_config::ConnectionConfig,
        source: &common_types::pipeline_spec::SourceSpec,
        cursor: Option<common_types::cursor::CursorValue>,
        batch_size: usize,
    ) -> anyhow::Result<ReadOutcome> {
        match source {
            common_types::pipeline_spec::SourceSpec::Postgres(pg) => {
                read::run(conn, pg, cursor, batch_size).await
            }
            common_types::pipeline_spec::SourceSpec::Wasm(_) => {
                anyhow::bail!("PostgresConnector received a SourceSpec::Wasm — dispatcher bug")
            }
        }
    }
}
