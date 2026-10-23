//! Rust-native Postgres source connector (Phase I.2).
pub mod cdc;
pub mod discover;
pub mod read;

use async_trait::async_trait;
use connector_sdk::{ReadOutcome, SourceConnector};

/// Quote `name` as a Postgres delimited identifier: wrap in `"` and double
/// any embedded `"`. NUL is rejected (Postgres can't store it in a name).
/// Shared by the source connector and the Postgres destination loader.
pub(crate) fn quote_ident(name: &str) -> anyhow::Result<String> {
    if name.contains('\0') {
        anyhow::bail!("postgres identifier {name:?} contains a NUL byte");
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

pub struct PostgresConnector;

#[async_trait]
impl SourceConnector for PostgresConnector {
    async fn discover(
        &self,
        conn: &common_types::connection_config::ConnectionConfig,
        source: &common_types::pipeline_spec::SourceSpec,
    ) -> anyhow::Result<arrow::datatypes::SchemaRef> {
        match source {
            common_types::pipeline_spec::SourceSpec::Postgres(pg) => discover::run(conn, pg).await,
            common_types::pipeline_spec::SourceSpec::Wasm(_) => {
                anyhow::bail!("PostgresConnector received a SourceSpec::Wasm — dispatcher bug")
            }
            common_types::pipeline_spec::SourceSpec::MysqlCdc(_) => {
                anyhow::bail!("PostgresConnector received a SourceSpec::MysqlCdc — dispatcher bug")
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
            common_types::pipeline_spec::SourceSpec::MysqlCdc(_) => {
                anyhow::bail!("PostgresConnector received a SourceSpec::MysqlCdc — dispatcher bug")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_wraps_plain_names() {
        assert_eq!(quote_ident("orders").unwrap(), "\"orders\"");
    }

    #[test]
    fn quote_ident_doubles_embedded_quotes() {
        assert_eq!(
            quote_ident("a\"; DROP TABLE x; --").unwrap(),
            "\"a\"\"; DROP TABLE x; --\""
        );
    }

    #[test]
    fn quote_ident_rejects_nul() {
        let err = quote_ident("a\0b").unwrap_err();
        assert!(format!("{err}").contains("NUL"), "got: {err}");
    }
}
