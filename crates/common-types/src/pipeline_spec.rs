use crate::cursor::CursorKind;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub source: SourceSpec,
    pub destination: DestinationSpec,
    /// Max rows per read_batch activity call.
    pub batch_size: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceSpec {
    Postgres(PostgresSourceSpec),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostgresSourceSpec {
    pub schema: String,
    pub table: String,
    pub cursor_column: String,
    pub cursor_kind: CursorKind,
    pub pk_columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DestinationSpec {
    LocalParquet(LocalParquetSpec),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalParquetSpec {
    /// Directory where Parquet files will be written. Created on demand.
    pub base_path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_roundtrip_pg_to_parquet() {
        let s = PipelineSpec {
            source: SourceSpec::Postgres(PostgresSourceSpec {
                schema: "public".into(),
                table: "customers".into(),
                cursor_column: "updated_at".into(),
                cursor_kind: CursorKind::TimestampTz,
                pk_columns: vec!["id".into()],
            }),
            destination: DestinationSpec::LocalParquet(LocalParquetSpec {
                base_path: "./data".into(),
            }),
            batch_size: 100,
        };
        let j = serde_json::to_string(&s).unwrap();
        let back: PipelineSpec = serde_json::from_str(&j).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), j);
    }

    #[test]
    fn source_serialized_form_is_tagged() {
        let s = SourceSpec::Postgres(PostgresSourceSpec {
            schema: "public".into(),
            table: "t".into(),
            cursor_column: "c".into(),
            cursor_kind: CursorKind::Int64,
            pk_columns: vec!["id".into()],
        });
        let j: serde_json::Value = serde_json::to_value(&s).unwrap();
        assert_eq!(j["type"], "postgres");
    }
}
