use crate::cursor::CursorKind;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub source: SourceSpec,
    pub destination: DestinationSpec,
    /// Max rows per read_batch activity call.
    pub batch_size: usize,
    #[serde(default)]
    pub transform: Option<crate::transform::TransformSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceSpec {
    Postgres(PostgresSourceSpec),
    Wasm(WasmSourceSpec),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    #[default]
    Cursor,
    Cdc,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostgresSourceSpec {
    pub schema: String,
    pub table: String,
    pub cursor_column: String,
    pub cursor_kind: CursorKind,
    pub pk_columns: Vec<String>,
    #[serde(default)]
    pub sync_mode: SyncMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmSourceSpec {
    /// Free-form JSON passed as-is to the guest via `source-config.json`.
    pub config: serde_json::Value,
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
                sync_mode: SyncMode::Cursor,
            }),
            destination: DestinationSpec::LocalParquet(LocalParquetSpec {
                base_path: "./data".into(),
            }),
            batch_size: 100,
            transform: None,
        };
        let j = serde_json::to_string(&s).unwrap();
        let back: PipelineSpec = serde_json::from_str(&j).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), j);
    }

    #[test]
    fn wasm_variant_roundtrips() {
        let s = SourceSpec::Wasm(WasmSourceSpec {
            config: serde_json::json!({"path": "/tmp/foo.csv", "has_header": true}),
        });
        let j = serde_json::to_string(&s).unwrap();
        let back: SourceSpec = serde_json::from_str(&j).unwrap();
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
            sync_mode: SyncMode::Cursor,
        });
        let j: serde_json::Value = serde_json::to_value(&s).unwrap();
        assert_eq!(j["type"], "postgres");
    }

    #[test]
    fn postgres_sync_mode_defaults_to_cursor() {
        let j = r#"{
            "type": "postgres", "schema": "public", "table": "customers",
            "cursor_column": "updated_at", "cursor_kind": "timestamp_tz",
            "pk_columns": ["id"]
        }"#;
        let s: SourceSpec = serde_json::from_str(j).unwrap();
        if let SourceSpec::Postgres(p) = s {
            assert_eq!(p.sync_mode, SyncMode::Cursor);
        } else {
            panic!("expected Postgres variant");
        }
    }

    #[test]
    fn postgres_sync_mode_cdc_parses() {
        let j = r#"{
            "type": "postgres", "schema": "public", "table": "customers",
            "cursor_column": "updated_at", "cursor_kind": "timestamp_tz",
            "pk_columns": ["id"], "sync_mode": "cdc"
        }"#;
        let s: SourceSpec = serde_json::from_str(j).unwrap();
        if let SourceSpec::Postgres(p) = s {
            assert_eq!(p.sync_mode, SyncMode::Cdc);
        } else {
            panic!();
        }
    }
}
