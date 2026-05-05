//! Reference CSV source connector (Phase I.3 demo).
//!
//! Config JSON: { "csv_text": "id,name,email\n1,Alice,...\n...", "has_header": true }
//! Cursor: row-index (Int64), strictly increasing from 0.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

wit_bindgen::generate!({
    path: "../../crates/connector-sdk/wit",
    world: "source-connector",
});

use platform::connector::host::{log, LogLevel};
use platform::connector::types::CursorKind;

struct Component;

export!(Component);

#[derive(serde::Deserialize)]
struct CsvConfig {
    csv_text: String,
    #[serde(default = "default_true")]
    has_header: bool,
}

fn default_true() -> bool {
    true
}

impl Guest for Component {
    fn discover(_conn: ConnectionConfig, source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        let cfg: CsvConfig = serde_json::from_str(&source.json)
            .map_err(|e| ConnectorError::InvalidConfig(format!("bad CSV config: {e}")))?;
        let schema = infer_schema(&cfg)?;
        ipc_schema_bytes(&schema).map_err(|e| ConnectorError::Other(format!("ipc: {e}")))
    }

    fn read_batch(
        _conn: ConnectionConfig,
        source: SourceConfig,
        cursor: Option<CursorValue>,
        batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        log(
            LogLevel::Info,
            &format!("csv: read_batch cursor={cursor:?}"),
        );
        let cfg: CsvConfig = serde_json::from_str(&source.json)
            .map_err(|e| ConnectorError::InvalidConfig(format!("bad CSV config: {e}")))?;
        let schema = infer_schema(&cfg)?;

        let start_row: i64 = match cursor.as_ref() {
            None => 0,
            Some(c) => c
                .value
                .parse()
                .map_err(|_| ConnectorError::InvalidConfig("cursor not i64".into()))?,
        };

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(cfg.has_header)
            .from_reader(cfg.csv_text.as_bytes());

        let mut rows_in_batch = 0u32;
        let mut collected: Vec<Vec<String>> = Vec::with_capacity(batch_size as usize);
        let mut last_row_idx: i64 = start_row;
        let mut logical_row_idx: i64 = 0;

        for result in rdr.records() {
            let rec = result.map_err(|e| ConnectorError::Other(format!("csv: {e}")))?;
            if logical_row_idx < start_row {
                logical_row_idx += 1;
                continue;
            }
            if rows_in_batch >= batch_size {
                break;
            }
            let row: Vec<String> = rec.iter().map(|s| s.to_string()).collect();
            collected.push(row);
            rows_in_batch += 1;
            last_row_idx = logical_row_idx + 1;
            logical_row_idx += 1;
        }

        // Detect is_final.
        let mut rdr2 = csv::ReaderBuilder::new()
            .has_headers(cfg.has_header)
            .from_reader(cfg.csv_text.as_bytes());
        let total_rows = rdr2.records().count() as i64;
        let is_final = last_row_idx >= total_rows;

        let batch = build_batch(&schema, &collected)
            .map_err(|e| ConnectorError::Other(format!("build batch: {e}")))?;
        let batch_ipc = ipc_batch_bytes(&schema, &batch)
            .map_err(|e| ConnectorError::Other(format!("ipc batch: {e}")))?;

        Ok(ReadOutcome {
            batch_ipc,
            rows: rows_in_batch,
            new_cursor: if rows_in_batch == 0 {
                None
            } else {
                Some(CursorValue {
                    kind: CursorKind::Int64,
                    value: last_row_idx.to_string(),
                })
            },
            is_final,
            stream_name: None,
        })
    }
}

fn infer_schema(cfg: &CsvConfig) -> Result<Arc<Schema>, ConnectorError> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(cfg.has_header)
        .from_reader(cfg.csv_text.as_bytes());
    let headers: Vec<String> = if cfg.has_header {
        rdr.headers()
            .map_err(|e| ConnectorError::Other(format!("csv header: {e}")))?
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        let first = rdr
            .records()
            .next()
            .and_then(|r| r.ok())
            .map(|r| r.len())
            .unwrap_or(0);
        (0..first).map(|i| format!("column_{}", i)).collect()
    };
    let mut fields = vec![Field::new("_row_index", DataType::Int64, false)];
    for h in &headers {
        fields.push(Field::new(h, DataType::Utf8, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn build_batch(
    schema: &Arc<Schema>,
    rows: &[Vec<String>],
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let n = rows.len();
    let row_idx: Int64Array = (0..n as i64).collect();
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(row_idx)];

    let num_data_cols = schema.fields().len().saturating_sub(1);
    for c in 0..num_data_cols {
        let vals: Vec<Option<&str>> = rows
            .iter()
            .map(|row| if c < row.len() { Some(row[c].as_str()) } else { None })
            .collect();
        let arr: StringArray = vals.into_iter().collect();
        arrays.push(Arc::new(arr));
    }
    RecordBatch::try_new(schema.clone(), arrays)
}

fn ipc_schema_bytes(schema: &Arc<Schema>) -> Result<Vec<u8>, arrow_schema::ArrowError> {
    let mut buf = Vec::new();
    let mut w = StreamWriter::try_new(&mut buf, schema)?;
    w.finish()?;
    Ok(buf)
}

fn ipc_batch_bytes(
    schema: &Arc<Schema>,
    batch: &RecordBatch,
) -> Result<Vec<u8>, arrow_schema::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema)?;
        if batch.num_rows() > 0 {
            w.write(batch)?;
        }
        w.finish()?;
    }
    Ok(buf)
}
