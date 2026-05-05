//! Minimal WASM source connector — emits three fixed rows once, then
//! EOF. The smallest viable example. Schema: id Int64, greeting Utf8.

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

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("greeting", DataType::Utf8, false),
    ])
}

fn ipc_schema_bytes(schema: &Schema) -> Result<Vec<u8>, arrow_schema::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema)?;
        w.finish()?;
    }
    Ok(buf)
}

fn ipc_batch_bytes(schema: &Schema, batch: &RecordBatch) -> Result<Vec<u8>, arrow_schema::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema)?;
        w.write(batch)?;
        w.finish()?;
    }
    Ok(buf)
}

impl Guest for Component {
    fn discover(_conn: ConnectionConfig, _source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        let s = schema();
        ipc_schema_bytes(&s).map_err(|e| ConnectorError::Other(format!("ipc: {e}")))
    }

    fn read_batch(
        _conn: ConnectionConfig,
        _source: SourceConfig,
        cursor: Option<CursorValue>,
        _batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        log(LogLevel::Info, &format!("hello-world: cursor={cursor:?}"));
        let page: i64 = cursor
            .as_ref()
            .map(|c| c.value.parse().unwrap_or(0))
            .unwrap_or(0);
        let s = schema();
        if page >= 1 {
            // Already served the one batch — return an empty batch + is_final.
            let batch_ipc =
                ipc_schema_bytes(&s).map_err(|e| ConnectorError::Other(format!("ipc: {e}")))?;
            return Ok(ReadOutcome {
                batch_ipc,
                rows: 0,
                new_cursor: cursor,
                is_final: true,
                stream_name: None,
            });
        }
        let ids = Int64Array::from(vec![1i64, 2, 3]);
        let greetings = StringArray::from(vec!["hello", "world", "platform"]);
        let cols: Vec<ArrayRef> = vec![Arc::new(ids), Arc::new(greetings)];
        let batch = RecordBatch::try_new(Arc::new(s.clone()), cols)
            .map_err(|e| ConnectorError::Other(format!("batch: {e}")))?;
        let batch_ipc =
            ipc_batch_bytes(&s, &batch).map_err(|e| ConnectorError::Other(format!("ipc: {e}")))?;
        Ok(ReadOutcome {
            batch_ipc,
            rows: 3,
            new_cursor: Some(CursorValue {
                kind: CursorKind::Int64,
                value: "1".into(),
            }),
            is_final: true,
            stream_name: None,
        })
    }
}
