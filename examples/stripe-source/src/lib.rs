//! stripe-source — Stripe /v1/customers source connector.

pub mod parse;

wit_bindgen::generate!({
    path: "wit",
    world: "source-connector",
});

use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

struct Component;

export!(Component);

fn schema() -> Schema {
    // TODO: replace with your source's columns.
    Schema::new(vec![Field::new("id", DataType::Int64, false)])
}

fn ipc_schema_bytes(s: &Schema) -> Result<Vec<u8>, arrow_schema::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, s)?;
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
        _cursor: Option<CursorValue>,
        _batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        // TODO: implement. Return rows after the cursor; set is_final
        // when fewer than batch_size rows are available.
        let s = schema();
        let batch_ipc = ipc_schema_bytes(&s)
            .map_err(|e| ConnectorError::Other(format!("ipc: {e}")))?;
        Ok(ReadOutcome {
            batch_ipc,
            rows: 0,
            new_cursor: None,
            is_final: true,
        })
    }
}
