//! Arrow IPC helpers — schema bytes for `discover`, batch bytes for `read_batch`.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("_cdc.op", DataType::Utf8, false),
        Field::new("_cdc.position", DataType::Utf8, false),
    ]))
}

pub fn schema_ipc_bytes() -> Result<Vec<u8>, String> {
    let s = schema();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, s.as_ref()).map_err(|e| e.to_string())?;
        w.finish().map_err(|e| e.to_string())?;
    }
    Ok(buf)
}

pub struct Row {
    pub id: i64,
    pub name: Option<String>,
    pub op: char,
    pub position: String,
}

pub fn rows_to_ipc(rows: &[Row]) -> Result<Vec<u8>, String> {
    let s = schema();
    let ids: Int64Array = rows.iter().map(|r| Some(r.id)).collect();
    let names: StringArray = rows.iter().map(|r| r.name.clone()).collect();
    let ops: StringArray = rows.iter().map(|r| Some(r.op.to_string())).collect();
    let pos: StringArray = rows.iter().map(|r| Some(r.position.clone())).collect();

    let batch = RecordBatch::try_new(
        s.clone(),
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(names) as ArrayRef,
            Arc::new(ops) as ArrayRef,
            Arc::new(pos) as ArrayRef,
        ],
    )
    .map_err(|e| e.to_string())?;

    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, s.as_ref()).map_err(|e| e.to_string())?;
        w.write(&batch).map_err(|e| e.to_string())?;
        w.finish().map_err(|e| e.to_string())?;
    }
    Ok(buf)
}
