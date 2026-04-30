//! Pure JSON → Arrow IPC parsing for Stripe /v1/customers responses.
//! Lives in its own module so it's testable without the wit-bindgen
//! generated types (the host can't compile against `Guest`).

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize, Debug)]
struct ListResp {
    data: Vec<Customer>,
    has_more: bool,
}

#[derive(Deserialize, Debug)]
struct Customer {
    id: String,
    email: Option<String>,
    name: Option<String>,
    created: i64,
}

pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("created", DataType::Int64, false),
    ])
}

pub fn schema_ipc_bytes() -> Result<Vec<u8>, ArrowError> {
    let s = schema();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &s)?;
        w.finish()?;
    }
    Ok(buf)
}

#[derive(Debug)]
pub struct ParsedPage {
    pub batch_ipc: Vec<u8>,
    pub rows: u32,
    pub last_id: Option<String>,
    pub has_more: bool,
}

pub fn parse_page(json_bytes: &[u8]) -> Result<ParsedPage, String> {
    let resp: ListResp = serde_json::from_slice(json_bytes)
        .map_err(|e| format!("Stripe JSON parse: {e}"))?;
    let s = schema();
    let ids: Vec<String> = resp.data.iter().map(|c| c.id.clone()).collect();
    let last_id = ids.last().cloned();
    let emails: Vec<Option<String>> = resp.data.iter().map(|c| c.email.clone()).collect();
    let names: Vec<Option<String>> = resp.data.iter().map(|c| c.name.clone()).collect();
    let created: Vec<i64> = resp.data.iter().map(|c| c.created).collect();

    let id_arr = StringArray::from(ids);
    let email_arr = StringArray::from(emails);
    let name_arr = StringArray::from(names);
    let created_arr = Int64Array::from(created);

    let batch = RecordBatch::try_new(
        Arc::new(s.clone()),
        vec![
            Arc::new(id_arr),
            Arc::new(email_arr),
            Arc::new(name_arr),
            Arc::new(created_arr),
        ],
    )
    .map_err(|e| format!("Arrow batch: {e}"))?;

    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &s)
            .map_err(|e| format!("StreamWriter::try_new: {e}"))?;
        w.write(&batch)
            .map_err(|e| format!("StreamWriter::write: {e}"))?;
        w.finish()
            .map_err(|e| format!("StreamWriter::finish: {e}"))?;
    }
    Ok(ParsedPage {
        batch_ipc: buf,
        rows: batch.num_rows() as u32,
        last_id,
        has_more: resp.has_more,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_two_customers() {
        let body = br#"{
            "data": [
                {"id":"cus_1","email":"a@x.com","name":"Alice","created":1700000000},
                {"id":"cus_2","email":null,"name":null,"created":1700000123}
            ],
            "has_more": false
        }"#;
        let page = parse_page(body).unwrap();
        assert_eq!(page.rows, 2);
        assert_eq!(page.last_id.as_deref(), Some("cus_2"));
        assert!(!page.has_more);
    }

    #[test]
    fn parses_empty_page() {
        let body = br#"{"data":[], "has_more":false}"#;
        let page = parse_page(body).unwrap();
        assert_eq!(page.rows, 0);
        assert!(page.last_id.is_none());
        assert!(!page.has_more);
    }

    #[test]
    fn rejects_malformed_json() {
        let err = parse_page(b"not json").unwrap_err();
        assert!(err.to_lowercase().contains("parse"));
    }

    #[test]
    fn schema_has_four_columns() {
        let s = schema();
        let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "email", "name", "created"]);
    }
}
