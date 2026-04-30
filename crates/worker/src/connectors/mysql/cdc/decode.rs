//! Binlog row-event value rendering.
//!
//! `mysql_common::binlog::events::Event::read_data()` does the heavy
//! lifting (typed `EventData` enum). This module provides the small
//! surface for converting a `BinlogValue` and `BinlogRow` into the
//! `Option<String>` representation the v1 schema uses for all data
//! columns. The stream loop in `stream.rs` consumes these helpers.

use anyhow::{Context, Result};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;

/// One row event in our internal representation.
#[derive(Clone, Debug, PartialEq)]
pub enum RowOp {
    Insert {
        table_id: u64,
        after: Vec<Option<String>>,
    },
    Update {
        table_id: u64,
        before: Option<Vec<Option<String>>>,
        after: Vec<Option<String>>,
    },
    Delete {
        table_id: u64,
        before: Vec<Option<String>>,
    },
}

/// Render a binlog `Value` as a textual `Option<String>` (None == SQL NULL).
/// JSONB and JsonDiff variants render to JSON-compatible text.
pub fn binlog_value_to_string(v: &BinlogValue<'_>) -> Result<Option<String>> {
    match v {
        BinlogValue::Value(inner) => Ok(value_to_string(inner)),
        BinlogValue::Jsonb(json) => {
            let parsed: serde_json::Value = json
                .clone()
                .parse()
                .context("parsing JSONB to JSON")?
                .into();
            Ok(Some(parsed.to_string()))
        }
        BinlogValue::JsonDiff(_) => {
            // We don't surface partial JSON diffs in v1 — full row image
            // (binlog_row_image=FULL) means JsonDiff should not appear.
            // If it does, fall back to a sentinel so the value isn't lost.
            Ok(Some("__partial_json_diff__".into()))
        }
    }
}

fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(u) => Some(u.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Double(d) => Some(d.to_string()),
        Value::Date(y, m, d, h, mi, s, us) => {
            if *h == 0 && *mi == 0 && *s == 0 && *us == 0 {
                Some(format!("{:04}-{:02}-{:02}", y, m, d))
            } else {
                Some(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                    y, m, d, h, mi, s, us
                ))
            }
        }
        Value::Time(neg, days, h, m, s, us) => {
            let sign = if *neg { "-" } else { "" };
            Some(format!(
                "{}{}d{:02}:{:02}:{:02}.{:06}",
                sign, days, h, m, s, us
            ))
        }
    }
}

/// Convert a binlog row to a vector of textual values, one per column,
/// in column index order. Columns missing from the row image (which
/// happens when `binlog_row_image != FULL` on update before-images) are
/// rendered as `None`.
pub fn binlog_row_to_strings(row: &BinlogRow) -> Result<Vec<Option<String>>> {
    let mut out = Vec::with_capacity(row.len());
    for i in 0..row.len() {
        match row.as_ref(i) {
            Some(v) => out.push(binlog_value_to_string(v)?),
            None => out.push(None),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_null() {
        let v = BinlogValue::Value(Value::NULL);
        assert_eq!(binlog_value_to_string(&v).unwrap(), None);
    }

    #[test]
    fn renders_int() {
        let v = BinlogValue::Value(Value::Int(42));
        assert_eq!(binlog_value_to_string(&v).unwrap().as_deref(), Some("42"));
    }

    #[test]
    fn renders_bytes_as_utf8() {
        let v = BinlogValue::Value(Value::Bytes(b"alice@x.com".to_vec()));
        assert_eq!(
            binlog_value_to_string(&v).unwrap().as_deref(),
            Some("alice@x.com")
        );
    }

    #[test]
    fn renders_date_only() {
        let v = BinlogValue::Value(Value::Date(2026, 1, 1, 0, 0, 0, 0));
        assert_eq!(
            binlog_value_to_string(&v).unwrap().as_deref(),
            Some("2026-01-01")
        );
    }

    #[test]
    fn renders_datetime_with_time() {
        let v = BinlogValue::Value(Value::Date(2026, 1, 1, 12, 30, 45, 0));
        assert_eq!(
            binlog_value_to_string(&v).unwrap().as_deref(),
            Some("2026-01-01 12:30:45.000000")
        );
    }

    #[test]
    fn renders_double() {
        let v = BinlogValue::Value(Value::Double(3.14));
        assert_eq!(binlog_value_to_string(&v).unwrap().as_deref(), Some("3.14"));
    }
}
