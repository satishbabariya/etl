use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CdcOp {
    Insert,
    Update,
    Delete,
    Snapshot,
    SchemaChange,
}

impl CdcOp {
    pub fn as_wire(&self) -> &'static str {
        match self {
            CdcOp::Insert => "i",
            CdcOp::Update => "u",
            CdcOp::Delete => "d",
            CdcOp::Snapshot => "s",
            CdcOp::SchemaChange => "c",
        }
    }
}

pub const COL_OP: &str = "_cdc.op";
pub const COL_LSN: &str = "_cdc.lsn";
pub const COL_COMMIT_TS: &str = "_cdc.commit_ts";
pub const COL_TXID: &str = "_cdc.txid";
/// Columns an update left untouched and sent without a value (Postgres
/// TOAST "unchanged"), as a JSON array of column names; NULL when there
/// are none. Loaders keep the destination's value for these columns
/// rather than writing NULL. Present only in batches that need it.
pub const COL_UNCHANGED: &str = "_cdc.unchanged";

pub fn encode_unchanged<S: AsRef<str>>(names: &[S]) -> Option<String> {
    if names.is_empty() {
        return None;
    }
    let names: Vec<&str> = names.iter().map(|n| n.as_ref()).collect();
    Some(serde_json::to_string(&names).expect("string list serializes"))
}

pub fn decode_unchanged(raw: &str) -> Result<Vec<String>, serde_json::Error> {
    serde_json::from_str(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchanged_round_trips_names_with_commas() {
        assert_eq!(encode_unchanged::<&str>(&[]), None);
        let enc = encode_unchanged(&["body", "a,b"]).unwrap();
        assert_eq!(decode_unchanged(&enc).unwrap(), vec!["body", "a,b"]);
    }

    #[test]
    fn cdc_op_wire_chars() {
        assert_eq!(CdcOp::Insert.as_wire(), "i");
        assert_eq!(CdcOp::Update.as_wire(), "u");
        assert_eq!(CdcOp::Delete.as_wire(), "d");
        assert_eq!(CdcOp::Snapshot.as_wire(), "s");
        assert_eq!(CdcOp::SchemaChange.as_wire(), "c");
    }

    #[test]
    fn cdc_op_json_roundtrip() {
        let op = CdcOp::Update;
        let j = serde_json::to_string(&op).unwrap();
        assert_eq!(j, "\"update\"");
        let back: CdcOp = serde_json::from_str(&j).unwrap();
        assert_eq!(back, op);
    }
}
