use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorKind {
    Int64,
    TimestampTz,
}

/// Stringified cursor, kind-tagged. The string form is canonical and
/// survives serialization across Temporal and catalog JSONB.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorValue {
    pub kind: CursorKind,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip_int() {
        let c = CursorValue { kind: CursorKind::Int64, value: "42".into() };
        let j = serde_json::to_string(&c).unwrap();
        let back: CursorValue = serde_json::from_str(&j).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn cursor_kind_serializes_snake_case() {
        let j = serde_json::to_string(&CursorKind::TimestampTz).unwrap();
        assert_eq!(j, "\"timestamp_tz\"");
    }
}
