use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorKind {
    Int64,
    TimestampTz,
    Lsn,
    Gtid,
    SnapshotPk,
}

pub fn lsn_to_string(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}

pub fn lsn_from_string(s: &str) -> anyhow::Result<u64> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("LSN missing '/': {s}"))?;
    let hi: u64 = u64::from_str_radix(hi, 16)?;
    let lo: u64 = u64::from_str_radix(lo, 16)?;
    Ok((hi << 32) | lo)
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

    #[test]
    fn lsn_roundtrips() {
        let v = CursorValue { kind: CursorKind::Lsn, value: "16/B374D848".into() };
        let j = serde_json::to_string(&v).unwrap();
        let back: CursorValue = serde_json::from_str(&j).unwrap();
        assert_eq!(back.kind, CursorKind::Lsn);
    }

    #[test]
    fn lsn_pair_roundtrip() {
        let lsn: u64 = 0x16_B374_D848;
        let s = super::lsn_to_string(lsn);
        assert_eq!(s, "16/B374D848");
        assert_eq!(super::lsn_from_string(&s).unwrap(), lsn);
    }
}
