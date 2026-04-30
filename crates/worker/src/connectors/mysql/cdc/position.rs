//! GTID set parsing, formatting, and merging.
//!
//! A GTID set is one or more `uuid:start[-end][:start[-end]]*` segments,
//! comma-separated. Empty string is the empty set (used when the source
//! has never executed a transaction with GTID).

use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GtidSet {
    by_uuid: BTreeMap<String, Vec<(u64, u64)>>,
}

impl GtidSet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.by_uuid.is_empty()
    }

    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::empty());
        }
        let mut out = BTreeMap::<String, Vec<(u64, u64)>>::new();
        for segment in s.split(',') {
            let segment = segment.trim();
            let (uuid, ranges) = segment
                .split_once(':')
                .ok_or_else(|| anyhow!("missing ':' in GTID segment '{segment}'"))?;
            let uuid = uuid.trim().to_string();
            let entry = out.entry(uuid).or_default();
            for r in ranges.split(':') {
                let (lo, hi) = match r.split_once('-') {
                    Some((a, b)) => (
                        a.parse::<u64>().context("GTID lo")?,
                        b.parse::<u64>().context("GTID hi")?,
                    ),
                    None => {
                        let n = r.parse::<u64>().context("GTID single")?;
                        (n, n)
                    }
                };
                if lo > hi {
                    return Err(anyhow!("inverted GTID range {lo}-{hi}"));
                }
                entry.push((lo, hi));
            }
        }
        for v in out.values_mut() {
            normalize(v);
        }
        Ok(Self { by_uuid: out })
    }

    pub fn format(&self) -> String {
        let mut parts = Vec::new();
        for (uuid, ranges) in &self.by_uuid {
            let mut s = uuid.clone();
            for (lo, hi) in ranges {
                if lo == hi {
                    s.push_str(&format!(":{lo}"));
                } else {
                    s.push_str(&format!(":{lo}-{hi}"));
                }
            }
            parts.push(s);
        }
        parts.join(",")
    }

    pub fn union_with(&mut self, other: &Self) {
        for (uuid, ranges) in &other.by_uuid {
            let entry = self.by_uuid.entry(uuid.clone()).or_default();
            entry.extend(ranges.iter().copied());
            normalize(entry);
        }
    }
}

fn normalize(v: &mut Vec<(u64, u64)>) {
    v.sort();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(v.len());
    for (lo, hi) in v.drain(..) {
        if let Some(last) = out.last_mut() {
            if lo <= last.1.saturating_add(1) {
                last.1 = last.1.max(hi);
                continue;
            }
        }
        out.push((lo, hi));
    }
    *v = out;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_string_parses_to_empty_set() {
        let g = GtidSet::parse("").unwrap();
        assert!(g.is_empty());
        assert_eq!(g.format(), "");
    }

    #[test]
    fn single_interval_roundtrips() {
        let g = GtidSet::parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-23").unwrap();
        assert_eq!(g.format(), "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-23");
    }

    #[test]
    fn single_point_roundtrips() {
        let g = GtidSet::parse("aaaa:5").unwrap();
        assert_eq!(g.format(), "aaaa:5");
    }

    #[test]
    fn union_merges_adjacent_intervals() {
        let mut a = GtidSet::parse("u:1-10").unwrap();
        let b = GtidSet::parse("u:11-20").unwrap();
        a.union_with(&b);
        assert_eq!(a.format(), "u:1-20");
    }

    #[test]
    fn union_keeps_disjoint_intervals_separate() {
        let mut a = GtidSet::parse("u:1-10").unwrap();
        let b = GtidSet::parse("u:20-30").unwrap();
        a.union_with(&b);
        assert_eq!(a.format(), "u:1-10:20-30");
    }

    #[test]
    fn union_across_uuids() {
        let mut a = GtidSet::parse("u1:1-5").unwrap();
        let b = GtidSet::parse("u2:1-3").unwrap();
        a.union_with(&b);
        assert_eq!(a.format(), "u1:1-5,u2:1-3");
    }

    #[test]
    fn parse_rejects_inverted_range() {
        assert!(GtidSet::parse("u:10-1").is_err());
    }

    #[test]
    fn parse_rejects_missing_colon() {
        assert!(GtidSet::parse("uuidonly").is_err());
    }
}
