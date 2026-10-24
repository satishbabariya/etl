use anyhow::{anyhow, Context};
use byteorder::{BigEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io::Read;

#[derive(Debug, Clone, PartialEq)]
pub struct RelationInfo {
    pub rel_id: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub type_oid: u32,
    pub is_key: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CdcEvent {
    Begin {
        final_lsn: u64,
        commit_ts_micros: i64,
        xid: u32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
        commit_ts_micros: i64,
    },
    Relation(RelationInfo),
    Insert {
        rel_id: u32,
        row: Vec<Option<String>>,
    },
    Update {
        rel_id: u32,
        row: Vec<Option<String>>,
    },
    Delete {
        rel_id: u32,
        key: Vec<Option<String>>,
    },
    /// TRUNCATE on one or more relations. Phase I.6 emits no Arrow rows
    /// for truncates — the LSN advances past the message and downstream
    /// pipelines see the absence of rows. A future phase can fold this
    /// into the Parquet stream as a `_cdc.op = 't'` marker.
    Truncate {
        rel_ids: Vec<u32>,
    },
    /// Logical-replication housekeeping messages we don't act on:
    /// pgoutput tags `M` (logical message), `Y` (type info), `O`
    /// (origin). Decoded as a no-op so the slot's LSN can advance past
    /// them — failing to decode would loop the workflow forever.
    Skip,
}

/// Decodes a single pgoutput message body (the bytes after the
/// `w` XLogData header has been stripped by the caller).
pub fn decode_message(body: &[u8]) -> anyhow::Result<CdcEvent> {
    let mut rdr = body;
    let tag = rdr.read_u8().context("reading message tag")?;
    match tag {
        b'B' => decode_begin(rdr),
        b'C' => decode_commit(rdr),
        b'R' => decode_relation(rdr),
        b'I' => decode_insert(rdr),
        b'U' => decode_update(rdr),
        b'D' => decode_delete(rdr),
        b'T' => decode_truncate(rdr),
        b'M' | b'Y' | b'O' => Ok(CdcEvent::Skip),
        other => Err(anyhow!("unsupported pgoutput tag {other}")),
    }
}

fn decode_truncate(mut rdr: &[u8]) -> anyhow::Result<CdcEvent> {
    let n_rels = rdr.read_u32::<BigEndian>()? as usize;
    let _flags = rdr.read_u8()?;
    let mut rel_ids = Vec::with_capacity(n_rels);
    for _ in 0..n_rels {
        rel_ids.push(rdr.read_u32::<BigEndian>()?);
    }
    Ok(CdcEvent::Truncate { rel_ids })
}

fn decode_begin(mut rdr: &[u8]) -> anyhow::Result<CdcEvent> {
    let final_lsn = rdr.read_u64::<BigEndian>()?;
    let commit_ts = rdr.read_i64::<BigEndian>()?;
    let xid = rdr.read_u32::<BigEndian>()?;
    Ok(CdcEvent::Begin {
        final_lsn,
        commit_ts_micros: commit_ts,
        xid,
    })
}

fn decode_commit(mut rdr: &[u8]) -> anyhow::Result<CdcEvent> {
    let _flags = rdr.read_u8()?;
    let commit_lsn = rdr.read_u64::<BigEndian>()?;
    let end_lsn = rdr.read_u64::<BigEndian>()?;
    let commit_ts = rdr.read_i64::<BigEndian>()?;
    Ok(CdcEvent::Commit {
        commit_lsn,
        end_lsn,
        commit_ts_micros: commit_ts,
    })
}

fn read_cstr(rdr: &mut &[u8]) -> anyhow::Result<String> {
    let nul = rdr
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| anyhow!("cstring missing NUL"))?;
    let s = std::str::from_utf8(&rdr[..nul])?.to_owned();
    *rdr = &rdr[nul + 1..];
    Ok(s)
}

fn decode_relation(mut rdr: &[u8]) -> anyhow::Result<CdcEvent> {
    let rel_id = rdr.read_u32::<BigEndian>()?;
    let namespace = read_cstr(&mut rdr)?;
    let name = read_cstr(&mut rdr)?;
    let _replica_identity = rdr.read_u8()?;
    let n_cols = rdr.read_u16::<BigEndian>()? as usize;
    let mut columns = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        let flags = rdr.read_u8()?;
        let name = read_cstr(&mut rdr)?;
        let type_oid = rdr.read_u32::<BigEndian>()?;
        let _type_mod = rdr.read_i32::<BigEndian>()?;
        columns.push(ColumnInfo {
            name,
            type_oid,
            is_key: (flags & 0x01) != 0,
        });
    }
    Ok(CdcEvent::Relation(RelationInfo {
        rel_id,
        namespace,
        name,
        columns,
    }))
}

/// One column of a pgoutput tuple.
enum TupleCol {
    Null,
    /// TOASTed value the UPDATE didn't change; pgoutput sends no data.
    Unchanged,
    Text(String),
}

fn decode_tuple_cols(rdr: &mut &[u8]) -> anyhow::Result<Vec<TupleCol>> {
    let n = rdr.read_u16::<BigEndian>()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let kind = rdr.read_u8()?;
        match kind {
            b'n' => out.push(TupleCol::Null),
            b'u' => out.push(TupleCol::Unchanged),
            b't' => {
                let len = rdr.read_u32::<BigEndian>()? as usize;
                let mut buf = vec![0u8; len];
                rdr.read_exact(&mut buf)?;
                out.push(TupleCol::Text(String::from_utf8(buf)?));
            }
            other => return Err(anyhow!("unknown tuple col kind {other}")),
        }
    }
    Ok(out)
}

/// Tuple as values; an unchanged TOAST column has none, so it reads as
/// NULL (see `decode_update` for the case the old tuple can fill it).
fn decode_tuple(rdr: &mut &[u8]) -> anyhow::Result<Vec<Option<String>>> {
    Ok(decode_tuple_cols(rdr)?
        .into_iter()
        .map(|c| match c {
            TupleCol::Text(s) => Some(s),
            TupleCol::Null | TupleCol::Unchanged => None,
        })
        .collect())
}

fn decode_insert(mut rdr: &[u8]) -> anyhow::Result<CdcEvent> {
    let rel_id = rdr.read_u32::<BigEndian>()?;
    let tag = rdr.read_u8()?;
    anyhow::ensure!(tag == b'N', "insert tuple tag expected 'N', got {tag}");
    let row = decode_tuple(&mut rdr)?;
    Ok(CdcEvent::Insert { rel_id, row })
}

fn decode_update(mut rdr: &[u8]) -> anyhow::Result<CdcEvent> {
    let rel_id = rdr.read_u32::<BigEndian>()?;
    // May have K (key-only before image) or O (old tuple: REPLICA
    // IDENTITY FULL, with TOASTed values flattened in).
    let mut tag = rdr.read_u8()?;
    let mut old: Vec<Option<String>> = Vec::new();
    if tag == b'K' || tag == b'O' {
        old = decode_tuple(&mut rdr)?;
        tag = rdr.read_u8()?;
    }
    anyhow::ensure!(tag == b'N', "update new-tuple tag expected 'N', got {tag}");
    // An unchanged TOAST column carries no value in the new tuple. Its old
    // value is its current one, so take it from the old tuple when there
    // is one; otherwise it stays NULL (a loader applying the row would
    // overwrite the stored value — default replica identity only).
    let row = decode_tuple_cols(&mut rdr)?
        .into_iter()
        .enumerate()
        .map(|(i, c)| match c {
            TupleCol::Text(s) => Some(s),
            TupleCol::Null => None,
            TupleCol::Unchanged => old.get(i).cloned().flatten(),
        })
        .collect();
    Ok(CdcEvent::Update { rel_id, row })
}

fn decode_delete(mut rdr: &[u8]) -> anyhow::Result<CdcEvent> {
    let rel_id = rdr.read_u32::<BigEndian>()?;
    let _tag = rdr.read_u8()?; // K or O
    let key = decode_tuple(&mut rdr)?;
    Ok(CdcEvent::Delete { rel_id, key })
}

/// In-flight Relation messages keyed by rel_id.
pub type RelationTable = HashMap<u32, RelationInfo>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_begin() {
        let body = [
            b'B', 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3,
        ];
        match decode_message(&body).unwrap() {
            CdcEvent::Begin {
                final_lsn,
                commit_ts_micros,
                xid,
            } => {
                assert_eq!(final_lsn, 1);
                assert_eq!(commit_ts_micros, 2);
                assert_eq!(xid, 3);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn decodes_insert_single_text_col() {
        let body = [
            b'I', 0, 0, 0, 7, b'N', 0, 1, b't', 0, 0, 0, 3, b'a', b'b', b'c',
        ];
        match decode_message(&body).unwrap() {
            CdcEvent::Insert { rel_id, row } => {
                assert_eq!(rel_id, 7);
                assert_eq!(row, vec![Some("abc".to_string())]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn decodes_delete_with_key() {
        let body = [b'D', 0, 0, 0, 7, b'K', 0, 1, b't', 0, 0, 0, 1, b'9'];
        match decode_message(&body).unwrap() {
            CdcEvent::Delete { rel_id, key } => {
                assert_eq!(rel_id, 7);
                assert_eq!(key, vec![Some("9".to_string())]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn decodes_truncate() {
        let body = [
            b'T', 0, 0, 0, 2, // n_rels = 2
            0, // flags
            0, 0, 0, 7, // rel_id 7
            0, 0, 0, 9, // rel_id 9
        ];
        match decode_message(&body).unwrap() {
            CdcEvent::Truncate { rel_ids } => assert_eq!(rel_ids, vec![7, 9]),
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn skips_message_type_origin() {
        for tag in [b'M', b'Y', b'O'] {
            let body = [tag];
            assert_eq!(decode_message(&body).unwrap(), CdcEvent::Skip);
        }
    }

    #[test]
    fn null_and_toast_unchanged_decode_as_none() {
        let body = [b'I', 0, 0, 0, 7, b'N', 0, 2, b'n', b'u'];
        match decode_message(&body).unwrap() {
            CdcEvent::Insert { row, .. } => assert_eq!(row, vec![None, None]),
            _ => panic!(),
        }
    }

    #[test]
    fn update_fills_unchanged_toast_from_old_tuple() {
        // 'U' rel 7, old tuple O(t"1", t"big"), new tuple N(t"1", u).
        let mut body = vec![b'U', 0, 0, 0, 7, b'O', 0, 2];
        body.extend([b't', 0, 0, 0, 1, b'1', b't', 0, 0, 0, 3]);
        body.extend(b"big");
        body.extend([b'N', 0, 2, b't', 0, 0, 0, 1, b'1', b'u']);
        match decode_message(&body).unwrap() {
            CdcEvent::Update { row, .. } => {
                assert_eq!(row, vec![Some("1".into()), Some("big".into())])
            }
            other => panic!("got {other:?}"),
        }
        // Key-only old tuple (default identity): nothing to fill from.
        let body = [
            b'U', 0, 0, 0, 7, b'K', 0, 2, b't', 0, 0, 0, 1, b'1', b'n', b'N', 0, 2, b't', 0, 0, 0,
            1, b'1', b'u',
        ];
        match decode_message(&body).unwrap() {
            CdcEvent::Update { row, .. } => assert_eq!(row, vec![Some("1".into()), None]),
            other => panic!("got {other:?}"),
        }
    }
}
