//! `read_window`: open a binlog stream from a GTID set, drain up to N
//! row-event groups, build an Arrow `RecordBatch` for the configured
//! table, return new GTID set.
//!
//! Single connection per call; we do not maintain a long-lived stream
//! across activity invocations (mirrors the Postgres CDC pattern in
//! `connectors/postgres/cdc/stream.rs`).

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{ArrayRef, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures_util::StreamExt;
use mysql_async::binlog::events::{EventData, RowsEventData, TableMapEvent};
use mysql_async::Conn;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::decode::{binlog_row_to_strings, RowOp};
use super::position::GtidSet;

pub struct ReadWindowOutput {
    pub batch: Option<RecordBatch>,
    pub rows: usize,
    pub new_gtid: GtidSet,
}

#[allow(clippy::too_many_arguments)]
pub async fn read_window(
    conn_url: &str,
    server_id: u32,
    schema_name: &str,
    table_name: &str,
    start_gtid: &GtidSet,
    max_events: usize,
    arrow_schema: SchemaRef,
    _heartbeat_secs: u32,
    idle_timeout_secs: u64,
) -> Result<ReadWindowOutput> {
    let conn = Conn::from_url(conn_url).await.context("mysql connect")?;
    let req = build_request(server_id, start_gtid)?;
    let mut stream = conn
        .get_binlog_stream(req)
        .await
        .context("open binlog stream")?;

    let mut new_gtid = start_gtid.clone();
    let mut current_uuid_gno: Option<(String, u64)> = None;
    let mut current_commit_ts: Option<i64> = None;

    // Cache of TableMapEvents we've actually seen for our target table.
    // Keyed by table_id. The MySQL server may use multiple table_ids
    // for a single table (e.g. across DDL); we accept all that match
    // schema.table.
    let mut tme_cache: HashMap<u64, TableMapEvent<'static>> = HashMap::new();

    let mut ops: Vec<(RowOp, GtidSet, Option<i64>)> = Vec::new();

    while ops.len() < max_events {
        let next = match tokio::time::timeout(
            Duration::from_secs(idle_timeout_secs),
            stream.next(),
        )
        .await
        {
            Ok(Some(Ok(ev))) => ev,
            Ok(Some(Err(e))) => return Err(anyhow!("binlog stream error: {e}")),
            Ok(None) => break,
            Err(_) => break, // idle window exhausted
        };

        let data = match next.read_data().context("read_data")? {
            Some(d) => d,
            None => continue,
        };

        match data {
            EventData::GtidEvent(g) => {
                let uuid = uuid_bytes_to_string(g.sid());
                current_uuid_gno = Some((uuid, g.gno()));
                let micros = g.immediate_commit_timestamp();
                if micros != 0 {
                    current_commit_ts = Some(micros as i64);
                }
            }
            EventData::XidEvent(_) => {
                if let Some((uuid, gno)) = current_uuid_gno.take() {
                    let mut single = GtidSet::empty();
                    let s = format!("{}:{}", uuid, gno);
                    let parsed = GtidSet::parse(&s)
                        .with_context(|| format!("re-parse own GTID '{}'", s))?;
                    single.union_with(&parsed);
                    new_gtid.union_with(&single);
                }
            }
            EventData::TableMapEvent(tme) => {
                if tme.database_name() == schema_name && tme.table_name() == table_name {
                    tme_cache.insert(tme.table_id(), tme.into_owned());
                }
            }
            EventData::RowsEvent(rd) => {
                let tid = rd.table_id();
                let tme = match tme_cache.get(&tid) {
                    Some(t) => t,
                    None => continue, // not our table
                };
                drain_rows(&rd, tme, &mut ops, &new_gtid, current_commit_ts)?;
            }
            EventData::HeartbeatEvent => {}
            _ => {}
        }
    }

    stream.close().await.ok();

    if ops.is_empty() {
        return Ok(ReadWindowOutput {
            batch: None,
            rows: 0,
            new_gtid,
        });
    }
    let batch = build_record_batch(&ops, arrow_schema)?;
    let rows = ops.len();
    Ok(ReadWindowOutput {
        batch: Some(batch),
        rows,
        new_gtid,
    })
}

fn build_request<'a>(
    server_id: u32,
    start_gtid: &GtidSet,
) -> Result<mysql_async::BinlogStreamRequest<'a>> {
    use mysql_async::BinlogStreamRequest;
    let req = BinlogStreamRequest::new(server_id);
    if start_gtid.is_empty() {
        // Server-default position; MySQL will start from the beginning
        // of the available binlog. Acceptable for "no GTID history" case.
        Ok(req)
    } else {
        let sids = gtid_set_to_sids(start_gtid)?;
        Ok(req.with_gtid().with_gtid_set(sids))
    }
}

fn gtid_set_to_sids<'a>(set: &GtidSet) -> Result<Vec<mysql_async::Sid<'a>>> {
    use mysql_async::{GnoInterval, Sid};
    // Reparse the formatted set so we expose internal state through a
    // stable surface. Only reads; no allocations beyond what Sid needs.
    let formatted = set.format();
    let mut sids: Vec<Sid<'a>> = Vec::new();
    for segment in formatted.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        let (uuid_str, ranges) = segment
            .split_once(':')
            .ok_or_else(|| anyhow!("malformed GTID segment: {segment}"))?;
        let uuid = uuid::Uuid::parse_str(uuid_str)
            .with_context(|| format!("parse uuid '{uuid_str}'"))?;
        let mut intervals: Vec<GnoInterval> = Vec::new();
        for r in ranges.split(':') {
            let (lo, hi) = match r.split_once('-') {
                Some((a, b)) => (a.parse::<u64>()?, b.parse::<u64>()?),
                None => {
                    let n = r.parse::<u64>()?;
                    (n, n)
                }
            };
            // GnoInterval is half-open [start, end); GtidSet stores closed [lo, hi].
            intervals.push(GnoInterval::new(lo, hi.saturating_add(1)));
        }
        sids.push(Sid::new(uuid.into_bytes()).with_intervals(intervals));
    }
    Ok(sids)
}

fn uuid_bytes_to_string(b: [u8; 16]) -> String {
    uuid::Uuid::from_bytes(b).hyphenated().to_string()
}

fn drain_rows(
    rd: &RowsEventData<'_>,
    tme: &TableMapEvent<'static>,
    ops: &mut Vec<(RowOp, GtidSet, Option<i64>)>,
    new_gtid: &GtidSet,
    commit_ts: Option<i64>,
) -> Result<()> {
    let tid = rd.table_id();
    match rd {
        RowsEventData::WriteRowsEvent(ev) => {
            for row_pair in ev.rows(tme) {
                let (_before, after) = row_pair.context("decode WRITE row")?;
                let after_row = after.ok_or_else(|| anyhow!("WRITE row missing after-image"))?;
                let after_strs = binlog_row_to_strings(&after_row)?;
                ops.push((
                    RowOp::Insert {
                        table_id: tid,
                        after: after_strs,
                    },
                    new_gtid.clone(),
                    commit_ts,
                ));
            }
        }
        RowsEventData::UpdateRowsEvent(ev) => {
            for row_pair in ev.rows(tme) {
                let (before, after) = row_pair.context("decode UPDATE row")?;
                let before_strs = before.as_ref().map(binlog_row_to_strings).transpose()?;
                let after_row = after.ok_or_else(|| anyhow!("UPDATE row missing after-image"))?;
                let after_strs = binlog_row_to_strings(&after_row)?;
                ops.push((
                    RowOp::Update {
                        table_id: tid,
                        before: before_strs,
                        after: after_strs,
                    },
                    new_gtid.clone(),
                    commit_ts,
                ));
            }
        }
        RowsEventData::DeleteRowsEvent(ev) => {
            for row_pair in ev.rows(tme) {
                let (before, _after) = row_pair.context("decode DELETE row")?;
                let before_row =
                    before.ok_or_else(|| anyhow!("DELETE row missing before-image"))?;
                let before_strs = binlog_row_to_strings(&before_row)?;
                ops.push((
                    RowOp::Delete {
                        table_id: tid,
                        before: before_strs,
                    },
                    new_gtid.clone(),
                    commit_ts,
                ));
            }
        }
        // v1 row events (very old MySQL): not supported in v1 of this
        // connector — we require ROW format on 5.7+ which emits v2.
        RowsEventData::WriteRowsEventV1(_)
        | RowsEventData::UpdateRowsEventV1(_)
        | RowsEventData::DeleteRowsEventV1(_) => {
            bail!("row event v1 not supported; require MySQL 5.7+ binlog row format");
        }
        // PartialUpdate rows appear when binlog_row_value_options=PARTIAL_JSON
        // is set on the source. We require FULL row image; surface the
        // misconfiguration rather than silently degrading.
        RowsEventData::PartialUpdateRowsEvent(_) => {
            bail!(
                "partial JSON row updates not supported; \
                 set binlog_row_value_options to its default (empty)"
            );
        }
    }
    Ok(())
}

fn build_record_batch(
    ops: &[(RowOp, GtidSet, Option<i64>)],
    arrow_schema: SchemaRef,
) -> Result<RecordBatch> {
    // arrow_schema is data columns followed by _cdc.op, _cdc.lsn,
    // _cdc.commit_ts (in that order — see schema.rs).
    let n_data = arrow_schema
        .fields()
        .len()
        .checked_sub(3)
        .ok_or_else(|| anyhow!("schema must have at least 3 _cdc.* metadata columns"))?;

    let mut col_builders: Vec<StringBuilder> =
        (0..n_data).map(|_| StringBuilder::new()).collect();
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();

    for (op, gtid, ts) in ops {
        match op {
            RowOp::Insert { after, .. } => {
                push_row(&mut col_builders, after);
                op_b.append_value("i");
            }
            RowOp::Update { after, .. } => {
                push_row(&mut col_builders, after);
                op_b.append_value("u");
            }
            RowOp::Delete { before, .. } => {
                push_row(&mut col_builders, before);
                op_b.append_value("d");
            }
        }
        lsn_b.append_value(gtid.format());
        ts_b.append_option(*ts);
    }

    let mut cols: Vec<ArrayRef> = col_builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect();
    cols.push(Arc::new(op_b.finish()));
    cols.push(Arc::new(lsn_b.finish()));
    cols.push(Arc::new(ts_b.finish().with_timezone("UTC")));
    Ok(RecordBatch::try_new(arrow_schema, cols)?)
}

fn push_row(builders: &mut [StringBuilder], values: &[Option<String>]) {
    for (b, v) in builders.iter_mut().zip(values.iter()) {
        b.append_option(v.as_deref());
    }
    // If the row has fewer columns than data builders (shouldn't happen
    // in v1 with FULL row image), append nulls so column counts stay
    // consistent with the schema.
    if values.len() < builders.len() {
        for b in &mut builders[values.len()..] {
            b.append_null();
        }
    }
}
