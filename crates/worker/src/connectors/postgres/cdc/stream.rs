//! Streaming CDC via `pg_logical_slot_get_binary_changes`.
//!
//! We deliberately do not use the streaming replication protocol in
//! Phase I.6 MVP — tokio-postgres 0.7 does not expose it, and the SQL
//! function returns the same pgoutput bytes. Trade-off: a shorter
//! polling tick rather than server-push latency. Good enough for local
//! dogfood; streaming-protocol migration is a Phase II task.
//!
//! Each `read_window` call issues one SQL query that drains up to
//! `max_events` rows from the slot. The slot advances automatically:
//! `get_binary_changes` commits position on success.

use anyhow::{Context, Result};
use sqlx::{Connection, PgConnection, Row};

use super::decode::{self, CdcEvent, RelationTable};
use common_types::cursor::lsn_to_string;

pub struct WindowOutput {
    pub events: Vec<CdcEvent>,
    pub relations: RelationTable,
    pub new_position: Option<u64>,
    pub is_empty: bool,
}

pub async fn read_window(
    conn_url: &str,
    slot_name: &str,
    publication: &str,
    _start_lsn: Option<&str>, // informational only — pg_logical_slot_get advances the slot itself
    max_events: usize,
    mut relations: RelationTable,
) -> Result<WindowOutput> {
    let mut c = PgConnection::connect(conn_url).await?;
    // Drain up to max_events rows; proto_version=1 matches our decoder.
    let stmt = "SELECT lsn::text, data \
                FROM pg_logical_slot_get_binary_changes($1, NULL, $2, \
                    'proto_version', '1', 'publication_names', $3)";
    let rows = sqlx::query(stmt)
        .bind(slot_name)
        .bind(max_events as i32)
        .bind(publication)
        .fetch_all(&mut c)
        .await
        .context("pg_logical_slot_get_binary_changes")?;
    let mut events: Vec<CdcEvent> = Vec::with_capacity(rows.len());
    let mut last_lsn_str: Option<String> = None;
    for r in &rows {
        let lsn: String = r.try_get(0)?;
        let data: Vec<u8> = r.try_get(1)?;
        let ev = decode::decode_message(&data)
            .with_context(|| format!("decoding pgoutput msg @ {lsn}"))?;
        if let CdcEvent::Relation(rel) = &ev {
            relations.insert(rel.rel_id, rel.clone());
        }
        events.push(ev);
        last_lsn_str = Some(lsn);
    }
    let new_position = last_lsn_str
        .map(|s| common_types::cursor::lsn_from_string(&s))
        .transpose()
        .unwrap_or(None);
    let is_empty = events.is_empty();
    Ok(WindowOutput { events, relations, new_position, is_empty })
}

/// Convert a flush of `read_window` into a `RecordBatch`. Begin/Commit/
/// Relation are folded into per-row `_cdc.lsn` / `_cdc.commit_ts` /
/// `_cdc.txid` metadata; only data rows (i/u/d) become Arrow rows.
pub fn events_to_batch(
    events: &[CdcEvent],
    relations: &RelationTable,
    rel_id_filter: u32,
    schema: arrow::datatypes::SchemaRef,
) -> anyhow::Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{ArrayBuilder, ArrayRef, Int64Builder, StringBuilder, TimestampMicrosecondBuilder};
    use std::sync::Arc;
    let rel = relations
        .get(&rel_id_filter)
        .ok_or_else(|| anyhow::anyhow!("no Relation seen for rel_id {rel_id_filter}"))?;
    let n_data_cols = rel.columns.len();
    let mut col_builders: Vec<Box<dyn ArrayBuilder>> = (0..n_data_cols)
        .map(|i| make_pg_builder(schema.field(i).data_type()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();
    let mut tx_b = Int64Builder::new();
    let mut current_txid: Option<u32> = None;
    let mut current_commit_ts: Option<i64> = None;
    let mut current_lsn: Option<u64> = None;
    for ev in events {
        match ev {
            CdcEvent::Begin { xid, commit_ts_micros, .. } => {
                current_txid = Some(*xid);
                current_commit_ts = Some(*commit_ts_micros);
            }
            CdcEvent::Commit { end_lsn, .. } => {
                current_lsn = Some(*end_lsn);
            }
            CdcEvent::Relation(_) => {}
            CdcEvent::Insert { rel_id, row } if *rel_id == rel_id_filter => {
                append_pg_row(&mut col_builders, &schema, row)?;
                op_b.append_value("i");
                lsn_b.append_value(lsn_to_string(current_lsn.unwrap_or(0)));
                ts_b.append_option(current_commit_ts);
                tx_b.append_option(current_txid.map(|t| t as i64));
            }
            CdcEvent::Update { rel_id, row } if *rel_id == rel_id_filter => {
                append_pg_row(&mut col_builders, &schema, row)?;
                op_b.append_value("u");
                lsn_b.append_value(lsn_to_string(current_lsn.unwrap_or(0)));
                ts_b.append_option(current_commit_ts);
                tx_b.append_option(current_txid.map(|t| t as i64));
            }
            CdcEvent::Delete { rel_id, key } if *rel_id == rel_id_filter => {
                append_pg_row_partial(&mut col_builders, &schema, key, &rel.columns)?;
                op_b.append_value("d");
                lsn_b.append_value(lsn_to_string(current_lsn.unwrap_or(0)));
                ts_b.append_option(current_commit_ts);
                tx_b.append_option(current_txid.map(|t| t as i64));
            }
            _ => {}
        }
    }
    let mut cols: Vec<ArrayRef> = col_builders.into_iter().map(|mut b| b.finish()).collect();
    cols.push(Arc::new(op_b.finish()));
    cols.push(Arc::new(lsn_b.finish()));
    cols.push(Arc::new(ts_b.finish()));
    cols.push(Arc::new(tx_b.finish()));
    Ok(arrow::record_batch::RecordBatch::try_new(schema, cols)?)
}

fn make_pg_builder(
    dt: &arrow::datatypes::DataType,
) -> anyhow::Result<Box<dyn arrow::array::ArrayBuilder>> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use std::sync::Arc;
    Ok(match dt {
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::new();
            if let Some(tz) = tz.as_ref() {
                b = b.with_timezone(Arc::clone(tz));
            }
            Box::new(b)
        }
        other => anyhow::bail!("no pg builder for DataType {:?}", other),
    })
}

fn append_pg_row(
    builders: &mut [Box<dyn arrow::array::ArrayBuilder>],
    schema: &arrow::datatypes::SchemaRef,
    row: &[Option<String>],
) -> anyhow::Result<()> {
    if row.len() != builders.len() {
        anyhow::bail!(
            "row has {} columns but {} builders",
            row.len(),
            builders.len()
        );
    }
    for (i, value_opt) in row.iter().enumerate() {
        let dt = schema.field(i).data_type();
        let parsed = match value_opt.as_deref() {
            Some(s) => super::types::parse_pg_text(s, dt)?,
            None => None,
        };
        append_pg_scalar(&mut **builders.get_mut(i).unwrap(), parsed.as_ref(), dt)?;
    }
    Ok(())
}

fn append_pg_row_partial(
    builders: &mut [Box<dyn arrow::array::ArrayBuilder>],
    schema: &arrow::datatypes::SchemaRef,
    key: &[Option<String>],
    columns: &[super::decode::ColumnInfo],
) -> anyhow::Result<()> {
    for (i, col) in columns.iter().enumerate() {
        let dt = schema.field(i).data_type();
        let parsed = if col.is_key {
            match key.get(i).and_then(|v| v.as_deref()) {
                Some(s) => super::types::parse_pg_text(s, dt)?,
                None => None,
            }
        } else {
            None
        };
        append_pg_scalar(&mut **builders.get_mut(i).unwrap(), parsed.as_ref(), dt)?;
    }
    Ok(())
}

fn append_pg_scalar(
    builder: &mut dyn arrow::array::ArrayBuilder,
    scalar: Option<&super::types::PgScalarValue>,
    dt: &arrow::datatypes::DataType,
) -> anyhow::Result<()> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use super::types::PgScalarValue;
    match (scalar, dt) {
        (None, DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Int32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Int32(v)), DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Int32Builder"))?
            .append_value(*v),
        (None, DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Int64Builder"))?
            .append_null(),
        (Some(PgScalarValue::Int64(v)), DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Int64Builder"))?
            .append_value(*v),
        (None, DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Float32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Float32(v)), DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Float32Builder"))?
            .append_value(*v),
        (None, DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Float64Builder"))?
            .append_null(),
        (Some(PgScalarValue::Float64(v)), DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Float64Builder"))?
            .append_value(*v),
        (None, DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: StringBuilder"))?
            .append_null(),
        (Some(PgScalarValue::Utf8(s)), DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: StringBuilder"))?
            .append_value(s),
        (None, DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: BooleanBuilder"))?
            .append_null(),
        (Some(PgScalarValue::Boolean(b)), DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: BooleanBuilder"))?
            .append_value(*b),
        (None, DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Date32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Date32(d)), DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Date32Builder"))?
            .append_value(*d),
        (None, DataType::Timestamp(TimeUnit::Microsecond, _)) => builder
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
            .append_null(),
        (Some(PgScalarValue::TimestampMicros(t)), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| anyhow::anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
                .append_value(*t)
        }
        (Some(other_v), other_dt) => {
            anyhow::bail!("scalar/builder mismatch: {:?} into {:?}", other_v, other_dt)
        }
        (None, other_dt) => {
            anyhow::bail!("no null-append path for builder type {:?}", other_dt)
        }
    }
    Ok(())
}
