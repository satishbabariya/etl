//! CDC apply for the Snowflake loader (phase 2-5b, mirroring 2-4b).
//!
//! A batch whose schema carries `_cdc.op` is applied by key, not row by
//! row: Snowflake's MERGE errors on duplicate source keys
//! (ERROR_ON_NONDETERMINISTIC_MERGE), and a CDC batch routinely holds
//! several changes to one row. Applying i/u/s/d in order always leaves
//! the state of the key's *last* change (every upsert writes the full
//! row), so each key collapses to its last change and the batch splits
//! into disjoint delete keys and upsert rows — one DELETE plus the 2-5a
//! MERGE, in the same transaction as the idempotency log.

use anyhow::bail;
use arrow::array::UInt32Array;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

use super::sql::sql_literal;
use crate::loaders::postgres::{cdc_data_field_indices, cdc_op_at};

/// A CDC batch reduced to its final state per key. Both batches carry the
/// data columns only (`_cdc.*` stripped).
pub struct CdcPlan {
    pub upserts: RecordBatch,
    pub deletes: RecordBatch,
    /// i/u/s/d rows in the input, before collapsing (what the PG loader
    /// reports as applied).
    pub applied: usize,
    /// The batch carries a `c` event: evolve the destination (see
    /// `evolution`) before writing. The `c` row itself has no data.
    pub schema_change: bool,
}

pub fn plan(batch: &RecordBatch, pk_columns: &[String]) -> anyhow::Result<CdcPlan> {
    if pk_columns.is_empty() {
        bail!("CDC batches require pk_columns on the Snowflake destination (deletes need a key)");
    }
    let schema = batch.schema();
    let pk_idx: Vec<usize> = pk_columns
        .iter()
        .map(|pk| {
            schema
                .index_of(pk)
                .map_err(|_| anyhow::anyhow!("pk column {pk:?} not in batch"))
        })
        .collect::<anyhow::Result<_>>()?;

    // key (rendered pk literals) → (last row, last op was a delete)
    let mut last: HashMap<Vec<String>, (usize, bool)> = HashMap::new();
    let mut applied = 0usize;
    let mut schema_change = false;
    for row in 0..batch.num_rows() {
        let delete = match cdc_op_at(batch, row)? {
            "i" | "u" | "s" => false,
            "d" => true,
            "t" => {
                tracing::warn!(
                    target: "loader.snowflake.cdc",
                    "truncate CDC event skipped (destructive ops not auto-applied)"
                );
                continue;
            }
            "c" => {
                schema_change = true;
                continue;
            }
            other => bail!("unknown CDC op {other:?} at row {row}"),
        };
        let key = pk_idx
            .iter()
            .map(|&i| sql_literal(batch.column(i).as_ref(), row))
            .collect::<anyhow::Result<Vec<_>>>()?;
        last.insert(key, (row, delete));
        applied += 1;
    }

    // Batch order keeps the rendered SQL deterministic.
    let mut rows: Vec<(usize, bool)> = last.into_values().collect();
    rows.sort_unstable();
    let pick = |want_delete: bool| -> UInt32Array {
        rows.iter()
            .filter(|(_, d)| *d == want_delete)
            .map(|(r, _)| *r as u32)
            .collect()
    };

    let data = batch.project(&cdc_data_field_indices(schema.as_ref()))?;
    Ok(CdcPlan {
        upserts: arrow::compute::take_record_batch(&data, &pick(false))?,
        deletes: arrow::compute::take_record_batch(&data, &pick(true))?,
        applied,
        schema_change,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// (id, name, op) rows → a CDC batch with a trailing `_cdc.lsn`.
    fn cdc_batch(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("_cdc.op", DataType::Utf8, false),
            Field::new("_cdc.lsn", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec![Some("0/1"); rows.len()])),
            ],
        )
        .unwrap()
    }

    fn ids(b: &RecordBatch) -> Vec<i64> {
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        (0..a.len()).map(|i| a.value(i)).collect()
    }

    fn names(b: &RecordBatch) -> Vec<Option<String>> {
        let a = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        (0..a.len())
            .map(|i| (!a.is_null(i)).then(|| a.value(i).to_string()))
            .collect()
    }

    fn pk() -> Vec<String> {
        vec!["id".into()]
    }

    #[test]
    fn last_change_per_key_wins() {
        let b = cdc_batch(&[
            (1, Some("a"), "i"),
            (2, Some("b"), "i"),
            (1, Some("a2"), "u"), // 1: i → u ⇒ upsert a2
            (2, None, "d"),       // 2: i → d ⇒ delete
            (3, None, "d"),
            (3, Some("c"), "i"), // 3: d → i ⇒ upsert c
            (4, Some("s"), "s"),
        ]);
        let p = plan(&b, &pk()).unwrap();
        assert_eq!(p.applied, 7);
        assert_eq!(ids(&p.upserts), vec![1, 3, 4]);
        assert_eq!(
            names(&p.upserts),
            vec![Some("a2".into()), Some("c".into()), Some("s".into())]
        );
        assert_eq!(ids(&p.deletes), vec![2]);
    }

    #[test]
    fn cdc_columns_are_stripped() {
        let p = plan(&cdc_batch(&[(1, Some("a"), "i")]), &pk()).unwrap();
        let schema = p.upserts.schema();
        let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(cols, vec!["id", "name"]);
        assert_eq!(p.deletes.schema(), p.upserts.schema());
    }

    #[test]
    fn truncate_is_skipped_and_not_counted() {
        let p = plan(&cdc_batch(&[(1, Some("a"), "i"), (0, None, "t")]), &pk()).unwrap();
        assert_eq!(p.applied, 1);
        assert_eq!(ids(&p.upserts), vec![1]);
        assert_eq!(p.deletes.num_rows(), 0);
    }

    #[test]
    fn schema_change_is_flagged_and_carries_no_data() {
        let p = plan(&cdc_batch(&[(0, None, "c"), (1, Some("a"), "i")]), &pk()).unwrap();
        assert!(p.schema_change);
        assert_eq!(p.applied, 1);
        assert_eq!(ids(&p.upserts), vec![1]);
        let p = plan(&cdc_batch(&[(1, Some("a"), "i")]), &pk()).unwrap();
        assert!(!p.schema_change);
    }

    #[test]
    fn unknown_op_and_missing_pk_are_errors() {
        assert!(plan(&cdc_batch(&[(1, Some("a"), "x")]), &pk()).is_err());
        let err = plan(&cdc_batch(&[(1, Some("a"), "i")]), &[]).err().unwrap();
        assert!(format!("{err}").contains("pk_columns"), "got {err}");
        assert!(plan(&cdc_batch(&[(1, Some("a"), "i")]), &["nope".into()]).is_err());
    }
}
