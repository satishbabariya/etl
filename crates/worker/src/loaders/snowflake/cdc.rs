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
use std::collections::{BTreeMap, HashMap};

use super::sql::sql_literal;
use crate::loaders::postgres::{cdc_data_field_indices, cdc_op_at, unchanged_at};

/// Upsert rows that share the same TOAST-unchanged columns: their MERGE
/// must leave `keep_existing` alone (no value was sent for them).
pub struct UpsertGroup {
    pub batch: RecordBatch,
    pub keep_existing: Vec<String>,
}

/// A CDC batch reduced to its final state per key. All batches carry the
/// data columns only (`_cdc.*` stripped).
pub struct CdcPlan {
    pub upserts: Vec<UpsertGroup>,
    pub deletes: RecordBatch,
    /// i/u/s/d rows in the input, before collapsing (what the PG loader
    /// reports as applied).
    pub applied: usize,
    /// The batch carries a `c` event: evolve the destination (see
    /// `evolution`) before writing. The `c` row itself has no data.
    pub schema_change: bool,
}

/// Per key: the row of its last change, whether that was a delete, and for
/// each data column the row that last supplied a value. An update that
/// sent a column as TOAST "unchanged" keeps the earlier source (an insert
/// in the same batch, say); None means no row in this batch has a value
/// and the destination's must be kept.
struct KeyState {
    last_row: usize,
    delete: bool,
    src: Vec<Option<usize>>,
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
    let data = batch.project(&cdc_data_field_indices(schema.as_ref()))?;
    let data_schema = data.schema();
    let names: Vec<&str> = data_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    let mut keys: HashMap<Vec<String>, KeyState> = HashMap::new();
    let mut applied = 0usize;
    let mut schema_change = false;
    for row in 0..batch.num_rows() {
        let op = cdc_op_at(batch, row)?;
        match op {
            "i" | "u" | "s" | "d" => {}
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
        }
        let key = pk_idx
            .iter()
            .map(|&i| sql_literal(batch.column(i).as_ref(), row))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let unchanged = if op == "u" {
            unchanged_at(batch, row)?
        } else {
            Vec::new()
        };
        let prev = keys.remove(&key);
        let src = names
            .iter()
            .enumerate()
            .map(|(j, name)| {
                if unchanged.iter().any(|u| u == name) {
                    prev.as_ref().filter(|p| !p.delete).and_then(|p| p.src[j])
                } else {
                    Some(row)
                }
            })
            .collect();
        keys.insert(
            key,
            KeyState {
                last_row: row,
                delete: op == "d",
                src,
            },
        );
        applied += 1;
    }

    // Batch order keeps the rendered SQL deterministic.
    let mut states: Vec<KeyState> = keys.into_values().collect();
    states.sort_unstable_by_key(|k| k.last_row);

    let delete_rows: UInt32Array = states
        .iter()
        .filter(|k| k.delete)
        .map(|k| k.last_row as u32)
        .collect();
    let deletes = arrow::compute::take_record_batch(&data, &delete_rows)?;

    let mut groups: BTreeMap<Vec<String>, Vec<&KeyState>> = BTreeMap::new();
    for k in states.iter().filter(|k| !k.delete) {
        let keep: Vec<String> = names
            .iter()
            .zip(&k.src)
            .filter(|(_, s)| s.is_none())
            .map(|(n, _)| n.to_string())
            .collect();
        groups.entry(keep).or_default().push(k);
    }
    let upserts = groups
        .into_iter()
        .map(|(keep_existing, ks)| {
            // Each column is taken from the row that supplied its value.
            let columns = (0..names.len())
                .map(|j| {
                    let idx: UInt32Array = ks
                        .iter()
                        .map(|k| k.src[j].unwrap_or(k.last_row) as u32)
                        .collect();
                    Ok(arrow::compute::take(data.column(j).as_ref(), &idx, None)?)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(UpsertGroup {
                batch: RecordBatch::try_new(data_schema.clone(), columns)?,
                keep_existing,
            })
        })
        .collect::<anyhow::Result<_>>()?;

    Ok(CdcPlan {
        upserts,
        deletes,
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

    /// The plan's only upsert group, which must keep no columns.
    fn single(p: &CdcPlan) -> &RecordBatch {
        assert_eq!(p.upserts.len(), 1, "expected one upsert group");
        assert!(p.upserts[0].keep_existing.is_empty());
        &p.upserts[0].batch
    }

    /// `cdc_batch` plus a `_cdc.unchanged` column (JSON names per row).
    fn with_unchanged(
        rows: &[(i64, Option<&str>, &str)],
        unchanged: &[Option<&str>],
    ) -> RecordBatch {
        let b = cdc_batch(rows);
        let mut fields: Vec<Field> = b
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(
            common_types::cdc::COL_UNCHANGED,
            DataType::Utf8,
            true,
        ));
        let mut cols = b.columns().to_vec();
        cols.push(Arc::new(StringArray::from(unchanged.to_vec())));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    #[test]
    fn unchanged_column_carries_value_from_earlier_row() {
        // i (name=a) then u that left name TOAST-unchanged (no value sent).
        let b = with_unchanged(
            &[(1, Some("a"), "i"), (1, None, "u")],
            &[None, Some(r#"["name"]"#)],
        );
        let p = plan(&b, &pk()).unwrap();
        assert_eq!(names(single(&p)), vec![Some("a".into())]);
    }

    #[test]
    fn unchanged_column_without_a_value_is_kept_at_destination() {
        let b = with_unchanged(
            &[(1, None, "u"), (2, Some("b"), "u")],
            &[Some(r#"["name"]"#), None],
        );
        let p = plan(&b, &pk()).unwrap();
        assert_eq!(p.upserts.len(), 2);
        let keep: Vec<&RecordBatch> = p
            .upserts
            .iter()
            .filter(|g| g.keep_existing == vec!["name".to_string()])
            .map(|g| &g.batch)
            .collect();
        assert_eq!(keep.len(), 1);
        assert_eq!(ids(keep[0]), vec![1]);
        let plain = p
            .upserts
            .iter()
            .find(|g| g.keep_existing.is_empty())
            .unwrap();
        assert_eq!(ids(&plain.batch), vec![2]);
    }

    #[test]
    fn delete_then_reinsert_does_not_carry_old_values() {
        let b = with_unchanged(
            &[(1, Some("a"), "i"), (1, None, "d"), (1, Some("n"), "i")],
            &[None, None, None],
        );
        let p = plan(&b, &pk()).unwrap();
        assert_eq!(names(single(&p)), vec![Some("n".into())]);
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
        assert_eq!(ids(single(&p)), vec![1, 3, 4]);
        assert_eq!(
            names(single(&p)),
            vec![Some("a2".into()), Some("c".into()), Some("s".into())]
        );
        assert_eq!(ids(&p.deletes), vec![2]);
    }

    #[test]
    fn cdc_columns_are_stripped() {
        let p = plan(&cdc_batch(&[(1, Some("a"), "i")]), &pk()).unwrap();
        let schema = single(&p).schema();
        let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(cols, vec!["id", "name"]);
        assert_eq!(p.deletes.schema(), single(&p).schema());
    }

    #[test]
    fn truncate_is_skipped_and_not_counted() {
        let p = plan(&cdc_batch(&[(1, Some("a"), "i"), (0, None, "t")]), &pk()).unwrap();
        assert_eq!(p.applied, 1);
        assert_eq!(ids(single(&p)), vec![1]);
        assert_eq!(p.deletes.num_rows(), 0);
    }

    #[test]
    fn schema_change_is_flagged_and_carries_no_data() {
        let p = plan(&cdc_batch(&[(0, None, "c"), (1, Some("a"), "i")]), &pk()).unwrap();
        assert!(p.schema_change);
        assert_eq!(p.applied, 1);
        assert_eq!(ids(single(&p)), vec![1]);
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
