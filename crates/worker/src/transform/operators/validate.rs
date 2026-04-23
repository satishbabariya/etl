use anyhow::anyhow;
use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use common_types::transform::ValidationRule;

pub fn apply(
    batch: RecordBatch,
    rules: &[ValidationRule],
) -> anyhow::Result<(RecordBatch, Option<RecordBatch>)> {
    let n = batch.num_rows();
    let mut pass = vec![true; n];
    for rule in rules {
        match rule {
            ValidationRule::NotNull { column } => {
                let a = batch
                    .column_by_name(column)
                    .ok_or_else(|| anyhow!("validate: column '{column}' not in batch"))?;
                for i in 0..n {
                    if a.is_null(i) {
                        pass[i] = false;
                    }
                }
            }
        }
    }
    if pass.iter().all(|b| *b) {
        return Ok((batch, None));
    }
    let kept_mask = BooleanArray::from(pass.clone());
    let rejected_mask = BooleanArray::from(pass.iter().map(|b| !b).collect::<Vec<_>>());
    let kept = filter_record_batch(&batch, &kept_mask)?;
    let rejected = filter_record_batch(&batch, &rejected_mask)?;
    let rejected_opt = if rejected.num_rows() > 0 { Some(rejected) } else { None };
    Ok((kept, rejected_opt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3), None])),
                Arc::new(StringArray::from(vec![Some("A"), Some("B"), None, Some("D")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn not_null_id_splits_correctly() {
        let (kept, rejected) = apply(
            batch(),
            &[ValidationRule::NotNull { column: "id".into() }],
        )
        .unwrap();
        assert_eq!(kept.num_rows(), 2);
        assert_eq!(rejected.as_ref().unwrap().num_rows(), 2);
    }

    #[test]
    fn name_has_one_rejection() {
        let (kept, rejected) = apply(
            batch(),
            &[ValidationRule::NotNull { column: "name".into() }],
        )
        .unwrap();
        assert_eq!(kept.num_rows(), 3);
        assert_eq!(rejected.unwrap().num_rows(), 1);
    }
}
