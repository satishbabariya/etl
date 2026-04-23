use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;

use crate::transform::predicate;

pub fn apply(batch: RecordBatch, predicate_text: &str) -> anyhow::Result<RecordBatch> {
    let p = predicate::parse(predicate_text)?;
    let mask = predicate::evaluate(&p, &batch)?;
    Ok(filter_record_batch(&batch, &mask)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    Some("a@x"),
                    None,
                    Some("b@x"),
                    None,
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn filter_is_not_null_keeps_two() {
        let out = apply(batch(), "email IS NOT NULL").unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn filter_eq_int_keeps_one() {
        let out = apply(batch(), "id = 3").unwrap();
        assert_eq!(out.num_rows(), 1);
    }
}
