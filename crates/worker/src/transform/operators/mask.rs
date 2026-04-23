use anyhow::{Context, anyhow, bail};
use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use common_types::transform::MaskStrategy;
use std::sync::Arc;

pub fn apply(
    batch: RecordBatch,
    column: &str,
    strategy: &MaskStrategy,
) -> anyhow::Result<RecordBatch> {
    let input_schema = batch.schema();
    let (idx, field) = input_schema
        .column_with_name(column)
        .ok_or_else(|| anyhow!("mask: column '{column}' not in batch"))?;
    let utf8 = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!(
            "mask Phase I.5: only Utf8 columns supported; '{column}' is {:?}",
            field.data_type()
        ))?;

    let masked: StringArray = match strategy {
        MaskStrategy::Hash => {
            let mut b = StringBuilder::with_capacity(utf8.len(), utf8.len() * 64);
            for i in 0..utf8.len() {
                if utf8.is_null(i) {
                    b.append_null();
                } else {
                    let digest = blake3::hash(utf8.value(i).as_bytes());
                    b.append_value(digest.to_hex().to_string());
                }
            }
            b.finish()
        }
        MaskStrategy::Null => {
            if !field.is_nullable() {
                bail!("mask Null: column '{column}' is non-nullable");
            }
            let mut b = StringBuilder::with_capacity(utf8.len(), 0);
            for _ in 0..utf8.len() {
                b.append_null();
            }
            b.finish()
        }
        MaskStrategy::Redact { replacement } => {
            let default = "[REDACTED]".to_string();
            let r = replacement.as_ref().unwrap_or(&default);
            let mut b = StringBuilder::with_capacity(utf8.len(), utf8.len() * r.len());
            for i in 0..utf8.len() {
                if utf8.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(r);
                }
            }
            b.finish()
        }
    };

    let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
    arrays[idx] = Arc::new(masked);

    let mut fields: Vec<Field> = input_schema.fields().iter().map(|f| (**f).clone()).collect();
    if matches!(strategy, MaskStrategy::Null) {
        fields[idx] = Field::new(column, fields[idx].data_type().clone(), true);
    }
    let out_schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(out_schema, arrays)?)
}

pub fn derive_schema(
    input: &Schema,
    column: &str,
    strategy: &MaskStrategy,
) -> anyhow::Result<Schema> {
    let (idx, field) = input
        .column_with_name(column)
        .with_context(|| format!("mask: column '{column}' not in input schema"))?;
    let mut fields: Vec<Field> = input.fields().iter().map(|f| (**f).clone()).collect();
    if matches!(strategy, MaskStrategy::Null) {
        fields[idx] = Field::new(column, field.data_type().clone(), true);
    }
    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a@x.com"), Some("b@x.com")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn hash_masks_preserve_schema_and_row_count() {
        let out = apply(batch(), "email", &MaskStrategy::Hash).unwrap();
        assert_eq!(out.num_rows(), 2);
        let a = out
            .column_by_name("email")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0).len(), 64);
        assert_ne!(a.value(0), "a@x.com");
    }

    #[test]
    fn redact_replaces_with_marker() {
        let out = apply(
            batch(),
            "email",
            &MaskStrategy::Redact { replacement: None },
        )
        .unwrap();
        let a = out
            .column_by_name("email")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "[REDACTED]");
    }

    #[test]
    fn null_requires_nullable_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, false)]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a@x"]))]).unwrap();
        assert!(apply(b, "email", &MaskStrategy::Null).is_err());
    }
}
