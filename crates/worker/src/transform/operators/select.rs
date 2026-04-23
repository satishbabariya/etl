use anyhow::Context;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub fn apply(batch: RecordBatch, columns: &[String]) -> anyhow::Result<RecordBatch> {
    let out_schema = derive_schema(batch.schema().as_ref(), columns)?;
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|c| {
            batch
                .column_by_name(c)
                .cloned()
                .with_context(|| format!("select: column '{c}' not found"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::new(out_schema), arrays)?)
}

pub fn derive_schema(input: &Schema, columns: &[String]) -> anyhow::Result<Schema> {
    let mut fields = Vec::with_capacity(columns.len());
    for name in columns {
        let field = input
            .field_with_name(name)
            .with_context(|| format!("select: column '{name}' not in input schema"))?
            .clone();
        fields.push(field);
    }
    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("A"), Some("B")])),
                Arc::new(StringArray::from(vec![Some("a@x"), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn select_subset_reorders() {
        let cols = vec!["email".into(), "id".into()];
        let out = apply(batch(), &cols).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(0).name(), "email");
        assert_eq!(out.schema().field(1).name(), "id");
    }

    #[test]
    fn select_unknown_column_errors() {
        let cols = vec!["ghost".into()];
        assert!(apply(batch(), &cols).is_err());
    }

    #[test]
    fn derive_schema_matches_apply() {
        let input = batch();
        let cols = vec!["name".into(), "email".into()];
        let derived = derive_schema(input.schema().as_ref(), &cols).unwrap();
        let applied = apply(input, &cols).unwrap();
        assert_eq!(derived, *applied.schema().as_ref());
    }
}
