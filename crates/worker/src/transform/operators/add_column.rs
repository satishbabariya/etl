use anyhow::{anyhow, bail};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common_types::transform::LiteralValue;
use std::sync::Arc;

pub fn apply(
    batch: RecordBatch,
    name: &str,
    value: &LiteralValue,
) -> anyhow::Result<RecordBatch> {
    if batch.schema().column_with_name(name).is_some() {
        bail!("add_column: column '{name}' already exists");
    }
    let n = batch.num_rows();
    let (new_field, new_array): (Field, ArrayRef) = match value {
        LiteralValue::Int(v) => (
            Field::new(name, DataType::Int64, false),
            Arc::new(Int64Array::from(vec![*v; n])),
        ),
        LiteralValue::Float(v) => (
            Field::new(name, DataType::Float64, false),
            Arc::new(Float64Array::from(vec![*v; n])),
        ),
        LiteralValue::String(v) => (
            Field::new(name, DataType::Utf8, false),
            Arc::new(StringArray::from(vec![v.as_str(); n])),
        ),
        LiteralValue::Bool(v) => (
            Field::new(name, DataType::Boolean, false),
            Arc::new(BooleanArray::from(vec![*v; n])),
        ),
        LiteralValue::Null => {
            return Err(anyhow!(
                "add_column: LiteralValue::Null requires a declared type — Phase I.5 doesn't support it"
            ));
        }
    };
    let mut fields: Vec<Field> = batch.schema().fields().iter().map(|f| (**f).clone()).collect();
    fields.push(new_field);
    let out_schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
    arrays.push(new_array);
    Ok(RecordBatch::try_new(out_schema, arrays)?)
}

pub fn derive_schema(
    input: &Schema,
    name: &str,
    value: &LiteralValue,
) -> anyhow::Result<Schema> {
    if input.column_with_name(name).is_some() {
        bail!("add_column: column '{name}' already exists");
    }
    let dt = match value {
        LiteralValue::Int(_) => DataType::Int64,
        LiteralValue::Float(_) => DataType::Float64,
        LiteralValue::String(_) => DataType::Utf8,
        LiteralValue::Bool(_) => DataType::Boolean,
        LiteralValue::Null => bail!("add_column Null literal not supported"),
    };
    let mut fields: Vec<Field> = input.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(name, dt, false));
    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[test]
    fn add_string_column() {
        let out = apply(batch(), "source", &LiteralValue::String("customers".into())).unwrap();
        assert_eq!(out.num_columns(), 2);
        let a = out
            .column_by_name("source")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "customers");
        assert_eq!(a.value(2), "customers");
    }

    #[test]
    fn add_existing_column_errors() {
        assert!(apply(batch(), "id", &LiteralValue::Int(0)).is_err());
    }
}
