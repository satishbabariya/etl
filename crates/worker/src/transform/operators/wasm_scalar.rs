use anyhow::{Context, anyhow, bail};
use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::wasm_runtime::WasmScalarRuntime;

pub async fn apply(
    batch: RecordBatch,
    udf: &str,
    input_column: &str,
    output_column: &str,
    runtime: &Arc<WasmScalarRuntime>,
) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    if schema.column_with_name(output_column).is_some() {
        bail!("wasm_scalar: output_column '{output_column}' already exists");
    }
    let (idx, field) = schema
        .column_with_name(input_column)
        .ok_or_else(|| anyhow!("wasm_scalar: input_column '{input_column}' not found"))?;
    let field_nullable = field.is_nullable();
    let field_data_type = field.data_type().clone();
    let utf8 = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            anyhow!(
                "wasm_scalar Phase I.5 only supports Utf8 input; '{input_column}' is {:?}",
                field_data_type
            )
        })?;

    let n = utf8.len();
    let mut inputs = Vec::with_capacity(n);
    let mut null_mask = vec![false; n];
    for i in 0..n {
        if utf8.is_null(i) {
            null_mask[i] = true;
            inputs.push(String::new());
        } else {
            inputs.push(utf8.value(i).to_string());
        }
    }

    let outputs = runtime
        .apply(udf, inputs)
        .await
        .with_context(|| format!("invoking scalar UDF '{udf}'"))?;

    if outputs.len() != n {
        bail!(
            "scalar UDF returned {} rows for {} inputs — must preserve length",
            outputs.len(),
            n
        );
    }
    let mut b = StringBuilder::with_capacity(n, outputs.iter().map(|s| s.len()).sum());
    for i in 0..n {
        if null_mask[i] {
            b.append_null();
        } else {
            b.append_value(&outputs[i]);
        }
    }
    let arr = b.finish();

    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(output_column, DataType::Utf8, field_nullable));
    let out_schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
    arrays.push(Arc::new(arr));
    Ok(RecordBatch::try_new(out_schema, arrays)?)
}

pub fn derive_schema(
    input: &Schema,
    input_column: &str,
    output_column: &str,
) -> anyhow::Result<Schema> {
    if input.column_with_name(output_column).is_some() {
        bail!("wasm_scalar: output_column '{output_column}' already exists");
    }
    let (_, field) = input
        .column_with_name(input_column)
        .ok_or_else(|| anyhow!("wasm_scalar: input_column '{input_column}' not found"))?;
    let mut fields: Vec<Field> = input.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(output_column, DataType::Utf8, field.is_nullable()));
    Ok(Schema::new(fields))
}
