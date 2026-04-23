use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use common_types::transform::Operator;
use std::sync::Arc;

use crate::wasm_runtime::WasmScalarRuntime;

pub mod add_column;
pub mod filter;
pub mod mask;
pub mod select;
pub mod validate;
pub mod wasm_scalar;

pub async fn apply_one(
    op: &Operator,
    batch: RecordBatch,
    scalar_runtime: &Arc<WasmScalarRuntime>,
) -> anyhow::Result<(RecordBatch, Option<RecordBatch>)> {
    match op {
        Operator::Select { columns } => Ok((select::apply(batch, columns)?, None)),
        Operator::Filter { predicate } => Ok((filter::apply(batch, predicate)?, None)),
        Operator::Mask { column, strategy } => Ok((mask::apply(batch, column, strategy)?, None)),
        Operator::AddColumn { name, value } => {
            Ok((add_column::apply(batch, name, value)?, None))
        }
        Operator::Validate { rules } => validate::apply(batch, rules),
        Operator::WasmScalar {
            udf,
            input_column,
            output_column,
        } => Ok((
            wasm_scalar::apply(batch, udf, input_column, output_column, scalar_runtime).await?,
            None,
        )),
    }
}

pub fn derive_one(op: &Operator, input: &Schema) -> anyhow::Result<Schema> {
    match op {
        Operator::Select { columns } => select::derive_schema(input, columns),
        Operator::Filter { .. } => Ok(input.clone()),
        Operator::Mask { column, strategy } => mask::derive_schema(input, column, strategy),
        Operator::AddColumn { name, value } => add_column::derive_schema(input, name, value),
        Operator::Validate { .. } => Ok(input.clone()),
        Operator::WasmScalar {
            input_column,
            output_column,
            ..
        } => wasm_scalar::derive_schema(input, input_column, output_column),
    }
}
