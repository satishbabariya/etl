//! Transformation DAG (RFC-12). Operators run in-memory between read_batch
//! and load_batch. Stateless, batch-local — no cross-batch state.

pub mod operators;
pub mod predicate;

use anyhow::Context;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use common_types::transform::Operator;
use std::sync::Arc;

use crate::wasm_runtime::WasmScalarRuntime;

#[derive(Debug)]
pub struct TransformOutcome {
    pub kept: RecordBatch,
    pub rejected: Option<RecordBatch>,
    pub per_operator: Vec<OperatorMetrics>,
}

#[derive(Debug, Clone)]
pub struct OperatorMetrics {
    pub op_index: usize,
    pub op_kind: &'static str,
    pub rows_in: usize,
    pub rows_out: usize,
    pub rows_rejected: usize,
}

pub async fn apply(
    batch: RecordBatch,
    operators: &[Operator],
    scalar_runtime: &Arc<WasmScalarRuntime>,
) -> anyhow::Result<TransformOutcome> {
    let mut current = batch;
    let mut rejected: Option<RecordBatch> = None;
    let mut per_operator = Vec::with_capacity(operators.len());

    for (idx, op) in operators.iter().enumerate() {
        let rows_in = current.num_rows();
        let (next, rej) = operators::apply_one(op, current, scalar_runtime)
            .await
            .with_context(|| format!("operator {idx} ({})", op.kind()))?;
        let rows_rejected = rej.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        per_operator.push(OperatorMetrics {
            op_index: idx,
            op_kind: op.kind(),
            rows_in,
            rows_out: next.num_rows(),
            rows_rejected,
        });
        if let Some(r) = rej {
            if rejected.is_some() {
                anyhow::bail!(
                    "Phase I.5: only one operator may emit rejected rows (validate), and it must be the last — got rejections from op {idx}"
                );
            }
            rejected = Some(r);
        }
        current = next;
    }

    Ok(TransformOutcome {
        kept: current,
        rejected,
        per_operator,
    })
}

pub fn derive_schema(input_schema: &Schema, operators: &[Operator]) -> anyhow::Result<Schema> {
    let mut current = input_schema.clone();
    for (idx, op) in operators.iter().enumerate() {
        current = operators::derive_one(op, &current)
            .with_context(|| format!("derive_schema: operator {idx} ({})", op.kind()))?;
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use common_types::transform::{LiteralValue, MaskStrategy, Operator};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("email", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![Some("a@x"), None, Some("c@x")])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn filter_then_mask_chains_correctly() {
        let rt = WasmScalarRuntime::new(std::env::temp_dir().join("etl-scalar-test")).unwrap();
        let ops = vec![
            Operator::Filter {
                predicate: "email IS NOT NULL".into(),
            },
            Operator::Mask {
                column: "email".into(),
                strategy: MaskStrategy::Hash,
            },
        ];
        let out = apply(batch(), &ops, &rt).await.unwrap();
        assert_eq!(out.kept.num_rows(), 2);
        assert!(out.rejected.is_none());
        assert_eq!(out.per_operator.len(), 2);
        assert_eq!(out.per_operator[0].rows_in, 3);
        assert_eq!(out.per_operator[0].rows_out, 2);
    }

    #[test]
    fn derive_schema_chains_select_and_add_column() {
        let input = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        let ops = vec![
            Operator::Select {
                columns: vec!["email".into()],
            },
            Operator::AddColumn {
                name: "source".into(),
                value: LiteralValue::String("customers".into()),
            },
        ];
        let out = derive_schema(&input, &ops).unwrap();
        assert_eq!(out.fields().len(), 2);
        assert_eq!(out.field(0).name(), "email");
        assert_eq!(out.field(1).name(), "source");
    }
}
