use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransformSpec {
    #[serde(default)]
    pub operators: Vec<Operator>,
    #[serde(default = "default_dead_letter_threshold")]
    pub dead_letter_threshold: f64,
}

impl Default for TransformSpec {
    fn default() -> Self {
        Self {
            operators: Vec::new(),
            dead_letter_threshold: default_dead_letter_threshold(),
        }
    }
}

fn default_dead_letter_threshold() -> f64 {
    0.01
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Operator {
    Select { columns: Vec<String> },
    Filter { predicate: String },
    Mask { column: String, strategy: MaskStrategy },
    AddColumn { name: String, value: LiteralValue },
    Validate { rules: Vec<ValidationRule> },
    WasmScalar {
        udf: String,
        input_column: String,
        output_column: String,
    },
}

impl Operator {
    pub fn kind(&self) -> &'static str {
        match self {
            Operator::Select { .. } => "select",
            Operator::Filter { .. } => "filter",
            Operator::Mask { .. } => "mask",
            Operator::AddColumn { .. } => "add_column",
            Operator::Validate { .. } => "validate",
            Operator::WasmScalar { .. } => "wasm_scalar",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MaskStrategy {
    Hash,
    Null,
    Redact {
        #[serde(default)]
        replacement: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "snake_case")]
pub enum ValidationRule {
    NotNull { column: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum LiteralValue {
    // Order matters: Bool first so "true"/"false" don't hit Int; Int before Float
    // so integers don't accidentally deserialize as Float.
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Null,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_roundtrips_filter() {
        let op = Operator::Filter { predicate: "email IS NOT NULL".into() };
        let j = serde_json::to_string(&op).unwrap();
        assert!(j.contains("\"type\":\"filter\""));
        let back: Operator = serde_json::from_str(&j).unwrap();
        assert!(matches!(back, Operator::Filter { .. }));
    }

    #[test]
    fn mask_strategy_tagged() {
        let m = MaskStrategy::Hash;
        let j = serde_json::to_string(&m).unwrap();
        assert_eq!(j, r#"{"kind":"hash"}"#);
    }

    #[test]
    fn transform_spec_default_threshold() {
        let j = r#"{"operators":[]}"#;
        let t: TransformSpec = serde_json::from_str(j).unwrap();
        assert!((t.dead_letter_threshold - 0.01).abs() < 1e-9);
    }

    #[test]
    fn literal_value_bool_before_int() {
        let j = "true";
        let v: LiteralValue = serde_json::from_str(j).unwrap();
        assert_eq!(v, LiteralValue::Bool(true));
        let j = "42";
        let v: LiteralValue = serde_json::from_str(j).unwrap();
        assert_eq!(v, LiteralValue::Int(42));
    }
}
