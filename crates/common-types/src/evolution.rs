use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionPolicy {
    /// Default: additive changes flow through; breaking changes fail the run.
    PropagateAdditive,
    /// Ignore all schema drift; stick with the current stored schema.
    Freeze,
    /// Any schema change fails the run.
    Strict,
}

impl Default for EvolutionPolicy {
    fn default() -> Self {
        Self::PropagateAdditive
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChangeKind {
    AddColumn {
        name: String,
        data_type: String,
        nullable: bool,
    },
    DropColumn {
        name: String,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    WidenType {
        name: String,
        from: String,
        to: String,
    },
    NarrowType {
        name: String,
        from: String,
        to: String,
    },
    MakeNullable {
        name: String,
    },
    MakeNonNullable {
        name: String,
    },
    ReorderColumns {
        before: Vec<String>,
        after: Vec<String>,
    },
}

impl ChangeKind {
    /// True if this change is safe to auto-apply under `propagate_additive`.
    pub fn is_additive(&self) -> bool {
        matches!(
            self,
            ChangeKind::AddColumn { nullable: true, .. }
                | ChangeKind::MakeNullable { .. }
                | ChangeKind::WidenType { .. }
                | ChangeKind::ReorderColumns { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_nullable_column_is_additive() {
        assert!(ChangeKind::AddColumn {
            name: "age".into(),
            data_type: "int64".into(),
            nullable: true,
        }
        .is_additive());
    }

    #[test]
    fn drop_column_is_breaking() {
        assert!(!ChangeKind::DropColumn { name: "email".into() }.is_additive());
    }

    #[test]
    fn widen_int32_to_int64_is_additive() {
        assert!(ChangeKind::WidenType {
            name: "id".into(),
            from: "int32".into(),
            to: "int64".into(),
        }
        .is_additive());
    }

    #[test]
    fn make_non_nullable_is_breaking() {
        assert!(!ChangeKind::MakeNonNullable { name: "name".into() }.is_additive());
    }
}
