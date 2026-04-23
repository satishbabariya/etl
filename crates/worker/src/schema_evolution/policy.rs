use common_types::evolution::{ChangeKind, EvolutionPolicy};

pub enum PolicyOutcome {
    NoOp,
    Accept,
    RetainOld,
    Reject { reason: String },
}

pub fn apply_policy(policy: EvolutionPolicy, changes: &[ChangeKind]) -> PolicyOutcome {
    if changes.is_empty() {
        return PolicyOutcome::NoOp;
    }
    match policy {
        EvolutionPolicy::Freeze => PolicyOutcome::RetainOld,
        EvolutionPolicy::Strict => PolicyOutcome::Reject {
            reason: format!("strict policy rejects {} change(s)", changes.len()),
        },
        EvolutionPolicy::PropagateAdditive => {
            let non_additive: Vec<&ChangeKind> =
                changes.iter().filter(|c| !c.is_additive()).collect();
            if non_additive.is_empty() {
                PolicyOutcome::Accept
            } else {
                PolicyOutcome::Reject {
                    reason: format!(
                        "propagate_additive rejects {} breaking change(s): {}",
                        non_additive.len(),
                        non_additive
                            .iter()
                            .map(|c| format!("{c:?}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_nullable_col() -> ChangeKind {
        ChangeKind::AddColumn {
            name: "age".into(), data_type: "Int64".into(), nullable: true,
        }
    }
    fn drop_col() -> ChangeKind {
        ChangeKind::DropColumn { name: "email".into() }
    }

    #[test]
    fn no_changes_is_noop() {
        assert!(matches!(
            apply_policy(EvolutionPolicy::PropagateAdditive, &[]),
            PolicyOutcome::NoOp
        ));
    }

    #[test]
    fn strict_rejects_any_change() {
        let out = apply_policy(EvolutionPolicy::Strict, &[add_nullable_col()]);
        assert!(matches!(out, PolicyOutcome::Reject { .. }));
    }

    #[test]
    fn freeze_retains_old_on_any_change() {
        let out = apply_policy(EvolutionPolicy::Freeze, &[add_nullable_col(), drop_col()]);
        assert!(matches!(out, PolicyOutcome::RetainOld));
    }

    #[test]
    fn propagate_additive_accepts_additive_only() {
        let out = apply_policy(EvolutionPolicy::PropagateAdditive, &[add_nullable_col()]);
        assert!(matches!(out, PolicyOutcome::Accept));
    }

    #[test]
    fn propagate_additive_rejects_breaking() {
        let out = apply_policy(
            EvolutionPolicy::PropagateAdditive,
            &[add_nullable_col(), drop_col()],
        );
        match out {
            PolicyOutcome::Reject { reason } => assert!(reason.contains("breaking change")),
            _ => panic!("expected Reject"),
        }
    }
}
