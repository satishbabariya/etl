use serde::{Deserialize, Serialize};

/// Phase II.2.b RBAC roles. Admin > Operator > Viewer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    Operator,
    Viewer,
}

/// Coarse action classes that the CLI/API check before forwarding to
/// the catalog. RLS in the database is the second line of defense.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Read,
    Run,
    Write,
    Admin,
}

impl Role {
    pub fn permits(self, a: Action) -> bool {
        match (self, a) {
            (Role::Admin, _) => true,
            (Role::Operator, Action::Read | Action::Run | Action::Write) => true,
            (Role::Viewer, Action::Read) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_permits_everything() {
        for a in [Action::Read, Action::Run, Action::Write, Action::Admin] {
            assert!(Role::Admin.permits(a));
        }
    }

    #[test]
    fn viewer_only_reads() {
        assert!(Role::Viewer.permits(Action::Read));
        assert!(!Role::Viewer.permits(Action::Run));
        assert!(!Role::Viewer.permits(Action::Write));
        assert!(!Role::Viewer.permits(Action::Admin));
    }

    #[test]
    fn operator_runs_and_writes_but_not_admin() {
        assert!(Role::Operator.permits(Action::Read));
        assert!(Role::Operator.permits(Action::Run));
        assert!(Role::Operator.permits(Action::Write));
        assert!(!Role::Operator.permits(Action::Admin));
    }
}
