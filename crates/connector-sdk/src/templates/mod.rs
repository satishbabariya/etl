//! Embedded source-connector templates per language.

pub mod rust;
pub mod typescript;

pub use rust::materialize_source_template;
pub use typescript::materialize_source_template_typescript;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_template_is_reachable() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme-source");
        materialize_source_template(&target, "acme-source").unwrap();
        assert!(target.join("Cargo.toml").exists());
    }

    #[test]
    fn ts_template_is_reachable() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme-source-ts");
        materialize_source_template_typescript(&target, "acme-source-ts").unwrap();
        assert!(target.join("package.json").exists());
    }
}
