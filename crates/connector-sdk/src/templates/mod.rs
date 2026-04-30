//! Embedded source-connector templates per language.

pub mod rust;

pub use rust::materialize_source_template;

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
}
