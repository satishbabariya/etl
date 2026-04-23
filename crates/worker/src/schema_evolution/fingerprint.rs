use arrow::datatypes::{DataType, Field, Schema};
use blake3::Hasher;
use common_types::schema_fingerprint::SchemaFingerprint;

/// Produce a BLAKE3 fingerprint of a schema's structural shape.
///
/// Fields are sorted by name (reorder is a separate ChangeKind).
/// Each field contributes <name>\0<type>\0<nullable>\0<metadata>\0.
pub fn fingerprint_schema(schema: &Schema) -> SchemaFingerprint {
    let mut fields: Vec<&Field> = schema.fields().iter().map(|f| f.as_ref()).collect();
    fields.sort_by(|a, b| a.name().cmp(b.name()));

    let mut hasher = Hasher::new();
    for f in fields {
        hasher.update(f.name().as_bytes());
        hasher.update(b"\0");
        hasher.update(datatype_string(f.data_type()).as_bytes());
        hasher.update(b"\0");
        hasher.update(if f.is_nullable() { b"1" } else { b"0" });
        hasher.update(b"\0");
        hasher.update(metadata_string(f.metadata()).as_bytes());
        hasher.update(b"\0");
    }
    hasher.update(metadata_string(schema.metadata()).as_bytes());
    let digest = hasher.finalize();
    SchemaFingerprint::from_hex(digest.to_hex().to_string())
}

fn datatype_string(dt: &DataType) -> String {
    format!("{dt:?}")
}

fn metadata_string(md: &std::collections::HashMap<String, String>) -> String {
    let mut entries: Vec<(&String, &String)> = md.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let joined: Vec<String> = entries.iter().map(|(k, v)| format!("{k}={v}")).collect();
    joined.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    #[test]
    fn identical_schemas_fingerprint_equal() {
        let a = schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let b = schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        assert_eq!(fingerprint_schema(&a), fingerprint_schema(&b));
    }

    #[test]
    fn column_order_does_not_affect_fingerprint() {
        let a = schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let b = schema(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
        ]);
        assert_eq!(fingerprint_schema(&a), fingerprint_schema(&b));
    }

    #[test]
    fn nullability_change_affects_fingerprint() {
        let a = schema(vec![Field::new("x", DataType::Int64, false)]);
        let b = schema(vec![Field::new("x", DataType::Int64, true)]);
        assert_ne!(fingerprint_schema(&a), fingerprint_schema(&b));
    }

    #[test]
    fn type_change_affects_fingerprint() {
        let a = schema(vec![Field::new("x", DataType::Int32, false)]);
        let b = schema(vec![Field::new("x", DataType::Int64, false)]);
        assert_ne!(fingerprint_schema(&a), fingerprint_schema(&b));
    }

    #[test]
    fn added_column_affects_fingerprint() {
        let a = schema(vec![Field::new("x", DataType::Int64, false)]);
        let b = schema(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Utf8, true),
        ]);
        assert_ne!(fingerprint_schema(&a), fingerprint_schema(&b));
    }
}
