use arrow::datatypes::{DataType, Schema};
use common_types::evolution::ChangeKind;
use std::collections::{BTreeMap, HashSet};

pub fn diff_schemas(old: &Schema, new: &Schema) -> Vec<ChangeKind> {
    let mut changes = Vec::new();

    let old_fields: BTreeMap<&str, &arrow::datatypes::Field> =
        old.fields().iter().map(|f| (f.name().as_str(), f.as_ref())).collect();
    let new_fields: BTreeMap<&str, &arrow::datatypes::Field> =
        new.fields().iter().map(|f| (f.name().as_str(), f.as_ref())).collect();

    let old_names: HashSet<&str> = old_fields.keys().copied().collect();
    let new_names: HashSet<&str> = new_fields.keys().copied().collect();

    for name in new_names.difference(&old_names) {
        let f = new_fields[name];
        changes.push(ChangeKind::AddColumn {
            name: name.to_string(),
            data_type: datatype_string(f.data_type()),
            nullable: f.is_nullable(),
        });
    }
    for name in old_names.difference(&new_names) {
        changes.push(ChangeKind::DropColumn { name: name.to_string() });
    }
    for name in old_names.intersection(&new_names) {
        let o = old_fields[name];
        let n = new_fields[name];
        let o_ty = datatype_string(o.data_type());
        let n_ty = datatype_string(n.data_type());
        if o_ty != n_ty {
            if is_widening(o.data_type(), n.data_type()) {
                changes.push(ChangeKind::WidenType {
                    name: name.to_string(), from: o_ty, to: n_ty,
                });
            } else {
                changes.push(ChangeKind::NarrowType {
                    name: name.to_string(), from: o_ty, to: n_ty,
                });
            }
        }
        if o.is_nullable() != n.is_nullable() {
            if n.is_nullable() {
                changes.push(ChangeKind::MakeNullable { name: name.to_string() });
            } else {
                changes.push(ChangeKind::MakeNonNullable { name: name.to_string() });
            }
        }
    }

    let old_order: Vec<String> = old
        .fields()
        .iter()
        .filter(|f| new_names.contains(f.name().as_str()))
        .map(|f| f.name().clone())
        .collect();
    let new_order: Vec<String> = new
        .fields()
        .iter()
        .filter(|f| old_names.contains(f.name().as_str()))
        .map(|f| f.name().clone())
        .collect();
    if old_order != new_order && !old_order.is_empty() {
        changes.push(ChangeKind::ReorderColumns {
            before: old_order, after: new_order,
        });
    }

    changes
}

fn datatype_string(dt: &DataType) -> String {
    format!("{dt:?}")
}

fn is_widening(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    matches!(
        (from, to),
        (Int8, Int16 | Int32 | Int64)
            | (Int16, Int32 | Int64)
            | (Int32, Int64)
            | (UInt8, UInt16 | UInt32 | UInt64)
            | (UInt16, UInt32 | UInt64)
            | (UInt32, UInt64)
            | (Float32, Float64)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn identical_schemas_no_changes() {
        let s = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        assert!(diff_schemas(&s, &s).is_empty());
    }

    #[test]
    fn add_nullable_column() {
        let old = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let new = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int64, true),
        ]);
        let c = diff_schemas(&old, &new);
        assert_eq!(c.len(), 1);
        assert!(matches!(
            &c[0],
            ChangeKind::AddColumn { name, nullable: true, .. } if name == "age"
        ));
    }

    #[test]
    fn drop_column() {
        let old = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        let new = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let c = diff_schemas(&old, &new);
        assert!(c.iter().any(|ck| matches!(ck, ChangeKind::DropColumn { name } if name == "email")));
    }

    #[test]
    fn widen_int32_to_int64() {
        let old = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let new = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
        let c = diff_schemas(&old, &new);
        assert_eq!(c.len(), 1);
        assert!(matches!(&c[0], ChangeKind::WidenType { name, .. } if name == "x"));
    }

    #[test]
    fn narrow_int64_to_int32() {
        let old = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
        let new = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let c = diff_schemas(&old, &new);
        assert!(c.iter().any(|ck| matches!(ck, ChangeKind::NarrowType { .. })));
    }

    #[test]
    fn make_nullable() {
        let old = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
        let new = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        let c = diff_schemas(&old, &new);
        assert!(c.iter().any(|ck| matches!(ck, ChangeKind::MakeNullable { .. })));
    }
}
