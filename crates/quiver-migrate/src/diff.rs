//! Schema diffing -- compare two Quiver schemas and produce migration steps.

use quiver_schema::Schema;
use quiver_schema::ast::*;

use crate::step::MigrationStep;

/// Compare two schemas and produce the migration steps needed to go from `old` to `new`.
///
/// If `old` is `None`, generates steps to create all models from scratch.
pub fn diff_schemas(old: Option<&Schema>, new: &Schema) -> Vec<MigrationStep> {
    let empty_schema = Schema {
        config: None,
        generate: None,
        enums: Vec::new(),
        models: Vec::new(),
    };
    let old = old.unwrap_or(&empty_schema);

    let mut steps = Vec::new();

    diff_enums(old, new, &mut steps);
    diff_models(old, new, &mut steps);

    steps
}

fn diff_enums(old: &Schema, new: &Schema, steps: &mut Vec<MigrationStep>) {
    // Detect added enums
    for new_enum in &new.enums {
        let old_enum = old.enums.iter().find(|e| e.name == new_enum.name);
        match old_enum {
            None => {
                steps.push(MigrationStep::CreateEnum {
                    name: new_enum.name.clone(),
                    values: new_enum.values.iter().map(|v| v.name.clone()).collect(),
                });
            }
            Some(old_e) => {
                // Detect added values
                for new_val in &new_enum.values {
                    if !old_e.values.iter().any(|v| v.name == new_val.name) {
                        steps.push(MigrationStep::AddEnumValue {
                            enum_name: new_enum.name.clone(),
                            value: new_val.name.clone(),
                        });
                    }
                }
                // Detect removed values
                for old_val in &old_e.values {
                    if !new_enum.values.iter().any(|v| v.name == old_val.name) {
                        steps.push(MigrationStep::RemoveEnumValue {
                            enum_name: new_enum.name.clone(),
                            value: old_val.name.clone(),
                        });
                    }
                }
            }
        }
    }

    // Detect dropped enums
    for old_enum in &old.enums {
        if !new.enums.iter().any(|e| e.name == old_enum.name) {
            steps.push(MigrationStep::DropEnum {
                name: old_enum.name.clone(),
            });
        }
    }
}

fn diff_models(old: &Schema, new: &Schema, steps: &mut Vec<MigrationStep>) {
    // Detect added and modified models
    for new_model in &new.models {
        let old_model = old.models.iter().find(|m| m.name == new_model.name);
        match old_model {
            None => {
                steps.push(MigrationStep::CreateModel {
                    name: new_model.name.clone(),
                    model: new_model.clone(),
                });
                // Also emit CreateIndex steps for any INDEX on the new model
                let empty = ModelDef {
                    name: new_model.name.clone(),
                    fields: Vec::new(),
                    attributes: Vec::new(),
                    span: new_model.span,
                };
                diff_model_attributes(&empty, new_model, steps);
            }
            Some(old_m) => {
                diff_model_fields(old_m, new_model, steps);
                diff_model_attributes(old_m, new_model, steps);
            }
        }
    }

    // Detect dropped models
    for old_model in &old.models {
        if !new.models.iter().any(|m| m.name == old_model.name) {
            steps.push(MigrationStep::DropModel {
                name: old_model.name.clone(),
            });
        }
    }
}

fn diff_model_fields(old: &ModelDef, new: &ModelDef, steps: &mut Vec<MigrationStep>) {
    // Detect added fields
    for new_field in &new.fields {
        let old_field = old.fields.iter().find(|f| f.name == new_field.name);
        match old_field {
            None => {
                steps.push(MigrationStep::AddField {
                    model: new.name.clone(),
                    field: new_field.clone(),
                });
            }
            Some(old_f) => {
                if field_changed(old_f, new_field) {
                    steps.push(MigrationStep::AlterField {
                        model: new.name.clone(),
                        old_field: old_f.clone(),
                        new_field: new_field.clone(),
                    });
                }
            }
        }
    }

    // Detect dropped fields
    for old_field in &old.fields {
        if !new.fields.iter().any(|f| f.name == old_field.name) {
            steps.push(MigrationStep::DropField {
                model: new.name.clone(),
                field_name: old_field.name.clone(),
            });
        }
    }
}

fn diff_model_attributes(old: &ModelDef, new: &ModelDef, steps: &mut Vec<MigrationStep>) {
    let old_indexes = extract_indexes(old);
    let new_indexes = extract_indexes(new);

    // Added indexes
    for (name, cols) in &new_indexes {
        if !old_indexes.contains_key(name) {
            steps.push(MigrationStep::CreateIndex {
                model: new.name.clone(),
                index_name: name.clone(),
                columns: cols.clone(),
            });
        }
    }

    // Dropped indexes
    for name in old_indexes.keys() {
        if !new_indexes.contains_key(name) {
            steps.push(MigrationStep::DropIndex {
                index_name: name.clone(),
            });
        }
    }
}

/// Check if a field has changed in a way that requires migration.
fn field_changed(old: &FieldDef, new: &FieldDef) -> bool {
    // Type change
    if !type_equal(&old.type_expr, &new.type_expr) {
        return true;
    }

    // Nullability change
    if old.type_expr.nullable != new.type_expr.nullable {
        return true;
    }

    // Unique constraint change
    if has_attr_unique(old) != has_attr_unique(new) {
        return true;
    }

    // Default change
    if !defaults_equal(old, new) {
        return true;
    }

    false
}

fn type_equal(a: &TypeExpr, b: &TypeExpr) -> bool {
    a.base == b.base && a.nullable == b.nullable
}

fn has_attr_unique(f: &FieldDef) -> bool {
    f.attributes
        .iter()
        .any(|a| matches!(a, FieldAttribute::Unique))
}

fn defaults_equal(a: &FieldDef, b: &FieldDef) -> bool {
    let a_default = a.attributes.iter().find_map(|a| {
        if let FieldAttribute::Default(d) = a {
            Some(d)
        } else {
            None
        }
    });
    let b_default = b.attributes.iter().find_map(|a| {
        if let FieldAttribute::Default(d) = a {
            Some(d)
        } else {
            None
        }
    });
    a_default == b_default
}

/// Extract named indexes from model attributes.
fn extract_indexes(m: &ModelDef) -> std::collections::HashMap<String, Vec<String>> {
    let mut indexes = std::collections::HashMap::new();
    let table_name = get_table_name(m);

    for attr in &m.attributes {
        if let ModelAttribute::Index(cols) = attr {
            let index_name = format!("idx_{}_{}", table_name, cols.join("_"));
            indexes.insert(index_name, cols.clone());
        }
    }
    indexes
}

fn get_table_name(m: &ModelDef) -> &str {
    for attr in &m.attributes {
        if let ModelAttribute::Map(name) = attr {
            return name;
        }
    }
    &m.name
}

#[cfg(test)]
mod tests {
    use super::*;
    use quiver_schema::parse;

    #[test]
    fn diff_empty_to_schema_creates_models() {
        let new = parse(
            r#"
            enum Role { User Admin }
            model Account {
                id    Int32 PRIMARY KEY AUTOINCREMENT
                email Utf8  UNIQUE
                role  Role
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(None, &new);
        assert_eq!(steps.len(), 2); // CreateEnum + CreateModel

        assert!(matches!(&steps[0], MigrationStep::CreateEnum { name, .. } if name == "Role"));
        assert!(matches!(&steps[1], MigrationStep::CreateModel { name, .. } if name == "Account"));
    }

    #[test]
    fn diff_add_field() {
        let old = parse(
            r#"
            model User {
                id   Int32 PRIMARY KEY
                name Utf8
            }
        "#,
        )
        .unwrap();

        let new = parse(
            r#"
            model User {
                id    Int32 PRIMARY KEY
                name  Utf8
                email Utf8
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        assert_eq!(steps.len(), 1);
        assert!(
            matches!(&steps[0], MigrationStep::AddField { model, field } if model == "User" && field.name == "email")
        );
    }

    #[test]
    fn diff_drop_field() {
        let old = parse(
            r#"
            model User {
                id   Int32 PRIMARY KEY
                name Utf8
                age  Int32
            }
        "#,
        )
        .unwrap();

        let new = parse(
            r#"
            model User {
                id   Int32 PRIMARY KEY
                name Utf8
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        assert_eq!(steps.len(), 1);
        assert!(
            matches!(&steps[0], MigrationStep::DropField { model, field_name } if model == "User" && field_name == "age")
        );
    }

    #[test]
    fn diff_add_model() {
        let old = parse(
            r#"
            model User {
                id Int32 PRIMARY KEY
            }
        "#,
        )
        .unwrap();

        let new = parse(
            r#"
            model User {
                id Int32 PRIMARY KEY
            }
            model Post {
                id    Int32 PRIMARY KEY
                title Utf8
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        assert_eq!(steps.len(), 1);
        assert!(matches!(&steps[0], MigrationStep::CreateModel { name, .. } if name == "Post"));
    }

    #[test]
    fn diff_drop_model() {
        let old = parse(
            r#"
            model User { id Int32 PRIMARY KEY }
            model Post { id Int32 PRIMARY KEY }
        "#,
        )
        .unwrap();

        let new = parse(
            r#"
            model User { id Int32 PRIMARY KEY }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        assert_eq!(steps.len(), 1);
        assert!(matches!(&steps[0], MigrationStep::DropModel { name } if name == "Post"));
    }

    #[test]
    fn diff_alter_field_type() {
        let old = parse(
            r#"
            model User {
                id   Int32 PRIMARY KEY
                name Utf8
            }
        "#,
        )
        .unwrap();

        let new = parse(
            r#"
            model User {
                id   Int32 PRIMARY KEY
                name LargeUtf8
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        assert_eq!(steps.len(), 1);
        assert!(matches!(&steps[0], MigrationStep::AlterField { model, .. } if model == "User"));
    }

    #[test]
    fn diff_nullability_change() {
        let old = parse(
            r#"
            model User {
                id   Int32 PRIMARY KEY
                bio  Utf8
            }
        "#,
        )
        .unwrap();

        let new = parse(
            r#"
            model User {
                id   Int32 PRIMARY KEY
                bio  Utf8?
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        assert_eq!(steps.len(), 1);
        assert!(matches!(&steps[0], MigrationStep::AlterField { .. }));
    }

    #[test]
    fn diff_add_index() {
        let old = parse(
            r#"
            model User {
                id    Int32 PRIMARY KEY
                email Utf8
            }
        "#,
        )
        .unwrap();

        let new = parse(
            r#"
            model User {
                id    Int32 PRIMARY KEY
                email Utf8
                INDEX (email)
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        assert_eq!(steps.len(), 1);
        assert!(matches!(&steps[0], MigrationStep::CreateIndex { .. }));
    }

    #[test]
    fn diff_add_enum() {
        let old = parse(
            r#"
            model User { id Int32 PRIMARY KEY }
        "#,
        )
        .unwrap();

        let new = parse(
            r#"
            enum Role { User Admin }
            model User { id Int32 PRIMARY KEY }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        assert_eq!(steps.len(), 1);
        assert!(matches!(&steps[0], MigrationStep::CreateEnum { name, .. } if name == "Role"));
    }

    #[test]
    fn diff_add_enum_value() {
        let old = parse(
            r#"
            enum Role { User Admin }
        "#,
        )
        .unwrap();

        let new = parse(
            r#"
            enum Role { User Admin Moderator }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        assert_eq!(steps.len(), 1);
        assert!(
            matches!(&steps[0], MigrationStep::AddEnumValue { enum_name, value } if enum_name == "Role" && value == "Moderator")
        );
    }

    #[test]
    fn diff_identical_schemas_no_steps() {
        let schema = parse(
            r#"
            enum Role { User Admin }
            model Account {
                id    Int32 PRIMARY KEY AUTOINCREMENT
                email Utf8  UNIQUE
                role  Role  DEFAULT User
                INDEX (email)
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&schema), &schema);
        assert!(steps.is_empty());
    }
}
