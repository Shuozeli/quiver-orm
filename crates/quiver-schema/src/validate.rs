use crate::ast::*;
use quiver_error::QuiverError;

/// Validate a parsed schema for semantic correctness.
pub fn validate(schema: &Schema) -> Result<(), Vec<QuiverError>> {
    let mut errors = Vec::new();

    check_duplicate_names(schema, &mut errors);
    check_enum_references(schema, &mut errors);
    check_relation_references(schema, &mut errors);
    check_primary_keys(schema, &mut errors);
    check_relation_fields(schema, &mut errors);
    check_type_params(schema, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn check_duplicate_names(schema: &Schema, errors: &mut Vec<QuiverError>) {
    let mut seen = std::collections::HashSet::new();

    for e in &schema.enums {
        if !seen.insert(&e.name) {
            errors.push(QuiverError::Validation(format!(
                "duplicate type name '{}'",
                e.name
            )));
        }
    }
    for m in &schema.models {
        if !seen.insert(&m.name) {
            errors.push(QuiverError::Validation(format!(
                "duplicate type name '{}'",
                m.name
            )));
        }
        // Check duplicate field names within model
        let mut field_names = std::collections::HashSet::new();
        for f in &m.fields {
            if !field_names.insert(&f.name) {
                errors.push(QuiverError::Validation(format!(
                    "duplicate field '{}' in model '{}'",
                    f.name, m.name
                )));
            }
        }
    }
}

fn check_enum_references(schema: &Schema, errors: &mut Vec<QuiverError>) {
    let enum_names: std::collections::HashSet<&str> =
        schema.enums.iter().map(|e| e.name.as_str()).collect();
    let model_names: std::collections::HashSet<&str> =
        schema.models.iter().map(|m| m.name.as_str()).collect();

    for m in &schema.models {
        for f in &m.fields {
            check_named_type_refs(&f.type_expr, &enum_names, &model_names, &m.name, errors);
        }
    }
}

fn check_named_type_refs(
    type_expr: &TypeExpr,
    enum_names: &std::collections::HashSet<&str>,
    model_names: &std::collections::HashSet<&str>,
    context_model: &str,
    errors: &mut Vec<QuiverError>,
) {
    match &type_expr.base {
        BaseType::Named(name) => {
            if !enum_names.contains(name.as_str()) && !model_names.contains(name.as_str()) {
                errors.push(QuiverError::Validation(format!(
                    "unknown type '{}' in model '{}'",
                    name, context_model
                )));
            }
        }
        BaseType::List(inner) | BaseType::LargeList(inner) => {
            check_named_type_refs(inner, enum_names, model_names, context_model, errors);
        }
        BaseType::Map { key, value } => {
            check_named_type_refs(key, enum_names, model_names, context_model, errors);
            check_named_type_refs(value, enum_names, model_names, context_model, errors);
        }
        BaseType::Struct(fields) => {
            for sf in fields {
                check_named_type_refs(
                    &sf.type_expr,
                    enum_names,
                    model_names,
                    context_model,
                    errors,
                );
            }
        }
        _ => {}
    }
}

fn check_relation_references(schema: &Schema, errors: &mut Vec<QuiverError>) {
    let model_names: std::collections::HashSet<&str> =
        schema.models.iter().map(|m| m.name.as_str()).collect();

    for m in &schema.models {
        for attr in &m.attributes {
            if let ModelAttribute::ForeignKey {
                fields,
                references_model,
                references_columns,
                ..
            } = attr
            {
                // Check that FK fields exist in this model
                for fk in fields {
                    if !m.fields.iter().any(|mf| mf.name == *fk) {
                        errors.push(QuiverError::Validation(format!(
                            "FOREIGN KEY in model '{}': field '{}' does not exist",
                            m.name, fk
                        )));
                    }
                }
                // Check that the referenced model exists
                if !model_names.contains(references_model.as_str()) {
                    errors.push(QuiverError::Validation(format!(
                        "FOREIGN KEY in model '{}': referenced model '{}' not found",
                        m.name, references_model
                    )));
                }
                // Check field count matches reference count
                if fields.len() != references_columns.len() {
                    errors.push(QuiverError::Validation(format!(
                        "FOREIGN KEY in model '{}': fields and references must have same length",
                        m.name
                    )));
                }
            }
        }
    }
}

fn check_primary_keys(schema: &Schema, errors: &mut Vec<QuiverError>) {
    for m in &schema.models {
        let has_field_id = m
            .fields
            .iter()
            .any(|f| f.attributes.iter().any(|a| matches!(a, FieldAttribute::Id)));
        let has_model_id = m
            .attributes
            .iter()
            .any(|a| matches!(a, ModelAttribute::Id(_)));

        if !has_field_id && !has_model_id {
            // Relations-only models (join tables) might not have explicit IDs,
            // but regular models should. Warn, don't error.
        }

        // Check @@id fields exist
        for attr in &m.attributes {
            if let ModelAttribute::Id(fields) = attr {
                for f in fields {
                    if !m.fields.iter().any(|mf| mf.name == *f) {
                        errors.push(QuiverError::Validation(format!(
                            "@@id in model '{}': field '{}' does not exist",
                            m.name, f
                        )));
                    }
                }
            }
        }
    }
}

fn check_relation_fields(schema: &Schema, errors: &mut Vec<QuiverError>) {
    // Check @@index and @@unique reference existing fields
    for m in &schema.models {
        for attr in &m.attributes {
            let (kind, fields) = match attr {
                ModelAttribute::Index(f) => ("INDEX", f),
                ModelAttribute::Unique(f) => ("UNIQUE", f),
                _ => continue,
            };
            for f in fields {
                if !m.fields.iter().any(|mf| mf.name == *f) {
                    errors.push(QuiverError::Validation(format!(
                        "{kind} in model '{}': field '{f}' does not exist",
                        m.name
                    )));
                }
            }
        }
    }
}

fn check_type_params(schema: &Schema, errors: &mut Vec<QuiverError>) {
    for m in &schema.models {
        for f in &m.fields {
            check_type_expr_params(&f.type_expr, &m.name, &f.name, errors);
        }
    }
}

fn check_type_expr_params(
    type_expr: &TypeExpr,
    model: &str,
    field: &str,
    errors: &mut Vec<QuiverError>,
) {
    match &type_expr.base {
        BaseType::Decimal128 { precision, scale } | BaseType::Decimal256 { precision, scale } => {
            if *precision == 0 || *precision > 76 {
                errors.push(QuiverError::Validation(format!(
                    "field '{field}' in model '{model}': decimal precision must be 1-76, got {precision}"
                )));
            }
            if *scale < 0 || *scale > *precision as i8 {
                errors.push(QuiverError::Validation(format!(
                    "field '{field}' in model '{model}': decimal scale must be 0-{precision}, got {scale}"
                )));
            }
        }
        BaseType::FixedSizeBinary { size } => {
            if *size <= 0 {
                errors.push(QuiverError::Validation(format!(
                    "field '{field}' in model '{model}': FixedSizeBinary size must be > 0, got {size}"
                )));
            }
        }
        BaseType::List(inner) | BaseType::LargeList(inner) => {
            check_type_expr_params(inner, model, field, errors);
        }
        BaseType::Map { key, value } => {
            check_type_expr_params(key, model, field, errors);
            check_type_expr_params(value, model, field, errors);
        }
        BaseType::Struct(fields) => {
            for sf in fields {
                check_type_expr_params(&sf.type_expr, model, field, errors);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_unvalidated as parse;

    #[test]
    fn valid_schema_passes() {
        let schema = parse(
            r#"
            enum Role { User Admin }
            model User {
                id   Int32 PRIMARY KEY
                role Role
            }
        "#,
        )
        .unwrap();
        assert!(validate(&schema).is_ok());
    }

    #[test]
    fn duplicate_model_name() {
        let schema = parse(
            r#"
            model User { id Int32 PRIMARY KEY }
            model User { id Int32 PRIMARY KEY }
        "#,
        )
        .unwrap();
        let errs = validate(&schema).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("duplicate type name"))
        );
    }

    #[test]
    fn unknown_type_reference() {
        let schema = parse(
            r#"
            model User {
                id   Int32 PRIMARY KEY
                role UnknownEnum
            }
        "#,
        )
        .unwrap();
        let errs = validate(&schema).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("unknown type 'UnknownEnum'"))
        );
    }

    #[test]
    fn foreign_key_field_missing() {
        let schema = parse(
            r#"
            model User { id Int32 PRIMARY KEY }
            model Post {
                id       Int32 PRIMARY KEY

                FOREIGN KEY (authorId) REFERENCES User (id)
            }
        "#,
        )
        .unwrap();
        let errs = validate(&schema).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("field 'authorId' does not exist"))
        );
    }

    #[test]
    fn composite_id_field_missing() {
        let schema = parse(
            r#"
            model Pair {
                a Int32
                PRIMARY KEY (a, b)
            }
        "#,
        )
        .unwrap();
        let errs = validate(&schema).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("field 'b' does not exist"))
        );
    }

    #[test]
    fn duplicate_field_name() {
        let schema = parse(
            r#"
            model User {
                id Int32 PRIMARY KEY
                id Utf8
            }
        "#,
        )
        .unwrap();
        let errs = validate(&schema).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("duplicate field 'id'"))
        );
    }

    #[test]
    fn index_field_missing() {
        let schema = parse(
            r#"
            model User {
                id Int32 PRIMARY KEY
                INDEX (nonexistent)
            }
        "#,
        )
        .unwrap();
        let errs = validate(&schema).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("field 'nonexistent' does not exist"))
        );
    }
}
