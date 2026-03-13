//! Relation metadata and eager-loading (include) support.
//!
//! Relations are defined by schema `FOREIGN KEY` constraints. This module provides
//! types to describe relations and execute eager-load queries.

use quiver_driver_core::{DynConnection, Row, Statement, Value};
use quiver_error::QuiverError;

use crate::builder::build_find_many_internal;
use crate::filter::Filter;
use crate::safe_ident::SafeIdent;

/// Cardinality of a relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationType {
    /// This model holds the FK (many-to-one or one-to-one owner side).
    ManyToOne,
    /// The other model holds the FK (one-to-many).
    OneToMany,
    /// One-to-one (this model holds the FK).
    OneToOne,
}

/// Describes a relation between two models.
#[derive(Debug, Clone)]
pub struct RelationDef {
    /// Name of this relation field (e.g., "author", "posts").
    pub name: String,
    /// The model that owns this relation field.
    pub from_model: String,
    /// The related model.
    pub to_model: String,
    /// FK columns on the owning side.
    pub fields: Vec<String>,
    /// Referenced columns on the related side.
    pub references: Vec<String>,
    /// Cardinality.
    pub relation_type: RelationType,
}

/// Specifies which relations to eager-load.
#[derive(Debug, Clone)]
pub struct Include {
    /// The relation to load.
    pub relation: RelationDef,
    /// Nested includes on the related model.
    pub nested: Vec<Include>,
}

impl Include {
    /// Create an include for a relation with no nested includes.
    pub fn new(relation: RelationDef) -> Self {
        Self {
            relation,
            nested: Vec::new(),
        }
    }

    /// Add a nested include.
    pub fn with_nested(mut self, nested: Include) -> Self {
        self.nested.push(nested);
        self
    }
}

/// A parent row with its eagerly-loaded related rows.
#[derive(Debug, Clone)]
pub struct RowWithRelations {
    /// The parent row data.
    pub row: Row,
    /// Related rows keyed by relation name.
    pub relations: Vec<(String, Vec<RowWithRelations>)>,
}

/// Execute a parent query and eagerly load specified relations.
///
/// Strategy: run the parent query first, collect FK values, then run one
/// query per relation using `IN (...)` to batch-load related rows.
pub async fn find_with_includes(
    conn: &dyn DynConnection,
    parent_query: &Statement,
    includes: &[Include],
) -> Result<Vec<RowWithRelations>, QuiverError> {
    let parent_rows = conn.dyn_query(parent_query).await?;

    if parent_rows.is_empty() || includes.is_empty() {
        return Ok(parent_rows
            .into_iter()
            .map(|row| RowWithRelations {
                row,
                relations: Vec::new(),
            })
            .collect());
    }

    let mut results: Vec<RowWithRelations> = parent_rows
        .into_iter()
        .map(|row| RowWithRelations {
            row,
            relations: Vec::new(),
        })
        .collect();

    for include in includes {
        load_relation(conn, &mut results, include).await?;
    }

    Ok(results)
}

/// Load a single relation for a set of parent rows.
fn load_relation<'a>(
    conn: &'a dyn DynConnection,
    parent_results: &'a mut [RowWithRelations],
    include: &'a Include,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), QuiverError>> + Send + 'a>> {
    Box::pin(async move {
        let rel = &include.relation;

        match rel.relation_type {
            RelationType::ManyToOne | RelationType::OneToOne => {
                load_fk_side(conn, parent_results, include).await
            }
            RelationType::OneToMany => load_reverse_side(conn, parent_results, include).await,
        }
    })
}

/// Parent holds FK columns -> collect FK values, query related table by reference columns.
async fn load_fk_side(
    conn: &dyn DynConnection,
    parent_results: &mut [RowWithRelations],
    include: &Include,
) -> Result<(), QuiverError> {
    let rel = &include.relation;

    if rel.fields.len() != 1 || rel.references.len() != 1 {
        return Err(QuiverError::Validation(
            "Multi-column FK includes not yet supported".into(),
        ));
    }

    let fk_field = &rel.fields[0];
    let ref_field = &rel.references[0];

    // Collect unique FK values from parent rows
    let fk_values: Vec<Value> = parent_results
        .iter()
        .filter_map(|r| r.row.get_by_name(fk_field).cloned())
        .filter(|v| !v.is_null())
        .collect();

    if fk_values.is_empty() {
        for result in parent_results.iter_mut() {
            result.relations.push((rel.name.clone(), Vec::new()));
        }
        return Ok(());
    }

    let unique_values = dedup_values(&fk_values);

    // Validate the ref_field identifier
    validate_schema_ident(ref_field)?;
    let ref_ident = SafeIdent::new(leak_string(ref_field.clone()));

    // Build query using internal builder (schema-derived identifiers)
    let filter = Filter::in_ident(ref_ident, unique_values);
    let child_query = build_find_many_internal(&rel.to_model, Some(filter));

    let child_rows = conn.dyn_query(&child_query).await?;

    let mut child_results: Vec<RowWithRelations> = child_rows
        .into_iter()
        .map(|row| RowWithRelations {
            row,
            relations: Vec::new(),
        })
        .collect();

    for nested in &include.nested {
        load_relation(conn, &mut child_results, nested).await?;
    }

    for parent in parent_results.iter_mut() {
        let parent_fk = parent.row.get_by_name(fk_field).cloned();
        let matching: Vec<RowWithRelations> = child_results
            .iter()
            .filter(|child| {
                let child_ref = child.row.get_by_name(ref_field).cloned();
                match (&parent_fk, &child_ref) {
                    (Some(a), Some(b)) => values_equal(a, b),
                    _ => false,
                }
            })
            .cloned()
            .collect();
        parent.relations.push((rel.name.clone(), matching));
    }

    Ok(())
}

/// Related table holds FK -> collect parent PK values, query related table by FK columns.
async fn load_reverse_side(
    conn: &dyn DynConnection,
    parent_results: &mut [RowWithRelations],
    include: &Include,
) -> Result<(), QuiverError> {
    let rel = &include.relation;

    if rel.fields.len() != 1 || rel.references.len() != 1 {
        return Err(QuiverError::Validation(
            "Multi-column FK includes not yet supported".into(),
        ));
    }

    let child_fk_field = &rel.fields[0];
    let parent_pk_field = &rel.references[0];

    let pk_values: Vec<Value> = parent_results
        .iter()
        .filter_map(|r| r.row.get_by_name(parent_pk_field).cloned())
        .filter(|v| !v.is_null())
        .collect();

    if pk_values.is_empty() {
        for result in parent_results.iter_mut() {
            result.relations.push((rel.name.clone(), Vec::new()));
        }
        return Ok(());
    }

    let unique_values = dedup_values(&pk_values);

    validate_schema_ident(child_fk_field)?;
    let fk_ident = SafeIdent::new(leak_string(child_fk_field.clone()));

    let filter = Filter::in_ident(fk_ident, unique_values);
    let child_query = build_find_many_internal(&rel.to_model, Some(filter));

    let child_rows = conn.dyn_query(&child_query).await?;

    let mut child_results: Vec<RowWithRelations> = child_rows
        .into_iter()
        .map(|row| RowWithRelations {
            row,
            relations: Vec::new(),
        })
        .collect();

    for nested in &include.nested {
        load_relation(conn, &mut child_results, nested).await?;
    }

    for parent in parent_results.iter_mut() {
        let parent_pk = parent.row.get_by_name(parent_pk_field).cloned();
        let matching: Vec<RowWithRelations> = child_results
            .iter()
            .filter(|child| {
                let child_fk = child.row.get_by_name(child_fk_field).cloned();
                match (&parent_pk, &child_fk) {
                    (Some(a), Some(b)) => values_equal(a, b),
                    _ => false,
                }
            })
            .cloned()
            .collect();
        parent.relations.push((rel.name.clone(), matching));
    }

    Ok(())
}

/// Validate that a schema-derived identifier is safe for SQL.
fn validate_schema_ident(ident: &str) -> Result<(), QuiverError> {
    if ident.is_empty() {
        return Err(QuiverError::Validation("empty identifier".into()));
    }
    for ch in ident.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '_' && ch != '.' {
            return Err(QuiverError::Validation(format!(
                "invalid character '{}' in identifier '{}'",
                ch, ident
            )));
        }
    }
    Ok(())
}

/// Leak a String to get a `&'static str`.
///
/// Used only for schema-derived identifiers that have been validated via
/// `validate_schema_ident`. These live for the duration of the program
/// (schema metadata is loaded once at startup).
fn leak_string(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

/// Compare two Values for equality (used for FK matching).
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::UInt(a), Value::UInt(b)) => a == b,
        (Value::Text(a), Value::Text(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Blob(a), Value::Blob(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::Null, Value::Null) => false, // NULL != NULL
        _ => false,
    }
}

/// Deduplicate values (preserving order).
fn dedup_values(values: &[Value]) -> Vec<Value> {
    let mut seen = Vec::new();
    for v in values {
        if !seen.iter().any(|s| values_equal(s, v)) {
            seen.push(v.clone());
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_ints() {
        let vals = vec![Value::Int(1), Value::Int(2), Value::Int(1), Value::Int(3)];
        let deduped = dedup_values(&vals);
        assert_eq!(deduped.len(), 3);
    }

    #[test]
    fn dedup_strings() {
        let vals = vec![Value::Int(1), Value::Int(1)];
        let deduped = dedup_values(&vals);
        assert_eq!(deduped.len(), 1);
    }

    #[test]
    fn values_equal_different_types() {
        assert!(!values_equal(&Value::Int(1), &Value::Text("1".into())));
    }

    #[test]
    fn validate_good_idents() {
        assert!(validate_schema_ident("users").is_ok());
        assert!(validate_schema_ident("user_id").is_ok());
        assert!(validate_schema_ident("Account.name").is_ok());
    }

    #[test]
    fn validate_bad_idents() {
        assert!(validate_schema_ident("").is_err());
        assert!(validate_schema_ident("users;").is_err());
        assert!(validate_schema_ident("users DROP").is_err());
        assert!(validate_schema_ident("users\"").is_err());
    }
}
