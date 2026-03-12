//! Migration step types -- the atomic operations that make up a migration.

use quiver_schema::ast::{FieldAttribute, FieldDef, ModelDef};
use serde::{Deserialize, Serialize};

use crate::sql_gen::TrustedSql;

/// A single migration operation.
#[derive(Debug, Clone)]
pub enum MigrationStep {
    /// Create a new model (table).
    CreateModel { name: String, model: ModelDef },
    /// Drop an existing model.
    DropModel { name: String },
    /// Add a field (column) to an existing model.
    AddField { model: String, field: FieldDef },
    /// Drop a field from an existing model.
    DropField { model: String, field_name: String },
    /// Alter a field (type change, nullability, default, etc.).
    AlterField {
        model: String,
        old_field: FieldDef,
        new_field: FieldDef,
    },
    /// Create an index.
    CreateIndex {
        model: String,
        index_name: String,
        columns: Vec<String>,
    },
    /// Drop an index.
    DropIndex { index_name: String },
    /// Create a new enum type.
    CreateEnum { name: String, values: Vec<String> },
    /// Drop an enum type.
    DropEnum { name: String },
    /// Add a value to an existing enum.
    AddEnumValue { enum_name: String, value: String },
    /// Remove a value from an existing enum.
    RemoveEnumValue { enum_name: String, value: String },
}

/// Returns true if this field represents a relation object (has `@relation` attribute).
pub fn is_relation_object(f: &FieldDef) -> bool {
    f.attributes
        .iter()
        .any(|a| matches!(a, FieldAttribute::Relation { .. }))
}

/// A complete migration with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    /// Unique migration identifier (timestamp-based).
    pub id: String,
    /// Human-readable description.
    pub description: String,
    /// SQL statements to apply (forward).
    pub up: Vec<TrustedSql>,
    /// SQL statements to rollback (reverse).
    pub down: Vec<TrustedSql>,
}
