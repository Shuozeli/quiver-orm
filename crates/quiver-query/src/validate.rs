//! Schema-aware query validation.
//!
//! Validates that queries reference valid table names, field names, and that
//! filter columns exist on the target model. Catches errors at build time
//! (Rust build time, not SQL execution time).

use quiver_error::QuiverError;
use quiver_schema::Schema;
use quiver_schema::ast::{FieldAttribute, FieldDef, ModelAttribute, ModelDef};

use crate::builder::{
    AggregateBuilder, AggregateFunc, CreateBuilder, CreateManyBuilder, DeleteBuilder,
    FindFirstBuilder, FindManyBuilder, UpdateBuilder, UpsertBuilder,
};
use crate::filter::Filter;
use crate::safe_ident::SafeIdent;

/// Schema context for validating queries.
///
/// Wraps a [`Schema`] reference and provides lookup methods for models and fields.
pub struct SchemaValidator<'a> {
    schema: &'a Schema,
}

impl<'a> SchemaValidator<'a> {
    /// Create a new validator from a schema.
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    /// Find a model by table name (checking both the model name and @@map).
    pub fn find_model(&self, table: &str) -> Result<&'a ModelDef, QuiverError> {
        self.schema
            .models
            .iter()
            .find(|m| table_name_for(m) == table)
            .ok_or_else(|| {
                QuiverError::Validation(format!(
                    "unknown table '{}'; available models: {}",
                    table,
                    self.schema
                        .models
                        .iter()
                        .map(table_name_for)
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })
    }

    /// Validate that a field name exists on a model (checking both name and @map).
    pub fn find_field<'m>(
        &self,
        model: &'m ModelDef,
        field_name: &str,
    ) -> Result<&'m FieldDef, QuiverError> {
        model
            .fields
            .iter()
            .find(|f| column_name_for(f) == field_name || f.name == field_name)
            .ok_or_else(|| {
                let available: Vec<String> = model
                    .fields
                    .iter()
                    .filter(|f| !is_relation_object(f))
                    .map(column_name_for)
                    .collect();
                QuiverError::Validation(format!(
                    "unknown field '{}' on model '{}'; available fields: {}",
                    field_name,
                    model.name,
                    available.join(", ")
                ))
            })
    }

    /// Validate a table name.
    fn validate_table(&self, table: &SafeIdent) -> Result<&'a ModelDef, QuiverError> {
        self.find_model(table.as_str())
    }

    /// Validate a column name against a model.
    fn validate_column(&self, model: &ModelDef, col: &SafeIdent) -> Result<(), QuiverError> {
        let col_str = col.as_str();
        // Handle table-qualified columns (Table.column)
        let field_name = if let Some((_table, column)) = col_str.split_once('.') {
            column
        } else {
            col_str
        };
        self.find_field(model, field_name)?;
        Ok(())
    }

    /// Validate all column references in a filter against a model.
    fn validate_filter(&self, model: &ModelDef, filter: &Filter) -> Result<(), QuiverError> {
        match filter {
            Filter::Eq(col, _)
            | Filter::Neq(col, _)
            | Filter::Gt(col, _)
            | Filter::Gte(col, _)
            | Filter::Lt(col, _)
            | Filter::Lte(col, _)
            | Filter::Like(col, _) => self.validate_column(model, col),
            Filter::In(col, _) | Filter::NotIn(col, _) => self.validate_column(model, col),
            Filter::IsNull(col) | Filter::IsNotNull(col) => self.validate_column(model, col),
            Filter::Between(col, _, _) => self.validate_column(model, col),
            Filter::And(filters) | Filter::Or(filters) => {
                for f in filters {
                    self.validate_filter(model, f)?;
                }
                Ok(())
            }
            Filter::Not(inner) => self.validate_filter(model, inner),
            Filter::Raw { .. } => Ok(()), // Raw SQL bypasses validation
        }
    }

    /// Validate an aggregate function's column reference.
    fn validate_aggregate(&self, model: &ModelDef, agg: &AggregateFunc) -> Result<(), QuiverError> {
        match agg {
            AggregateFunc::CountAll => Ok(()),
            AggregateFunc::Count(col)
            | AggregateFunc::Sum(col)
            | AggregateFunc::Avg(col)
            | AggregateFunc::Min(col)
            | AggregateFunc::Max(col) => self.validate_column(model, col),
        }
    }
}

// --- Validated build methods for each builder ---

impl FindManyBuilder {
    /// Validate all identifiers against the schema, then build.
    pub fn build_validated(
        self,
        validator: &SchemaValidator<'_>,
    ) -> Result<super::BuiltQuery, QuiverError> {
        let model = validator.validate_table(&self.table)?;
        for col in &self.select {
            validator.validate_column(model, col)?;
        }
        for f in &self.filters {
            validator.validate_filter(model, f)?;
        }
        for (col, _) in &self.order {
            validator.validate_column(model, col)?;
        }
        Ok(self.build())
    }
}

impl FindFirstBuilder {
    /// Validate all identifiers against the schema, then build.
    pub fn build_validated(
        self,
        validator: &SchemaValidator<'_>,
    ) -> Result<super::BuiltQuery, QuiverError> {
        let model = validator.validate_table(&self.table)?;
        for col in &self.select {
            validator.validate_column(model, col)?;
        }
        for f in &self.filters {
            validator.validate_filter(model, f)?;
        }
        for (col, _) in &self.order {
            validator.validate_column(model, col)?;
        }
        Ok(self.build())
    }
}

impl CreateBuilder {
    /// Validate all identifiers against the schema, then build.
    pub fn build_validated(
        self,
        validator: &SchemaValidator<'_>,
    ) -> Result<super::BuiltQuery, QuiverError> {
        let model = validator.validate_table(&self.table)?;
        for col in &self.columns {
            validator.validate_column(model, col)?;
        }
        Ok(self.build())
    }
}

impl CreateManyBuilder {
    /// Validate all identifiers against the schema, then build.
    pub fn build_validated(
        self,
        validator: &SchemaValidator<'_>,
    ) -> Result<super::BuiltQuery, QuiverError> {
        let model = validator.validate_table(&self.table)?;
        for col in &self.columns {
            validator.validate_column(model, col)?;
        }
        Ok(self.build())
    }
}

impl UpdateBuilder {
    /// Validate all identifiers against the schema, then build.
    pub fn build_validated(
        self,
        validator: &SchemaValidator<'_>,
    ) -> Result<super::BuiltQuery, QuiverError> {
        let model = validator.validate_table(&self.table)?;
        for col in &self.sets {
            validator.validate_column(model, col)?;
        }
        for f in &self.filters {
            validator.validate_filter(model, f)?;
        }
        Ok(self.build())
    }
}

impl DeleteBuilder {
    /// Validate all identifiers against the schema, then build.
    pub fn build_validated(
        self,
        validator: &SchemaValidator<'_>,
    ) -> Result<super::BuiltQuery, QuiverError> {
        let model = validator.validate_table(&self.table)?;
        for f in &self.filters {
            validator.validate_filter(model, f)?;
        }
        Ok(self.build())
    }
}

impl UpsertBuilder {
    /// Validate all identifiers against the schema, then build.
    pub fn build_validated(
        self,
        validator: &SchemaValidator<'_>,
    ) -> Result<super::BuiltQuery, QuiverError> {
        let model = validator.validate_table(&self.table)?;
        for col in &self.columns {
            validator.validate_column(model, col)?;
        }
        for col in &self.conflict_columns {
            validator.validate_column(model, col)?;
        }
        for col in &self.update_sets {
            validator.validate_column(model, col)?;
        }
        Ok(self.build())
    }
}

impl AggregateBuilder {
    /// Validate all identifiers against the schema, then build.
    pub fn build_validated(
        self,
        validator: &SchemaValidator<'_>,
    ) -> Result<super::BuiltQuery, QuiverError> {
        let model = validator.validate_table(&self.table)?;
        for agg in &self.aggregates {
            validator.validate_aggregate(model, agg)?;
        }
        for col in &self.group_by {
            validator.validate_column(model, col)?;
        }
        for f in &self.filters {
            validator.validate_filter(model, f)?;
        }
        for (agg, _, _) in &self.having {
            validator.validate_aggregate(model, agg)?;
        }
        for (col, _) in &self.order {
            validator.validate_column(model, col)?;
        }
        Ok(self.build())
    }
}

// --- Helper functions ---

fn table_name_for(m: &ModelDef) -> String {
    for attr in &m.attributes {
        if let ModelAttribute::Map(name) = attr {
            return name.clone();
        }
    }
    m.name.clone()
}

fn column_name_for(f: &FieldDef) -> String {
    for attr in &f.attributes {
        if let FieldAttribute::Map(name) = attr {
            return name.clone();
        }
    }
    f.name.clone()
}

fn is_relation_object(f: &FieldDef) -> bool {
    f.attributes
        .iter()
        .any(|a| matches!(a, FieldAttribute::Relation { .. }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Filter, Order, Query};
    use quiver_schema::parse;

    fn user_schema() -> Schema {
        parse(
            r#"
            model User {
                id    Int32 @id @autoincrement
                name  Utf8
                email Utf8  @unique
                age   Int32?
            }
        "#,
        )
        .unwrap()
    }

    fn mapped_schema() -> Schema {
        parse(
            r#"
            model User {
                id    Int32 @id
                name  Utf8
                @@map("users")
            }
        "#,
        )
        .unwrap()
    }

    #[test]
    fn valid_find_many_passes() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .find_many()
            .select(&["id", "email"])
            .filter(Filter::eq("name", "Alice"))
            .order_by("age", Order::Desc)
            .build_validated(&v);
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_table_name_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("NonExistent").find_many().build_validated(&v);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown table 'NonExistent'"));
        assert!(err.contains("User"));
    }

    #[test]
    fn invalid_select_field_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .find_many()
            .select(&["id", "nonexistent"])
            .build_validated(&v);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown field 'nonexistent'"));
        assert!(err.contains("User"));
    }

    #[test]
    fn invalid_filter_field_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .find_many()
            .filter(Filter::eq("bogus", "value"))
            .build_validated(&v);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown field 'bogus'")
        );
    }

    #[test]
    fn invalid_order_field_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .find_many()
            .order_by("missing", Order::Asc)
            .build_validated(&v);
        assert!(result.is_err());
    }

    #[test]
    fn valid_create_passes() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .create()
            .set("name", "Alice")
            .set("email", "alice@test.com")
            .build_validated(&v);
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_create_field_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .create()
            .set("name", "Alice")
            .set("phone", "123")
            .build_validated(&v);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown field 'phone'")
        );
    }

    #[test]
    fn valid_update_passes() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .update()
            .set("name", "Bob")
            .filter(Filter::eq("id", 1i32))
            .build_validated(&v);
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_update_set_field_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .update()
            .set("nonexistent", "value")
            .filter(Filter::eq("id", 1i32))
            .build_validated(&v);
        assert!(result.is_err());
    }

    #[test]
    fn valid_delete_passes() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .delete()
            .filter(Filter::eq("id", 1i32))
            .build_validated(&v);
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_delete_filter_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .delete()
            .filter(Filter::eq("missing_field", 1i32))
            .build_validated(&v);
        assert!(result.is_err());
    }

    #[test]
    fn valid_aggregate_passes() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .aggregate()
            .count_all()
            .sum("age")
            .group_by("name")
            .build_validated(&v);
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_aggregate_field_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .aggregate()
            .sum("nonexistent")
            .build_validated(&v);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_group_by_field_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .aggregate()
            .count_all()
            .group_by("fake")
            .build_validated(&v);
        assert!(result.is_err());
    }

    #[test]
    fn valid_upsert_passes() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .upsert()
            .set("email", "alice@test.com")
            .set("name", "Alice")
            .conflict_on(&["email"])
            .on_conflict_set("name", "Alice Updated")
            .build_validated(&v);
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_upsert_conflict_field_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .upsert()
            .set("email", "alice@test.com")
            .conflict_on(&["nonexistent"])
            .on_conflict_set("name", "Alice")
            .build_validated(&v);
        assert!(result.is_err());
    }

    #[test]
    fn valid_create_many_passes() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .create_many()
            .columns(&["name", "email"])
            .values(vec!["Alice".into(), "alice@test.com".into()])
            .build_validated(&v);
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_create_many_field_fails() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .create_many()
            .columns(&["name", "bogus"])
            .values(vec!["Alice".into(), "value".into()])
            .build_validated(&v);
        assert!(result.is_err());
    }

    #[test]
    fn mapped_table_name_works() {
        let schema = mapped_schema();
        let v = SchemaValidator::new(&schema);
        // Access via mapped name
        let result = Query::table("users")
            .find_many()
            .filter(Filter::eq("name", "Alice"))
            .build_validated(&v);
        assert!(result.is_ok());
    }

    #[test]
    fn nested_filter_validates_all_columns() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .find_many()
            .filter(Filter::and(vec![
                Filter::eq("name", "Alice"),
                Filter::or(vec![
                    Filter::gte("age", 18i32),
                    Filter::eq("bad_field", "x"),
                ]),
            ]))
            .build_validated(&v);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown field 'bad_field'")
        );
    }

    #[test]
    fn not_filter_validates_inner() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .find_many()
            .filter(Filter::Not(Box::new(Filter::eq("ghost", "value"))))
            .build_validated(&v);
        assert!(result.is_err());
    }

    #[test]
    fn find_first_validated() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .find_first()
            .filter(Filter::eq("id", 1i32))
            .build_validated(&v);
        assert!(result.is_ok());

        let result = Query::table("User")
            .find_first()
            .filter(Filter::eq("nope", 1i32))
            .build_validated(&v);
        assert!(result.is_err());
    }

    #[test]
    fn error_message_lists_available_fields() {
        let schema = user_schema();
        let v = SchemaValidator::new(&schema);
        let result = Query::table("User")
            .find_many()
            .select(&["bad"])
            .build_validated(&v);
        let err = result.unwrap_err().to_string();
        assert!(err.contains("id"));
        assert!(err.contains("name"));
        assert!(err.contains("email"));
        assert!(err.contains("age"));
    }
}
