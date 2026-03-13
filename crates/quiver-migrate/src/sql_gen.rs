//! SQL DDL generation from migration steps.
//!
//! **Security model**: All SQL is constructed via `TrustedSqlBuilder`, which
//! only accepts `&'static str` for SQL text. Dynamic values (enum values,
//! default strings) go through bind parameters. Identifiers (table/column
//! names) go through `TrustedSqlBuilder::push_ident` which validates and
//! double-quote-escapes them. There is intentionally no function that accepts
//! arbitrary `&str` as raw SQL content.

use quiver_driver_core::Value;
use quiver_error::QuiverError;
use quiver_schema::Schema;
use quiver_schema::ast::*;
use serde::{Deserialize, Serialize};

use crate::step::MigrationStep;

// ---------------------------------------------------------------------------
// TrustedSql -- parameterized SQL built from static fragments only
// ---------------------------------------------------------------------------

/// A SQL statement built exclusively from trusted (static) string fragments.
///
/// Dynamic values are stored as bind parameters. There is intentionally no
/// way to embed an arbitrary runtime `&str` into the SQL text.
///
/// A single `TrustedSql` may contain multiple SQL statements separated by
/// `";\n"` (e.g. `CREATE TABLE ...;\nINSERT INTO ...`). The executor
/// (`MigrationTracker`) splits on this separator and dispatches each
/// sub-statement: pure DDL via `execute_ddl`, parameterized via `execute`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustedSql {
    pub sql: String,
    pub params: Vec<Value>,
}

impl TrustedSql {
    /// True if this statement has bind parameters (needs `execute` not `execute_ddl`).
    pub fn has_params(&self) -> bool {
        !self.params.is_empty()
    }
}

/// Builder for [`TrustedSql`].
///
/// All SQL content must go through one of the builder methods. The internal
/// `sql` field is never accessed directly -- this ensures every fragment is
/// either a compile-time literal, a validated identifier, a validated literal,
/// a safe numeric value, or a bind parameter.
///
/// Builder methods:
/// - `push_static` -- compile-time-known SQL text
/// - `push_ident` -- validated, double-quoted identifier
/// - `push_param` -- bind parameter (`?` placeholder)
/// - `push_validated_literal` -- validated string in single quotes
/// - `push_int` -- safe integer (digits + optional minus)
/// - `push_float` -- safe float (digits + minus + dot + e/E)
/// - `push_builder` -- compose sub-statements
pub struct TrustedSqlBuilder {
    sql: String,
    params: Vec<Value>,
}

impl TrustedSqlBuilder {
    fn new() -> Self {
        Self {
            sql: String::new(),
            params: Vec::new(),
        }
    }

    /// Append a trusted static SQL fragment. Only accepts `&'static str`.
    fn push_static(&mut self, s: &'static str) -> &mut Self {
        self.sql.push_str(s);
        self
    }

    /// Append a SQL identifier (table name, column name), validated and
    /// wrapped in double quotes. Rejects identifiers containing `"`.
    fn push_ident(&mut self, ident: &str) -> Result<&mut Self, QuiverError> {
        if ident.contains('"') {
            return Err(QuiverError::Migration(format!(
                "identifier contains double quote: {}",
                ident
            )));
        }
        self.sql.push('"');
        self.sql.push_str(ident);
        self.sql.push('"');
        Ok(self)
    }

    /// Append a bind parameter placeholder (`?`) and store the value.
    fn push_param(&mut self, value: Value) -> &mut Self {
        self.sql.push('?');
        self.params.push(value);
        self
    }

    /// Append a validated string literal wrapped in single quotes.
    ///
    /// DDL DEFAULT values and enum values cannot use bind parameters, so we
    /// validate the content instead. Rejects any string containing SQL
    /// metacharacters (`'`, `"`, `;`, `--`, `\`, null bytes).
    fn push_validated_literal(&mut self, v: &str) -> Result<&mut Self, QuiverError> {
        validate_safe_literal(v)?;
        self.sql.push('\'');
        self.sql.push_str(v);
        self.sql.push('\'');
        Ok(self)
    }

    /// Append an integer value. Safe because `i64::to_string()` only produces
    /// digits and an optional leading minus sign.
    fn push_int(&mut self, v: i64) -> &mut Self {
        self.sql.push_str(&v.to_string());
        self
    }

    /// Append a float value. Safe because `f64::to_string()` only produces
    /// digits, minus, decimal point, and `e`/`E` for scientific notation.
    fn push_float(&mut self, v: f64) -> &mut Self {
        self.sql.push_str(&v.to_string());
        self
    }

    /// Append the result of another builder (for composing sub-statements).
    fn push_builder(&mut self, other: TrustedSqlBuilder) -> &mut Self {
        self.sql.push_str(&other.sql);
        self.params.extend(other.params);
        self
    }

    fn build(self) -> TrustedSql {
        TrustedSql {
            sql: self.sql,
            params: self.params,
        }
    }
}

/// Shorthand: start a builder with a static string.
fn trusted(s: &'static str) -> TrustedSqlBuilder {
    let mut b = TrustedSqlBuilder::new();
    b.push_static(s);
    b
}

/// SQL dialect for migration DDL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
    Mysql,
}

/// Generates forward (up) and reverse (down) SQL from migration steps.
pub struct MigrationSqlGenerator;

impl MigrationSqlGenerator {
    /// Generate the forward SQL statement for a single migration step.
    pub fn step_up(
        step: &MigrationStep,
        schema: &Schema,
        dialect: SqlDialect,
    ) -> Result<TrustedSql, QuiverError> {
        match step {
            MigrationStep::CreateModel { name, model } => {
                create_table(name, model, schema, dialect)
            }
            MigrationStep::DropModel { name } => {
                let mut b = trusted("DROP TABLE IF EXISTS ");
                b.push_ident(name)?;
                Ok(b.build())
            }
            MigrationStep::AddField { model, field } => {
                let col_name = column_name_for(field);
                let sql_type = base_type_to_sql(&field.type_expr.base, schema, dialect);

                let mut b = trusted("ALTER TABLE ");
                b.push_ident(model)?;
                b.push_static(" ADD COLUMN ");
                b.push_ident(&col_name)?;
                b.push_static(" ");
                b.push_static(sql_type);

                if !field.type_expr.nullable {
                    b.push_static(" NOT NULL");
                }

                if has_attr(field, |a| matches!(a, FieldAttribute::Unique)) {
                    b.push_static(" UNIQUE");
                }

                push_default(&mut b, field, schema, dialect)?;

                Ok(b.build())
            }
            MigrationStep::DropField { model, field_name } => {
                let mut b = trusted("ALTER TABLE ");
                b.push_ident(model)?;
                b.push_static(" DROP COLUMN ");
                b.push_ident(field_name)?;
                Ok(b.build())
            }
            MigrationStep::AlterField {
                model,
                old_field,
                new_field,
            } => match dialect {
                SqlDialect::Sqlite => {
                    sqlite_alter_field_rebuild(model, old_field, new_field, schema)
                }
                SqlDialect::Postgres => postgres_alter_field(model, old_field, new_field, schema),
                SqlDialect::Mysql => mysql_alter_field(model, new_field, schema),
            },
            MigrationStep::CreateIndex {
                model,
                index_name,
                columns,
            } => {
                let mut b = trusted("CREATE INDEX IF NOT EXISTS ");
                b.push_ident(index_name)?;
                b.push_static(" ON ");
                b.push_ident(model)?;
                b.push_static(" (");
                for (i, col) in columns.iter().enumerate() {
                    if i > 0 {
                        b.push_static(", ");
                    }
                    b.push_ident(col)?;
                }
                b.push_static(")");
                Ok(b.build())
            }
            MigrationStep::DropIndex { index_name } => {
                let mut b = trusted("DROP INDEX IF EXISTS ");
                b.push_ident(index_name)?;
                Ok(b.build())
            }
            MigrationStep::CreateEnum { name, values } => match dialect {
                SqlDialect::Sqlite => {
                    let enum_table = format!("_enum_{}", name);
                    let mut b = trusted("CREATE TABLE IF NOT EXISTS ");
                    b.push_ident(&enum_table)?;
                    b.push_static(" (value TEXT PRIMARY KEY);\nINSERT OR IGNORE INTO ");
                    b.push_ident(&enum_table)?;
                    b.push_static(" (value) VALUES ");
                    for (i, v) in values.iter().enumerate() {
                        if i > 0 {
                            b.push_static(", ");
                        }
                        b.push_static("(");
                        b.push_param(Value::Text(v.clone()));
                        b.push_static(")");
                    }
                    Ok(b.build())
                }
                SqlDialect::Postgres => {
                    let mut b = trusted("CREATE TYPE ");
                    b.push_ident(name)?;
                    b.push_static(" AS ENUM (");
                    for (i, v) in values.iter().enumerate() {
                        if i > 0 {
                            b.push_static(", ");
                        }
                        b.push_validated_literal(v)?;
                    }
                    b.push_static(")");
                    Ok(b.build())
                }
                SqlDialect::Mysql => {
                    // MySQL enums are inline in column definitions; no-op here.
                    Ok(trusted("-- MySQL: enums are inline in column definitions").build())
                }
            },
            MigrationStep::DropEnum { name } => match dialect {
                SqlDialect::Sqlite => {
                    let enum_table = format!("_enum_{}", name);
                    let mut b = trusted("DROP TABLE IF EXISTS ");
                    b.push_ident(&enum_table)?;
                    Ok(b.build())
                }
                SqlDialect::Postgres => {
                    let mut b = trusted("DROP TYPE IF EXISTS ");
                    b.push_ident(name)?;
                    Ok(b.build())
                }
                SqlDialect::Mysql => {
                    Ok(trusted("-- MySQL: enums are inline in column definitions").build())
                }
            },
            MigrationStep::AddEnumValue { enum_name, value } => match dialect {
                SqlDialect::Sqlite => {
                    let enum_table = format!("_enum_{}", enum_name);
                    let mut b = trusted("INSERT OR IGNORE INTO ");
                    b.push_ident(&enum_table)?;
                    b.push_static(" (value) VALUES (");
                    b.push_param(Value::Text(value.clone()));
                    b.push_static(")");
                    Ok(b.build())
                }
                SqlDialect::Postgres => {
                    let mut b = trusted("ALTER TYPE ");
                    b.push_ident(enum_name)?;
                    b.push_static(" ADD VALUE ");
                    b.push_validated_literal(value)?;
                    Ok(b.build())
                }
                SqlDialect::Mysql => {
                    Ok(trusted("-- MySQL: enums are inline in column definitions").build())
                }
            },
            MigrationStep::RemoveEnumValue { enum_name, value } => match dialect {
                SqlDialect::Sqlite => {
                    let enum_table = format!("_enum_{}", enum_name);
                    let mut b = trusted("DELETE FROM ");
                    b.push_ident(&enum_table)?;
                    b.push_static(" WHERE value = ");
                    b.push_param(Value::Text(value.clone()));
                    Ok(b.build())
                }
                SqlDialect::Postgres => Err(QuiverError::Migration(format!(
                    "PostgreSQL does not support removing enum values from type '{}'.\
                     Value '{}' cannot be removed without recreating the type.",
                    enum_name, value
                ))),
                SqlDialect::Mysql => {
                    Ok(trusted("-- MySQL: enums are inline in column definitions").build())
                }
            },
        }
    }

    /// Generate the reverse SQL statement for a single migration step.
    pub fn step_down(
        step: &MigrationStep,
        schema: &Schema,
        dialect: SqlDialect,
    ) -> Result<TrustedSql, QuiverError> {
        match step {
            MigrationStep::CreateModel { name, .. } => {
                let mut b = trusted("DROP TABLE IF EXISTS ");
                b.push_ident(name)?;
                Ok(b.build())
            }
            MigrationStep::DropModel { name, .. } => {
                let mut b = trusted("-- Cannot reverse DROP TABLE; original schema required -- ");
                b.push_ident(name)?;
                Ok(b.build())
            }
            MigrationStep::AddField { model, field } => {
                let col_name = column_name_for(field);
                let mut b = trusted("ALTER TABLE ");
                b.push_ident(model)?;
                b.push_static(" DROP COLUMN ");
                b.push_ident(&col_name)?;
                Ok(b.build())
            }
            MigrationStep::DropField { model, .. } => {
                let mut b = trusted(
                    "-- Cannot reverse DROP COLUMN; original field definition required -- ",
                );
                b.push_ident(model)?;
                Ok(b.build())
            }
            MigrationStep::AlterField {
                model,
                old_field,
                new_field: _,
            } => Self::step_up(
                &MigrationStep::AlterField {
                    model: model.clone(),
                    old_field: old_field.clone(),
                    new_field: old_field.clone(),
                },
                schema,
                dialect,
            ),
            MigrationStep::CreateIndex { index_name, .. } => {
                let mut b = trusted("DROP INDEX IF EXISTS ");
                b.push_ident(index_name)?;
                Ok(b.build())
            }
            MigrationStep::DropIndex { index_name } => {
                let mut b =
                    trusted("-- Cannot reverse DROP INDEX; original index definition required -- ");
                b.push_ident(index_name)?;
                Ok(b.build())
            }
            MigrationStep::CreateEnum { name, .. } => match dialect {
                SqlDialect::Sqlite => {
                    let enum_table = format!("_enum_{}", name);
                    let mut b = trusted("DROP TABLE IF EXISTS ");
                    b.push_ident(&enum_table)?;
                    Ok(b.build())
                }
                SqlDialect::Postgres => {
                    let mut b = trusted("DROP TYPE IF EXISTS ");
                    b.push_ident(name)?;
                    Ok(b.build())
                }
                SqlDialect::Mysql => {
                    Ok(trusted("-- MySQL: enums are inline in column definitions").build())
                }
            },
            MigrationStep::DropEnum { name } => match dialect {
                SqlDialect::Sqlite => {
                    let mut b =
                        trusted("-- Cannot reverse DROP ENUM; original values required -- ");
                    b.push_ident(name)?;
                    Ok(b.build())
                }
                SqlDialect::Postgres => {
                    let mut b =
                        trusted("-- Cannot reverse DROP TYPE; original values required -- ");
                    b.push_ident(name)?;
                    Ok(b.build())
                }
                SqlDialect::Mysql => {
                    Ok(trusted("-- MySQL: enums are inline in column definitions").build())
                }
            },
            MigrationStep::AddEnumValue { enum_name, value } => match dialect {
                SqlDialect::Sqlite => {
                    let enum_table = format!("_enum_{}", enum_name);
                    let mut b = trusted("DELETE FROM ");
                    b.push_ident(&enum_table)?;
                    b.push_static(" WHERE value = ");
                    b.push_param(Value::Text(value.clone()));
                    Ok(b.build())
                }
                SqlDialect::Postgres => Err(QuiverError::Migration(format!(
                    "PostgreSQL does not support removing enum values from type '{}'.\
                     Cannot reverse AddEnumValue for value '{}'.",
                    enum_name, value
                ))),
                SqlDialect::Mysql => {
                    Ok(trusted("-- MySQL: enums are inline in column definitions").build())
                }
            },
            MigrationStep::RemoveEnumValue { enum_name, value } => match dialect {
                SqlDialect::Sqlite => {
                    let enum_table = format!("_enum_{}", enum_name);
                    let mut b = trusted("INSERT OR IGNORE INTO ");
                    b.push_ident(&enum_table)?;
                    b.push_static(" (value) VALUES (");
                    b.push_param(Value::Text(value.clone()));
                    b.push_static(")");
                    Ok(b.build())
                }
                SqlDialect::Postgres => {
                    let mut b = trusted("ALTER TYPE ");
                    b.push_ident(enum_name)?;
                    b.push_static(" ADD VALUE ");
                    b.push_validated_literal(value)?;
                    Ok(b.build())
                }
                SqlDialect::Mysql => {
                    Ok(trusted("-- MySQL: enums are inline in column definitions").build())
                }
            },
        }
    }

    /// Generate all forward SQL statements for a list of migration steps.
    pub fn generate_up(
        steps: &[MigrationStep],
        schema: &Schema,
        dialect: SqlDialect,
    ) -> Result<Vec<TrustedSql>, QuiverError> {
        steps
            .iter()
            .map(|s| Self::step_up(s, schema, dialect))
            .collect()
    }

    /// Generate all reverse SQL statements for a list of migration steps (in reverse order).
    pub fn generate_down(
        steps: &[MigrationStep],
        schema: &Schema,
        dialect: SqlDialect,
    ) -> Result<Vec<TrustedSql>, QuiverError> {
        steps
            .iter()
            .rev()
            .map(|s| Self::step_down(s, schema, dialect))
            .collect()
    }
}

/// SQLite ALTER COLUMN via table rebuild.
///
/// SQLite does not support ALTER COLUMN directly. This generates a multi-statement
/// sequence that:
/// 1. Renames the old table to a temporary name
/// 2. Creates a new table with the updated column definition
/// 3. Copies data from the old table to the new table
/// 4. Drops the old (renamed) table
///
/// **IMPORTANT**: This generates multiple statements separated by `;\n`.
/// The caller MUST execute these within a transaction to prevent partial
/// state if any step fails. `MigrationTracker::apply()` wraps execution
/// in a transaction at the driver level.
///
/// The new_field replaces old_field in the model's column list for the CREATE.
fn sqlite_alter_field_rebuild(
    model_name: &str,
    old_field: &FieldDef,
    new_field: &FieldDef,
    schema: &Schema,
) -> Result<TrustedSql, QuiverError> {
    let model = schema
        .models
        .iter()
        .find(|m| m.name == model_name)
        .ok_or_else(|| {
            QuiverError::Migration(format!(
                "model '{}' not found in schema for AlterField",
                model_name
            ))
        })?;

    let table_name = table_name_for(model).unwrap_or_else(|| model_name.to_string());
    let tmp_table = format!("_quiver_old_{}", table_name);

    let mut new_model = model.clone();
    for f in &mut new_model.fields {
        if f.name == old_field.name {
            *f = new_field.clone();
        }
    }

    // Collect column names for the data copy
    let copy_columns: Vec<String> = model.fields.iter().map(column_name_for).collect();

    let mut b = trusted("ALTER TABLE ");
    b.push_ident(&table_name)?;
    b.push_static(" RENAME TO ");
    b.push_ident(&tmp_table)?;
    b.push_static(";\n");

    let create_builder = create_table_builder(&table_name, &new_model, schema, SqlDialect::Sqlite)?;
    b.push_builder(create_builder);
    b.push_static(";\n");

    b.push_static("INSERT INTO ");
    b.push_ident(&table_name)?;
    b.push_static(" (");
    for (i, col) in copy_columns.iter().enumerate() {
        if i > 0 {
            b.push_static(", ");
        }
        b.push_ident(col)?;
    }
    b.push_static(") SELECT ");
    for (i, col) in copy_columns.iter().enumerate() {
        if i > 0 {
            b.push_static(", ");
        }
        b.push_ident(col)?;
    }
    b.push_static(" FROM ");
    b.push_ident(&tmp_table)?;
    b.push_static(";\nDROP TABLE ");
    b.push_ident(&tmp_table)?;

    Ok(b.build())
}

/// PostgreSQL ALTER COLUMN via separate ALTER statements.
///
/// PostgreSQL supports ALTER COLUMN directly. This generates separate statements
/// for type change, nullability change, and default change.
fn postgres_alter_field(
    model_name: &str,
    _old_field: &FieldDef,
    new_field: &FieldDef,
    schema: &Schema,
) -> Result<TrustedSql, QuiverError> {
    let col_name = column_name_for(new_field);
    let sql_type = base_type_to_sql(&new_field.type_expr.base, schema, SqlDialect::Postgres);

    // ALTER COLUMN ... TYPE
    let mut b = trusted("ALTER TABLE ");
    b.push_ident(model_name)?;
    b.push_static(" ALTER COLUMN ");
    b.push_ident(&col_name)?;
    b.push_static(" TYPE ");
    b.push_static(sql_type);

    // ALTER COLUMN ... SET/DROP NOT NULL
    b.push_static(";\nALTER TABLE ");
    b.push_ident(model_name)?;
    b.push_static(" ALTER COLUMN ");
    b.push_ident(&col_name)?;
    if new_field.type_expr.nullable {
        b.push_static(" DROP NOT NULL");
    } else {
        b.push_static(" SET NOT NULL");
    }

    // ALTER COLUMN ... SET DEFAULT (if present)
    for attr in &new_field.attributes {
        if let FieldAttribute::Default(val) = attr {
            b.push_static(";\nALTER TABLE ");
            b.push_ident(model_name)?;
            b.push_static(" ALTER COLUMN ");
            b.push_ident(&col_name)?;
            b.push_static(" SET");
            push_default_value(&mut b, val, new_field, schema, SqlDialect::Postgres)?;
            break;
        }
    }

    Ok(b.build())
}

/// MySQL ALTER COLUMN via MODIFY COLUMN.
///
/// MySQL uses MODIFY COLUMN which requires the full column definition to be restated.
fn mysql_alter_field(
    model_name: &str,
    new_field: &FieldDef,
    schema: &Schema,
) -> Result<TrustedSql, QuiverError> {
    let col_name = column_name_for(new_field);
    let sql_type = base_type_to_sql(&new_field.type_expr.base, schema, SqlDialect::Mysql);

    let mut b = trusted("ALTER TABLE ");
    b.push_ident(model_name)?;
    b.push_static(" MODIFY COLUMN ");
    b.push_ident(&col_name)?;
    b.push_static(" ");
    b.push_static(sql_type);

    if !new_field.type_expr.nullable {
        b.push_static(" NOT NULL");
    }

    push_default(&mut b, new_field, schema, SqlDialect::Mysql)?;

    Ok(b.build())
}

/// Push a DEFAULT clause from a DefaultValue (used by postgres_alter_field for SET DEFAULT).
fn push_default_value(
    b: &mut TrustedSqlBuilder,
    val: &DefaultValue,
    f: &FieldDef,
    schema: &Schema,
    dialect: SqlDialect,
) -> Result<(), QuiverError> {
    match val {
        DefaultValue::Int(v) => {
            b.push_static(" DEFAULT ");
            b.push_int(*v);
        }
        DefaultValue::Float(v) => {
            b.push_static(" DEFAULT ");
            b.push_float(*v);
        }
        DefaultValue::String(v) => {
            b.push_static(" DEFAULT ");
            b.push_validated_literal(v)?;
        }
        DefaultValue::Bool(v) => match dialect {
            SqlDialect::Postgres => {
                if *v {
                    b.push_static(" DEFAULT true");
                } else {
                    b.push_static(" DEFAULT false");
                }
            }
            SqlDialect::Sqlite | SqlDialect::Mysql => {
                if *v {
                    b.push_static(" DEFAULT 1");
                } else {
                    b.push_static(" DEFAULT 0");
                }
            }
        },
        DefaultValue::Now => match dialect {
            SqlDialect::Sqlite => {
                b.push_static(" DEFAULT (datetime('now'))");
            }
            SqlDialect::Postgres => {
                b.push_static(" DEFAULT now()");
            }
            SqlDialect::Mysql => {
                b.push_static(" DEFAULT CURRENT_TIMESTAMP(6)");
            }
        },
        DefaultValue::Uuid | DefaultValue::Cuid => {}
        DefaultValue::EmptyList => {
            b.push_static(" DEFAULT '[]'");
        }
        DefaultValue::EmptyMap => {
            b.push_static(" DEFAULT '{}'");
        }
        DefaultValue::EnumVariant(v) => {
            let is_enum = matches!(
                &f.type_expr.base,
                BaseType::Named(n) if schema.enums.iter().any(|e| e.name == *n)
            );
            if is_enum {
                b.push_static(" DEFAULT ");
                b.push_validated_literal(v)?;
            }
        }
    }
    Ok(())
}

fn create_table(
    name: &str,
    model: &ModelDef,
    schema: &Schema,
    dialect: SqlDialect,
) -> Result<TrustedSql, QuiverError> {
    Ok(create_table_builder(name, model, schema, dialect)?.build())
}

fn create_table_builder(
    name: &str,
    model: &ModelDef,
    schema: &Schema,
    dialect: SqlDialect,
) -> Result<TrustedSqlBuilder, QuiverError> {
    let table_name = table_name_for(model).unwrap_or_else(|| name.to_string());
    let mut b = trusted("CREATE TABLE IF NOT EXISTS ");
    b.push_ident(&table_name)?;
    b.push_static(" (\n");

    let mut first = true;
    let mut has_single_pk = false;

    for f in &model.fields {
        if !first {
            b.push_static(",\n");
        }
        first = false;

        let col_name = column_name_for(f);
        let sql_type = base_type_to_sql(&f.type_expr.base, schema, dialect);
        b.push_static("  ");
        b.push_ident(&col_name)?;
        b.push_static(" ");
        b.push_static(sql_type);

        if has_attr(f, |a| matches!(a, FieldAttribute::Id)) {
            has_single_pk = true;
            b.push_static(" PRIMARY KEY");
            if has_attr(f, |a| matches!(a, FieldAttribute::Autoincrement)) {
                match dialect {
                    SqlDialect::Sqlite => {
                        b.push_static(" AUTOINCREMENT");
                    }
                    SqlDialect::Postgres => {
                        b.push_static(" GENERATED ALWAYS AS IDENTITY");
                    }
                    SqlDialect::Mysql => {
                        b.push_static(" AUTO_INCREMENT");
                    }
                }
            }
        }

        if !f.type_expr.nullable && !has_attr(f, |a| matches!(a, FieldAttribute::Id)) {
            b.push_static(" NOT NULL");
        }

        if has_attr(f, |a| matches!(a, FieldAttribute::Unique)) {
            b.push_static(" UNIQUE");
        }

        push_default(&mut b, f, schema, dialect)?;
    }

    // Composite primary key
    if !has_single_pk {
        for attr in &model.attributes {
            if let ModelAttribute::Id(fields) = attr {
                if !first {
                    b.push_static(",\n");
                }
                first = false;
                b.push_static("  PRIMARY KEY (");
                for (i, f) in fields.iter().enumerate() {
                    if i > 0 {
                        b.push_static(", ");
                    }
                    b.push_ident(f)?;
                }
                b.push_static(")");
            }
        }
    }

    // Foreign keys from model-level FOREIGN KEY attributes
    for attr in &model.attributes {
        if let ModelAttribute::ForeignKey {
            fields,
            references_model,
            references_columns,
            on_delete,
            on_update,
        } = attr
        {
            let target_table = schema
                .models
                .iter()
                .find(|m| m.name == *references_model)
                .and_then(table_name_for)
                .unwrap_or_else(|| references_model.clone());
            for (fk_field, ref_field) in fields.iter().zip(references_columns.iter()) {
                if !first {
                    b.push_static(",\n");
                }
                first = false;
                b.push_static("  FOREIGN KEY (");
                b.push_ident(fk_field)?;
                b.push_static(") REFERENCES ");
                b.push_ident(&target_table)?;
                b.push_static("(");
                b.push_ident(ref_field)?;
                b.push_static(")");
                if let Some(action) = on_delete {
                    b.push_static(match action {
                        ReferentialAction::Cascade => " ON DELETE CASCADE",
                        ReferentialAction::Restrict => " ON DELETE RESTRICT",
                        ReferentialAction::SetNull => " ON DELETE SET NULL",
                        ReferentialAction::SetDefault => " ON DELETE SET DEFAULT",
                        ReferentialAction::NoAction => " ON DELETE NO ACTION",
                    });
                }
                if let Some(action) = on_update {
                    b.push_static(match action {
                        ReferentialAction::Cascade => " ON UPDATE CASCADE",
                        ReferentialAction::Restrict => " ON UPDATE RESTRICT",
                        ReferentialAction::SetNull => " ON UPDATE SET NULL",
                        ReferentialAction::SetDefault => " ON UPDATE SET DEFAULT",
                        ReferentialAction::NoAction => " ON UPDATE NO ACTION",
                    });
                }
            }
        }
    }

    b.push_static("\n)");
    Ok(b)
}

fn table_name_for(m: &ModelDef) -> Option<String> {
    for attr in &m.attributes {
        if let ModelAttribute::Map(name) = attr {
            return Some(name.clone());
        }
    }
    None
}

fn column_name_for(f: &FieldDef) -> String {
    for attr in &f.attributes {
        if let FieldAttribute::Map(name) = attr {
            return name.clone();
        }
    }
    f.name.clone()
}

fn base_type_to_sql(base: &BaseType, schema: &Schema, dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Sqlite => base_type_to_sqlite(base, schema),
        SqlDialect::Postgres => base_type_to_postgres(base, schema),
        SqlDialect::Mysql => base_type_to_mysql(base, schema),
    }
}

fn base_type_to_sqlite(base: &BaseType, schema: &Schema) -> &'static str {
    match base {
        BaseType::Int8
        | BaseType::Int16
        | BaseType::Int32
        | BaseType::Int64
        | BaseType::UInt8
        | BaseType::UInt16
        | BaseType::UInt32
        | BaseType::UInt64 => "INTEGER",
        BaseType::Float16 | BaseType::Float32 | BaseType::Float64 => "REAL",
        BaseType::Decimal128 { .. } | BaseType::Decimal256 { .. } => "TEXT",
        BaseType::Utf8 | BaseType::LargeUtf8 => "TEXT",
        BaseType::Binary | BaseType::LargeBinary | BaseType::FixedSizeBinary { .. } => "BLOB",
        BaseType::Boolean => "INTEGER",
        BaseType::Date32 | BaseType::Date64 => "TEXT",
        BaseType::Time32 { .. } | BaseType::Time64 { .. } => "TEXT",
        BaseType::Timestamp { .. } => "TEXT",
        BaseType::List(_) | BaseType::LargeList(_) | BaseType::Map { .. } | BaseType::Struct(_) => {
            "TEXT"
        }
        BaseType::Named(name) => {
            if schema.enums.iter().any(|e| e.name == *name) {
                "TEXT"
            } else {
                "INTEGER"
            }
        }
    }
}

fn base_type_to_postgres(base: &BaseType, schema: &Schema) -> &'static str {
    match base {
        BaseType::Int8 | BaseType::Int16 | BaseType::UInt8 | BaseType::UInt16 => "SMALLINT",
        BaseType::Int32 | BaseType::UInt32 => "INTEGER",
        BaseType::Int64 | BaseType::UInt64 => "BIGINT",
        BaseType::Float16 | BaseType::Float32 => "REAL",
        BaseType::Float64 => "DOUBLE PRECISION",
        BaseType::Decimal128 { .. } | BaseType::Decimal256 { .. } => "NUMERIC",
        BaseType::Utf8 | BaseType::LargeUtf8 => "TEXT",
        BaseType::Binary | BaseType::LargeBinary | BaseType::FixedSizeBinary { .. } => "BYTEA",
        BaseType::Boolean => "BOOLEAN",
        BaseType::Date32 | BaseType::Date64 => "DATE",
        BaseType::Time32 { .. } | BaseType::Time64 { .. } => "TIME",
        BaseType::Timestamp { .. } => "TIMESTAMPTZ",
        BaseType::List(_) | BaseType::LargeList(_) | BaseType::Map { .. } | BaseType::Struct(_) => {
            "JSONB"
        }
        BaseType::Named(name) => {
            if schema.enums.iter().any(|e| e.name == *name) {
                "TEXT"
            } else {
                "INTEGER"
            }
        }
    }
}

fn base_type_to_mysql(base: &BaseType, schema: &Schema) -> &'static str {
    match base {
        BaseType::Int8 => "TINYINT",
        BaseType::Int16 => "SMALLINT",
        BaseType::Int32 => "INT",
        BaseType::Int64 => "BIGINT",
        BaseType::UInt8 => "TINYINT UNSIGNED",
        BaseType::UInt16 => "SMALLINT UNSIGNED",
        BaseType::UInt32 => "INT UNSIGNED",
        BaseType::UInt64 => "BIGINT UNSIGNED",
        BaseType::Float16 | BaseType::Float32 => "FLOAT",
        BaseType::Float64 => "DOUBLE",
        BaseType::Decimal128 { .. } | BaseType::Decimal256 { .. } => "DECIMAL(38,18)",
        BaseType::Utf8 => "VARCHAR(255)",
        BaseType::LargeUtf8 => "TEXT",
        BaseType::Binary | BaseType::FixedSizeBinary { .. } => "VARBINARY(255)",
        BaseType::LargeBinary => "LONGBLOB",
        BaseType::Boolean => "TINYINT(1)",
        BaseType::Date32 | BaseType::Date64 => "DATE",
        BaseType::Time32 { .. } | BaseType::Time64 { .. } => "TIME",
        BaseType::Timestamp { .. } => "DATETIME(6)",
        BaseType::List(_) | BaseType::LargeList(_) | BaseType::Map { .. } | BaseType::Struct(_) => {
            "JSON"
        }
        BaseType::Named(name) => {
            if schema.enums.iter().any(|e| e.name == *name) {
                "TEXT"
            } else {
                "INT"
            }
        }
    }
}

fn has_attr(f: &FieldDef, pred: impl Fn(&FieldAttribute) -> bool) -> bool {
    f.attributes.iter().any(pred)
}

/// Append ` DEFAULT <value>` to the builder if the field has a default.
///
/// For string and enum defaults, uses bind parameters via a CHECK-less literal.
/// Since DEFAULT in DDL cannot use `?` bind parameters, string/enum values are
/// validated to contain only safe characters (no single quotes, semicolons, or
/// other SQL metacharacters). Values that fail validation are rejected with an error.
fn push_default(
    b: &mut TrustedSqlBuilder,
    f: &FieldDef,
    schema: &Schema,
    dialect: SqlDialect,
) -> Result<(), QuiverError> {
    for attr in &f.attributes {
        if let FieldAttribute::Default(val) = attr {
            match val {
                DefaultValue::Int(v) => {
                    b.push_static(" DEFAULT ");
                    b.push_int(*v);
                }
                DefaultValue::Float(v) => {
                    b.push_static(" DEFAULT ");
                    b.push_float(*v);
                }
                DefaultValue::String(v) => {
                    b.push_static(" DEFAULT ");
                    b.push_validated_literal(v)?;
                }
                DefaultValue::Bool(v) => match dialect {
                    SqlDialect::Postgres => {
                        if *v {
                            b.push_static(" DEFAULT true");
                        } else {
                            b.push_static(" DEFAULT false");
                        }
                    }
                    SqlDialect::Sqlite | SqlDialect::Mysql => {
                        if *v {
                            b.push_static(" DEFAULT 1");
                        } else {
                            b.push_static(" DEFAULT 0");
                        }
                    }
                },
                DefaultValue::Now => match dialect {
                    SqlDialect::Sqlite => {
                        b.push_static(" DEFAULT (datetime('now'))");
                    }
                    SqlDialect::Postgres => {
                        b.push_static(" DEFAULT now()");
                    }
                    SqlDialect::Mysql => {
                        b.push_static(" DEFAULT CURRENT_TIMESTAMP(6)");
                    }
                },
                DefaultValue::Uuid | DefaultValue::Cuid => {
                    // Cannot express as SQL DEFAULT -- handled at application level.
                }
                DefaultValue::EmptyList => {
                    b.push_static(" DEFAULT '[]'");
                }
                DefaultValue::EmptyMap => {
                    b.push_static(" DEFAULT '{}'");
                }
                DefaultValue::EnumVariant(v) => {
                    let is_enum = matches!(
                        &f.type_expr.base,
                        BaseType::Named(n) if schema.enums.iter().any(|e| e.name == *n)
                    );
                    if is_enum {
                        b.push_static(" DEFAULT ");
                        b.push_validated_literal(v)?;
                    }
                }
            }
            break;
        }
    }
    Ok(())
}

/// Validate that a string is safe to embed as a SQL literal in DDL context.
///
/// DDL DEFAULT values cannot use bind parameters, so we must validate the
/// content rather than parameterize it. This rejects any string containing
/// SQL metacharacters (`'`, `"`, `;`, `--`, `\`, null bytes).
///
/// This is deliberately strict: if a legitimate value is rejected, the schema
/// definition should be changed rather than weakening the validation.
fn validate_safe_literal(s: &str) -> Result<(), QuiverError> {
    for ch in s.chars() {
        match ch {
            '\'' | '"' | ';' | '\\' | '\0' => {
                return Err(QuiverError::Migration(format!(
                    "unsafe character '{}' in DDL literal: {}",
                    ch, s
                )));
            }
            _ => {}
        }
    }
    if s.contains("--") {
        return Err(QuiverError::Migration(format!(
            "SQL comment sequence '--' in DDL literal: {}",
            s
        )));
    }
    Ok(())
}

/// Validate that a string is safe to use as a SQL identifier in contexts
/// where bind parameters are not supported (e.g., SQLite PRAGMA statements).
///
/// Rejects identifiers containing double quotes, semicolons, comment sequences,
/// backslashes, null bytes, or newlines. This prevents SQL injection through
/// identifier interpolation.
pub(crate) fn validate_safe_ident(ident: &str) -> Result<(), QuiverError> {
    if ident.is_empty() {
        return Err(QuiverError::Migration("empty identifier".to_string()));
    }
    for ch in ident.chars() {
        match ch {
            '"' | ';' | '\\' | '\0' | '\n' | '\r' => {
                return Err(QuiverError::Migration(format!(
                    "unsafe character in identifier: {}",
                    ident
                )));
            }
            _ => {}
        }
    }
    if ident.contains("--") {
        return Err(QuiverError::Migration(format!(
            "SQL comment sequence '--' in identifier: {}",
            ident
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::diff_schemas;
    use quiver_schema::parse;

    #[test]
    fn create_model_generates_create_table() {
        let schema = parse(
            r#"
            model User {
                id    Int32 PRIMARY KEY AUTOINCREMENT
                email Utf8  UNIQUE
                name  Utf8?
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(None, &schema);
        let up = MigrationSqlGenerator::generate_up(&steps, &schema, SqlDialect::Sqlite).unwrap();

        assert_eq!(up.len(), 1);
        let sql = &up[0].sql;
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"User\""));
        assert!(sql.contains("\"id\" INTEGER PRIMARY KEY AUTOINCREMENT"));
        assert!(sql.contains("\"email\" TEXT NOT NULL UNIQUE"));
        assert!(sql.contains("\"name\" TEXT"));
        assert!(!sql.contains("\"name\" TEXT NOT NULL"));
        assert!(up[0].params.is_empty());
    }

    #[test]
    fn create_model_down_is_drop() {
        let schema = parse(
            r#"
            model User { id Int32 PRIMARY KEY }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(None, &schema);
        let down =
            MigrationSqlGenerator::generate_down(&steps, &schema, SqlDialect::Sqlite).unwrap();

        assert_eq!(down.len(), 1);
        assert!(down[0].sql.contains("DROP TABLE IF EXISTS \"User\""));
    }

    #[test]
    fn add_field_generates_alter_table() {
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
                email Utf8 UNIQUE
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        let up = MigrationSqlGenerator::generate_up(&steps, &new, SqlDialect::Sqlite).unwrap();

        assert_eq!(up.len(), 1);
        assert!(
            up[0]
                .sql
                .contains("ALTER TABLE \"User\" ADD COLUMN \"email\" TEXT NOT NULL UNIQUE")
        );
    }

    #[test]
    fn drop_field_generates_alter_table_drop() {
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
        let up = MigrationSqlGenerator::generate_up(&steps, &new, SqlDialect::Sqlite).unwrap();

        assert_eq!(up.len(), 1);
        assert!(
            up[0]
                .sql
                .contains("ALTER TABLE \"User\" DROP COLUMN \"age\"")
        );
    }

    #[test]
    fn create_index_generates_ddl() {
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
        let up = MigrationSqlGenerator::generate_up(&steps, &new, SqlDialect::Sqlite).unwrap();

        assert_eq!(up.len(), 1);
        assert!(up[0].sql.contains("CREATE INDEX IF NOT EXISTS"));
        assert!(up[0].sql.contains("\"email\""));
    }

    #[test]
    fn drop_index_generates_ddl() {
        let old = parse(
            r#"
            model User {
                id    Int32 PRIMARY KEY
                email Utf8
                INDEX (email)
            }
        "#,
        )
        .unwrap();

        let new = parse(
            r#"
            model User {
                id    Int32 PRIMARY KEY
                email Utf8
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        let up = MigrationSqlGenerator::generate_up(&steps, &new, SqlDialect::Sqlite).unwrap();

        assert_eq!(up.len(), 1);
        assert!(up[0].sql.contains("DROP INDEX IF EXISTS"));
    }

    #[test]
    fn create_enum_sqlite_uses_table() {
        let schema = parse(
            r#"
            enum Role { User Admin }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(None, &schema);
        let up = MigrationSqlGenerator::generate_up(&steps, &schema, SqlDialect::Sqlite).unwrap();

        assert_eq!(up.len(), 1);
        assert!(
            up[0]
                .sql
                .contains("CREATE TABLE IF NOT EXISTS \"_enum_Role\"")
        );
        // Enum values are bind parameters, not interpolated
        assert!(up[0].sql.contains("VALUES (?), (?)"));
        assert_eq!(up[0].params.len(), 2);
        assert_eq!(up[0].params[0], Value::Text("User".to_string()));
        assert_eq!(up[0].params[1], Value::Text("Admin".to_string()));
    }

    #[test]
    fn add_enum_value_generates_insert() {
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
        let up = MigrationSqlGenerator::generate_up(&steps, &new, SqlDialect::Sqlite).unwrap();

        assert_eq!(up.len(), 1);
        assert!(up[0].sql.contains("INSERT OR IGNORE INTO \"_enum_Role\""));
        // Value is a bind parameter
        assert_eq!(up[0].params.len(), 1);
        assert_eq!(up[0].params[0], Value::Text("Moderator".to_string()));
    }

    #[test]
    fn down_reverses_step_order() {
        let schema = parse(
            r#"
            enum Role { User Admin }
            model Account {
                id   Int32 PRIMARY KEY
                role Role
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(None, &schema);
        let down =
            MigrationSqlGenerator::generate_down(&steps, &schema, SqlDialect::Sqlite).unwrap();

        // Steps were: CreateEnum, CreateModel
        // Down should be reversed: DropModel first, then DropEnum
        assert_eq!(down.len(), 2);
        assert!(down[0].sql.contains("DROP TABLE IF EXISTS \"Account\""));
        assert!(down[1].sql.contains("DROP TABLE IF EXISTS \"_enum_Role\""));
    }

    #[test]
    fn full_migration_roundtrip() {
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
            enum Status { Active Inactive }
            model User {
                id     Int32  PRIMARY KEY
                name   Utf8
                email  Utf8   UNIQUE
                status Status DEFAULT Active
                INDEX (email)
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(Some(&old), &new);
        let up = MigrationSqlGenerator::generate_up(&steps, &new, SqlDialect::Sqlite).unwrap();
        let down = MigrationSqlGenerator::generate_down(&steps, &new, SqlDialect::Sqlite).unwrap();

        // Should have: CreateEnum + AddField(email) + AddField(status) + CreateIndex
        assert_eq!(up.len(), 4);
        assert_eq!(down.len(), 4);

        // Verify up order
        assert!(up[0].sql.contains("_enum_Status"));
        assert!(up[1].sql.contains("ADD COLUMN \"email\""));
        assert!(up[2].sql.contains("ADD COLUMN \"status\""));
        assert!(up[3].sql.contains("CREATE INDEX"));
    }

    #[test]
    fn create_model_with_referential_actions() {
        let schema = parse(
            r#"
            model User {
                id    Int32  PRIMARY KEY
            }
            model Post {
                id       Int32 PRIMARY KEY
                authorId Int32
                FOREIGN KEY (authorId) REFERENCES User (id) ON DELETE CASCADE ON UPDATE RESTRICT
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(None, &schema);
        let up = MigrationSqlGenerator::generate_up(&steps, &schema, SqlDialect::Sqlite).unwrap();

        // Find the Post table creation statement
        let post_sql = up
            .iter()
            .find(|s| s.sql.contains("\"Post\"") && s.sql.contains("CREATE TABLE"))
            .expect("should have CREATE TABLE for Post");
        assert!(
            post_sql.sql.contains(
                r#"FOREIGN KEY ("authorId") REFERENCES "User"("id") ON DELETE CASCADE ON UPDATE RESTRICT"#
            ),
            "SQL was: {}",
            post_sql.sql
        );
    }

    #[test]
    fn unsafe_default_string_rejected() {
        let schema = parse(
            r#"
            model Config {
                id    Int32 PRIMARY KEY
                label Utf8  DEFAULT "it's broken"
            }
        "#,
        )
        .unwrap();

        let steps = diff_schemas(None, &schema);
        let result = MigrationSqlGenerator::generate_up(&steps, &schema, SqlDialect::Sqlite);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unsafe character"));
    }

    #[test]
    fn unsafe_identifier_rejected() {
        // Verify that identifiers with double quotes are rejected.
        let mut b = TrustedSqlBuilder::new();
        let result = b.push_ident("table\"name");
        assert!(result.is_err());
    }
}
