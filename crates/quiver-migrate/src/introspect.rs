//! Schema introspection -- read database structure and generate a Quiver schema.
//!
//! Connects to an existing database and reverse-engineers the table definitions
//! into a [`Schema`] AST that can be serialized to a `.quiver` file.

use quiver_driver_core::{Connection, Row, Statement, Value};
use quiver_error::QuiverError;
use quiver_schema::Schema;
use quiver_schema::ast::*;

use crate::sql_gen::{SqlDialect, validate_safe_ident};

/// Introspect a database and produce a Quiver schema.
pub async fn introspect(conn: &dyn Connection, dialect: SqlDialect) -> Result<Schema, QuiverError> {
    match dialect {
        SqlDialect::Sqlite => introspect_sqlite(conn).await,
        SqlDialect::Postgres => introspect_postgres(conn).await,
        SqlDialect::Mysql => introspect_mysql(conn).await,
    }
}

async fn introspect_sqlite(conn: &dyn Connection) -> Result<Schema, QuiverError> {
    let tables = list_sqlite_tables(conn).await?;
    let mut models = Vec::new();
    let mut enums = Vec::new();

    for table_name in &tables {
        // Skip internal tables
        if table_name.starts_with("_quiver_") || table_name.starts_with("sqlite_") {
            continue;
        }

        // Detect enum tables (created by our migration engine)
        if let Some(enum_name) = table_name.strip_prefix("_enum_") {
            let values = read_enum_values(conn, table_name).await?;
            enums.push(EnumDef {
                name: enum_name.to_string(),
                values: values
                    .into_iter()
                    .map(|v| EnumValue {
                        name: v,
                        span: Span { line: 0, column: 0 },
                    })
                    .collect(),
                span: Span { line: 0, column: 0 },
            });
            continue;
        }

        let model = introspect_sqlite_table(conn, table_name, &enums).await?;
        models.push(model);
    }

    // Second pass: add FOREIGN KEY model attributes based on foreign keys
    for model in &mut models {
        let table_name = model
            .attributes
            .iter()
            .find_map(|a| {
                if let ModelAttribute::Map(name) = a {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| model.name.clone());
        let fks = list_sqlite_foreign_keys(conn, &table_name).await?;
        for fk in &fks {
            let references_model = to_pascal_case(&fk.to_table);
            model.attributes.push(ModelAttribute::ForeignKey {
                fields: vec![fk.from_col.clone()],
                references_model,
                references_columns: vec![fk.to_col.clone()],
                on_delete: None,
                on_update: None,
            });
        }
    }

    Ok(Schema {
        config: None,
        generate: None,
        enums,
        models,
    })
}

async fn introspect_sqlite_table(
    conn: &dyn Connection,
    table_name: &str,
    enums: &[EnumDef],
) -> Result<ModelDef, QuiverError> {
    let columns = list_sqlite_columns(conn, table_name).await?;
    let pk_columns = sqlite_pk_columns(conn, table_name).await?;
    let unique_columns = sqlite_unique_columns(conn, table_name).await?;
    let indexes = sqlite_indexes(conn, table_name).await?;

    let mut fields = Vec::new();
    let mut model_attrs: Vec<ModelAttribute> = Vec::new();

    let is_composite_pk = pk_columns.len() > 1;

    for col in &columns {
        let base_type = sqlite_type_to_base(&col.col_type, enums);
        let nullable = !col.not_null && !pk_columns.contains(&col.name);

        let mut attrs = Vec::new();

        // Single-column PK
        if !is_composite_pk && pk_columns.contains(&col.name) {
            attrs.push(FieldAttribute::Id);
            // Detect AUTOINCREMENT from the original SQL
            if col.is_autoincrement {
                attrs.push(FieldAttribute::Autoincrement);
            }
        }

        // Unique constraint (from single-column unique indexes)
        if unique_columns.contains(&col.name) {
            attrs.push(FieldAttribute::Unique);
        }

        // Default value
        if let Some(default) = &col.default_value {
            if let Some(dv) = parse_sqlite_default(default) {
                attrs.push(FieldAttribute::Default(dv));
            }
        }

        // Map attribute if table name differs from model name
        // (field-level MAP not needed unless column name differs)

        fields.push(FieldDef {
            name: col.name.clone(),
            type_expr: TypeExpr {
                base: base_type,
                nullable,
                span: Span { line: 0, column: 0 },
            },
            attributes: attrs,
            span: Span { line: 0, column: 0 },
        });
    }

    // Composite PK -> PRIMARY KEY (fields) attribute
    if is_composite_pk {
        model_attrs.push(ModelAttribute::Id(pk_columns));
    }

    // Indexes (non-unique, non-PK)
    for idx in &indexes {
        if !idx.unique {
            model_attrs.push(ModelAttribute::Index(idx.columns.clone()));
        }
    }

    // MAP if table name looks like it was mapped (lowercase/plural vs PascalCase)
    let model_name = to_pascal_case(table_name);
    if model_name != table_name {
        model_attrs.push(ModelAttribute::Map(table_name.to_string()));
    }

    Ok(ModelDef {
        name: model_name,
        fields,
        attributes: model_attrs,
        span: Span { line: 0, column: 0 },
    })
}

// --- PostgreSQL introspection ---

async fn introspect_postgres(conn: &dyn Connection) -> Result<Schema, QuiverError> {
    let tables = list_pg_tables(conn).await?;
    let pg_enums = list_pg_enums(conn).await?;
    let mut models = Vec::new();

    // Build enum defs from PostgreSQL enum types
    let mut enums: Vec<EnumDef> = Vec::new();
    let mut seen_enum_names: Vec<String> = Vec::new();
    for (type_name, label) in &pg_enums {
        if let Some(existing) = enums.iter_mut().find(|e| e.name == *type_name) {
            existing.values.push(EnumValue {
                name: label.clone(),
                span: Span { line: 0, column: 0 },
            });
        } else {
            seen_enum_names.push(type_name.clone());
            enums.push(EnumDef {
                name: type_name.clone(),
                values: vec![EnumValue {
                    name: label.clone(),
                    span: Span { line: 0, column: 0 },
                }],
                span: Span { line: 0, column: 0 },
            });
        }
    }

    for table_name in &tables {
        if table_name.starts_with("_quiver_") {
            continue;
        }

        let model = introspect_pg_table(conn, table_name, &seen_enum_names).await?;
        models.push(model);
    }

    // Second pass: add FOREIGN KEY model attributes based on foreign keys
    for model in &mut models {
        let table_name = pg_table_name_for(model);
        let fks = list_pg_foreign_keys(conn, &table_name).await?;
        for fk in &fks {
            let references_model = to_pascal_case(&fk.to_table);
            model.attributes.push(ModelAttribute::ForeignKey {
                fields: vec![fk.from_col.clone()],
                references_model,
                references_columns: vec![fk.to_col.clone()],
                on_delete: None,
                on_update: None,
            });
        }
    }

    Ok(Schema {
        config: None,
        generate: None,
        enums,
        models,
    })
}

async fn introspect_pg_table(
    conn: &dyn Connection,
    table_name: &str,
    enum_type_names: &[String],
) -> Result<ModelDef, QuiverError> {
    let columns = list_pg_columns(conn, table_name).await?;
    let pk_columns = list_pg_primary_keys(conn, table_name).await?;
    let indexes = list_pg_indexes(conn, table_name).await?;

    let mut fields = Vec::new();
    let mut model_attrs: Vec<ModelAttribute> = Vec::new();

    let is_composite_pk = pk_columns.len() > 1;

    for col in &columns {
        let base_type = pg_type_to_base(&col.data_type, &col.udt_name, enum_type_names);
        let nullable = col.is_nullable && !pk_columns.contains(&col.name);

        let mut attrs = Vec::new();

        if !is_composite_pk && pk_columns.contains(&col.name) {
            attrs.push(FieldAttribute::Id);
            // Detect serial/identity columns from default
            if let Some(default_val) = &col.default_value {
                if default_val.contains("nextval(") {
                    attrs.push(FieldAttribute::Autoincrement);
                }
            }
        }

        // Detect unique constraints from single-column unique indexes
        let is_unique = indexes
            .iter()
            .any(|idx| idx.unique && idx.columns.len() == 1 && idx.columns[0] == col.name);
        if is_unique {
            attrs.push(FieldAttribute::Unique);
        }

        if let Some(default_val) = &col.default_value {
            if !default_val.contains("nextval(") {
                if let Some(dv) = parse_pg_default(default_val) {
                    attrs.push(FieldAttribute::Default(dv));
                }
            }
        }

        fields.push(FieldDef {
            name: col.name.clone(),
            type_expr: TypeExpr {
                base: base_type,
                nullable,
                span: Span { line: 0, column: 0 },
            },
            attributes: attrs,
            span: Span { line: 0, column: 0 },
        });
    }

    if is_composite_pk {
        model_attrs.push(ModelAttribute::Id(pk_columns));
    }

    for idx in &indexes {
        if !idx.unique && !idx.is_primary {
            model_attrs.push(ModelAttribute::Index(idx.columns.clone()));
        }
    }

    let model_name = to_pascal_case(table_name);
    if model_name != table_name {
        model_attrs.push(ModelAttribute::Map(table_name.to_string()));
    }

    Ok(ModelDef {
        name: model_name,
        fields,
        attributes: model_attrs,
        span: Span { line: 0, column: 0 },
    })
}

async fn list_pg_tables(conn: &dyn Connection) -> Result<Vec<String>, QuiverError> {
    let stmt = Statement::sql(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name".to_string(),
    );
    let rows = conn.query(&stmt).await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| match r.values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect())
}

struct PgColumn {
    name: String,
    data_type: String,
    is_nullable: bool,
    default_value: Option<String>,
    udt_name: String,
}

async fn list_pg_columns(conn: &dyn Connection, table: &str) -> Result<Vec<PgColumn>, QuiverError> {
    let stmt = Statement::new(
        "SELECT column_name, data_type, is_nullable, column_default, udt_name \
         FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 \
         ORDER BY ordinal_position"
            .to_string(),
        vec![Value::Text(table.to_string())],
    );
    let rows = conn.query(&stmt).await?;

    let mut columns = Vec::new();
    for row in rows {
        let name = text_at(&row, 0);
        let data_type = text_at(&row, 1);
        let is_nullable = text_at(&row, 2) == "YES";
        let default_value = match row.values.get(3) {
            Some(Value::Text(s)) if !s.is_empty() => Some(s.clone()),
            _ => None,
        };
        let udt_name = text_at(&row, 4);

        columns.push(PgColumn {
            name,
            data_type,
            is_nullable,
            default_value,
            udt_name,
        });
    }

    Ok(columns)
}

async fn list_pg_primary_keys(
    conn: &dyn Connection,
    table: &str,
) -> Result<Vec<String>, QuiverError> {
    let stmt = Statement::new(
        "SELECT kcu.column_name \
         FROM information_schema.key_column_usage kcu \
         JOIN information_schema.table_constraints tc \
           ON kcu.constraint_name = tc.constraint_name \
           AND kcu.table_schema = tc.table_schema \
         WHERE tc.constraint_type = 'PRIMARY KEY' \
           AND tc.table_name = $1 \
           AND tc.table_schema = 'public' \
         ORDER BY kcu.ordinal_position"
            .to_string(),
        vec![Value::Text(table.to_string())],
    );
    let rows = conn.query(&stmt).await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| match r.values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect())
}

struct ForeignKeyInfo {
    from_col: String,
    to_table: String,
    to_col: String,
}

async fn list_pg_foreign_keys(
    conn: &dyn Connection,
    table: &str,
) -> Result<Vec<ForeignKeyInfo>, QuiverError> {
    let stmt = Statement::new(
        "SELECT kcu.column_name, ccu.table_name AS foreign_table_name, \
                ccu.column_name AS foreign_column_name \
         FROM information_schema.key_column_usage kcu \
         JOIN information_schema.referential_constraints rc \
           ON kcu.constraint_name = rc.constraint_name \
           AND kcu.table_schema = rc.constraint_schema \
         JOIN information_schema.constraint_column_usage ccu \
           ON rc.unique_constraint_name = ccu.constraint_name \
           AND rc.unique_constraint_schema = ccu.constraint_schema \
         WHERE kcu.table_name = $1 \
           AND kcu.table_schema = 'public'"
            .to_string(),
        vec![Value::Text(table.to_string())],
    );
    let rows = conn.query(&stmt).await?;
    Ok(rows
        .iter()
        .map(|r| ForeignKeyInfo {
            from_col: text_at(r, 0),
            to_table: text_at(r, 1),
            to_col: text_at(r, 2),
        })
        .collect())
}

struct PgIndex {
    columns: Vec<String>,
    unique: bool,
    is_primary: bool,
}

async fn list_pg_indexes(conn: &dyn Connection, table: &str) -> Result<Vec<PgIndex>, QuiverError> {
    let stmt = Statement::new(
        "SELECT indexname, indexdef FROM pg_indexes \
         WHERE tablename = $1 AND schemaname = 'public'"
            .to_string(),
        vec![Value::Text(table.to_string())],
    );
    let rows = conn.query(&stmt).await?;

    let mut indexes = Vec::new();
    for row in &rows {
        let idx_name = text_at(row, 0);
        let idx_def = text_at(row, 1);

        let is_primary = idx_name.ends_with("_pkey");
        let unique = idx_def.contains("UNIQUE") || is_primary;

        // Parse column names from indexdef: ... (col1, col2, ...)
        let columns = parse_pg_index_columns(&idx_def);

        if !columns.is_empty() {
            indexes.push(PgIndex {
                columns,
                unique,
                is_primary,
            });
        }
    }

    Ok(indexes)
}

/// Parse column names from a PostgreSQL index definition string.
///
/// Example: `CREATE UNIQUE INDEX users_email_key ON public.users USING btree (email)`
/// Returns: `["email"]`
fn parse_pg_index_columns(indexdef: &str) -> Vec<String> {
    // Find the last '(' and matching ')'
    if let Some(start) = indexdef.rfind('(') {
        if let Some(end) = indexdef[start..].find(')') {
            let inner = &indexdef[start + 1..start + end];
            return inner
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }
    Vec::new()
}

async fn list_pg_enums(conn: &dyn Connection) -> Result<Vec<(String, String)>, QuiverError> {
    let stmt = Statement::sql(
        "SELECT t.typname, e.enumlabel \
         FROM pg_type t \
         JOIN pg_enum e ON t.oid = e.enumtypid \
         ORDER BY t.typname, e.enumsortorder"
            .to_string(),
    );
    let rows = conn.query(&stmt).await?;
    Ok(rows
        .iter()
        .map(|r| (text_at(r, 0), text_at(r, 1)))
        .collect())
}

fn pg_type_to_base(data_type: &str, udt_name: &str, enum_type_names: &[String]) -> BaseType {
    let lower = data_type.to_lowercase();
    match lower.as_str() {
        "integer" => BaseType::Int32,
        "bigint" => BaseType::Int64,
        "smallint" => BaseType::Int16,
        "boolean" => BaseType::Boolean,
        "real" => BaseType::Float32,
        "double precision" => BaseType::Float64,
        "numeric" | "decimal" => BaseType::Decimal128 {
            precision: 38,
            scale: 10,
        },
        "text" | "character varying" | "character" => BaseType::Utf8,
        "bytea" => BaseType::Binary,
        "date" => BaseType::Date32,
        "time without time zone" | "time" => BaseType::Time64 {
            unit: TimeUnit::Microsecond,
        },
        "timestamp without time zone" | "timestamp with time zone" | "timestamp" => {
            BaseType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".to_string()),
            }
        }
        "jsonb" | "json" => BaseType::Struct(Vec::new()),
        "user-defined" => {
            // Check if it's a known enum type
            if enum_type_names.iter().any(|n| n == udt_name) {
                BaseType::Named(udt_name.to_string())
            } else {
                BaseType::Utf8
            }
        }
        _ => {
            // Try matching on udt_name for aliases
            match udt_name {
                "int4" => BaseType::Int32,
                "int8" => BaseType::Int64,
                "int2" => BaseType::Int16,
                "bool" => BaseType::Boolean,
                "float4" => BaseType::Float32,
                "float8" => BaseType::Float64,
                "varchar" => BaseType::Utf8,
                "timestamptz" => BaseType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: Some("UTC".to_string()),
                },
                _ => BaseType::Utf8,
            }
        }
    }
}

fn parse_pg_default(default: &str) -> Option<DefaultValue> {
    let trimmed = default.trim();

    if trimmed.eq_ignore_ascii_case("NULL") {
        return None;
    }

    // now() or CURRENT_TIMESTAMP
    if trimmed.contains("now()") || trimmed.contains("CURRENT_TIMESTAMP") {
        return Some(DefaultValue::Now);
    }

    // Numeric defaults (may have ::type cast suffix)
    let without_cast = trimmed.split("::").next().unwrap_or(trimmed).trim();
    if let Ok(v) = without_cast.parse::<i64>() {
        return Some(DefaultValue::Int(v));
    }
    if let Ok(v) = without_cast.parse::<f64>() {
        return Some(DefaultValue::Float(v));
    }

    // Boolean defaults
    if without_cast.eq_ignore_ascii_case("true") {
        return Some(DefaultValue::Bool(true));
    }
    if without_cast.eq_ignore_ascii_case("false") {
        return Some(DefaultValue::Bool(false));
    }

    // String defaults: 'value'::type or 'value'
    if without_cast.starts_with('\'') && without_cast.ends_with('\'') && without_cast.len() >= 2 {
        let inner = &without_cast[1..without_cast.len() - 1];
        if inner == "[]" {
            return Some(DefaultValue::EmptyList);
        }
        if inner == "{}" {
            return Some(DefaultValue::EmptyMap);
        }
        return Some(DefaultValue::String(inner.replace("''", "'")));
    }

    None
}

/// Get the SQL table name for a model (check MAP attribute).
fn pg_table_name_for(model: &ModelDef) -> String {
    for attr in &model.attributes {
        if let ModelAttribute::Map(name) = attr {
            return name.clone();
        }
    }
    model.name.clone()
}

// --- MySQL introspection ---

async fn introspect_mysql(conn: &dyn Connection) -> Result<Schema, QuiverError> {
    let tables = list_mysql_tables(conn).await?;
    let mut models = Vec::new();
    let mut enums: Vec<EnumDef> = Vec::new();

    for table_name in &tables {
        if table_name.starts_with("_quiver_") {
            continue;
        }

        let (model, table_enums) = introspect_mysql_table(conn, table_name).await?;
        for te in table_enums {
            // Avoid duplicate enum defs
            if !enums.iter().any(|e| e.name == te.name) {
                enums.push(te);
            }
        }
        models.push(model);
    }

    // Second pass: add FOREIGN KEY model attributes based on foreign keys
    for model in &mut models {
        let table_name = mysql_table_name_for(model);
        let fks = list_mysql_foreign_keys(conn, &table_name).await?;
        for fk in &fks {
            let references_model = to_pascal_case(&fk.to_table);
            model.attributes.push(ModelAttribute::ForeignKey {
                fields: vec![fk.from_col.clone()],
                references_model,
                references_columns: vec![fk.to_col.clone()],
                on_delete: None,
                on_update: None,
            });
        }
    }

    Ok(Schema {
        config: None,
        generate: None,
        enums,
        models,
    })
}

async fn introspect_mysql_table(
    conn: &dyn Connection,
    table_name: &str,
) -> Result<(ModelDef, Vec<EnumDef>), QuiverError> {
    let columns = list_mysql_columns(conn, table_name).await?;
    let pk_columns = list_mysql_primary_keys(conn, table_name).await?;
    let indexes = list_mysql_indexes(conn, table_name).await?;

    let mut fields = Vec::new();
    let mut model_attrs: Vec<ModelAttribute> = Vec::new();
    let mut table_enums: Vec<EnumDef> = Vec::new();

    let is_composite_pk = pk_columns.len() > 1;

    for col in &columns {
        let (base_type, maybe_enum) =
            mysql_type_to_base(&col.data_type, &col.column_type, table_name, &col.name);
        if let Some(enum_def) = maybe_enum {
            table_enums.push(enum_def);
        }
        let nullable = col.is_nullable && !pk_columns.contains(&col.name);

        let mut attrs = Vec::new();

        if !is_composite_pk && pk_columns.contains(&col.name) {
            attrs.push(FieldAttribute::Id);
            // Detect auto_increment from extra column
            if col.extra.contains("auto_increment") {
                attrs.push(FieldAttribute::Autoincrement);
            }
        }

        // Detect unique constraints from single-column unique indexes
        let is_unique = indexes
            .iter()
            .any(|idx| idx.unique && idx.columns.len() == 1 && idx.columns[0] == col.name);
        if is_unique {
            attrs.push(FieldAttribute::Unique);
        }

        if let Some(default_val) = &col.default_value {
            if let Some(dv) = parse_mysql_default(default_val) {
                attrs.push(FieldAttribute::Default(dv));
            }
        }

        fields.push(FieldDef {
            name: col.name.clone(),
            type_expr: TypeExpr {
                base: base_type,
                nullable,
                span: Span { line: 0, column: 0 },
            },
            attributes: attrs,
            span: Span { line: 0, column: 0 },
        });
    }

    if is_composite_pk {
        model_attrs.push(ModelAttribute::Id(pk_columns));
    }

    for idx in &indexes {
        if !idx.unique && !idx.is_primary {
            model_attrs.push(ModelAttribute::Index(idx.columns.clone()));
        }
    }

    let model_name = to_pascal_case(table_name);
    if model_name != table_name {
        model_attrs.push(ModelAttribute::Map(table_name.to_string()));
    }

    Ok((
        ModelDef {
            name: model_name,
            fields,
            attributes: model_attrs,
            span: Span { line: 0, column: 0 },
        },
        table_enums,
    ))
}

async fn list_mysql_tables(conn: &dyn Connection) -> Result<Vec<String>, QuiverError> {
    let stmt = Statement::sql(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' \
         ORDER BY table_name"
            .to_string(),
    );
    let rows = conn.query(&stmt).await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| match r.values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect())
}

struct MysqlColumn {
    name: String,
    data_type: String,
    is_nullable: bool,
    default_value: Option<String>,
    column_type: String,
    extra: String,
}

async fn list_mysql_columns(
    conn: &dyn Connection,
    table: &str,
) -> Result<Vec<MysqlColumn>, QuiverError> {
    let stmt = Statement::new(
        "SELECT column_name, data_type, is_nullable, column_default, column_type, extra \
         FROM information_schema.columns \
         WHERE table_schema = DATABASE() AND table_name = ? \
         ORDER BY ordinal_position"
            .to_string(),
        vec![Value::Text(table.to_string())],
    );
    let rows = conn.query(&stmt).await?;

    let mut columns = Vec::new();
    for row in rows {
        let name = text_at(&row, 0);
        let data_type = text_at(&row, 1);
        let is_nullable = text_at(&row, 2) == "YES";
        let default_value = match row.values.get(3) {
            Some(Value::Text(s)) if !s.is_empty() => Some(s.clone()),
            _ => None,
        };
        let column_type = text_at(&row, 4);
        let extra = text_at(&row, 5);

        columns.push(MysqlColumn {
            name,
            data_type,
            is_nullable,
            default_value,
            column_type,
            extra,
        });
    }

    Ok(columns)
}

async fn list_mysql_primary_keys(
    conn: &dyn Connection,
    table: &str,
) -> Result<Vec<String>, QuiverError> {
    let stmt = Statement::new(
        "SELECT kcu.column_name \
         FROM information_schema.key_column_usage kcu \
         JOIN information_schema.table_constraints tc \
           ON kcu.constraint_name = tc.constraint_name \
           AND kcu.table_schema = tc.table_schema \
         WHERE tc.constraint_type = 'PRIMARY KEY' \
           AND tc.table_name = ? \
           AND tc.table_schema = DATABASE() \
         ORDER BY kcu.ordinal_position"
            .to_string(),
        vec![Value::Text(table.to_string())],
    );
    let rows = conn.query(&stmt).await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| match r.values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect())
}

async fn list_mysql_foreign_keys(
    conn: &dyn Connection,
    table: &str,
) -> Result<Vec<ForeignKeyInfo>, QuiverError> {
    let stmt = Statement::new(
        "SELECT column_name, referenced_table_name, referenced_column_name \
         FROM information_schema.key_column_usage \
         WHERE table_schema = DATABASE() \
           AND table_name = ? \
           AND referenced_table_name IS NOT NULL"
            .to_string(),
        vec![Value::Text(table.to_string())],
    );
    let rows = conn.query(&stmt).await?;
    Ok(rows
        .iter()
        .map(|r| ForeignKeyInfo {
            from_col: text_at(r, 0),
            to_table: text_at(r, 1),
            to_col: text_at(r, 2),
        })
        .collect())
}

struct MysqlIndex {
    columns: Vec<String>,
    unique: bool,
    is_primary: bool,
}

async fn list_mysql_indexes(
    conn: &dyn Connection,
    table: &str,
) -> Result<Vec<MysqlIndex>, QuiverError> {
    let stmt = Statement::new(
        "SELECT index_name, non_unique, column_name \
         FROM information_schema.statistics \
         WHERE table_schema = DATABASE() AND table_name = ? \
         ORDER BY index_name, seq_in_index"
            .to_string(),
        vec![Value::Text(table.to_string())],
    );
    let rows = conn.query(&stmt).await?;

    // Group by index_name
    let mut index_map: Vec<(String, bool, bool, Vec<String>)> = Vec::new();
    for row in &rows {
        let idx_name = text_at(row, 0);
        let non_unique = match row.values.get(1) {
            Some(Value::Int(v)) => *v != 0,
            Some(Value::Text(s)) => s != "0",
            _ => true,
        };
        let col_name = text_at(row, 2);
        let is_primary = idx_name == "PRIMARY";
        let unique = !non_unique;

        if let Some(existing) = index_map
            .iter_mut()
            .find(|(name, _, _, _)| *name == idx_name)
        {
            existing.3.push(col_name);
        } else {
            index_map.push((idx_name, unique, is_primary, vec![col_name]));
        }
    }

    Ok(index_map
        .into_iter()
        .map(|(_, unique, is_primary, columns)| MysqlIndex {
            columns,
            unique,
            is_primary,
        })
        .collect())
}

/// Map MySQL data types to BaseType, handling the special `enum(...)` column_type.
///
/// Returns the base type and an optional enum definition if the column is an enum.
fn mysql_type_to_base(
    data_type: &str,
    column_type: &str,
    table_name: &str,
    column_name: &str,
) -> (BaseType, Option<EnumDef>) {
    let lower = data_type.to_lowercase();

    // Special case: tinyint(1) is boolean in MySQL
    if lower == "tinyint" {
        let col_type_lower = column_type.to_lowercase();
        if col_type_lower == "tinyint(1)" {
            return (BaseType::Boolean, None);
        }
        return (BaseType::Int8, None);
    }

    match lower.as_str() {
        "int" | "mediumint" => (BaseType::Int32, None),
        "bigint" => (BaseType::Int64, None),
        "smallint" => (BaseType::Int16, None),
        "float" => (BaseType::Float32, None),
        "double" => (BaseType::Float64, None),
        "decimal" => (
            BaseType::Decimal128 {
                precision: 38,
                scale: 10,
            },
            None,
        ),
        "varchar" | "char" | "text" | "longtext" | "mediumtext" | "tinytext" => {
            (BaseType::Utf8, None)
        }
        "varbinary" | "binary" | "blob" | "longblob" | "mediumblob" | "tinyblob" => {
            (BaseType::Binary, None)
        }
        "date" => (BaseType::Date32, None),
        "time" => (
            BaseType::Time64 {
                unit: TimeUnit::Microsecond,
            },
            None,
        ),
        "datetime" | "timestamp" => (
            BaseType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".to_string()),
            },
            None,
        ),
        "json" => (BaseType::Struct(Vec::new()), None),
        "enum" => {
            // Parse enum values from column_type: enum('val1','val2','val3')
            let values = parse_mysql_enum_values(column_type);
            let enum_name = format!(
                "{}_{}",
                to_pascal_case(table_name),
                to_pascal_case(column_name)
            );
            let enum_def = EnumDef {
                name: enum_name.clone(),
                values: values
                    .into_iter()
                    .map(|v| EnumValue {
                        name: v,
                        span: Span { line: 0, column: 0 },
                    })
                    .collect(),
                span: Span { line: 0, column: 0 },
            };
            (BaseType::Named(enum_name), Some(enum_def))
        }
        _ => (BaseType::Utf8, None),
    }
}

/// Parse enum values from a MySQL column_type string like `enum('val1','val2','val3')`.
fn parse_mysql_enum_values(column_type: &str) -> Vec<String> {
    let lower = column_type.to_lowercase();
    // Find content between "enum(" and ")"
    let start = match lower.find("enum(") {
        Some(pos) => pos + 5, // len("enum(") = 5
        None => return Vec::new(),
    };
    // Use the original (non-lowered) string for value extraction
    let end = match column_type[start..].find(')') {
        Some(pos) => start + pos,
        None => return Vec::new(),
    };
    let inner = &column_type[start..end];

    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;

    for ch in inner.chars() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
            }
            '\'' if in_quote => {
                in_quote = false;
                values.push(current.clone());
                current.clear();
            }
            ',' if !in_quote => {
                // skip comma between values
            }
            _ if in_quote => {
                current.push(ch);
            }
            _ => {
                // skip whitespace outside quotes
            }
        }
    }

    values
}

fn parse_mysql_default(default: &str) -> Option<DefaultValue> {
    let trimmed = default.trim();

    if trimmed.eq_ignore_ascii_case("NULL") {
        return None;
    }

    if trimmed.contains("CURRENT_TIMESTAMP") || trimmed.contains("current_timestamp") {
        return Some(DefaultValue::Now);
    }

    // Numeric defaults
    if let Ok(v) = trimmed.parse::<i64>() {
        return Some(DefaultValue::Int(v));
    }
    if let Ok(v) = trimmed.parse::<f64>() {
        return Some(DefaultValue::Float(v));
    }

    // String defaults (strip quotes)
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        let inner = &trimmed[1..trimmed.len() - 1];
        if inner == "[]" {
            return Some(DefaultValue::EmptyList);
        }
        if inner == "{}" {
            return Some(DefaultValue::EmptyMap);
        }
        return Some(DefaultValue::String(inner.replace("''", "'")));
    }

    // Bare string (MySQL sometimes returns defaults without quotes)
    Some(DefaultValue::String(trimmed.to_string()))
}

/// Get the SQL table name for a MySQL model (check MAP attribute).
fn mysql_table_name_for(model: &ModelDef) -> String {
    for attr in &model.attributes {
        if let ModelAttribute::Map(name) = attr {
            return name.clone();
        }
    }
    model.name.clone()
}

// --- SQLite metadata queries ---

async fn list_sqlite_tables(conn: &dyn Connection) -> Result<Vec<String>, QuiverError> {
    let stmt = Statement::sql(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name".to_string(),
    );
    let rows = conn.query(&stmt).await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| match r.values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect())
}

struct SqliteColumn {
    name: String,
    col_type: String,
    not_null: bool,
    default_value: Option<String>,
    is_autoincrement: bool,
}

async fn list_sqlite_columns(
    conn: &dyn Connection,
    table: &str,
) -> Result<Vec<SqliteColumn>, QuiverError> {
    validate_safe_ident(table)?;
    let stmt = Statement::sql(format!("PRAGMA table_info(\"{}\")", table));
    let rows = conn.query(&stmt).await?;

    // Check for AUTOINCREMENT in the CREATE TABLE SQL
    let create_sql = get_create_sql(conn, table).await?;
    let has_autoincrement = create_sql
        .as_ref()
        .is_some_and(|sql| sql.to_uppercase().contains("AUTOINCREMENT"));

    let mut columns = Vec::new();
    for row in rows {
        let name = text_at(&row, 1);
        let col_type = text_at(&row, 2);
        let not_null = matches!(row.values.get(3), Some(Value::Int(1)));
        let default_value = match row.values.get(4) {
            Some(Value::Text(s)) if !s.is_empty() => Some(s.clone()),
            _ => None,
        };
        let is_pk = matches!(row.values.get(5), Some(Value::Int(n)) if *n > 0);
        let is_autoincrement = is_pk && has_autoincrement;

        columns.push(SqliteColumn {
            name,
            col_type,
            not_null,
            default_value,
            is_autoincrement,
        });
    }

    Ok(columns)
}

async fn sqlite_pk_columns(conn: &dyn Connection, table: &str) -> Result<Vec<String>, QuiverError> {
    validate_safe_ident(table)?;
    let stmt = Statement::sql(format!("PRAGMA table_info(\"{}\")", table));
    let rows = conn.query(&stmt).await?;

    let mut pk_cols: Vec<(i64, String)> = Vec::new();
    for row in rows {
        if let Some(Value::Int(pk_idx)) = row.values.get(5) {
            if *pk_idx > 0 {
                let name = text_at(&row, 1);
                pk_cols.push((*pk_idx, name));
            }
        }
    }
    pk_cols.sort_by_key(|(idx, _)| *idx);
    Ok(pk_cols.into_iter().map(|(_, name)| name).collect())
}

struct SqliteIndex {
    columns: Vec<String>,
    unique: bool,
}

async fn sqlite_indexes(
    conn: &dyn Connection,
    table: &str,
) -> Result<Vec<SqliteIndex>, QuiverError> {
    validate_safe_ident(table)?;
    let stmt = Statement::sql(format!("PRAGMA index_list(\"{}\")", table));
    let rows = conn.query(&stmt).await?;

    let mut indexes = Vec::new();
    for row in rows {
        let idx_name = text_at(&row, 1);
        let unique = matches!(row.values.get(2), Some(Value::Int(1)));

        // Skip auto-created indexes. The `origin` column (index 3) exists in
        // SQLite 3.7.15+. If absent, skip indexes whose names start with
        // "sqlite_autoindex_" as a fallback heuristic.
        let origin = if row.values.len() > 3 {
            text_at(&row, 3)
        } else {
            String::new()
        };

        if origin == "pk" || origin == "u" {
            continue;
        }
        if idx_name.starts_with("sqlite_autoindex_") {
            continue;
        }

        validate_safe_ident(&idx_name)?;
        let col_stmt = Statement::sql(format!("PRAGMA index_info(\"{}\")", idx_name));
        let col_rows = conn.query(&col_stmt).await?;
        let columns: Vec<String> = col_rows.iter().map(|r| text_at(r, 2)).collect();

        if !columns.is_empty() {
            indexes.push(SqliteIndex { columns, unique });
        }
    }

    Ok(indexes)
}

async fn sqlite_unique_columns(
    conn: &dyn Connection,
    table: &str,
) -> Result<Vec<String>, QuiverError> {
    validate_safe_ident(table)?;
    let stmt = Statement::sql(format!("PRAGMA index_list(\"{}\")", table));
    let rows = conn.query(&stmt).await?;

    let mut unique_cols = Vec::new();
    for row in rows {
        let idx_name = text_at(&row, 1);
        let unique = matches!(row.values.get(2), Some(Value::Int(1)));

        if !unique {
            continue;
        }

        validate_safe_ident(&idx_name)?;
        let col_stmt = Statement::sql(format!("PRAGMA index_info(\"{}\")", idx_name));
        let col_rows = conn.query(&col_stmt).await?;

        // Only mark as UNIQUE if single-column unique index
        if col_rows.len() == 1 {
            unique_cols.push(text_at(&col_rows[0], 2));
        }
    }

    Ok(unique_cols)
}

struct ForeignKey {
    from_col: String,
    to_table: String,
    to_col: String,
}

async fn list_sqlite_foreign_keys(
    conn: &dyn Connection,
    table: &str,
) -> Result<Vec<ForeignKey>, QuiverError> {
    validate_safe_ident(table)?;
    let stmt = Statement::sql(format!("PRAGMA foreign_key_list(\"{}\")", table));
    let rows = conn.query(&stmt).await?;

    Ok(rows
        .iter()
        .map(|r| ForeignKey {
            to_table: text_at(r, 2),
            from_col: text_at(r, 3),
            to_col: text_at(r, 4),
        })
        .collect())
}

async fn get_create_sql(conn: &dyn Connection, table: &str) -> Result<Option<String>, QuiverError> {
    let stmt = Statement::new(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1".to_string(),
        vec![Value::Text(table.to_string())],
    );
    let row = conn.query_optional(&stmt).await?;
    Ok(row.and_then(|r| match r.values.into_iter().next() {
        Some(Value::Text(s)) => Some(s),
        _ => None,
    }))
}

async fn read_enum_values(
    conn: &dyn Connection,
    enum_table: &str,
) -> Result<Vec<String>, QuiverError> {
    validate_safe_ident(enum_table)?;
    let stmt = Statement::sql(format!(
        "SELECT value FROM \"{}\" ORDER BY rowid",
        enum_table
    ));
    let rows = conn.query(&stmt).await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| match r.values.into_iter().next() {
            Some(Value::Text(v)) => Some(v),
            _ => None,
        })
        .collect())
}

// --- Type mapping ---

fn sqlite_type_to_base(sql_type: &str, enums: &[EnumDef]) -> BaseType {
    let upper = sql_type.to_uppercase();
    match upper.as_str() {
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" => BaseType::Int64,
        "REAL" | "FLOAT" | "DOUBLE" => BaseType::Float64,
        "TEXT" | "VARCHAR" | "CHAR" | "CLOB" => {
            // TODO: use `enums` to detect Named(enum) types. Currently we can't
            // distinguish "TEXT column" from "TEXT column that stores an enum value"
            // from the SQL type alone. The caller handles enum detection via the
            // _enum_ table convention. Keeping the parameter for future heuristics
            // (e.g. matching column name to enum name).
            let _ = enums;
            BaseType::Utf8
        }
        "BLOB" | "BINARY" | "VARBINARY" => BaseType::Binary,
        "BOOLEAN" | "BOOL" => BaseType::Boolean,
        "DATE" => BaseType::Date32,
        "DATETIME" | "TIMESTAMP" => BaseType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: Some("UTC".to_string()),
        },
        "TIME" => BaseType::Time64 {
            unit: TimeUnit::Microsecond,
        },
        _ => {
            // Unknown type -- default to Utf8
            BaseType::Utf8
        }
    }
}

fn parse_sqlite_default(default: &str) -> Option<DefaultValue> {
    let trimmed = default.trim();

    // NULL default
    if trimmed.eq_ignore_ascii_case("NULL") {
        return None;
    }

    // datetime('now') -> Now
    if trimmed.contains("datetime('now')") || trimmed.contains("CURRENT_TIMESTAMP") {
        return Some(DefaultValue::Now);
    }

    // Numeric defaults
    if let Ok(v) = trimmed.parse::<i64>() {
        return Some(DefaultValue::Int(v));
    }
    if let Ok(v) = trimmed.parse::<f64>() {
        return Some(DefaultValue::Float(v));
    }

    // String defaults (strip quotes)
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        let inner = &trimmed[1..trimmed.len() - 1];
        // Special cases
        if inner == "[]" {
            return Some(DefaultValue::EmptyList);
        }
        if inner == "{}" {
            return Some(DefaultValue::EmptyMap);
        }
        return Some(DefaultValue::String(inner.replace("''", "'")));
    }

    None
}

// --- Helpers ---

fn text_at(row: &Row, idx: usize) -> String {
    match row.values.get(idx) {
        Some(Value::Text(s)) => s.clone(),
        _ => String::new(),
    }
}

fn to_pascal_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = true;
    for c in s.chars() {
        if c == '_' || c == '-' {
            capitalize_next = true;
        } else if capitalize_next {
            result.extend(c.to_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
fn pluralize(name: &str) -> String {
    let lower = name.to_lowercase();
    if lower.ends_with('s') {
        format!("{}es", lower)
    } else if lower.ends_with('y') {
        format!("{}ies", &lower[..lower.len() - 1])
    } else {
        format!("{}s", lower)
    }
}

/// Render a Schema to `.quiver` format string.
pub fn schema_to_quiver(schema: &Schema) -> String {
    let mut out = String::new();

    for e in &schema.enums {
        out.push_str(&format!("enum {} {{\n", e.name));
        for v in &e.values {
            out.push_str(&format!("    {}\n", v.name));
        }
        out.push_str("}\n\n");
    }

    for m in &schema.models {
        out.push_str(&format!("model {} {{\n", m.name));

        for f in &m.fields {
            let type_str = format_type(&f.type_expr);
            let attrs = format_field_attrs(&f.attributes);
            if attrs.is_empty() {
                out.push_str(&format!("    {} {}\n", f.name, type_str));
            } else {
                out.push_str(&format!("    {} {} {}\n", f.name, type_str, attrs));
            }
        }

        for attr in &m.attributes {
            match attr {
                ModelAttribute::Map(name) => {
                    out.push_str(&format!("    MAP \"{}\"\n", name));
                }
                ModelAttribute::Id(fields) => {
                    out.push_str(&format!("    PRIMARY KEY ({})\n", fields.join(", ")));
                }
                ModelAttribute::Index(fields) => {
                    out.push_str(&format!("    INDEX ({})\n", fields.join(", ")));
                }
                ModelAttribute::Unique(fields) => {
                    out.push_str(&format!("    UNIQUE ({})\n", fields.join(", ")));
                }
                ModelAttribute::ForeignKey {
                    fields,
                    references_model,
                    references_columns,
                    on_delete,
                    on_update,
                } => {
                    let mut fk_str = format!(
                        "    FOREIGN KEY ({}) REFERENCES {} ({})",
                        fields.join(", "),
                        references_model,
                        references_columns.join(", "),
                    );
                    if let Some(action) = on_delete {
                        fk_str.push_str(&format!(" ON DELETE {}", action));
                    }
                    if let Some(action) = on_update {
                        fk_str.push_str(&format!(" ON UPDATE {}", action));
                    }
                    fk_str.push('\n');
                    out.push_str(&fk_str);
                }
            }
        }

        out.push_str("}\n\n");
    }

    out.trim_end().to_string()
}

fn format_type(te: &TypeExpr) -> String {
    let base = format_base_type(&te.base);
    if te.nullable {
        format!("{}?", base)
    } else {
        base
    }
}

fn format_base_type(bt: &BaseType) -> String {
    match bt {
        BaseType::Int8 => "Int8".to_string(),
        BaseType::Int16 => "Int16".to_string(),
        BaseType::Int32 => "Int32".to_string(),
        BaseType::Int64 => "Int64".to_string(),
        BaseType::UInt8 => "UInt8".to_string(),
        BaseType::UInt16 => "UInt16".to_string(),
        BaseType::UInt32 => "UInt32".to_string(),
        BaseType::UInt64 => "UInt64".to_string(),
        BaseType::Float16 => "Float16".to_string(),
        BaseType::Float32 => "Float32".to_string(),
        BaseType::Float64 => "Float64".to_string(),
        BaseType::Utf8 => "Utf8".to_string(),
        BaseType::LargeUtf8 => "LargeUtf8".to_string(),
        BaseType::Binary => "Binary".to_string(),
        BaseType::LargeBinary => "LargeBinary".to_string(),
        BaseType::Boolean => "Boolean".to_string(),
        BaseType::Date32 => "Date32".to_string(),
        BaseType::Date64 => "Date64".to_string(),
        BaseType::Decimal128 { precision, scale } => {
            format!("Decimal128({}, {})", precision, scale)
        }
        BaseType::Decimal256 { precision, scale } => {
            format!("Decimal256({}, {})", precision, scale)
        }
        BaseType::FixedSizeBinary { size } => format!("FixedSizeBinary({})", size),
        BaseType::Timestamp { unit, timezone } => {
            let u = format_time_unit(unit);
            match timezone {
                Some(z) => format!("Timestamp({}, {})", u, z),
                None => format!("Timestamp({})", u),
            }
        }
        BaseType::Time32 { unit } => format!("Time32({})", format_time_unit(unit)),
        BaseType::Time64 { unit } => format!("Time64({})", format_time_unit(unit)),
        BaseType::List(inner) => format!("List<{}>", format_base_type(&inner.base)),
        BaseType::LargeList(inner) => format!("LargeList<{}>", format_base_type(&inner.base)),
        BaseType::Map { key, value } => {
            format!(
                "Map<{}, {}>",
                format_base_type(&key.base),
                format_base_type(&value.base)
            )
        }
        BaseType::Struct(fields) => {
            let f: Vec<String> = fields
                .iter()
                .map(|sf| format!("{}: {}", sf.name, format_base_type(&sf.type_expr.base)))
                .collect();
            format!("Struct<{}>", f.join(", "))
        }
        BaseType::Named(n) => n.clone(),
    }
}

fn format_time_unit(u: &TimeUnit) -> &'static str {
    match u {
        TimeUnit::Second => "Second",
        TimeUnit::Millisecond => "Millisecond",
        TimeUnit::Microsecond => "Microsecond",
        TimeUnit::Nanosecond => "Nanosecond",
    }
}

fn format_field_attrs(attrs: &[FieldAttribute]) -> String {
    let parts: Vec<String> = attrs
        .iter()
        .map(|a| match a {
            FieldAttribute::Id => "PRIMARY KEY".to_string(),
            FieldAttribute::Autoincrement => "AUTOINCREMENT".to_string(),
            FieldAttribute::Unique => "UNIQUE".to_string(),
            FieldAttribute::Map(name) => format!("MAP \"{}\"", name),
            FieldAttribute::Default(dv) => format!("DEFAULT {}", format_default(dv)),
        })
        .collect();
    parts.join(" ")
}

fn format_default(dv: &DefaultValue) -> String {
    match dv {
        DefaultValue::Int(v) => v.to_string(),
        DefaultValue::Float(v) => v.to_string(),
        DefaultValue::String(v) => format!("\"{}\"", v),
        DefaultValue::Bool(v) => v.to_string(),
        DefaultValue::Now => "now()".to_string(),
        DefaultValue::Uuid => "uuid()".to_string(),
        DefaultValue::Cuid => "cuid()".to_string(),
        DefaultValue::EmptyList => "[]".to_string(),
        DefaultValue::EmptyMap => "{}".to_string(),
        DefaultValue::EnumVariant(v) => v.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pascal_case_simple() {
        assert_eq!(to_pascal_case("users"), "Users");
        assert_eq!(to_pascal_case("user_roles"), "UserRoles");
        assert_eq!(to_pascal_case("User"), "User");
        assert_eq!(to_pascal_case("my-table"), "MyTable");
    }

    #[test]
    fn pluralize_basic() {
        assert_eq!(pluralize("Post"), "posts");
        assert_eq!(pluralize("Category"), "categories");
        assert_eq!(pluralize("Address"), "addresses");
    }

    #[test]
    fn parse_default_integer() {
        assert!(matches!(
            parse_sqlite_default("42"),
            Some(DefaultValue::Int(42))
        ));
        assert!(matches!(
            parse_sqlite_default("0"),
            Some(DefaultValue::Int(0))
        ));
    }

    #[test]
    fn parse_default_string() {
        assert!(matches!(
            parse_sqlite_default("'hello'"),
            Some(DefaultValue::String(s)) if s == "hello"
        ));
    }

    #[test]
    fn parse_default_now() {
        assert!(matches!(
            parse_sqlite_default("(datetime('now'))"),
            Some(DefaultValue::Now)
        ));
        assert!(matches!(
            parse_sqlite_default("CURRENT_TIMESTAMP"),
            Some(DefaultValue::Now)
        ));
    }

    #[test]
    fn parse_default_empty_collections() {
        assert!(matches!(
            parse_sqlite_default("'[]'"),
            Some(DefaultValue::EmptyList)
        ));
        assert!(matches!(
            parse_sqlite_default("'{}'"),
            Some(DefaultValue::EmptyMap)
        ));
    }

    #[test]
    fn parse_default_null_returns_none() {
        assert!(parse_sqlite_default("NULL").is_none());
    }

    #[test]
    fn sqlite_type_mapping() {
        let enums = vec![];
        assert!(matches!(
            sqlite_type_to_base("INTEGER", &enums),
            BaseType::Int64
        ));
        assert!(matches!(
            sqlite_type_to_base("REAL", &enums),
            BaseType::Float64
        ));
        assert!(matches!(
            sqlite_type_to_base("TEXT", &enums),
            BaseType::Utf8
        ));
        assert!(matches!(
            sqlite_type_to_base("BLOB", &enums),
            BaseType::Binary
        ));
        assert!(matches!(
            sqlite_type_to_base("BOOLEAN", &enums),
            BaseType::Boolean
        ));
        assert!(matches!(
            sqlite_type_to_base("DATETIME", &enums),
            BaseType::Timestamp { .. }
        ));
    }

    #[test]
    fn schema_to_quiver_roundtrip() {
        let schema = Schema {
            config: None,
            generate: None,
            enums: vec![EnumDef {
                name: "Role".to_string(),
                values: vec![
                    EnumValue {
                        name: "User".to_string(),
                        span: Span { line: 0, column: 0 },
                    },
                    EnumValue {
                        name: "Admin".to_string(),
                        span: Span { line: 0, column: 0 },
                    },
                ],
                span: Span { line: 0, column: 0 },
            }],
            models: vec![ModelDef {
                name: "Account".to_string(),
                fields: vec![
                    FieldDef {
                        name: "id".to_string(),
                        type_expr: TypeExpr {
                            base: BaseType::Int64,
                            nullable: false,
                            span: Span { line: 0, column: 0 },
                        },
                        attributes: vec![FieldAttribute::Id, FieldAttribute::Autoincrement],
                        span: Span { line: 0, column: 0 },
                    },
                    FieldDef {
                        name: "email".to_string(),
                        type_expr: TypeExpr {
                            base: BaseType::Utf8,
                            nullable: false,
                            span: Span { line: 0, column: 0 },
                        },
                        attributes: vec![FieldAttribute::Unique],
                        span: Span { line: 0, column: 0 },
                    },
                ],
                attributes: vec![ModelAttribute::Map("accounts".to_string())],
                span: Span { line: 0, column: 0 },
            }],
        };

        let output = schema_to_quiver(&schema);
        assert!(output.contains("enum Role {"));
        assert!(output.contains("    User"));
        assert!(output.contains("    Admin"));
        assert!(output.contains("model Account {"));
        assert!(output.contains("id Int64 PRIMARY KEY AUTOINCREMENT"));
        assert!(output.contains("email Utf8 UNIQUE"));
        assert!(output.contains("MAP \"accounts\""));
    }
}
