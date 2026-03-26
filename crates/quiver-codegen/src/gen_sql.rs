use quiver_error::QuiverError;
use quiver_schema::Schema;
use quiver_schema::ast::*;
pub use quiver_schema::sql_types::{SqlDialect, base_type_to_sql};

/// Generates SQL DDL statements from a Quiver schema.
pub struct SqlGenerator;

impl SqlGenerator {
    /// Generate `CREATE TABLE` DDL for all models in the schema.
    pub fn generate(schema: &Schema, dialect: SqlDialect) -> Result<String, QuiverError> {
        let mut out = String::new();

        for m in &schema.models {
            gen_create_table(&mut out, m, schema, dialect)?;
        }

        Ok(out)
    }
}

fn gen_create_table(
    out: &mut String,
    m: &ModelDef,
    schema: &Schema,
    dialect: SqlDialect,
) -> Result<(), QuiverError> {
    let table_name = m.table_name();
    out.push_str(&format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" (\n",
        table_name
    ));

    let mut columns: Vec<String> = Vec::new();
    let mut pk_fields: Vec<String> = Vec::new();
    let mut has_single_pk = false;

    for f in &m.fields {
        let col_name = f.column_name().to_string();
        let sql_type = base_type_to_sql(&f.type_expr.base, schema, dialect);
        let mut col = format!("  \"{}\" {}", col_name, sql_type);

        if has_attr(f, |a| matches!(a, FieldAttribute::Id)) {
            has_single_pk = true;
            pk_fields.push(col_name.clone());
            col.push_str(" PRIMARY KEY");
            if has_attr(f, |a| matches!(a, FieldAttribute::Autoincrement)) {
                match dialect {
                    SqlDialect::Sqlite => col.push_str(" AUTOINCREMENT"),
                    SqlDialect::Postgres => col.push_str(" GENERATED ALWAYS AS IDENTITY"),
                    SqlDialect::Mysql => col.push_str(" AUTO_INCREMENT"),
                }
            }
        }

        if !f.type_expr.nullable {
            // Don't repeat NOT NULL for PKs (implicit in SQLite)
            if !has_attr(f, |a| matches!(a, FieldAttribute::Id)) {
                col.push_str(" NOT NULL");
            }
        }

        if has_attr(f, |a| matches!(a, FieldAttribute::Unique)) {
            col.push_str(" UNIQUE");
        }

        if let Some(default) = get_default(f, schema, dialect)? {
            col.push_str(&format!(" DEFAULT {}", default));
        }

        columns.push(col);
    }

    // Composite primary key (@@id)
    if !has_single_pk {
        for attr in &m.attributes {
            if let ModelAttribute::Id(fields) = attr {
                let cols: Vec<String> = fields.iter().map(|f| format!("\"{}\"", f)).collect();
                columns.push(format!("  PRIMARY KEY ({})", cols.join(", ")));
                pk_fields.extend(fields.clone());
            }
        }
    }

    // Foreign keys (from @@fk model attributes)
    for attr in &m.attributes {
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
                .find(|mm| mm.name == *references_model)
                .map(|mm| mm.table_name().to_string())
                .unwrap_or_else(|| references_model.clone());
            let fk_cols: Vec<String> = fields.iter().map(|f| format!("\"{}\"", f)).collect();
            let ref_cols: Vec<String> = references_columns
                .iter()
                .map(|r| format!("\"{}\"", r))
                .collect();
            let mut fk = format!(
                "  FOREIGN KEY ({}) REFERENCES \"{}\"({})",
                fk_cols.join(", "),
                target_table,
                ref_cols.join(", "),
            );
            if let Some(action) = on_delete {
                fk.push_str(&format!(" ON DELETE {}", action));
            }
            if let Some(action) = on_update {
                fk.push_str(&format!(" ON UPDATE {}", action));
            }
            columns.push(fk);
        }
    }

    out.push_str(&columns.join(",\n"));
    out.push_str("\n);\n\n");

    // Indexes
    for attr in &m.attributes {
        match attr {
            ModelAttribute::Index(fields) => {
                let idx_name = format!("idx_{}_{}", table_name, fields.join("_"));
                let cols: Vec<String> = fields.iter().map(|f| format!("\"{}\"", f)).collect();
                out.push_str(&format!(
                    "CREATE INDEX IF NOT EXISTS \"{}\" ON \"{}\" ({});\n",
                    idx_name,
                    table_name,
                    cols.join(", ")
                ));
            }
            ModelAttribute::Unique(fields) => {
                let idx_name = format!("uq_{}_{}", table_name, fields.join("_"));
                let cols: Vec<String> = fields.iter().map(|f| format!("\"{}\"", f)).collect();
                out.push_str(&format!(
                    "CREATE UNIQUE INDEX IF NOT EXISTS \"{}\" ON \"{}\" ({});\n",
                    idx_name,
                    table_name,
                    cols.join(", ")
                ));
            }
            _ => {}
        }
    }

    // Extra newline after indexes
    if m.attributes
        .iter()
        .any(|a| matches!(a, ModelAttribute::Index(_) | ModelAttribute::Unique(_)))
    {
        out.push('\n');
    }

    Ok(())
}

fn has_attr(f: &FieldDef, pred: impl Fn(&FieldAttribute) -> bool) -> bool {
    f.attributes.iter().any(pred)
}

fn get_default(
    f: &FieldDef,
    schema: &Schema,
    dialect: SqlDialect,
) -> Result<Option<String>, QuiverError> {
    for attr in &f.attributes {
        if let FieldAttribute::Default(val) = attr {
            return Ok(match val {
                DefaultValue::Int(v) => Some(v.to_string()),
                DefaultValue::Float(v) => Some(v.to_string()),
                DefaultValue::String(v) => {
                    validate_safe_literal(v)?;
                    Some(format!("'{}'", v))
                }
                DefaultValue::Bool(v) => match dialect {
                    SqlDialect::Postgres => Some(if *v { "true".into() } else { "false".into() }),
                    SqlDialect::Sqlite | SqlDialect::Mysql => {
                        Some(if *v { "1".into() } else { "0".into() })
                    }
                },
                DefaultValue::Now => match dialect {
                    SqlDialect::Sqlite => Some("(datetime('now'))".into()),
                    SqlDialect::Postgres => Some("now()".into()),
                    SqlDialect::Mysql => Some("CURRENT_TIMESTAMP(6)".into()),
                },
                DefaultValue::Uuid => None, // handled at app level
                DefaultValue::Cuid => None, // handled at app level
                DefaultValue::EmptyList => match dialect {
                    SqlDialect::Sqlite | SqlDialect::Mysql => Some("'[]'".into()),
                    SqlDialect::Postgres => Some("'[]'::jsonb".into()),
                },
                DefaultValue::EmptyMap => match dialect {
                    SqlDialect::Sqlite | SqlDialect::Mysql => Some("'{}'".into()),
                    SqlDialect::Postgres => Some("'{}'::jsonb".into()),
                },
                DefaultValue::EnumVariant(v) => {
                    // Verify it's a valid enum variant
                    let is_enum = f.type_expr.nullable
                        || matches!(&f.type_expr.base, BaseType::Named(n) if schema.enums.iter().any(|e| e.name == *n));
                    if is_enum {
                        validate_safe_sql_name(v)?;
                        Some(format!("'{}'", v))
                    } else {
                        None
                    }
                }
            });
        }
    }
    Ok(None)
}

/// Validate that a name is safe to embed in SQL (alphanumeric + underscore only).
fn validate_safe_sql_name(name: &str) -> Result<(), QuiverError> {
    if name.is_empty() {
        return Err(QuiverError::Codegen("empty SQL name".to_string()));
    }
    for ch in name.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            return Err(QuiverError::Codegen(format!(
                "unsafe character '{}' in SQL name: {}",
                ch, name
            )));
        }
    }
    Ok(())
}

/// Validate that a string is safe to embed as a single-quoted SQL literal.
///
/// Rejects any string containing SQL metacharacters. This is deliberately
/// strict: prefer validation (reject bad input) over escaping (try to
/// neutralize bad input).
fn validate_safe_literal(s: &str) -> Result<(), QuiverError> {
    for ch in s.chars() {
        match ch {
            '\'' | '"' | ';' | '\\' | '\0' => {
                return Err(QuiverError::Codegen(format!(
                    "unsafe character '{}' in DDL literal: {}",
                    ch, s
                )));
            }
            _ => {}
        }
    }
    if s.contains("--") {
        return Err(QuiverError::Codegen(format!(
            "SQL comment sequence '--' in DDL literal: {}",
            s
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quiver_schema::parse;

    #[test]
    fn simple_create_table() {
        let schema = parse(
            r#"
            model User {
                id    Int32  PRIMARY KEY AUTOINCREMENT
                email Utf8   UNIQUE
                name  Utf8?
                active Boolean DEFAULT true
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"User\""));
        assert!(ddl.contains("\"id\" INTEGER PRIMARY KEY AUTOINCREMENT"));
        assert!(ddl.contains("\"email\" TEXT NOT NULL UNIQUE"));
        assert!(ddl.contains("\"name\" TEXT"));
        assert!(ddl.contains("\"active\" INTEGER NOT NULL DEFAULT 1"));
        // name should NOT have NOT NULL
        assert!(!ddl.contains("\"name\" TEXT NOT NULL"));
    }

    #[test]
    fn table_with_map_name() {
        let schema = parse(
            r#"
            model User {
                id Int32 PRIMARY KEY
                MAP "users"
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"users\""));
    }

    #[test]
    fn table_with_index() {
        let schema = parse(
            r#"
            model User {
                id    Int32 PRIMARY KEY
                email Utf8
                INDEX (email)
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).unwrap();
        assert!(ddl.contains("CREATE INDEX IF NOT EXISTS"));
        assert!(ddl.contains("\"email\""));
    }

    #[test]
    fn table_with_relation() {
        let schema = parse(
            r#"
            model User {
                id    Int32  PRIMARY KEY
            }
            model Post {
                id       Int32 PRIMARY KEY
                authorId Int32
                FOREIGN KEY (authorId) REFERENCES User (id)
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).unwrap();
        // Post table should have FK constraint
        assert!(ddl.contains("FOREIGN KEY (\"authorId\") REFERENCES \"User\"(\"id\")"));
    }

    #[test]
    fn enum_default() {
        let schema = parse(
            r#"
            enum Role { User Admin Moderator }
            model Account {
                id   Int32 PRIMARY KEY
                role Role  DEFAULT Admin
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).unwrap();
        assert!(ddl.contains("\"role\" TEXT NOT NULL DEFAULT 'Admin'"));
    }

    #[test]
    fn temporal_types_as_text() {
        let schema = parse(
            r#"
            model Event {
                id      Int32 PRIMARY KEY
                created Timestamp(Microsecond, UTC) DEFAULT now()
                day     Date32
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).unwrap();
        assert!(ddl.contains("\"created\" TEXT NOT NULL DEFAULT (datetime('now'))"));
        assert!(ddl.contains("\"day\" TEXT NOT NULL"));
    }

    #[test]
    fn simple_create_table_postgres() {
        let schema = parse(
            r#"
            model User {
                id    Int32  PRIMARY KEY AUTOINCREMENT
                email Utf8   UNIQUE
                name  Utf8?
                active Boolean DEFAULT true
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Postgres).unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"User\""));
        assert!(
            ddl.contains("\"id\" INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY"),
            "DDL was: {}",
            ddl
        );
        assert!(ddl.contains("\"email\" TEXT NOT NULL UNIQUE"));
        assert!(ddl.contains("\"name\" TEXT"));
        assert!(ddl.contains("\"active\" BOOLEAN NOT NULL DEFAULT true"));
        assert!(!ddl.contains("\"name\" TEXT NOT NULL"));
    }

    #[test]
    fn simple_create_table_mysql() {
        let schema = parse(
            r#"
            model User {
                id    Int32  PRIMARY KEY AUTOINCREMENT
                email Utf8   UNIQUE
                name  Utf8?
                active Boolean DEFAULT true
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Mysql).unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"User\""));
        assert!(
            ddl.contains("\"id\" INT PRIMARY KEY AUTO_INCREMENT"),
            "DDL was: {}",
            ddl
        );
        assert!(ddl.contains("\"email\" VARCHAR(255) NOT NULL UNIQUE"));
        assert!(ddl.contains("\"name\" VARCHAR(255)"));
        assert!(ddl.contains("\"active\" TINYINT(1) NOT NULL DEFAULT 1"));
        assert!(!ddl.contains("\"name\" VARCHAR(255) NOT NULL"));
    }

    #[test]
    fn temporal_types_postgres() {
        let schema = parse(
            r#"
            model Event {
                id      Int32 PRIMARY KEY
                created Timestamp(Microsecond, UTC) DEFAULT now()
                day     Date32
                t       Time64(Microsecond)
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Postgres).unwrap();
        assert!(
            ddl.contains("\"created\" TIMESTAMPTZ NOT NULL DEFAULT now()"),
            "DDL was: {}",
            ddl
        );
        assert!(ddl.contains("\"day\" DATE NOT NULL"));
        assert!(ddl.contains("\"t\" TIME NOT NULL"));
    }

    #[test]
    fn bool_default_postgres() {
        let schema = parse(
            r#"
            model Settings {
                id      Int32   PRIMARY KEY
                enabled Boolean DEFAULT true
                hidden  Boolean DEFAULT false
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Postgres).unwrap();
        assert!(
            ddl.contains("\"enabled\" BOOLEAN NOT NULL DEFAULT true"),
            "DDL was: {}",
            ddl
        );
        assert!(
            ddl.contains("\"hidden\" BOOLEAN NOT NULL DEFAULT false"),
            "DDL was: {}",
            ddl
        );
    }

    #[test]
    fn relation_with_referential_actions() {
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
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).unwrap();
        assert!(
            ddl.contains(
                r#"FOREIGN KEY ("authorId") REFERENCES "User"("id") ON DELETE CASCADE ON UPDATE RESTRICT"#
            ),
            "DDL was: {}",
            ddl
        );
    }

    #[test]
    fn list_as_json_text() {
        let schema = parse(
            r#"
            model Doc {
                id   Int32 PRIMARY KEY
                tags List<Utf8> DEFAULT []
            }
        "#,
        )
        .unwrap();
        let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).unwrap();
        assert!(ddl.contains("\"tags\" TEXT NOT NULL DEFAULT '[]'"));
    }
}
