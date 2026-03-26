//! Shared SQL dialect type-mapping functions.
//!
//! Maps Quiver `BaseType` to SQL column types for each supported dialect.
//! Used by both `quiver-codegen` (DDL generation) and `quiver-migrate`
//! (migration DDL generation) to avoid duplication.

use crate::Schema;
use crate::ast::*;

/// SQL dialect for DDL generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
    Mysql,
}

/// Map a `BaseType` to the SQL column type string for the given dialect.
pub fn base_type_to_sql(base: &BaseType, schema: &Schema, dialect: SqlDialect) -> &'static str {
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

        BaseType::Decimal128 { .. } | BaseType::Decimal256 { .. } => "TEXT", // stored as string

        BaseType::Utf8 | BaseType::LargeUtf8 => "TEXT",

        BaseType::Binary | BaseType::LargeBinary | BaseType::FixedSizeBinary { .. } => "BLOB",

        BaseType::Boolean => "INTEGER", // SQLite has no native bool, uses 0/1

        BaseType::Date32 | BaseType::Date64 => "TEXT", // ISO 8601 date strings

        BaseType::Time32 { .. } | BaseType::Time64 { .. } => "TEXT", // ISO 8601 time strings

        BaseType::Timestamp { .. } => "TEXT", // ISO 8601 datetime strings

        BaseType::List(_) | BaseType::LargeList(_) | BaseType::Map { .. } | BaseType::Struct(_) => {
            "TEXT" // JSON-encoded
        }

        BaseType::Named(name) => {
            if schema.enums.iter().any(|e| e.name == *name) {
                "TEXT" // Enums stored as text
            } else {
                "INTEGER" // FK reference
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    #[test]
    fn sqlite_type_mapping() {
        let schema = parse("model T { id Int32 PRIMARY KEY }").unwrap();
        assert_eq!(
            base_type_to_sql(&BaseType::Int32, &schema, SqlDialect::Sqlite),
            "INTEGER"
        );
        assert_eq!(
            base_type_to_sql(&BaseType::Float64, &schema, SqlDialect::Sqlite),
            "REAL"
        );
        assert_eq!(
            base_type_to_sql(&BaseType::Utf8, &schema, SqlDialect::Sqlite),
            "TEXT"
        );
        assert_eq!(
            base_type_to_sql(&BaseType::Boolean, &schema, SqlDialect::Sqlite),
            "INTEGER"
        );
    }

    #[test]
    fn postgres_type_mapping() {
        let schema = parse("model T { id Int32 PRIMARY KEY }").unwrap();
        assert_eq!(
            base_type_to_sql(&BaseType::Int32, &schema, SqlDialect::Postgres),
            "INTEGER"
        );
        assert_eq!(
            base_type_to_sql(&BaseType::Float64, &schema, SqlDialect::Postgres),
            "DOUBLE PRECISION"
        );
        assert_eq!(
            base_type_to_sql(&BaseType::Boolean, &schema, SqlDialect::Postgres),
            "BOOLEAN"
        );
    }

    #[test]
    fn mysql_type_mapping() {
        let schema = parse("model T { id Int32 PRIMARY KEY }").unwrap();
        assert_eq!(
            base_type_to_sql(&BaseType::Int32, &schema, SqlDialect::Mysql),
            "INT"
        );
        assert_eq!(
            base_type_to_sql(&BaseType::Float64, &schema, SqlDialect::Mysql),
            "DOUBLE"
        );
        assert_eq!(
            base_type_to_sql(&BaseType::UInt64, &schema, SqlDialect::Mysql),
            "BIGINT UNSIGNED"
        );
    }
}
