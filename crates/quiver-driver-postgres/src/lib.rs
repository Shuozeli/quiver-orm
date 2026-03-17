//! PostgreSQL driver for Quiver, delegating to the async ADBC PostgreSQL driver.
//!
//! All database operations go through `adbc_postgres::PostgresConnection`, which
//! uses `tokio_postgres` internally for native async I/O.
//!
//! For direct Arrow-native ADBC access, use the re-exported `adbc_postgres` crate.

pub mod pool;

pub use adbc_postgres;
pub use pool::PostgresPool;

use adbc::{DatabaseOption, OptionValue};
use quiver_driver_core::{AdbcConnection, AdbcTransaction, BoxFuture, Dialect, Driver, adbc_err};
use quiver_error::QuiverError;

/// PostgreSQL dialect: rewrites `?` placeholders to `$N` and splits DDL on `;`.
#[derive(Clone, Default)]
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    type AdbcConn = adbc_postgres::PostgresConnection;

    fn rewrite_sql(&self, sql: &str) -> String {
        rewrite_placeholders(sql)
    }

    fn split_ddl(&self) -> bool {
        true
    }
}

/// A PostgreSQL connection.
pub type PostgresConnection = AdbcConnection<PostgresDialect>;

/// An active PostgreSQL transaction.
pub type PostgresTransaction = AdbcTransaction<PostgresDialect>;

/// PostgreSQL driver factory.
pub struct PostgresDriver;

impl Driver for PostgresDriver {
    type Conn = PostgresConnection;

    fn connect<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Self::Conn, QuiverError>> {
        Box::pin(async move {
            let drv = adbc_postgres::PostgresDriver;
            let db = adbc::Driver::new_database_with_opts(
                &drv,
                [(DatabaseOption::Uri, OptionValue::String(url.into()))],
            )
            .await
            .map_err(adbc_err)?;
            let conn = adbc::Database::new_connection(&db)
                .await
                .map_err(adbc_err)?;

            Ok(AdbcConnection::new(conn, PostgresDialect))
        })
    }
}

// ---------------------------------------------------------------------------
// Placeholder rewriting: Quiver uses `?` but PostgreSQL uses `$1, $2, ...`
// ---------------------------------------------------------------------------

/// Rewrite `?` placeholders to `$1, $2, ...` for PostgreSQL.
///
/// Handles quoted strings (single and double quotes) so that `?` inside
/// string literals is not rewritten. Escaped quotes (`''` inside single-quoted
/// strings, `""` inside double-quoted identifiers) are handled correctly.
/// Also rewrites `?N` style (e.g. `?1`) to `$N`.
fn rewrite_placeholders(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut param_idx = 0u32;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_double_quote => {
                result.push(ch);
                // Handle escaped quotes: '' stays inside the string
                if in_single_quote && chars.peek() == Some(&'\'') {
                    result.push(chars.next().unwrap());
                } else {
                    in_single_quote = !in_single_quote;
                }
            }
            '"' if !in_single_quote => {
                result.push(ch);
                if in_double_quote && chars.peek() == Some(&'"') {
                    result.push(chars.next().unwrap());
                } else {
                    in_double_quote = !in_double_quote;
                }
            }
            '?' if !in_single_quote && !in_double_quote => {
                param_idx += 1;
                // Skip trailing digits (e.g. ?1 -> $1)
                while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                    chars.next();
                }
                result.push('$');
                result.push_str(&param_idx.to_string());
            }
            _ => result.push(ch),
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_simple_placeholders() {
        assert_eq!(
            rewrite_placeholders("SELECT * FROM t WHERE id = ? AND name = ?"),
            "SELECT * FROM t WHERE id = $1 AND name = $2"
        );
    }

    #[test]
    fn rewrite_numbered_placeholders() {
        assert_eq!(
            rewrite_placeholders("INSERT INTO t VALUES (?1, ?2)"),
            "INSERT INTO t VALUES ($1, $2)"
        );
    }

    #[test]
    fn rewrite_preserves_quoted_question_marks() {
        assert_eq!(
            rewrite_placeholders("SELECT '?' FROM t WHERE id = ?"),
            "SELECT '?' FROM t WHERE id = $1"
        );
    }

    #[test]
    fn rewrite_no_placeholders() {
        assert_eq!(rewrite_placeholders("SELECT 1"), "SELECT 1");
    }

    #[test]
    fn rewrite_escaped_single_quotes() {
        // '' inside a single-quoted string is an escaped quote, not end-of-string
        assert_eq!(
            rewrite_placeholders("SELECT * FROM t WHERE name = 'it''s' AND id = ?"),
            "SELECT * FROM t WHERE name = 'it''s' AND id = $1"
        );
    }

    #[test]
    fn rewrite_escaped_double_quotes() {
        assert_eq!(
            rewrite_placeholders(r#"SELECT * FROM "col""name" WHERE id = ?"#),
            r#"SELECT * FROM "col""name" WHERE id = $1"#
        );
    }
}
