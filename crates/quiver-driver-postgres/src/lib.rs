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

// Placeholder rewriting: Quiver uses `?` but PostgreSQL uses `$1, $2, ...`

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
    use quiver_driver_core::{
        Connection, DdlStatement, Statement, Transaction, Transactional, Value,
    };

    fn pg_url() -> String {
        std::env::var("QUIVER_PG_URL")
            .unwrap_or_else(|_| "postgresql://localhost:5432/quiver_test".to_string())
    }

    async fn pg_conn() -> PostgresConnection {
        PostgresDriver.connect(&pg_url()).await.unwrap()
    }

    fn ddl(sql: &str) -> DdlStatement {
        DdlStatement::new(sql.to_string())
    }

    fn stmt(sql: &str, params: &[Value]) -> Statement {
        Statement::new(sql.to_string(), params.to_vec())
    }

    #[tokio::test]
    #[ignore]
    async fn connect_and_execute_ddl() {
        let conn = pg_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_ddl"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_ddl (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
        ))
        .await
        .unwrap();
        conn.execute_ddl(&ddl("DROP TABLE test_ddl")).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn insert_and_query() {
        let conn = pg_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_iq"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_iq (id SERIAL PRIMARY KEY, name TEXT)",
        ))
        .await
        .unwrap();

        let affected = conn
            .execute(&stmt(
                "INSERT INTO test_iq (name) VALUES (?)",
                &[Value::from("Alice")],
            ))
            .await
            .unwrap();
        assert_eq!(affected, 1);

        let rows = conn
            .query(&stmt("SELECT id, name FROM test_iq", &[]))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_string(1), Some("Alice".into()));

        conn.execute_ddl(&ddl("DROP TABLE test_iq")).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn query_one_and_optional() {
        let conn = pg_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_qoo"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl("CREATE TABLE test_qoo (id INTEGER PRIMARY KEY)"))
            .await
            .unwrap();
        conn.execute(&stmt("INSERT INTO test_qoo (id) VALUES (1)", &[]))
            .await
            .unwrap();

        let row = conn
            .query_one(&stmt("SELECT id FROM test_qoo WHERE id = 1", &[]))
            .await
            .unwrap();
        assert_eq!(row.get_i64(0), Some(1));

        let none = conn
            .query_optional(&stmt("SELECT id FROM test_qoo WHERE id = 99", &[]))
            .await
            .unwrap();
        assert!(none.is_none());

        conn.execute_ddl(&ddl("DROP TABLE test_qoo")).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn transaction_commit() {
        let mut conn = pg_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_txc"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_txc (id SERIAL PRIMARY KEY, v TEXT)",
        ))
        .await
        .unwrap();

        {
            let tx = conn.begin().await.unwrap();
            tx.execute(&stmt(
                "INSERT INTO test_txc (v) VALUES (?)",
                &[Value::from("a")],
            ))
            .await
            .unwrap();
            tx.execute(&stmt(
                "INSERT INTO test_txc (v) VALUES (?)",
                &[Value::from("b")],
            ))
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }

        let rows = conn
            .query(&stmt("SELECT COUNT(*) FROM test_txc", &[]))
            .await
            .unwrap();
        assert_eq!(rows[0].get_i64(0), Some(2));

        conn.execute_ddl(&ddl("DROP TABLE test_txc")).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn transaction_explicit_rollback() {
        let mut conn = pg_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_txr"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_txr (id SERIAL PRIMARY KEY, v TEXT)",
        ))
        .await
        .unwrap();
        conn.execute(&stmt("INSERT INTO test_txr (v) VALUES ('keep')", &[]))
            .await
            .unwrap();

        {
            let tx = conn.begin().await.unwrap();
            tx.execute(&stmt(
                "INSERT INTO test_txr (v) VALUES (?)",
                &[Value::from("discard")],
            ))
            .await
            .unwrap();
            tx.rollback().await.unwrap();
        }

        let rows = conn
            .query(&stmt("SELECT COUNT(*) FROM test_txr", &[]))
            .await
            .unwrap();
        assert_eq!(rows[0].get_i64(0), Some(1));

        conn.execute_ddl(&ddl("DROP TABLE test_txr")).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn null_result() {
        let conn = pg_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_null"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_null (id SERIAL PRIMARY KEY, v TEXT)",
        ))
        .await
        .unwrap();
        // Insert NULL via raw SQL (PG ADBC does not bind null params)
        conn.execute(&stmt("INSERT INTO test_null (v) VALUES (NULL)", &[]))
            .await
            .unwrap();

        let row = conn
            .query_one(&stmt("SELECT v FROM test_null LIMIT 1", &[]))
            .await
            .unwrap();
        assert!(row.get(0).unwrap().is_null());

        conn.execute_ddl(&ddl("DROP TABLE test_null"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn float_roundtrip() {
        let conn = pg_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_float"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl("CREATE TABLE test_float (v DOUBLE PRECISION)"))
            .await
            .unwrap();
        conn.execute(&stmt(
            "INSERT INTO test_float (v) VALUES (?)",
            &[Value::from(1.234f64)],
        ))
        .await
        .unwrap();

        let row = conn
            .query_one(&stmt("SELECT v FROM test_float", &[]))
            .await
            .unwrap();
        let v = row.get_f64(0).unwrap();
        assert!((v - 1.234).abs() < f64::EPSILON);

        conn.execute_ddl(&ddl("DROP TABLE test_float"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn bool_roundtrip() {
        let conn = pg_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_bool"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl("CREATE TABLE test_bool (v BOOLEAN)"))
            .await
            .unwrap();
        conn.execute(&stmt(
            "INSERT INTO test_bool (v) VALUES (?)",
            &[Value::from(true)],
        ))
        .await
        .unwrap();

        let row = conn
            .query_one(&stmt("SELECT v FROM test_bool", &[]))
            .await
            .unwrap();
        assert_eq!(row.get_bool(0), Some(true));

        conn.execute_ddl(&ddl("DROP TABLE test_bool"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn get_by_column_name() {
        let conn = pg_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_colname"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_colname (id SERIAL PRIMARY KEY, name TEXT)",
        ))
        .await
        .unwrap();
        conn.execute(&stmt(
            "INSERT INTO test_colname (name) VALUES (?)",
            &[Value::from("Bob")],
        ))
        .await
        .unwrap();

        let row = conn
            .query_one(&stmt("SELECT id, name FROM test_colname", &[]))
            .await
            .unwrap();
        assert_eq!(row.get_by_name("name"), Some(&Value::Text("Bob".into())));

        conn.execute_ddl(&ddl("DROP TABLE test_colname"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn query_stream_basic() {
        use tokio_stream::StreamExt;

        let conn = pg_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_stream"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_stream (id BIGINT PRIMARY KEY, name TEXT)",
        ))
        .await
        .unwrap();

        for i in 1..=10i64 {
            conn.execute(&stmt(
                "INSERT INTO test_stream (id, name) VALUES (?, ?)",
                &[Value::Int(i), Value::from(format!("user{i}"))],
            ))
            .await
            .unwrap();
        }

        let mut stream = conn
            .query_stream(&stmt("SELECT id, name FROM test_stream ORDER BY id", &[]))
            .await
            .unwrap();

        let mut count = 0i64;
        while let Some(row) = stream.next().await {
            let row = row.unwrap();
            count += 1;
            assert_eq!(row.get_i64(0), Some(count));
        }
        assert_eq!(count, 10);

        conn.execute_ddl(&ddl("DROP TABLE test_stream"))
            .await
            .unwrap();
    }

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
