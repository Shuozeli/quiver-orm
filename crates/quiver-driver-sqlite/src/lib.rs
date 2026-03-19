//! SQLite driver for Quiver, delegating to the async ADBC SQLite driver.
//!
//! All database operations go through `adbc_sqlite::SqliteConnection`, which
//! handles `spawn_blocking` internally for the sync rusqlite layer.
//!
//! For direct Arrow-native ADBC access, use the re-exported `adbc_sqlite` crate.

pub use adbc_sqlite;

pub mod pool;

use adbc::DatabaseOption;
use adbc::OptionValue;
use quiver_driver_core::{AdbcConnection, AdbcTransaction, BoxFuture, Dialect, Driver, adbc_err};
use quiver_error::QuiverError;

pub use pool::SqlitePool;

/// SQLite dialect: uses `?` placeholders and handles multi-statement DDL natively.
#[derive(Clone, Default)]
pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    type AdbcConn = adbc_sqlite::SqliteConnection;

    fn rewrite_sql(&self, sql: &str) -> String {
        sql.to_owned()
    }

    fn split_ddl(&self) -> bool {
        false
    }
}

/// A SQLite connection.
pub type SqliteConnection = AdbcConnection<SqliteDialect>;

/// An active SQLite transaction.
pub type SqliteTransaction = AdbcTransaction<SqliteDialect>;

/// SQLite driver factory.
pub struct SqliteDriver;

impl Driver for SqliteDriver {
    type Conn = SqliteConnection;

    fn connect<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Self::Conn, QuiverError>> {
        Box::pin(async move {
            let drv = adbc_sqlite::SqliteDriver;
            let db = adbc::Driver::new_database_with_opts(
                &drv,
                [(DatabaseOption::Uri, OptionValue::String(url.into()))],
            )
            .await
            .map_err(adbc_err)?;
            let conn = adbc::Database::new_connection(&db)
                .await
                .map_err(adbc_err)?;

            // Enable foreign keys (SQLite default is OFF)
            let mut stmt = adbc::Connection::new_statement(&conn)
                .await
                .map_err(adbc_err)?;
            adbc::Statement::set_sql_query(&mut stmt, "PRAGMA foreign_keys = ON")
                .await
                .map_err(adbc_err)?;
            adbc::Statement::execute_update(&mut stmt)
                .await
                .map_err(adbc_err)?;

            Ok(AdbcConnection::new(conn, SqliteDialect))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quiver_driver_core::{
        Connection, DdlStatement, Statement, Transaction, Transactional, Value,
    };

    async fn mem_conn() -> SqliteConnection {
        SqliteDriver.connect(":memory:").await.unwrap()
    }

    fn ddl(sql: &str) -> DdlStatement {
        DdlStatement::new(sql.to_string())
    }

    fn stmt(sql: &str, params: &[Value]) -> Statement {
        Statement::new(sql.to_string(), params.to_vec())
    }

    #[tokio::test]
    async fn connect_and_execute_ddl() {
        let conn = mem_conn().await;
        conn.execute_ddl(&ddl(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn insert_and_query() {
        let conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);"))
            .await
            .unwrap();

        let affected = conn
            .execute(&stmt(
                "INSERT INTO t (name) VALUES (?1)",
                &[Value::from("Alice")],
            ))
            .await
            .unwrap();
        assert_eq!(affected, 1);

        let rows = conn
            .query(&stmt("SELECT id, name FROM t", &[]))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_i64(0), Some(1));
        assert_eq!(rows[0].get_string(1), Some("Alice".into()));
    }

    #[tokio::test]
    async fn query_one_and_optional() {
        let conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY);"))
            .await
            .unwrap();
        conn.execute(&stmt("INSERT INTO t (id) VALUES (1)", &[]))
            .await
            .unwrap();

        let row = conn
            .query_one(&stmt("SELECT id FROM t WHERE id = 1", &[]))
            .await
            .unwrap();
        assert_eq!(row.get_i64(0), Some(1));

        let none = conn
            .query_optional(&stmt("SELECT id FROM t WHERE id = 99", &[]))
            .await
            .unwrap();
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn transaction_commit() {
        let mut conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"))
            .await
            .unwrap();

        {
            let tx = conn.begin().await.unwrap();
            tx.execute(&stmt("INSERT INTO t (v) VALUES (?1)", &[Value::from("a")]))
                .await
                .unwrap();
            tx.execute(&stmt("INSERT INTO t (v) VALUES (?1)", &[Value::from("b")]))
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }

        let rows = conn
            .query(&stmt("SELECT COUNT(*) FROM t", &[]))
            .await
            .unwrap();
        assert_eq!(rows[0].get_i64(0), Some(2));
    }

    #[tokio::test]
    async fn transaction_rollback_on_drop() {
        let mut conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"))
            .await
            .unwrap();
        conn.execute(&stmt("INSERT INTO t (v) VALUES ('keep')", &[]))
            .await
            .unwrap();

        {
            let tx = conn.begin().await.unwrap();
            tx.execute(&stmt(
                "INSERT INTO t (v) VALUES (?1)",
                &[Value::from("discard")],
            ))
            .await
            .unwrap();
            // drop without commit -> rollback (synchronous via thread::join)
        }

        let rows = conn
            .query(&stmt("SELECT COUNT(*) FROM t", &[]))
            .await
            .unwrap();
        assert_eq!(rows[0].get_i64(0), Some(1));
    }

    #[tokio::test]
    async fn transaction_explicit_rollback() {
        let mut conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (v TEXT);"))
            .await
            .unwrap();

        {
            let tx = conn.begin().await.unwrap();
            tx.execute(&stmt("INSERT INTO t (v) VALUES ('gone')", &[]))
                .await
                .unwrap();
            tx.rollback().await.unwrap();
        }

        let rows = conn
            .query(&stmt("SELECT COUNT(*) FROM t", &[]))
            .await
            .unwrap();
        assert_eq!(rows[0].get_i64(0), Some(0));
    }

    #[tokio::test]
    async fn null_parameter_and_result() {
        let conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"))
            .await
            .unwrap();
        conn.execute(&stmt("INSERT INTO t (v) VALUES (?1)", &[Value::Null]))
            .await
            .unwrap();

        let row = conn
            .query_one(&stmt("SELECT v FROM t WHERE id = 1", &[]))
            .await
            .unwrap();
        assert!(row.get(0).unwrap().is_null());
    }

    #[tokio::test]
    async fn blob_roundtrip() {
        let conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB);"))
            .await
            .unwrap();

        let bytes = vec![0xDE_u8, 0xAD, 0xBE, 0xEF];
        conn.execute(&stmt(
            "INSERT INTO t (data) VALUES (?1)",
            &[Value::Blob(bytes.clone())],
        ))
        .await
        .unwrap();

        let row = conn
            .query_one(&stmt("SELECT data FROM t", &[]))
            .await
            .unwrap();
        assert_eq!(row.get(0).unwrap().as_bytes(), Some(bytes.as_slice()));
    }

    #[tokio::test]
    async fn float_roundtrip() {
        let conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (v REAL);"))
            .await
            .unwrap();
        conn.execute(&stmt(
            "INSERT INTO t (v) VALUES (?1)",
            &[Value::from(1.234f64)],
        ))
        .await
        .unwrap();

        let row = conn.query_one(&stmt("SELECT v FROM t", &[])).await.unwrap();
        let v = row.get_f64(0).unwrap();
        assert!((v - 1.234).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn bool_roundtrip() {
        let conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (v INTEGER);"))
            .await
            .unwrap();
        conn.execute(&stmt("INSERT INTO t (v) VALUES (?1)", &[Value::from(true)]))
            .await
            .unwrap();

        let row = conn.query_one(&stmt("SELECT v FROM t", &[])).await.unwrap();
        assert_eq!(row.get_i64(0), Some(1));
    }

    #[tokio::test]
    async fn get_by_column_name() {
        let conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);"))
            .await
            .unwrap();
        conn.execute(&stmt(
            "INSERT INTO t (name) VALUES (?1)",
            &[Value::from("Bob")],
        ))
        .await
        .unwrap();

        let row = conn
            .query_one(&stmt("SELECT id, name FROM t", &[]))
            .await
            .unwrap();
        assert_eq!(row.get_by_name("name"), Some(&Value::Text("Bob".into())));
        assert_eq!(row.get_by_name("id"), Some(&Value::Int(1)));
    }

    #[tokio::test]
    async fn query_stream_basic() {
        use tokio_stream::StreamExt;

        let conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);"))
            .await
            .unwrap();

        for i in 1..=10 {
            conn.execute(&stmt(
                "INSERT INTO t (id, name) VALUES (?1, ?2)",
                &[Value::Int(i), Value::from(format!("user{i}"))],
            ))
            .await
            .unwrap();
        }

        let mut stream = conn
            .query_stream(&stmt("SELECT id, name FROM t ORDER BY id", &[]))
            .await
            .unwrap();

        let mut count = 0;
        while let Some(row) = stream.next().await {
            let row = row.unwrap();
            count += 1;
            assert_eq!(row.get_i64(0), Some(count));
        }
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn query_stream_empty() {
        use tokio_stream::StreamExt;

        let conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY);"))
            .await
            .unwrap();

        let mut stream = conn
            .query_stream(&stmt("SELECT * FROM t", &[]))
            .await
            .unwrap();

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn query_stream_matches_query() {
        use tokio_stream::StreamExt;

        let conn = mem_conn().await;
        conn.execute_ddl(&ddl("CREATE TABLE t (v TEXT);"))
            .await
            .unwrap();
        for i in 0..50 {
            conn.execute(&stmt(
                "INSERT INTO t (v) VALUES (?1)",
                &[Value::from(format!("val{i}"))],
            ))
            .await
            .unwrap();
        }

        let buffered = conn.query(&stmt("SELECT v FROM t", &[])).await.unwrap();
        let streamed: Vec<_> = conn
            .query_stream(&stmt("SELECT v FROM t", &[]))
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(buffered.len(), streamed.len());
        for (b, s) in buffered.iter().zip(streamed.iter()) {
            let s = s.as_ref().unwrap();
            assert_eq!(b.get_string(0), s.get_string(0));
        }
    }

    #[tokio::test]
    async fn e2e_schema_to_driver() {
        use quiver_codegen::{SqlDialect, SqlGenerator};
        use quiver_schema::parse;

        let schema = parse(
            r#"
            enum Role { User Admin }
            model Account {
                id    Int32  PRIMARY KEY AUTOINCREMENT
                email Utf8   UNIQUE
                role  Role   DEFAULT User
                score Int32  DEFAULT 0
            }
        "#,
        )
        .unwrap();

        let ddl_sql = SqlGenerator::generate(&schema, SqlDialect::Sqlite).unwrap();
        let conn = mem_conn().await;
        conn.execute_ddl(&DdlStatement::new(ddl_sql)).await.unwrap();

        conn.execute(&stmt(
            "INSERT INTO \"Account\" (email, role) VALUES (?1, ?2)",
            &[Value::from("alice@test.com"), Value::from("Admin")],
        ))
        .await
        .unwrap();

        let row = conn
            .query_one(&stmt(
                "SELECT id, email, role, score FROM \"Account\" WHERE email = ?1",
                &[Value::from("alice@test.com")],
            ))
            .await
            .unwrap();

        assert_eq!(row.get_i64(0), Some(1));
        assert_eq!(row.get_string(1), Some("alice@test.com".into()));
        assert_eq!(row.get_string(2), Some("Admin".into()));
        assert_eq!(row.get_i64(3), Some(0));
    }
}
