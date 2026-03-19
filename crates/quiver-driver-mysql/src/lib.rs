//! MySQL driver for Quiver, delegating to the async ADBC MySQL driver.
//!
//! All database operations go through `adbc_mysql::MysqlConnection`, which
//! uses `mysql_async` internally for native async I/O.
//!
//! For direct Arrow-native ADBC access, use the re-exported `adbc_mysql` crate.

pub mod pool;

pub use adbc_mysql;
pub use pool::MysqlPool;

use adbc::{DatabaseOption, OptionValue};
use quiver_driver_core::{AdbcConnection, AdbcTransaction, BoxFuture, Dialect, Driver, adbc_err};
use quiver_error::QuiverError;

/// MySQL dialect: uses `?` placeholders and splits DDL on `;`.
#[derive(Clone, Default)]
pub struct MysqlDialect;

impl Dialect for MysqlDialect {
    type AdbcConn = adbc_mysql::MysqlConnection;

    fn rewrite_sql(&self, sql: &str) -> String {
        sql.to_owned()
    }

    fn split_ddl(&self) -> bool {
        true
    }
}

/// A MySQL connection.
pub type MysqlConnection = AdbcConnection<MysqlDialect>;

/// An active MySQL transaction.
pub type MysqlTransaction = AdbcTransaction<MysqlDialect>;

/// MySQL driver factory.
pub struct MysqlDriver;

impl Driver for MysqlDriver {
    type Conn = MysqlConnection;

    fn connect<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Self::Conn, QuiverError>> {
        Box::pin(async move {
            let drv = adbc_mysql::MysqlDriver;
            let db = adbc::Driver::new_database_with_opts(
                &drv,
                [(DatabaseOption::Uri, OptionValue::String(url.into()))],
            )
            .await
            .map_err(adbc_err)?;
            let conn = adbc::Database::new_connection(&db)
                .await
                .map_err(adbc_err)?;

            Ok(AdbcConnection::new(conn, MysqlDialect))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quiver_driver_core::{
        Connection, DdlStatement, Statement, Transaction, Transactional, Value,
    };

    fn mysql_url() -> String {
        std::env::var("QUIVER_MYSQL_URL")
            .unwrap_or_else(|_| "mysql://localhost:3306/quiver_test".to_string())
    }

    async fn mysql_conn() -> MysqlConnection {
        MysqlDriver.connect(&mysql_url()).await.unwrap()
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
        let conn = mysql_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_ddl"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_ddl (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT NOT NULL)",
        ))
        .await
        .unwrap();
        conn.execute_ddl(&ddl("DROP TABLE test_ddl")).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn insert_and_query() {
        let conn = mysql_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_iq"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_iq (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)",
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
        let conn = mysql_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_qoo"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl("CREATE TABLE test_qoo (id INT PRIMARY KEY)"))
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
        let mut conn = mysql_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_txc"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_txc (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)",
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
        let mut conn = mysql_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_txr"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_txr (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)",
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
        let conn = mysql_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_null"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_null (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)",
        ))
        .await
        .unwrap();
        // Insert NULL via raw SQL (ADBC does not bind null params)
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
        let conn = mysql_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_float"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl("CREATE TABLE test_float (v DOUBLE)"))
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
    async fn get_by_column_name() {
        let conn = mysql_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_colname"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_colname (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)",
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

        let conn = mysql_conn().await;
        conn.execute_ddl(&ddl("DROP TABLE IF EXISTS test_stream"))
            .await
            .unwrap();
        conn.execute_ddl(&ddl(
            "CREATE TABLE test_stream (id INT PRIMARY KEY, name TEXT)",
        ))
        .await
        .unwrap();

        for i in 1..=10 {
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

        let mut count = 0;
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
}
