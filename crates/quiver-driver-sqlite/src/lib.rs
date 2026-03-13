//! SQLite driver for Quiver, delegating to the async ADBC SQLite driver.
//!
//! All database operations go through `adbc_sqlite::SqliteConnection`, which
//! handles `spawn_blocking` internally for the sync rusqlite layer.
//!
//! For direct Arrow-native ADBC access, use the re-exported `adbc_sqlite` crate.

pub use adbc_sqlite;

pub mod pool;

use std::sync::Arc;

use adbc::{ConnectionOption, DatabaseOption, OptionValue};
use arrow_array::RecordBatch;
use quiver_driver_core::arrow::{record_batch_to_rows, values_to_param_batch};
use quiver_driver_core::{
    Connection, DdlStatement, Driver, Row, RowStream, Statement, Transaction, Transactional,
};
use quiver_error::QuiverError;

pub use pool::SqlitePool;

/// SQLite driver factory.
pub struct SqliteDriver;

impl Driver for SqliteDriver {
    type Conn = SqliteConnection;

    async fn connect(&self, url: &str) -> Result<Self::Conn, QuiverError> {
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

        Ok(SqliteConnection {
            conn: Arc::new(conn),
        })
    }
}

/// A SQLite connection wrapping an ADBC `SqliteConnection`.
pub struct SqliteConnection {
    conn: Arc<adbc_sqlite::SqliteConnection>,
}

impl SqliteConnection {
    /// Create from an existing ADBC SQLite connection.
    pub fn from_adbc(conn: adbc_sqlite::SqliteConnection) -> Self {
        Self {
            conn: Arc::new(conn),
        }
    }
}

// ---------------------------------------------------------------------------
// Connection impl -- delegates to ADBC
// ---------------------------------------------------------------------------

impl Connection for SqliteConnection {
    async fn execute(&self, stmt: &Statement) -> Result<u64, QuiverError> {
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &stmt.sql)
            .await
            .map_err(adbc_err)?;

        if !stmt.params.is_empty() {
            let batch = params_to_batch(&stmt.params)?;
            adbc::Statement::bind(&mut adbc_stmt, batch)
                .await
                .map_err(adbc_err)?;
        }

        let n = adbc::Statement::execute_update(&mut adbc_stmt)
            .await
            .map_err(adbc_err)?;
        Ok(n as u64)
    }

    async fn query(&self, stmt: &Statement) -> Result<Vec<Row>, QuiverError> {
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &stmt.sql)
            .await
            .map_err(adbc_err)?;

        if !stmt.params.is_empty() {
            let batch = params_to_batch(&stmt.params)?;
            adbc::Statement::bind(&mut adbc_stmt, batch)
                .await
                .map_err(adbc_err)?;
        }

        let (reader, _) = adbc::Statement::execute(&mut adbc_stmt)
            .await
            .map_err(adbc_err)?;

        let mut all_rows = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| QuiverError::Driver(e.to_string()))?;
            let rows = record_batch_to_rows(&batch)?;
            all_rows.extend(rows);
        }
        Ok(all_rows)
    }

    async fn execute_ddl(&self, ddl: &DdlStatement) -> Result<(), QuiverError> {
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &ddl.sql)
            .await
            .map_err(adbc_err)?;
        adbc::Statement::execute_update(&mut adbc_stmt)
            .await
            .map_err(adbc_err)?;
        Ok(())
    }

    async fn query_stream(&self, stmt: &Statement) -> Result<RowStream, QuiverError> {
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &stmt.sql)
            .await
            .map_err(adbc_err)?;

        if !stmt.params.is_empty() {
            let batch = params_to_batch(&stmt.params)?;
            adbc::Statement::bind(&mut adbc_stmt, batch)
                .await
                .map_err(adbc_err)?;
        }

        let (reader, _) = adbc::Statement::execute(&mut adbc_stmt)
            .await
            .map_err(adbc_err)?;

        let (tx, rx) = tokio::sync::mpsc::channel(256);

        // The reader is Send, so we can process it in a blocking task
        tokio::task::spawn_blocking(move || {
            for batch_result in reader {
                match batch_result {
                    Ok(batch) => match record_batch_to_rows(&batch) {
                        Ok(rows) => {
                            for row in rows {
                                if tx.blocking_send(Ok(row)).is_err() {
                                    return; // receiver dropped
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(e));
                            return;
                        }
                    },
                    Err(e) => {
                        let _ = tx.blocking_send(Err(QuiverError::Driver(e.to_string())));
                        return;
                    }
                }
            }
        });

        Ok(RowStream::from_receiver(rx))
    }
}

// ---------------------------------------------------------------------------
// Transaction support via ADBC autocommit toggling
// ---------------------------------------------------------------------------

impl Transactional for SqliteConnection {
    type Transaction<'a> = SqliteTransaction;

    async fn begin(&mut self) -> Result<Self::Transaction<'_>, QuiverError> {
        adbc::Connection::set_option(self.conn.as_ref(), ConnectionOption::AutoCommit(false))
            .await
            .map_err(adbc_err)?;

        Ok(SqliteTransaction {
            conn: Arc::clone(&self.conn),
            finished: false,
        })
    }
}

/// An active SQLite transaction.
///
/// Rolls back on drop unless `commit()` is called. Uses the ADBC connection's
/// transaction management (autocommit toggle + commit/rollback).
pub struct SqliteTransaction {
    conn: Arc<adbc_sqlite::SqliteConnection>,
    finished: bool,
}

impl Connection for SqliteTransaction {
    async fn execute(&self, stmt: &Statement) -> Result<u64, QuiverError> {
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &stmt.sql)
            .await
            .map_err(adbc_err)?;

        if !stmt.params.is_empty() {
            let batch = params_to_batch(&stmt.params)?;
            adbc::Statement::bind(&mut adbc_stmt, batch)
                .await
                .map_err(adbc_err)?;
        }

        let n = adbc::Statement::execute_update(&mut adbc_stmt)
            .await
            .map_err(adbc_err)?;
        Ok(n as u64)
    }

    async fn query(&self, stmt: &Statement) -> Result<Vec<Row>, QuiverError> {
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &stmt.sql)
            .await
            .map_err(adbc_err)?;

        if !stmt.params.is_empty() {
            let batch = params_to_batch(&stmt.params)?;
            adbc::Statement::bind(&mut adbc_stmt, batch)
                .await
                .map_err(adbc_err)?;
        }

        let (reader, _) = adbc::Statement::execute(&mut adbc_stmt)
            .await
            .map_err(adbc_err)?;

        let mut all_rows = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| QuiverError::Driver(e.to_string()))?;
            let rows = record_batch_to_rows(&batch)?;
            all_rows.extend(rows);
        }
        Ok(all_rows)
    }

    async fn execute_ddl(&self, ddl: &DdlStatement) -> Result<(), QuiverError> {
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &ddl.sql)
            .await
            .map_err(adbc_err)?;
        adbc::Statement::execute_update(&mut adbc_stmt)
            .await
            .map_err(adbc_err)?;
        Ok(())
    }

    async fn query_stream(&self, stmt: &Statement) -> Result<RowStream, QuiverError> {
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &stmt.sql)
            .await
            .map_err(adbc_err)?;

        if !stmt.params.is_empty() {
            let batch = params_to_batch(&stmt.params)?;
            adbc::Statement::bind(&mut adbc_stmt, batch)
                .await
                .map_err(adbc_err)?;
        }

        let (reader, _) = adbc::Statement::execute(&mut adbc_stmt)
            .await
            .map_err(adbc_err)?;

        let (tx, rx) = tokio::sync::mpsc::channel(256);

        tokio::task::spawn_blocking(move || {
            for batch_result in reader {
                match batch_result {
                    Ok(batch) => match record_batch_to_rows(&batch) {
                        Ok(rows) => {
                            for row in rows {
                                if tx.blocking_send(Ok(row)).is_err() {
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(e));
                            return;
                        }
                    },
                    Err(e) => {
                        let _ = tx.blocking_send(Err(QuiverError::Driver(e.to_string())));
                        return;
                    }
                }
            }
        });

        Ok(RowStream::from_receiver(rx))
    }
}

impl Transaction for SqliteTransaction {
    async fn commit(mut self) -> Result<(), QuiverError> {
        self.finished = true;
        adbc::Connection::commit(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Connection::set_option(self.conn.as_ref(), ConnectionOption::AutoCommit(true))
            .await
            .map_err(adbc_err)?;
        Ok(())
    }

    async fn rollback(mut self) -> Result<(), QuiverError> {
        self.finished = true;
        adbc::Connection::rollback(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Connection::set_option(self.conn.as_ref(), ConnectionOption::AutoCommit(true))
            .await
            .map_err(adbc_err)?;
        Ok(())
    }
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        if !self.finished {
            let conn = Arc::clone(&self.conn);
            let handle = std::thread::spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread().build() {
                    Ok(rt) => rt,
                    Err(e) => {
                        eprintln!("quiver: failed to create rollback runtime: {e}");
                        return;
                    }
                };
                rt.block_on(async {
                    let _ = adbc::Connection::rollback(conn.as_ref()).await;
                    let _ = adbc::Connection::set_option(
                        conn.as_ref(),
                        ConnectionOption::AutoCommit(true),
                    )
                    .await;
                });
            });
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert Quiver `Value` params to a single-row `RecordBatch` for ADBC binding.
fn params_to_batch(params: &[quiver_driver_core::Value]) -> Result<RecordBatch, QuiverError> {
    let owned_names: Vec<String> = (0..params.len()).map(|i| format!("p{i}")).collect();
    let name_refs: Vec<&str> = owned_names.iter().map(|s| s.as_str()).collect();
    values_to_param_batch(params, &name_refs).map_err(|e| QuiverError::Driver(e.to_string()))
}

fn adbc_err(e: adbc::Error) -> QuiverError {
    QuiverError::Driver(quiver_driver_core::sanitize_connection_error(&e.message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quiver_driver_core::Value;

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
