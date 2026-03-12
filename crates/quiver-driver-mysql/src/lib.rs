//! MySQL driver for Quiver, delegating to the async ADBC MySQL driver.
//!
//! All database operations go through `adbc_mysql::MysqlConnection`, which
//! uses `mysql_async` internally for native async I/O.
//!
//! For direct Arrow-native ADBC access, use the re-exported `adbc_mysql` crate.

pub mod pool;

// Re-export the ADBC MySQL driver from our submodule.
pub use adbc_mysql;
pub use pool::MysqlPool;

use std::sync::Arc;

use adbc::{ConnectionOption, DatabaseOption, OptionValue};
use arrow_array::RecordBatch;
use quiver_driver_core::arrow::{record_batch_to_rows, values_to_param_batch};
use quiver_driver_core::{
    Connection, DdlStatement, Driver, Row, RowStream, Statement, Transaction, Transactional,
};
use quiver_error::QuiverError;

/// MySQL driver factory.
pub struct MysqlDriver;

impl Driver for MysqlDriver {
    type Conn = MysqlConnection;

    async fn connect(&self, url: &str) -> Result<Self::Conn, QuiverError> {
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

        Ok(MysqlConnection {
            conn: Arc::new(conn),
        })
    }
}

/// A MySQL connection wrapping an ADBC `MysqlConnection`.
pub struct MysqlConnection {
    conn: Arc<adbc_mysql::MysqlConnection>,
}

impl MysqlConnection {
    /// Create from an existing ADBC MySQL connection.
    pub fn from_adbc(conn: adbc_mysql::MysqlConnection) -> Self {
        Self {
            conn: Arc::new(conn),
        }
    }
}

// ---------------------------------------------------------------------------
// Connection impl -- delegates to ADBC
// ---------------------------------------------------------------------------

impl Connection for MysqlConnection {
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
        // Split on `;` for multi-statement DDL, executing each via ADBC.
        for part in ddl.sql.split(';') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
                .await
                .map_err(adbc_err)?;
            adbc::Statement::set_sql_query(&mut adbc_stmt, trimmed)
                .await
                .map_err(adbc_err)?;
            adbc::Statement::execute_update(&mut adbc_stmt)
                .await
                .map_err(adbc_err)?;
        }
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

// ---------------------------------------------------------------------------
// Transaction support via ADBC autocommit toggling
// ---------------------------------------------------------------------------

impl Transactional for MysqlConnection {
    type Transaction<'a> = MysqlTransaction;

    async fn begin(&mut self) -> Result<Self::Transaction<'_>, QuiverError> {
        adbc::Connection::set_option(self.conn.as_ref(), ConnectionOption::AutoCommit(false))
            .await
            .map_err(adbc_err)?;

        Ok(MysqlTransaction {
            conn: Arc::clone(&self.conn),
            finished: false,
        })
    }
}

/// An active MySQL transaction.
///
/// Rolls back on drop unless `commit()` is called. Uses the ADBC connection's
/// transaction management (autocommit toggle + commit/rollback).
pub struct MysqlTransaction {
    conn: Arc<adbc_mysql::MysqlConnection>,
    finished: bool,
}

impl Connection for MysqlTransaction {
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
        for part in ddl.sql.split(';') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
                .await
                .map_err(adbc_err)?;
            adbc::Statement::set_sql_query(&mut adbc_stmt, trimmed)
                .await
                .map_err(adbc_err)?;
            adbc::Statement::execute_update(&mut adbc_stmt)
                .await
                .map_err(adbc_err)?;
        }
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

impl Transaction for MysqlTransaction {
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

impl Drop for MysqlTransaction {
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
