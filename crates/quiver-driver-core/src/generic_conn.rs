//! Generic ADBC-backed connection and transaction.
//!
//! `AdbcConnection<D>` and `AdbcTransaction<D>` provide a single implementation
//! of the Quiver `Connection`, `Transactional`, and `Transaction` traits for any
//! database dialect. Driver crates only need to define a `Dialect` and a `Driver`.

use std::sync::Arc;

use adbc::ConnectionOption;
use quiver_error::QuiverError;

use crate::arrow::record_batch_to_rows;
use crate::dialect::Dialect;
use crate::helpers::{adbc_err, params_to_batch};
use crate::{
    BoxFuture, Connection, DdlStatement, Row, RowStream, Statement, Transaction, Transactional,
};

/// A database connection wrapping an ADBC connection, parameterized by dialect.
pub struct AdbcConnection<D: Dialect> {
    conn: Arc<D::AdbcConn>,
    dialect: D,
}

impl<D: Dialect> AdbcConnection<D> {
    /// Create a new connection from an ADBC connection and dialect.
    pub fn new(conn: D::AdbcConn, dialect: D) -> Self {
        Self {
            conn: Arc::new(conn),
            dialect,
        }
    }

    /// Create from an existing ADBC connection using a default dialect.
    pub fn from_adbc(conn: D::AdbcConn) -> Self
    where
        D: Default,
    {
        Self::new(conn, D::default())
    }
}

// Shared ADBC execution helpers

/// Execute a DML/query-update statement via ADBC, returning affected row count.
async fn adbc_execute_update<C: adbc::Connection>(
    conn: &C,
    sql: &str,
    params: &[crate::Value],
) -> Result<u64, QuiverError> {
    let mut adbc_stmt = adbc::Connection::new_statement(conn)
        .await
        .map_err(adbc_err)?;
    adbc::Statement::set_sql_query(&mut adbc_stmt, sql)
        .await
        .map_err(adbc_err)?;

    if !params.is_empty() {
        let batch = params_to_batch(params)?;
        adbc::Statement::bind(&mut adbc_stmt, batch)
            .await
            .map_err(adbc_err)?;
    }

    let n = adbc::Statement::execute_update(&mut adbc_stmt)
        .await
        .map_err(adbc_err)?;
    Ok(n as u64)
}

/// Execute a query via ADBC, returning all rows.
async fn adbc_query<C: adbc::Connection>(
    conn: &C,
    sql: &str,
    params: &[crate::Value],
) -> Result<Vec<Row>, QuiverError> {
    let mut adbc_stmt = adbc::Connection::new_statement(conn)
        .await
        .map_err(adbc_err)?;
    adbc::Statement::set_sql_query(&mut adbc_stmt, sql)
        .await
        .map_err(adbc_err)?;

    if !params.is_empty() {
        let batch = params_to_batch(params)?;
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

/// Execute a query via ADBC and stream the results.
async fn adbc_query_stream<C: adbc::Connection>(
    conn: &C,
    sql: &str,
    params: &[crate::Value],
) -> Result<RowStream, QuiverError> {
    let mut adbc_stmt = adbc::Connection::new_statement(conn)
        .await
        .map_err(adbc_err)?;
    adbc::Statement::set_sql_query(&mut adbc_stmt, sql)
        .await
        .map_err(adbc_err)?;

    if !params.is_empty() {
        let batch = params_to_batch(params)?;
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

/// Execute DDL, optionally splitting on `;`.
async fn adbc_execute_ddl<C: adbc::Connection>(
    conn: &C,
    ddl: &DdlStatement,
    split: bool,
) -> Result<(), QuiverError> {
    if split {
        for part in ddl.sql.split(';') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            let mut adbc_stmt = adbc::Connection::new_statement(conn)
                .await
                .map_err(adbc_err)?;
            adbc::Statement::set_sql_query(&mut adbc_stmt, trimmed)
                .await
                .map_err(adbc_err)?;
            adbc::Statement::execute_update(&mut adbc_stmt)
                .await
                .map_err(adbc_err)?;
        }
    } else {
        let mut adbc_stmt = adbc::Connection::new_statement(conn)
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &ddl.sql)
            .await
            .map_err(adbc_err)?;
        adbc::Statement::execute_update(&mut adbc_stmt)
            .await
            .map_err(adbc_err)?;
    }
    Ok(())
}

impl<D: Dialect> Connection for AdbcConnection<D> {
    fn execute<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<u64, QuiverError>> {
        Box::pin(async move {
            let sql = self.dialect.rewrite_sql(&stmt.sql);
            adbc_execute_update(self.conn.as_ref(), &sql, &stmt.params).await
        })
    }

    fn query<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<Vec<Row>, QuiverError>> {
        Box::pin(async move {
            let sql = self.dialect.rewrite_sql(&stmt.sql);
            adbc_query(self.conn.as_ref(), &sql, &stmt.params).await
        })
    }

    fn execute_ddl<'a>(&'a self, ddl: &'a DdlStatement) -> BoxFuture<'a, Result<(), QuiverError>> {
        Box::pin(async move {
            adbc_execute_ddl(self.conn.as_ref(), ddl, self.dialect.split_ddl()).await
        })
    }

    fn query_stream<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<RowStream, QuiverError>> {
        Box::pin(async move {
            let sql = self.dialect.rewrite_sql(&stmt.sql);
            adbc_query_stream(self.conn.as_ref(), &sql, &stmt.params).await
        })
    }
}

impl<D: Dialect> Transactional for AdbcConnection<D> {
    type Transaction<'a> = AdbcTransaction<D>;

    fn begin(&mut self) -> BoxFuture<'_, Result<Self::Transaction<'_>, QuiverError>> {
        Box::pin(async {
            adbc::Connection::set_option(self.conn.as_ref(), ConnectionOption::AutoCommit(false))
                .await
                .map_err(adbc_err)?;

            Ok(AdbcTransaction {
                conn: Arc::clone(&self.conn),
                dialect: self.dialect.clone(),
                finished: false,
            })
        })
    }
}

/// An active database transaction over an ADBC connection.
///
/// Rolls back on drop unless `commit()` is called. Uses the ADBC connection's
/// transaction management (autocommit toggle + commit/rollback).
pub struct AdbcTransaction<D: Dialect> {
    conn: Arc<D::AdbcConn>,
    dialect: D,
    finished: bool,
}

impl<D: Dialect> Connection for AdbcTransaction<D> {
    fn execute<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<u64, QuiverError>> {
        Box::pin(async move {
            let sql = self.dialect.rewrite_sql(&stmt.sql);
            adbc_execute_update(self.conn.as_ref(), &sql, &stmt.params).await
        })
    }

    fn query<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<Vec<Row>, QuiverError>> {
        Box::pin(async move {
            let sql = self.dialect.rewrite_sql(&stmt.sql);
            adbc_query(self.conn.as_ref(), &sql, &stmt.params).await
        })
    }

    fn execute_ddl<'a>(&'a self, ddl: &'a DdlStatement) -> BoxFuture<'a, Result<(), QuiverError>> {
        Box::pin(async move {
            adbc_execute_ddl(self.conn.as_ref(), ddl, self.dialect.split_ddl()).await
        })
    }

    fn query_stream<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<RowStream, QuiverError>> {
        Box::pin(async move {
            let sql = self.dialect.rewrite_sql(&stmt.sql);
            adbc_query_stream(self.conn.as_ref(), &sql, &stmt.params).await
        })
    }
}

impl<D: Dialect> Transaction for AdbcTransaction<D> {
    fn commit(mut self) -> BoxFuture<'static, Result<(), QuiverError>> {
        Box::pin(async move {
            self.finished = true;
            adbc::Connection::commit(self.conn.as_ref())
                .await
                .map_err(adbc_err)?;
            adbc::Connection::set_option(self.conn.as_ref(), ConnectionOption::AutoCommit(true))
                .await
                .map_err(adbc_err)?;
            Ok(())
        })
    }

    fn rollback(mut self) -> BoxFuture<'static, Result<(), QuiverError>> {
        Box::pin(async move {
            self.finished = true;
            adbc::Connection::rollback(self.conn.as_ref())
                .await
                .map_err(adbc_err)?;
            adbc::Connection::set_option(self.conn.as_ref(), ConnectionOption::AutoCommit(true))
                .await
                .map_err(adbc_err)?;
            Ok(())
        })
    }
}

impl<D: Dialect> Drop for AdbcTransaction<D> {
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
