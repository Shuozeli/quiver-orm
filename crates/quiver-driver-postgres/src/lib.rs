//! PostgreSQL driver for Quiver, delegating to the async ADBC PostgreSQL driver.
//!
//! All database operations go through `adbc_postgres::PostgresConnection`, which
//! uses `tokio_postgres` internally for native async I/O.
//!
//! For direct Arrow-native ADBC access, use the re-exported `adbc_postgres` crate.

pub mod pool;

// Re-export the ADBC PostgreSQL driver from our submodule.
pub use adbc_postgres;
pub use pool::PostgresPool;

use std::sync::Arc;

use adbc::{ConnectionOption, DatabaseOption, OptionValue};
use arrow_array::RecordBatch;
use quiver_driver_core::arrow::{record_batch_to_rows, values_to_param_batch};
use quiver_driver_core::{
    Connection, DdlStatement, Driver, Row, RowStream, Statement, Transaction, Transactional,
};
use quiver_error::QuiverError;

/// PostgreSQL driver factory.
pub struct PostgresDriver;

impl Driver for PostgresDriver {
    type Conn = PostgresConnection;

    async fn connect(&self, url: &str) -> Result<Self::Conn, QuiverError> {
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

        Ok(PostgresConnection {
            conn: Arc::new(conn),
        })
    }
}

/// A PostgreSQL connection wrapping an ADBC `PostgresConnection`.
pub struct PostgresConnection {
    conn: Arc<adbc_postgres::PostgresConnection>,
}

impl PostgresConnection {
    /// Create from an existing ADBC PostgreSQL connection.
    pub fn from_adbc(conn: adbc_postgres::PostgresConnection) -> Self {
        Self {
            conn: Arc::new(conn),
        }
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

// ---------------------------------------------------------------------------
// Connection impl -- delegates to ADBC
// ---------------------------------------------------------------------------

impl Connection for PostgresConnection {
    async fn execute(&self, stmt: &Statement) -> Result<u64, QuiverError> {
        let sql = rewrite_placeholders(&stmt.sql);
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &sql)
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
        let sql = rewrite_placeholders(&stmt.sql);
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &sql)
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
        let sql = rewrite_placeholders(&stmt.sql);
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &sql)
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

impl Transactional for PostgresConnection {
    type Transaction<'a> = PostgresTransaction;

    async fn begin(&mut self) -> Result<Self::Transaction<'_>, QuiverError> {
        adbc::Connection::set_option(self.conn.as_ref(), ConnectionOption::AutoCommit(false))
            .await
            .map_err(adbc_err)?;

        Ok(PostgresTransaction {
            conn: Arc::clone(&self.conn),
            finished: false,
        })
    }
}

/// An active PostgreSQL transaction.
///
/// Rolls back on drop unless `commit()` is called. Uses the ADBC connection's
/// transaction management (autocommit toggle + commit/rollback).
pub struct PostgresTransaction {
    conn: Arc<adbc_postgres::PostgresConnection>,
    finished: bool,
}

impl Connection for PostgresTransaction {
    async fn execute(&self, stmt: &Statement) -> Result<u64, QuiverError> {
        let sql = rewrite_placeholders(&stmt.sql);
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &sql)
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
        let sql = rewrite_placeholders(&stmt.sql);
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &sql)
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
        let sql = rewrite_placeholders(&stmt.sql);
        let mut adbc_stmt = adbc::Connection::new_statement(self.conn.as_ref())
            .await
            .map_err(adbc_err)?;
        adbc::Statement::set_sql_query(&mut adbc_stmt, &sql)
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

impl Transaction for PostgresTransaction {
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

impl Drop for PostgresTransaction {
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
