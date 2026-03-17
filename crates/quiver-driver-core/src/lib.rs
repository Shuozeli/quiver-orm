//! Database driver abstraction for Quiver (async-first).
//!
//! Provides two layers of abstraction:
//!
//! 1. **Quiver traits** (`Connection`, `Transactional`, `Transaction`, `Driver`) --
//!    Async row-oriented interface for query execution with `Value`/`Row` types.
//!
//! 2. **ADBC traits** (re-exported from `adbc`) -- Arrow-native columnar
//!    interface following the Arrow Database Connectivity specification. Drivers
//!    that implement ADBC traits can interoperate with any ADBC-compatible tool.
//!
//! All Quiver traits are async and use `BoxFuture` for object safety.
//! `dyn Connection` can be used directly for dynamic dispatch.

pub mod arrow;
mod async_api;
mod async_client;
pub mod dialect;
mod generic_conn;
pub mod helpers;
pub mod pool;
mod sanitize;
mod stream;
mod types;

pub use async_api::{BoxFuture, Connection, Driver, Transaction, Transactional};
pub use async_client::{QuiverClient, RetryPolicy};
pub use dialect::Dialect;
pub use generic_conn::{AdbcConnection, AdbcTransaction};
pub use helpers::{adbc_err, params_to_batch};
pub use pool::{Pool, PoolConfig, PoolGuard};
pub use sanitize::sanitize_connection_error;
pub use stream::RowStream;
pub use types::{Column, Row, RowError, Value};

// Re-export adbc so drivers and consumers can use the ADBC interface.
pub use adbc;

/// A parameterized SQL statement produced by the query builder.
///
/// This is the only way to pass queries to the database. Construct via
/// the query builder API (`Query::table(...).find_many().build()`) or
/// via [`Statement::new`] for trusted internal SQL.
#[derive(Debug, Clone)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Value>,
}

impl Statement {
    /// Create a statement from SQL and parameters.
    pub fn new(sql: String, params: Vec<Value>) -> Self {
        Self { sql, params }
    }

    /// Create a statement with no parameters.
    pub fn sql(sql: String) -> Self {
        Self {
            sql,
            params: Vec::new(),
        }
    }
}

/// A DDL statement (CREATE TABLE, DROP, ALTER, etc.).
///
/// Separate from [`Statement`] because DDL has no parameters and no result rows.
/// Produced by the codegen layer (`SqlGenerator`).
#[derive(Debug, Clone)]
pub struct DdlStatement {
    pub sql: String,
}

impl DdlStatement {
    /// Create a DDL statement from raw SQL.
    pub fn new(sql: String) -> Self {
        Self { sql }
    }
}
