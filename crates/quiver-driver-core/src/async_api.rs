//! Database traits for Quiver (async-first, BoxFuture-based).
//!
//! All traits use `BoxFuture` returns instead of RPITIT (`impl Future`).
//! This makes traits work correctly through `Box::pin(async move { ... })`,
//! `Arc<T>`, and other indirection patterns that RPITIT struggles with.
//!
//! The heap allocation cost is negligible for database operations.

use std::future::Future;
use std::pin::Pin;

use crate::{DdlStatement, Row, RowStream, Statement};
use quiver_error::QuiverError;

/// Boxed future type used throughout Quiver's async traits.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ── Core traits ─────────────────────────────────────────────────────

/// A database connection that can execute queries.
pub trait Connection: Send + Sync {
    /// Execute a statement that modifies data (INSERT, UPDATE, DELETE).
    fn execute<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<u64, QuiverError>>;

    /// Execute a query and return all result rows.
    fn query<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<Vec<Row>, QuiverError>>;

    /// Execute a query and return exactly one row, or an error if not found.
    fn query_one<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<Row, QuiverError>> {
        Box::pin(async move {
            let rows = self.query(stmt).await?;
            rows.into_iter()
                .next()
                .ok_or_else(|| QuiverError::Validation("expected one row, got none".into()))
        })
    }

    /// Execute a query and return an optional single row.
    fn query_optional<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<Option<Row>, QuiverError>> {
        Box::pin(async move {
            let rows = self.query(stmt).await?;
            Ok(rows.into_iter().next())
        })
    }

    /// Execute raw DDL (CREATE TABLE, etc.).
    fn execute_ddl<'a>(&'a self, ddl: &'a DdlStatement) -> BoxFuture<'a, Result<(), QuiverError>>;

    /// Execute a query and return results as a stream of rows.
    ///
    /// The default implementation buffers all rows then yields them one
    /// by one. Drivers should override this for true streaming to avoid
    /// loading the entire result set into memory.
    fn query_stream<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<RowStream, QuiverError>> {
        Box::pin(async move {
            let rows = self.query(stmt).await?;
            Ok(RowStream::from_vec(rows))
        })
    }
}

/// A connection that supports transactions.
pub trait Transactional: Connection {
    /// The transaction type returned by `begin`.
    type Transaction<'a>: Transaction + 'a
    where
        Self: 'a;

    /// Begin a new transaction.
    fn begin(&mut self) -> BoxFuture<'_, Result<Self::Transaction<'_>, QuiverError>>;
}

/// An active database transaction.
///
/// Automatically rolls back on drop unless `commit()` is called.
pub trait Transaction: Connection {
    /// Commit this transaction.
    fn commit(self) -> BoxFuture<'static, Result<(), QuiverError>>;

    /// Explicitly roll back this transaction.
    fn rollback(self) -> BoxFuture<'static, Result<(), QuiverError>>;
}

/// Factory for creating database connections.
pub trait Driver: Send + Sync {
    /// The connection type produced by this driver.
    type Conn: Transactional;

    /// Open a connection to the database.
    fn connect<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Self::Conn, QuiverError>>;
}
