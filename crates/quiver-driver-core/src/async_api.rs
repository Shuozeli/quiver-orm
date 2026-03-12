//! Database traits for Quiver (async-first).
//!
//! All database operations are async. Uses native `async fn` in traits
//! (Rust 1.85+ RPITIT) -- no `async-trait` crate needed.
//!
//! All methods return `impl Future + Send` to enable both static dispatch
//! and the `DynConnection` blanket impl for dyn dispatch.

use std::future::Future;
use std::pin::Pin;

use crate::{DdlStatement, Row, RowStream, Statement};
use quiver_error::QuiverError;

// ── Static-dispatch traits ──────────────────────────────────────────

/// A database connection that can execute queries.
pub trait Connection: Send + Sync {
    /// Execute a statement that modifies data (INSERT, UPDATE, DELETE).
    fn execute(&self, stmt: &Statement) -> impl Future<Output = Result<u64, QuiverError>> + Send;

    /// Execute a query and return all result rows.
    fn query(&self, stmt: &Statement)
    -> impl Future<Output = Result<Vec<Row>, QuiverError>> + Send;

    /// Execute a query and return exactly one row, or an error if not found.
    fn query_one(&self, stmt: &Statement) -> impl Future<Output = Result<Row, QuiverError>> + Send {
        async {
            let rows = self.query(stmt).await?;
            rows.into_iter()
                .next()
                .ok_or_else(|| QuiverError::Validation("expected one row, got none".into()))
        }
    }

    /// Execute a query and return an optional single row.
    fn query_optional(
        &self,
        stmt: &Statement,
    ) -> impl Future<Output = Result<Option<Row>, QuiverError>> + Send {
        async {
            let rows = self.query(stmt).await?;
            Ok(rows.into_iter().next())
        }
    }

    /// Execute raw DDL (CREATE TABLE, etc.).
    fn execute_ddl(
        &self,
        ddl: &DdlStatement,
    ) -> impl Future<Output = Result<(), QuiverError>> + Send;

    /// Execute a query and return results as a stream of rows.
    ///
    /// The default implementation buffers all rows then yields them one
    /// by one. Drivers should override this for true streaming to avoid
    /// loading the entire result set into memory.
    fn query_stream(
        &self,
        stmt: &Statement,
    ) -> impl Future<Output = Result<RowStream, QuiverError>> + Send {
        async {
            let rows = self.query(stmt).await?;
            Ok(RowStream::from_vec(rows))
        }
    }
}

/// A connection that supports transactions.
pub trait Transactional: Connection {
    /// The transaction type returned by `begin`.
    type Transaction<'a>: Transaction + 'a
    where
        Self: 'a;

    /// Begin a new transaction.
    fn begin(&mut self) -> impl Future<Output = Result<Self::Transaction<'_>, QuiverError>> + Send;
}

/// An active database transaction.
///
/// Automatically rolls back on drop unless `commit()` is called.
pub trait Transaction: Connection {
    /// Commit this transaction.
    fn commit(self) -> impl Future<Output = Result<(), QuiverError>> + Send;

    /// Explicitly roll back this transaction.
    fn rollback(self) -> impl Future<Output = Result<(), QuiverError>> + Send;
}

/// Factory for creating database connections.
pub trait Driver: Send + Sync {
    /// The connection type produced by this driver.
    type Conn: Transactional;

    /// Open a connection to the database.
    fn connect(&self, url: &str) -> impl Future<Output = Result<Self::Conn, QuiverError>> + Send;
}

// ── Dyn-dispatch traits (boxed futures) ─────────────────────────────

/// A type alias for boxed futures used in dyn-dispatch traits.
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Object-safe version of `Connection` using boxed futures.
///
/// Native RPITIT in traits is not dyn-safe. This trait provides
/// boxed-future wrappers for use with `&dyn DynConnection`.
/// A blanket impl is provided so every `Connection` is automatically
/// a `DynConnection`.
pub trait DynConnection: Send + Sync {
    /// Execute a statement that modifies data.
    fn dyn_execute<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<u64, QuiverError>>;

    /// Execute a query and return all result rows.
    fn dyn_query<'a>(&'a self, stmt: &'a Statement)
    -> BoxFuture<'a, Result<Vec<Row>, QuiverError>>;

    /// Execute a query and return exactly one row.
    fn dyn_query_one<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<Row, QuiverError>>;

    /// Execute a query and return an optional single row.
    fn dyn_query_optional<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<Option<Row>, QuiverError>>;

    /// Execute raw DDL.
    fn dyn_execute_ddl<'a>(
        &'a self,
        ddl: &'a DdlStatement,
    ) -> BoxFuture<'a, Result<(), QuiverError>>;

    /// Execute a query and return results as a stream.
    fn dyn_query_stream<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<RowStream, QuiverError>>;
}

/// Blanket impl: every `Connection` is automatically a `DynConnection`.
impl<T: Connection> DynConnection for T {
    fn dyn_execute<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<u64, QuiverError>> {
        Box::pin(self.execute(stmt))
    }

    fn dyn_query<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<Vec<Row>, QuiverError>> {
        Box::pin(self.query(stmt))
    }

    fn dyn_query_one<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<Row, QuiverError>> {
        Box::pin(self.query_one(stmt))
    }

    fn dyn_query_optional<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<Option<Row>, QuiverError>> {
        Box::pin(self.query_optional(stmt))
    }

    fn dyn_execute_ddl<'a>(
        &'a self,
        ddl: &'a DdlStatement,
    ) -> BoxFuture<'a, Result<(), QuiverError>> {
        Box::pin(self.execute_ddl(ddl))
    }

    fn dyn_query_stream<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<RowStream, QuiverError>> {
        Box::pin(self.query_stream(stmt))
    }
}

/// `DynConnection` delegated through `Box<dyn DynConnection>`.
impl DynConnection for Box<dyn DynConnection> {
    fn dyn_execute<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<u64, QuiverError>> {
        (**self).dyn_execute(stmt)
    }

    fn dyn_query<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<Vec<Row>, QuiverError>> {
        (**self).dyn_query(stmt)
    }

    fn dyn_query_one<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<Row, QuiverError>> {
        (**self).dyn_query_one(stmt)
    }

    fn dyn_query_optional<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<Option<Row>, QuiverError>> {
        (**self).dyn_query_optional(stmt)
    }

    fn dyn_execute_ddl<'a>(
        &'a self,
        ddl: &'a DdlStatement,
    ) -> BoxFuture<'a, Result<(), QuiverError>> {
        (**self).dyn_execute_ddl(ddl)
    }

    fn dyn_query_stream<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<RowStream, QuiverError>> {
        (**self).dyn_query_stream(stmt)
    }
}
