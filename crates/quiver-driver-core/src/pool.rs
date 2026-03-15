//! Connection pool abstraction.
//!
//! Provides a [`Pool`] trait for managing reusable database connections
//! and a [`PoolGuard`] that returns connections to the pool on drop.

use std::future::Future;
use std::pin::Pin;

use crate::{Connection, DdlStatement, DynConnection, Row, RowStream, Statement, Transactional};
use quiver_error::QuiverError;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Configuration for a connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Database URL.
    pub url: String,
    /// Maximum number of connections in the pool.
    pub max_connections: usize,
}

impl PoolConfig {
    pub fn new(url: impl Into<String>, max_connections: usize) -> Self {
        Self {
            url: url.into(),
            max_connections,
        }
    }
}

/// A connection pool that vends connections implementing [`Connection`].
///
/// Acquired connections are wrapped in a [`PoolGuard`] which returns the
/// connection to the pool when dropped.
pub trait Pool: Send + Sync {
    /// The connection type managed by this pool.
    type Conn: Connection;

    /// Acquire a connection from the pool.
    ///
    /// Waits asynchronously until a connection is available if the pool
    /// is exhausted.
    fn acquire(&self) -> impl Future<Output = Result<PoolGuard<Self::Conn>, QuiverError>> + Send;

    /// Return the number of idle (available) connections.
    fn idle_count(&self) -> usize;

    /// Return the maximum pool size.
    fn max_size(&self) -> usize;
}

/// Object-safe version of [`Pool`] that returns `Box<dyn DynConnection>`.
pub trait DynPool: Send + Sync {
    /// Acquire a connection as a boxed `DynConnection`.
    fn dyn_acquire(&self) -> BoxFuture<'_, Result<Box<dyn DynConnection>, QuiverError>>;

    /// Return the number of idle connections.
    fn idle_count(&self) -> usize;

    /// Return the maximum pool size.
    fn max_size(&self) -> usize;
}

/// Blanket impl: every `Pool` whose connections implement `Connection + 'static`
/// is automatically a `DynPool`.
impl<P> DynPool for P
where
    P: Pool,
    P::Conn: 'static,
{
    fn dyn_acquire(&self) -> BoxFuture<'_, Result<Box<dyn DynConnection>, QuiverError>> {
        Box::pin(async {
            let guard = self.acquire().await?;
            Ok(Box::new(guard) as Box<dyn DynConnection>)
        })
    }

    fn idle_count(&self) -> usize {
        Pool::idle_count(self)
    }

    fn max_size(&self) -> usize {
        Pool::max_size(self)
    }
}

/// A guard that returns its connection to the pool when dropped.
///
/// Implements [`Connection`] by delegating to the inner connection.
/// When the guard is dropped, the connection is sent back through the
/// channel for reuse.
pub struct PoolGuard<C: Connection> {
    conn: Option<C>,
    return_tx: tokio::sync::mpsc::Sender<C>,
}

impl<C: Connection> PoolGuard<C> {
    /// Create a new pool guard.
    pub fn new(conn: C, return_tx: tokio::sync::mpsc::Sender<C>) -> Self {
        Self {
            conn: Some(conn),
            return_tx,
        }
    }

    fn inner(&self) -> &C {
        self.conn.as_ref().expect("PoolGuard used after drop")
    }
}

impl<C: Connection + 'static> Connection for PoolGuard<C> {
    fn execute(&self, stmt: &Statement) -> impl Future<Output = Result<u64, QuiverError>> + Send {
        self.inner().execute(stmt)
    }

    fn query(
        &self,
        stmt: &Statement,
    ) -> impl Future<Output = Result<Vec<Row>, QuiverError>> + Send {
        self.inner().query(stmt)
    }

    fn query_one(&self, stmt: &Statement) -> impl Future<Output = Result<Row, QuiverError>> + Send {
        self.inner().query_one(stmt)
    }

    fn query_optional(
        &self,
        stmt: &Statement,
    ) -> impl Future<Output = Result<Option<Row>, QuiverError>> + Send {
        self.inner().query_optional(stmt)
    }

    fn execute_ddl(
        &self,
        ddl: &DdlStatement,
    ) -> impl Future<Output = Result<(), QuiverError>> + Send {
        self.inner().execute_ddl(ddl)
    }

    fn query_stream(
        &self,
        stmt: &Statement,
    ) -> impl Future<Output = Result<RowStream, QuiverError>> + Send {
        self.inner().query_stream(stmt)
    }
}

impl<C: Transactional + 'static> Transactional for PoolGuard<C> {
    type Transaction<'a>
        = C::Transaction<'a>
    where
        Self: 'a;

    async fn begin(&mut self) -> Result<Self::Transaction<'_>, QuiverError> {
        self.conn
            .as_mut()
            .expect("PoolGuard used after drop")
            .begin()
            .await
    }
}

impl<C: Connection> Drop for PoolGuard<C> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Best-effort return to pool. If the pool is shut down
            // (receiver dropped), the connection is simply dropped.
            let _ = self.return_tx.try_send(conn);
        }
    }
}
