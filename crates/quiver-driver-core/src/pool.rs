//! Connection pool abstraction.
//!
//! Provides a [`Pool`] trait for managing reusable database connections
//! and a [`PoolGuard`] that returns connections to the pool on drop.

use crate::{
    BoxFuture, Connection, DdlStatement, Driver, Row, RowStream, Statement, Transactional,
};
use quiver_error::QuiverError;

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
    fn acquire(&self) -> BoxFuture<'_, Result<PoolGuard<Self::Conn>, QuiverError>>;

    /// Return the number of idle (available) connections.
    fn idle_count(&self) -> usize;

    /// Return the maximum pool size.
    fn max_size(&self) -> usize;
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
    fn execute<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<u64, QuiverError>> {
        self.inner().execute(stmt)
    }

    fn query<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<Vec<Row>, QuiverError>> {
        self.inner().query(stmt)
    }

    fn query_one<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<Row, QuiverError>> {
        self.inner().query_one(stmt)
    }

    fn query_optional<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<Option<Row>, QuiverError>> {
        self.inner().query_optional(stmt)
    }

    fn execute_ddl<'a>(&'a self, ddl: &'a DdlStatement) -> BoxFuture<'a, Result<(), QuiverError>> {
        self.inner().execute_ddl(ddl)
    }

    fn query_stream<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> BoxFuture<'a, Result<RowStream, QuiverError>> {
        self.inner().query_stream(stmt)
    }
}

impl<C: Transactional + 'static> Transactional for PoolGuard<C> {
    type Transaction<'a>
        = C::Transaction<'a>
    where
        Self: 'a;

    fn begin(&mut self) -> BoxFuture<'_, Result<Self::Transaction<'_>, QuiverError>> {
        Box::pin(async {
            self.conn
                .as_mut()
                .expect("PoolGuard used after drop")
                .begin()
                .await
        })
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

// ---------------------------------------------------------------------------
// Generic pool backed by a Driver
// ---------------------------------------------------------------------------

/// A generic connection pool that eagerly creates connections using a [`Driver`].
///
/// This eliminates the need for each driver crate to copy-paste pool logic.
/// Driver-specific pool types (e.g. `SqlitePool`) can wrap this and delegate.
pub struct DriverPool<D: Driver> {
    rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<D::Conn>>,
    tx: tokio::sync::mpsc::Sender<D::Conn>,
    max_size: usize,
}

impl<D: Driver> DriverPool<D> {
    /// Create a new pool, eagerly opening `config.max_connections` connections.
    pub async fn new(config: PoolConfig, driver: D) -> Result<Self, QuiverError> {
        let (tx, rx) = tokio::sync::mpsc::channel(config.max_connections);

        for _ in 0..config.max_connections {
            let conn = driver.connect(&config.url).await?;
            tx.send(conn)
                .await
                .map_err(|_| QuiverError::Driver("failed to initialize pool".into()))?;
        }

        Ok(Self {
            rx: tokio::sync::Mutex::new(rx),
            tx,
            max_size: config.max_connections,
        })
    }

    /// Acquire a connection from the pool.
    pub async fn acquire(&self) -> Result<PoolGuard<D::Conn>, QuiverError> {
        let mut rx = self.rx.lock().await;
        let conn = rx
            .recv()
            .await
            .ok_or_else(|| QuiverError::Driver("pool closed".into()))?;
        Ok(PoolGuard::new(conn, self.tx.clone()))
    }

    /// Return the number of idle (available) connections.
    pub fn idle_count(&self) -> usize {
        self.tx.capacity() - (self.tx.max_capacity() - self.max_size)
    }

    /// Return the maximum pool size.
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}
