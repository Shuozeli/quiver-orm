//! PostgreSQL connection pool.

use quiver_driver_core::pool::DriverPool;
use quiver_driver_core::{BoxFuture, PoolConfig, PoolGuard};
use quiver_error::QuiverError;

use crate::{PostgresConnection, PostgresDriver};

/// A pool of PostgreSQL connections.
pub struct PostgresPool {
    inner: DriverPool<PostgresDriver>,
}

impl PostgresPool {
    /// Create a new pool, eagerly opening `config.max_connections` connections.
    pub async fn new(config: PoolConfig) -> Result<Self, QuiverError> {
        Ok(Self {
            inner: DriverPool::new(config, PostgresDriver).await?,
        })
    }
}

impl quiver_driver_core::Pool for PostgresPool {
    type Conn = PostgresConnection;

    fn acquire(&self) -> BoxFuture<'_, Result<PoolGuard<PostgresConnection>, QuiverError>> {
        Box::pin(async { self.inner.acquire().await })
    }

    fn idle_count(&self) -> usize {
        self.inner.idle_count()
    }

    fn max_size(&self) -> usize {
        self.inner.max_size()
    }
}
