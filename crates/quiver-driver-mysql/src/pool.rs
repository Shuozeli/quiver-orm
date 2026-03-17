//! MySQL connection pool.

use quiver_driver_core::pool::DriverPool;
use quiver_driver_core::{BoxFuture, PoolConfig, PoolGuard};
use quiver_error::QuiverError;

use crate::{MysqlConnection, MysqlDriver};

/// A pool of MySQL connections.
pub struct MysqlPool {
    inner: DriverPool<MysqlDriver>,
}

impl MysqlPool {
    /// Create a new pool, eagerly opening `config.max_connections` connections.
    pub async fn new(config: PoolConfig) -> Result<Self, QuiverError> {
        Ok(Self {
            inner: DriverPool::new(config, MysqlDriver).await?,
        })
    }
}

impl quiver_driver_core::Pool for MysqlPool {
    type Conn = MysqlConnection;

    fn acquire(&self) -> BoxFuture<'_, Result<PoolGuard<MysqlConnection>, QuiverError>> {
        Box::pin(async { self.inner.acquire().await })
    }

    fn idle_count(&self) -> usize {
        self.inner.idle_count()
    }

    fn max_size(&self) -> usize {
        self.inner.max_size()
    }
}
