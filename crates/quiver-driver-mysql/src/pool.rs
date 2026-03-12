//! MySQL connection pool.

use quiver_driver_core::{Pool, PoolConfig, PoolGuard};
use quiver_error::QuiverError;

use crate::{MysqlConnection, MysqlDriver};

/// A pool of MySQL connections.
pub struct MysqlPool {
    rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<MysqlConnection>>,
    tx: tokio::sync::mpsc::Sender<MysqlConnection>,
    max_size: usize,
}

impl MysqlPool {
    /// Create a new pool, eagerly opening `config.max_connections` connections.
    pub async fn new(config: PoolConfig) -> Result<Self, QuiverError> {
        use quiver_driver_core::Driver;

        let (tx, rx) = tokio::sync::mpsc::channel(config.max_connections);

        for _ in 0..config.max_connections {
            let conn = MysqlDriver.connect(&config.url).await?;
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
}

impl Pool for MysqlPool {
    type Conn = MysqlConnection;

    async fn acquire(&self) -> Result<PoolGuard<MysqlConnection>, QuiverError> {
        let mut rx = self.rx.lock().await;
        let conn = rx
            .recv()
            .await
            .ok_or_else(|| QuiverError::Driver("pool closed".into()))?;
        Ok(PoolGuard::new(conn, self.tx.clone()))
    }

    fn idle_count(&self) -> usize {
        self.tx.capacity() - (self.tx.max_capacity() - self.max_size)
    }

    fn max_size(&self) -> usize {
        self.max_size
    }
}
