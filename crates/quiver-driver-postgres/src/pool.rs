//! PostgreSQL connection pool.

use quiver_driver_core::{Pool, PoolConfig, PoolGuard};
use quiver_error::QuiverError;

use crate::{PostgresConnection, PostgresDriver};

/// A pool of PostgreSQL connections.
pub struct PostgresPool {
    rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<PostgresConnection>>,
    tx: tokio::sync::mpsc::Sender<PostgresConnection>,
    max_size: usize,
}

impl PostgresPool {
    /// Create a new pool, eagerly opening `config.max_connections` connections.
    pub async fn new(config: PoolConfig) -> Result<Self, QuiverError> {
        use quiver_driver_core::Driver;

        let (tx, rx) = tokio::sync::mpsc::channel(config.max_connections);

        for _ in 0..config.max_connections {
            let conn = PostgresDriver.connect(&config.url).await?;
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

impl Pool for PostgresPool {
    type Conn = PostgresConnection;

    async fn acquire(&self) -> Result<PoolGuard<PostgresConnection>, QuiverError> {
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
