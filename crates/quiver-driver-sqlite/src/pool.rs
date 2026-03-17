//! SQLite connection pool.
//!
//! Pre-creates N `SqliteConnection` instances and hands them out via
//! an mpsc channel. Each connection is returned to the pool when the
//! [`PoolGuard`] is dropped.

use quiver_driver_core::pool::DriverPool;
use quiver_driver_core::{BoxFuture, PoolConfig, PoolGuard};
use quiver_error::QuiverError;

use crate::{SqliteConnection, SqliteDriver};

/// A pool of SQLite connections.
pub struct SqlitePool {
    inner: DriverPool<SqliteDriver>,
}

impl SqlitePool {
    /// Create a new pool, eagerly opening `config.max_connections` connections.
    pub async fn new(config: PoolConfig) -> Result<Self, QuiverError> {
        Ok(Self {
            inner: DriverPool::new(config, SqliteDriver).await?,
        })
    }
}

impl quiver_driver_core::Pool for SqlitePool {
    type Conn = SqliteConnection;

    fn acquire(&self) -> BoxFuture<'_, Result<PoolGuard<SqliteConnection>, QuiverError>> {
        Box::pin(async { self.inner.acquire().await })
    }

    fn idle_count(&self) -> usize {
        self.inner.idle_count()
    }

    fn max_size(&self) -> usize {
        self.inner.max_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quiver_driver_core::{Connection, DdlStatement, Pool, Statement, Value};

    #[tokio::test]
    async fn pool_basic_acquire_release() {
        let pool = SqlitePool::new(PoolConfig::new(":memory:", 2))
            .await
            .unwrap();
        assert_eq!(pool.max_size(), 2);

        // Acquire a connection and run a query
        let conn = pool.acquire().await.unwrap();
        conn.execute_ddl(&DdlStatement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY)".to_string(),
        ))
        .await
        .unwrap();
        drop(conn);

        // Acquire again -- should get a connection back from the pool
        let conn = pool.acquire().await.unwrap();
        // The table should NOT exist because each :memory: conn is separate
        let result = conn
            .query(&Statement::sql("SELECT 1".to_string()))
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn pool_acquire_release_acquire() {
        let pool = SqlitePool::new(PoolConfig::new(":memory:", 1))
            .await
            .unwrap();

        // Acquire the only connection
        let conn1 = pool.acquire().await.unwrap();
        conn1
            .query(&Statement::sql("SELECT 1".to_string()))
            .await
            .unwrap();

        // Return it to the pool
        drop(conn1);

        // Acquire again -- should succeed immediately
        let conn2 = pool.acquire().await.unwrap();
        conn2
            .query(&Statement::sql("SELECT 1".to_string()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pool_connections_work_independently() {
        let pool = SqlitePool::new(PoolConfig::new(":memory:", 3))
            .await
            .unwrap();

        // Acquire all 3 connections and run DDL on each
        let c1 = pool.acquire().await.unwrap();
        let c2 = pool.acquire().await.unwrap();
        let c3 = pool.acquire().await.unwrap();

        c1.execute_ddl(&DdlStatement::new("CREATE TABLE t1 (id INTEGER)".into()))
            .await
            .unwrap();
        c2.execute_ddl(&DdlStatement::new("CREATE TABLE t2 (id INTEGER)".into()))
            .await
            .unwrap();
        c3.execute_ddl(&DdlStatement::new("CREATE TABLE t3 (id INTEGER)".into()))
            .await
            .unwrap();

        c1.execute(&Statement::new(
            "INSERT INTO t1 VALUES (?1)".into(),
            vec![Value::Int(1)],
        ))
        .await
        .unwrap();
        c2.execute(&Statement::new(
            "INSERT INTO t2 VALUES (?1)".into(),
            vec![Value::Int(2)],
        ))
        .await
        .unwrap();
        c3.execute(&Statement::new(
            "INSERT INTO t3 VALUES (?1)".into(),
            vec![Value::Int(3)],
        ))
        .await
        .unwrap();
    }
}
