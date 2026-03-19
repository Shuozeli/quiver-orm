//! `QuiverClient` -- the user-facing database client.
//!
//! Enforces that all data operations (reads and writes) happen within an
//! explicit transaction. Only DDL (schema changes) can run outside a
//! transaction.
//!
//! This is intentional: Quiver is a biased ORM that prevents error-prone
//! database usage patterns. Bare queries without transactions are not
//! exposed through this API.

use std::time::Duration;

use crate::{BoxFuture, DdlStatement, Transaction, Transactional};
use quiver_error::QuiverError;

/// A database client that enforces transactional access.
///
/// All data operations (queries, inserts, updates, deletes) must go through
/// [`transaction()`](QuiverClient::transaction) or
/// [`transaction_with_retry()`](QuiverClient::transaction_with_retry).
///
/// Only DDL statements (CREATE TABLE, ALTER, DROP) can run outside a
/// transaction via [`execute_ddl()`](QuiverClient::execute_ddl).
pub struct QuiverClient<C: Transactional> {
    conn: C,
}

impl<C: Transactional> QuiverClient<C> {
    /// Wrap an existing connection in a `QuiverClient`.
    pub fn new(conn: C) -> Self {
        Self { conn }
    }

    /// Execute raw DDL (CREATE TABLE, ALTER, DROP, etc.).
    ///
    /// This is the only operation allowed outside a transaction, because
    /// many databases do not support transactional DDL.
    pub async fn execute_ddl(&self, ddl: &DdlStatement) -> Result<(), QuiverError> {
        self.conn.execute_ddl(ddl).await
    }

    /// Execute a closure within a transaction.
    ///
    /// The closure receives a `&Transaction` which implements `Connection`,
    /// giving access to `execute()`, `query()`, `query_one()`, etc.
    ///
    /// If the closure returns `Ok`, the transaction is committed.
    /// If the closure returns `Err`, the transaction is rolled back.
    pub async fn transaction<F, T>(&mut self, f: F) -> Result<T, QuiverError>
    where
        F: for<'a> FnOnce(&'a C::Transaction<'_>) -> BoxFuture<'a, Result<T, QuiverError>>,
        T: Send,
    {
        let tx = self.conn.begin().await?;
        match f(&tx).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(e) => {
                if let Err(rb_err) = tx.rollback().await {
                    return Err(QuiverError::Driver(format!(
                        "rollback failed: {rb_err} (original error: {e})"
                    )));
                }
                Err(e)
            }
        }
    }

    /// Execute a closure within a transaction, retrying on transient failures.
    ///
    /// Retries on errors where [`QuiverError::is_retryable()`] returns true
    /// (serialization failures, deadlocks, lock timeouts). Non-retryable
    /// errors are returned immediately.
    ///
    /// The closure must implement `Fn` (not just `FnOnce`) because it may
    /// be called multiple times across retries.
    pub async fn transaction_with_retry<F, T>(
        &mut self,
        policy: RetryPolicy,
        f: F,
    ) -> Result<T, QuiverError>
    where
        F: for<'a> Fn(&'a C::Transaction<'_>) -> BoxFuture<'a, Result<T, QuiverError>> + Send,
        T: Send,
    {
        let mut attempts = 0u32;
        loop {
            let tx = self.conn.begin().await?;
            match f(&tx).await {
                Ok(result) => {
                    tx.commit().await?;
                    return Ok(result);
                }
                Err(e) => {
                    if let Err(rb_err) = tx.rollback().await {
                        return Err(QuiverError::Driver(format!(
                            "rollback failed: {rb_err} (original error: {e})"
                        )));
                    }
                    attempts += 1;
                    if attempts > policy.max_retries || !e.is_retryable() {
                        return Err(e);
                    }
                    let backoff = policy.backoff_duration(attempts);
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

/// Configuration for transaction retry behavior.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (0 = no retries).
    pub max_retries: u32,
    /// Initial backoff duration before the first retry.
    pub initial_backoff: Duration,
    /// Maximum backoff duration (caps exponential growth).
    pub max_backoff: Duration,
}

impl RetryPolicy {
    /// Create a retry policy with exponential backoff.
    pub fn new(max_retries: u32, initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            max_retries,
            initial_backoff,
            max_backoff,
        }
    }

    /// Compute the backoff duration for a given attempt number (1-based).
    fn backoff_duration(&self, attempt: u32) -> Duration {
        let multiplier = 2u64.saturating_pow(attempt.saturating_sub(1));
        self.initial_backoff
            .saturating_mul(multiplier as u32)
            .min(self.max_backoff)
    }
}

impl Default for RetryPolicy {
    /// Default: 3 retries, 100ms initial backoff, 5s max.
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}
