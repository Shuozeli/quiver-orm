use thiserror::Error;

#[derive(Debug, Error)]
pub enum QuiverError {
    #[error("parse error at {line}:{column}: {message}")]
    Parse {
        line: usize,
        column: usize,
        message: String,
    },

    #[error("validation error: {0}")]
    Validation(String),

    #[error("codegen error: {0}")]
    Codegen(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("driver error: {0}")]
    Driver(String),

    #[error("migration error: {0}")]
    Migration(String),
}

/// Known patterns in driver error messages that indicate transient failures
/// safe to retry (serialization conflicts, deadlocks, lock timeouts).
const RETRYABLE_PATTERNS: &[&str] = &[
    // PostgreSQL
    "serialization failure",
    "deadlock detected",
    "could not serialize access",
    // MySQL
    "deadlock found",
    "lock wait timeout exceeded",
    // SQLite
    "database is locked",
    "database table is locked",
];

impl QuiverError {
    /// Returns true if this error represents a transient database failure
    /// that may succeed on retry (serialization conflicts, deadlocks, lock
    /// timeouts).
    ///
    /// Only `Driver` errors are considered retryable. Parse, validation,
    /// codegen, IO, and migration errors are never retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            QuiverError::Driver(msg) => {
                let lower = msg.to_lowercase();
                RETRYABLE_PATTERNS
                    .iter()
                    .any(|pattern| lower.contains(pattern))
            }
            _ => false,
        }
    }
}
