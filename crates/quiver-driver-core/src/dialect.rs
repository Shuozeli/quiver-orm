//! Dialect trait for driver-specific SQL behavior.
//!
//! Each database has slightly different requirements for SQL execution:
//! - PostgreSQL uses `$1, $2, ...` instead of `?` placeholders
//! - PostgreSQL and MySQL require DDL statements to be split on `;`
//!
//! The `Dialect` trait captures these differences so that a single generic
//! connection implementation can serve all ADBC-backed drivers.

/// Driver-specific SQL behavior.
///
/// Implementations handle differences in placeholder syntax and DDL
/// statement handling across database engines. The associated type
/// `AdbcConn` ties the dialect to its concrete ADBC connection type.
pub trait Dialect: Send + Sync + Clone + 'static {
    /// The concrete ADBC connection type for this dialect.
    type AdbcConn: adbc::Connection + Send + Sync + 'static;

    /// Rewrite SQL from Quiver's canonical `?N` placeholder style to the
    /// dialect's native format.
    ///
    /// The default implementation returns the SQL unchanged, which is correct
    /// for SQLite and MySQL (both use `?`).
    fn rewrite_sql(&self, sql: &str) -> String {
        sql.to_owned()
    }

    /// Whether DDL statements should be split on `;` and executed individually.
    ///
    /// PostgreSQL and MySQL ADBC drivers do not support multi-statement
    /// execution. SQLite handles it natively.
    fn split_ddl(&self) -> bool {
        false
    }
}
