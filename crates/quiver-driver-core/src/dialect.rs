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
    /// Return the SQL unchanged for dialects that use `?` (SQLite, MySQL).
    /// PostgreSQL must rewrite to `$1, $2, ...`.
    fn rewrite_sql(&self, sql: &str) -> String;

    /// Whether DDL statements should be split on `;` and executed individually.
    ///
    /// PostgreSQL and MySQL ADBC drivers do not support multi-statement
    /// execution, so they must return `true`. SQLite handles multi-statement
    /// DDL natively and should return `false`.
    fn split_ddl(&self) -> bool;
}
