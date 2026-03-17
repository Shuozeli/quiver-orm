//! MySQL driver for Quiver, delegating to the async ADBC MySQL driver.
//!
//! All database operations go through `adbc_mysql::MysqlConnection`, which
//! uses `mysql_async` internally for native async I/O.
//!
//! For direct Arrow-native ADBC access, use the re-exported `adbc_mysql` crate.

pub mod pool;

pub use adbc_mysql;
pub use pool::MysqlPool;

use adbc::{DatabaseOption, OptionValue};
use quiver_driver_core::{AdbcConnection, AdbcTransaction, BoxFuture, Dialect, Driver, adbc_err};
use quiver_error::QuiverError;

/// MySQL dialect: uses `?` placeholders and splits DDL on `;`.
#[derive(Clone, Default)]
pub struct MysqlDialect;

impl Dialect for MysqlDialect {
    type AdbcConn = adbc_mysql::MysqlConnection;

    // Default rewrite_sql (no-op) is correct for MySQL.

    fn split_ddl(&self) -> bool {
        true
    }
}

/// A MySQL connection.
pub type MysqlConnection = AdbcConnection<MysqlDialect>;

/// An active MySQL transaction.
pub type MysqlTransaction = AdbcTransaction<MysqlDialect>;

/// MySQL driver factory.
pub struct MysqlDriver;

impl Driver for MysqlDriver {
    type Conn = MysqlConnection;

    fn connect<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Self::Conn, QuiverError>> {
        Box::pin(async move {
            let drv = adbc_mysql::MysqlDriver;
            let db = adbc::Driver::new_database_with_opts(
                &drv,
                [(DatabaseOption::Uri, OptionValue::String(url.into()))],
            )
            .await
            .map_err(adbc_err)?;
            let conn = adbc::Database::new_connection(&db)
                .await
                .map_err(adbc_err)?;

            Ok(AdbcConnection::new(conn, MysqlDialect))
        })
    }
}
