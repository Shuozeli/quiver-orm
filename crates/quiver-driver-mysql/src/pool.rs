//! MySQL connection pool.

use quiver_driver_core::pool::DriverPool;

use crate::MysqlDriver;

/// A pool of MySQL connections.
pub type MysqlPool = DriverPool<MysqlDriver>;
