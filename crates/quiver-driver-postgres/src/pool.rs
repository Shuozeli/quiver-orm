//! PostgreSQL connection pool.

use quiver_driver_core::pool::DriverPool;

use crate::PostgresDriver;

/// A pool of PostgreSQL connections.
pub type PostgresPool = DriverPool<PostgresDriver>;
