//! Schema migration engine for Quiver ORM.
//!
//! Provides schema diffing, DDL generation, and migration tracking.

pub mod diff;
pub mod introspect;
pub mod sql_gen;
pub mod step;
pub mod tracker;

pub use diff::diff_schemas;
pub use introspect::{introspect, schema_to_quiver};
pub use sql_gen::{MigrationSqlGenerator, SqlDialect, TrustedSql};
pub use step::{Migration, MigrationStep};
pub use tracker::MigrationTracker;
