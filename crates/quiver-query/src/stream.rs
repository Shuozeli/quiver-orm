//! Streaming query results.
//!
//! Provides [`find_many_stream`] for streaming large result sets without
//! buffering all rows in memory.

use quiver_driver_core::{DynConnection, RowStream, Statement};
use quiver_error::QuiverError;

/// Execute a query and return results as a stream of rows.
///
/// This is the streaming equivalent of `conn.dyn_query()`. Use for large
/// result sets where buffering all rows in memory is undesirable.
///
/// The returned [`RowStream`] implements `futures_core::Stream` and can
/// be consumed with `tokio_stream::StreamExt` or `futures::StreamExt`.
pub async fn find_many_stream(
    conn: &dyn DynConnection,
    stmt: &Statement,
) -> Result<RowStream, QuiverError> {
    conn.dyn_query_stream(stmt).await
}
