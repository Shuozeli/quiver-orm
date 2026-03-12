//! Streaming query results.
//!
//! [`RowStream`] wraps a `Stream<Item = Result<Row, QuiverError>>` for
//! row-by-row consumption of large result sets without buffering.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;

use crate::Row;
use quiver_error::QuiverError;

/// A stream of query result rows.
///
/// Returned by [`Connection::query_stream`](crate::Connection::query_stream).
/// For large result sets this avoids buffering all rows in memory.
pub struct RowStream {
    inner: Pin<Box<dyn Stream<Item = Result<Row, QuiverError>> + Send>>,
}

impl RowStream {
    /// Create a stream from a pre-buffered `Vec<Row>`.
    ///
    /// This is the default implementation used by drivers that don't
    /// support true streaming -- all rows are loaded first, then yielded
    /// one by one from the iterator.
    pub fn from_vec(rows: Vec<Row>) -> Self {
        Self {
            inner: Box::pin(tokio_stream::iter(rows.into_iter().map(Ok))),
        }
    }

    /// Create a stream from an mpsc receiver.
    ///
    /// Used by drivers that implement true streaming: a background task
    /// sends rows through the channel and the stream yields them as they
    /// arrive.
    pub fn from_receiver(rx: tokio::sync::mpsc::Receiver<Result<Row, QuiverError>>) -> Self {
        Self {
            inner: Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)),
        }
    }
}

impl Stream for RowStream {
    type Item = Result<Row, QuiverError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
