//! Shared helper functions for ADBC-backed drivers.

use arrow_array::RecordBatch;
use quiver_error::QuiverError;

use crate::Value;
use crate::arrow::values_to_param_batch;
use crate::sanitize_connection_error;

/// Convert Quiver `Value` params to a single-row `RecordBatch` for ADBC binding.
pub fn params_to_batch(params: &[Value]) -> Result<RecordBatch, QuiverError> {
    let owned_names: Vec<String> = (0..params.len()).map(|i| format!("p{i}")).collect();
    let name_refs: Vec<&str> = owned_names.iter().map(|s| s.as_str()).collect();
    values_to_param_batch(params, &name_refs).map_err(|e| QuiverError::Driver(e.to_string()))
}

/// Convert an ADBC error to a `QuiverError`, sanitizing connection strings.
pub fn adbc_err(e: adbc::Error) -> QuiverError {
    QuiverError::Driver(sanitize_connection_error(&e.message))
}
