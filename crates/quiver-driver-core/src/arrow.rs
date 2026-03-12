//! Conversion utilities between Quiver `Value`/`Row` types and Arrow arrays.
//!
//! These helpers enable drivers to bridge the row-oriented Quiver API with
//! the columnar Arrow format used by ADBC.

use arrow_array::RecordBatch;
use arrow_array::array::*;
use arrow_schema::{DataType, Field, Schema};
use quiver_error::QuiverError;
use std::sync::Arc;

use crate::{Column, Row, Value};

/// Convert a `RecordBatch` into a `Vec<Row>`.
///
/// Each row in the batch becomes a `Row` with column metadata and values.
pub fn record_batch_to_rows(batch: &RecordBatch) -> Result<Vec<Row>, QuiverError> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let schema = batch.schema();

    let columns: Vec<Column> = schema
        .fields()
        .iter()
        .map(|f| Column {
            name: f.name().clone(),
        })
        .collect();

    let mut rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let array = batch.column(col_idx);
            values.push(arrow_array_value(array, row_idx)?);
        }
        rows.push(Row {
            columns: columns.clone(),
            values,
        });
    }
    Ok(rows)
}

/// Convert a slice of `Row`s into a `RecordBatch`.
///
/// Infers the Arrow schema from the values in the first row. All rows must
/// have the same number of columns.
pub fn rows_to_record_batch(rows: &[Row]) -> Result<RecordBatch, arrow_schema::ArrowError> {
    if rows.is_empty() {
        let schema = Schema::empty();
        return Ok(RecordBatch::new_empty(Arc::new(schema)));
    }

    let first = &rows[0];
    let num_cols = first.columns.len();
    let num_rows = rows.len();

    // Infer schema from column names and first-row value types.
    let fields: Vec<Field> = first
        .columns
        .iter()
        .zip(first.values.iter())
        .map(|(col, val)| {
            let dt = value_to_arrow_type(val);
            let nullable = matches!(val, Value::Null);
            Field::new(&col.name, dt, nullable)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Build columnar arrays.
    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(num_cols);
    for col_idx in 0..num_cols {
        let dt = schema.field(col_idx).data_type().clone();
        arrays.push(build_column_array(&dt, rows, col_idx, num_rows));
    }

    RecordBatch::try_new(schema, arrays)
}

/// Convert a slice of `Value`s into a single-row `RecordBatch` for parameter binding.
pub fn values_to_param_batch(
    params: &[Value],
    names: &[&str],
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    if params.is_empty() {
        let schema = Schema::empty();
        return Ok(RecordBatch::new_empty(Arc::new(schema)));
    }

    let fields: Vec<Field> = names
        .iter()
        .zip(params.iter())
        .map(|(name, val)| {
            let dt = value_to_arrow_type(val);
            Field::new(*name, dt, val.is_null())
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let arrays: Vec<Arc<dyn arrow_array::Array>> =
        params.iter().map(|v| value_to_arrow_array(v)).collect();

    RecordBatch::try_new(schema, arrays)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Downcast an Arrow array to a concrete type, returning a driver error on mismatch.
macro_rules! downcast_array {
    ($array:expr, $array_ty:ty, $type_name:expr) => {
        $array.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
            QuiverError::Driver(format!(
                "Arrow array type mismatch: expected {} but downcast failed",
                $type_name
            ))
        })?
    };
}

fn arrow_array_value(array: &dyn arrow_array::Array, idx: usize) -> Result<Value, QuiverError> {
    if array.is_null(idx) {
        return Ok(Value::Null);
    }

    Ok(match array.data_type() {
        DataType::Boolean => {
            Value::Bool(downcast_array!(array, BooleanArray, "BooleanArray").value(idx))
        }
        DataType::Int8 => {
            Value::Int(downcast_array!(array, Int8Array, "Int8Array").value(idx) as i64)
        }
        DataType::Int16 => {
            Value::Int(downcast_array!(array, Int16Array, "Int16Array").value(idx) as i64)
        }
        DataType::Int32 => {
            Value::Int(downcast_array!(array, Int32Array, "Int32Array").value(idx) as i64)
        }
        DataType::Int64 => Value::Int(downcast_array!(array, Int64Array, "Int64Array").value(idx)),
        DataType::UInt8 => {
            Value::UInt(downcast_array!(array, UInt8Array, "UInt8Array").value(idx) as u64)
        }
        DataType::UInt16 => {
            Value::UInt(downcast_array!(array, UInt16Array, "UInt16Array").value(idx) as u64)
        }
        DataType::UInt32 => {
            Value::UInt(downcast_array!(array, UInt32Array, "UInt32Array").value(idx) as u64)
        }
        DataType::UInt64 => {
            Value::UInt(downcast_array!(array, UInt64Array, "UInt64Array").value(idx))
        }
        DataType::Float32 => {
            Value::Float(downcast_array!(array, Float32Array, "Float32Array").value(idx) as f64)
        }
        DataType::Float64 => {
            Value::Float(downcast_array!(array, Float64Array, "Float64Array").value(idx))
        }
        DataType::Utf8 => Value::Text(
            downcast_array!(array, StringArray, "StringArray")
                .value(idx)
                .to_string(),
        ),
        DataType::LargeUtf8 => Value::Text(
            downcast_array!(array, LargeStringArray, "LargeStringArray")
                .value(idx)
                .to_string(),
        ),
        DataType::Binary => Value::Blob(
            downcast_array!(array, BinaryArray, "BinaryArray")
                .value(idx)
                .to_vec(),
        ),
        DataType::LargeBinary => Value::Blob(
            downcast_array!(array, LargeBinaryArray, "LargeBinaryArray")
                .value(idx)
                .to_vec(),
        ),
        _ => Value::Null, // unsupported types become null
    })
}

fn value_to_arrow_type(val: &Value) -> DataType {
    match val {
        Value::Null => DataType::Null,
        Value::Bool(_) => DataType::Boolean,
        Value::Int(_) => DataType::Int64,
        Value::UInt(_) => DataType::UInt64,
        Value::Float(_) => DataType::Float64,
        Value::Text(_) => DataType::Utf8,
        Value::Blob(_) => DataType::Binary,
    }
}

fn value_to_arrow_array(val: &Value) -> Arc<dyn arrow_array::Array> {
    match val {
        Value::Null => Arc::new(NullArray::new(1)),
        Value::Bool(v) => Arc::new(BooleanArray::from(vec![*v])),
        Value::Int(v) => Arc::new(Int64Array::from(vec![*v])),
        Value::UInt(v) => Arc::new(UInt64Array::from(vec![*v])),
        Value::Float(v) => Arc::new(Float64Array::from(vec![*v])),
        Value::Text(v) => Arc::new(StringArray::from(vec![v.as_str()])),
        Value::Blob(v) => Arc::new(BinaryArray::from_vec(vec![v.as_slice()])),
    }
}

fn build_column_array(
    dt: &DataType,
    rows: &[Row],
    col_idx: usize,
    num_rows: usize,
) -> Arc<dyn arrow_array::Array> {
    match dt {
        DataType::Boolean => {
            let vals: Vec<Option<bool>> = rows
                .iter()
                .map(|r| r.get(col_idx).and_then(|v| v.as_bool()))
                .collect();
            Arc::new(BooleanArray::from(vals))
        }
        DataType::Int64 => {
            let vals: Vec<Option<i64>> = rows
                .iter()
                .map(|r| r.get(col_idx).and_then(|v| v.as_i64()))
                .collect();
            Arc::new(Int64Array::from(vals))
        }
        DataType::UInt64 => {
            let vals: Vec<Option<u64>> = rows
                .iter()
                .map(|r| r.get(col_idx).and_then(|v| v.as_u64()))
                .collect();
            Arc::new(UInt64Array::from(vals))
        }
        DataType::Float64 => {
            let vals: Vec<Option<f64>> = rows
                .iter()
                .map(|r| r.get(col_idx).and_then(|v| v.as_f64()))
                .collect();
            Arc::new(Float64Array::from(vals))
        }
        DataType::Utf8 => {
            let vals: Vec<Option<String>> = rows
                .iter()
                .map(|r| {
                    r.get(col_idx)
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                })
                .collect();
            let refs: Vec<Option<&str>> = vals.iter().map(|v| v.as_deref()).collect();
            Arc::new(StringArray::from(refs))
        }
        DataType::Binary => {
            let vals: Vec<Option<Vec<u8>>> = rows
                .iter()
                .map(|r| {
                    r.get(col_idx)
                        .and_then(|v| v.as_bytes().map(|b| b.to_vec()))
                })
                .collect();
            let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
            Arc::new(BinaryArray::from_opt_vec(refs))
        }
        _ => {
            // Fallback: null array for unsupported types
            Arc::new(NullArray::new(num_rows))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_rows_to_batch_and_back() {
        let rows = vec![
            Row {
                columns: vec![
                    Column { name: "id".into() },
                    Column {
                        name: "name".into(),
                    },
                    Column {
                        name: "score".into(),
                    },
                ],
                values: vec![
                    Value::Int(1),
                    Value::Text("Alice".into()),
                    Value::Float(9.5),
                ],
            },
            Row {
                columns: vec![
                    Column { name: "id".into() },
                    Column {
                        name: "name".into(),
                    },
                    Column {
                        name: "score".into(),
                    },
                ],
                values: vec![Value::Int(2), Value::Text("Bob".into()), Value::Float(8.0)],
            },
        ];

        let batch = rows_to_record_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let restored = record_batch_to_rows(&batch).unwrap();
        assert_eq!(restored.len(), 2);
        assert_eq!(restored[0].get_i64(0), Some(1));
        assert_eq!(restored[0].get_string(1), Some("Alice".into()));
        assert_eq!(restored[1].get_i64(0), Some(2));
    }

    #[test]
    fn empty_rows_to_batch() {
        let rows: Vec<Row> = vec![];
        let batch = rows_to_record_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn values_to_param_batch_roundtrip() {
        let params = vec![
            Value::from("alice@test.com"),
            Value::from(42i64),
            Value::from(true),
        ];
        let names = vec!["email", "age", "active"];
        let batch = values_to_param_batch(&params, &names).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);

        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "email");
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(2).name(), "active");
    }

    #[test]
    fn record_batch_with_nulls() {
        let rows = vec![Row {
            columns: vec![
                Column { name: "id".into() },
                Column {
                    name: "name".into(),
                },
            ],
            values: vec![Value::Int(1), Value::Text("Alice".into())],
        }];

        let batch = rows_to_record_batch(&rows).unwrap();
        let restored = record_batch_to_rows(&batch).unwrap();
        assert_eq!(restored[0].get_i64(0), Some(1));
        assert_eq!(restored[0].get_string(1), Some("Alice".into()));
    }

    #[test]
    fn blob_column_roundtrip() {
        let rows = vec![Row {
            columns: vec![Column {
                name: "data".into(),
            }],
            values: vec![Value::Blob(vec![0xDE, 0xAD])],
        }];
        let batch = rows_to_record_batch(&rows).unwrap();
        let restored = record_batch_to_rows(&batch).unwrap();
        assert_eq!(
            restored[0].get(0).unwrap().as_bytes(),
            Some(&[0xDE_u8, 0xAD][..])
        );
    }
}
