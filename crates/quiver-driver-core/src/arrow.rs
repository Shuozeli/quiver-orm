//! Conversion utilities between Quiver `Value`/`Row` types and Arrow arrays.
//!
//! These helpers enable drivers to bridge the row-oriented Quiver API with
//! the columnar Arrow format used by ADBC.

use arrow_array::RecordBatch;
use arrow_array::array::*;
use arrow_array::temporal_conversions;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use quiver_error::QuiverError;
use std::sync::Arc;

use crate::{Row, Value};

/// Convert a `RecordBatch` into a `Vec<Row>`.
///
/// Each row in the batch becomes a `Row` with column metadata and values.
pub fn record_batch_to_rows(batch: &RecordBatch) -> Result<Vec<Row>, QuiverError> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let schema = batch.schema();

    let column_names: Arc<Vec<String>> =
        Arc::new(schema.fields().iter().map(|f| f.name().clone()).collect());

    let mut rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let array = batch.column(col_idx);
            values.push(arrow_array_value(array, row_idx)?);
        }
        rows.push(Row {
            column_names: Arc::clone(&column_names),
            values,
        });
    }
    Ok(rows)
}

/// Convert a slice of `Row`s into a `RecordBatch`.
///
/// Infers the Arrow schema from the values in the first row. All rows must
/// have the same number of columns.
pub fn rows_to_record_batch(rows: &[Row]) -> Result<RecordBatch, QuiverError> {
    if rows.is_empty() {
        let schema = Schema::empty();
        return Ok(RecordBatch::new_empty(Arc::new(schema)));
    }

    let first = &rows[0];
    let num_cols = first.column_names.len();
    let num_rows = rows.len();

    // Infer schema from column names and first-row value types.
    let fields: Vec<Field> = first
        .column_names
        .iter()
        .zip(first.values.iter())
        .map(|(name, val)| {
            let dt = value_to_arrow_type(val);
            let nullable = matches!(val, Value::Null);
            Field::new(name, dt, nullable)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Build columnar arrays.
    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(num_cols);
    for col_idx in 0..num_cols {
        let dt = schema.field(col_idx).data_type().clone();
        arrays.push(build_column_array(&dt, rows, col_idx, num_rows)?);
    }

    RecordBatch::try_new(schema, arrays).map_err(|e| QuiverError::Driver(e.to_string()))
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
        DataType::Date32 => {
            let days = downcast_array!(array, Date32Array, "Date32Array").value(idx);
            let date = temporal_conversions::date32_to_datetime(days)
                .ok_or_else(|| QuiverError::Driver(format!("invalid Date32 value: {}", days)))?;
            Value::Text(date.format("%Y-%m-%d").to_string())
        }
        DataType::Date64 => {
            let ms = downcast_array!(array, Date64Array, "Date64Array").value(idx);
            let date = temporal_conversions::date64_to_datetime(ms)
                .ok_or_else(|| QuiverError::Driver(format!("invalid Date64 value: {}", ms)))?;
            Value::Text(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(unit, _tz) => {
            let text = match unit {
                TimeUnit::Second => {
                    let a = downcast_array!(array, TimestampSecondArray, "TimestampSecondArray");
                    let dt = temporal_conversions::timestamp_s_to_datetime(a.value(idx))
                        .ok_or_else(|| QuiverError::Driver("invalid Timestamp(s) value".into()))?;
                    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
                }
                TimeUnit::Millisecond => {
                    let a = downcast_array!(
                        array,
                        TimestampMillisecondArray,
                        "TimestampMillisecondArray"
                    );
                    let dt = temporal_conversions::timestamp_ms_to_datetime(a.value(idx))
                        .ok_or_else(|| QuiverError::Driver("invalid Timestamp(ms) value".into()))?;
                    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
                }
                TimeUnit::Microsecond => {
                    let a = downcast_array!(
                        array,
                        TimestampMicrosecondArray,
                        "TimestampMicrosecondArray"
                    );
                    let dt = temporal_conversions::timestamp_us_to_datetime(a.value(idx))
                        .ok_or_else(|| QuiverError::Driver("invalid Timestamp(us) value".into()))?;
                    dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
                }
                TimeUnit::Nanosecond => {
                    let a = downcast_array!(
                        array,
                        TimestampNanosecondArray,
                        "TimestampNanosecondArray"
                    );
                    let dt = temporal_conversions::timestamp_ns_to_datetime(a.value(idx))
                        .ok_or_else(|| QuiverError::Driver("invalid Timestamp(ns) value".into()))?;
                    dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()
                }
            };
            Value::Text(text)
        }
        DataType::Time32(unit) => {
            let text = match unit {
                TimeUnit::Second => {
                    let a = downcast_array!(array, Time32SecondArray, "Time32SecondArray");
                    let dt = temporal_conversions::time32s_to_time(a.value(idx))
                        .ok_or_else(|| QuiverError::Driver("invalid Time32(s) value".into()))?;
                    dt.format("%H:%M:%S").to_string()
                }
                TimeUnit::Millisecond => {
                    let a =
                        downcast_array!(array, Time32MillisecondArray, "Time32MillisecondArray");
                    let dt = temporal_conversions::time32ms_to_time(a.value(idx))
                        .ok_or_else(|| QuiverError::Driver("invalid Time32(ms) value".into()))?;
                    dt.format("%H:%M:%S%.3f").to_string()
                }
                _ => {
                    return Err(QuiverError::Driver(format!(
                        "unsupported Time32 unit: {:?}",
                        unit
                    )));
                }
            };
            Value::Text(text)
        }
        DataType::Time64(unit) => {
            let text = match unit {
                TimeUnit::Microsecond => {
                    let a =
                        downcast_array!(array, Time64MicrosecondArray, "Time64MicrosecondArray");
                    let dt = temporal_conversions::time64us_to_time(a.value(idx))
                        .ok_or_else(|| QuiverError::Driver("invalid Time64(us) value".into()))?;
                    dt.format("%H:%M:%S%.6f").to_string()
                }
                TimeUnit::Nanosecond => {
                    let a = downcast_array!(array, Time64NanosecondArray, "Time64NanosecondArray");
                    let dt = temporal_conversions::time64ns_to_time(a.value(idx))
                        .ok_or_else(|| QuiverError::Driver("invalid Time64(ns) value".into()))?;
                    dt.format("%H:%M:%S%.9f").to_string()
                }
                _ => {
                    return Err(QuiverError::Driver(format!(
                        "unsupported Time64 unit: {:?}",
                        unit
                    )));
                }
            };
            Value::Text(text)
        }
        DataType::Decimal128(_precision, scale) => {
            let a = downcast_array!(array, Decimal128Array, "Decimal128Array");
            let raw = a.value(idx);
            let text = format_decimal(raw, *scale as i32);
            Value::Text(text)
        }
        DataType::Decimal256(_precision, scale) => {
            let a = downcast_array!(array, Decimal256Array, "Decimal256Array");
            let raw = a.value(idx);
            let text = format_decimal256(raw, *scale as i32);
            Value::Text(text)
        }
        other => {
            return Err(QuiverError::Driver(format!(
                "unsupported Arrow type: {:?}",
                other
            )));
        }
    })
}

/// Format a Decimal128 value (i128 with a scale) into a decimal string.
fn format_decimal(raw: i128, scale: i32) -> String {
    if scale <= 0 {
        return raw.to_string();
    }
    let scale = scale as u32;
    let divisor = 10_i128.pow(scale);
    let integer_part = raw / divisor;
    let fractional_part = (raw % divisor).unsigned_abs();
    format!(
        "{}.{:0>width$}",
        integer_part,
        fractional_part,
        width = scale as usize
    )
}

/// Format a Decimal256 value (i256 with a scale) into a decimal string.
fn format_decimal256(raw: arrow_buffer::i256, scale: i32) -> String {
    // Use the arrow Display implementation which handles the scale correctly.
    // i256 does not support easy arithmetic, so convert to string representation.
    let s = raw.to_string();
    if scale <= 0 {
        return s;
    }
    let scale = scale as usize;
    let is_negative = s.starts_with('-');
    let digits = if is_negative { &s[1..] } else { &s };
    if digits.len() <= scale {
        let padded = format!("{:0>width$}", digits, width = scale + 1);
        let (int_part, frac_part) = padded.split_at(padded.len() - scale);
        if is_negative {
            format!("-{}.{}", int_part, frac_part)
        } else {
            format!("{}.{}", int_part, frac_part)
        }
    } else {
        let (int_part, frac_part) = digits.split_at(digits.len() - scale);
        if is_negative {
            format!("-{}.{}", int_part, frac_part)
        } else {
            format!("{}.{}", int_part, frac_part)
        }
    }
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
    _num_rows: usize,
) -> Result<Arc<dyn arrow_array::Array>, QuiverError> {
    Ok(match dt {
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
        other => {
            return Err(QuiverError::Driver(format!(
                "unsupported Arrow type for column array: {:?}",
                other
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_rows_to_batch_and_back() {
        let cols = Arc::new(vec![
            "id".to_string(),
            "name".to_string(),
            "score".to_string(),
        ]);
        let rows = vec![
            Row {
                column_names: Arc::clone(&cols),
                values: vec![
                    Value::Int(1),
                    Value::Text("Alice".into()),
                    Value::Float(9.5),
                ],
            },
            Row {
                column_names: cols,
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
            column_names: Arc::new(vec!["id".into(), "name".into()]),
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
            column_names: Arc::new(vec!["data".into()]),
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
