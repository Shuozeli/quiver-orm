use serde::{Deserialize, Serialize};

/// A database value that can be sent as a parameter or received in a result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            Value::Int(v) => Some(*v != 0),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::UInt(v) => Some(*v),
            Value::Int(v) if *v >= 0 => Some(*v as u64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            Value::Int(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(v) => Some(v),
            _ => None,
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::UInt(v as u64)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::UInt(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Blob(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

/// A column descriptor in a query result.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

/// A single row from a query result.
#[derive(Debug, Clone)]
pub struct Row {
    pub columns: Vec<Column>,
    pub values: Vec<Value>,
}

impl Row {
    /// Get a value by column index.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Get a value by column name.
    pub fn get_by_name(&self, name: &str) -> Option<&Value> {
        let idx = self.columns.iter().position(|c| c.name == name)?;
        self.values.get(idx)
    }

    /// Get a column value as i64.
    pub fn get_i64(&self, index: usize) -> Option<i64> {
        self.get(index)?.as_i64()
    }

    /// Get a column value as String.
    pub fn get_string(&self, index: usize) -> Option<String> {
        self.get(index)?.as_str().map(|s| s.to_string())
    }

    /// Get a column value as bool.
    pub fn get_bool(&self, index: usize) -> Option<bool> {
        self.get(index)?.as_bool()
    }

    /// Get a column value as f64.
    pub fn get_f64(&self, index: usize) -> Option<f64> {
        self.get(index)?.as_f64()
    }

    // -- Named column accessors with error reporting --
    // These return descriptive errors instead of Option, making them suitable
    // for generated `TryFrom<&Row>` implementations where a missing/mistyped
    // column should produce a clear diagnostic.

    /// Get a required text column by name.
    /// Returns empty string for NULL (matches SQLite DEFAULT "" behavior).
    pub fn text(&self, col: &str) -> Result<String, RowError> {
        match self.get_by_name(col) {
            Some(Value::Text(s)) => Ok(s.clone()),
            Some(Value::Null) => Ok(String::new()),
            Some(other) => Err(RowError::type_mismatch(col, "text", other)),
            None => Err(RowError::missing_column(col)),
        }
    }

    /// Get an optional text column by name.
    /// Returns None for NULL.
    pub fn optional_text(&self, col: &str) -> Result<Option<String>, RowError> {
        match self.get_by_name(col) {
            Some(Value::Text(s)) => Ok(Some(s.clone())),
            Some(Value::Null) | None => Ok(None),
            Some(other) => Err(RowError::type_mismatch(col, "text", other)),
        }
    }

    /// Get a required i32 column by name.
    /// Returns 0 for NULL (matches SQLite DEFAULT 0 behavior).
    pub fn int32(&self, col: &str) -> Result<i32, RowError> {
        match self.get_by_name(col) {
            Some(Value::Int(v)) => Ok(*v as i32),
            Some(Value::UInt(v)) => Ok(*v as i32),
            Some(Value::Null) => Ok(0),
            Some(other) => Err(RowError::type_mismatch(col, "int", other)),
            None => Err(RowError::missing_column(col)),
        }
    }

    /// Get an optional i32 column by name.
    /// Returns None for NULL.
    pub fn optional_int32(&self, col: &str) -> Result<Option<i32>, RowError> {
        match self.get_by_name(col) {
            Some(Value::Int(v)) => Ok(Some(*v as i32)),
            Some(Value::UInt(v)) => Ok(Some(*v as i32)),
            Some(Value::Null) | None => Ok(None),
            Some(other) => Err(RowError::type_mismatch(col, "int", other)),
        }
    }

    /// Get a required i64 column by name.
    /// Returns 0 for NULL.
    pub fn int64(&self, col: &str) -> Result<i64, RowError> {
        match self.get_by_name(col) {
            Some(Value::Int(v)) => Ok(*v),
            Some(Value::UInt(v)) => Ok(*v as i64),
            Some(Value::Null) => Ok(0),
            Some(other) => Err(RowError::type_mismatch(col, "int", other)),
            None => Err(RowError::missing_column(col)),
        }
    }

    /// Get an optional i64 column by name.
    /// Returns None for NULL.
    pub fn optional_int64(&self, col: &str) -> Result<Option<i64>, RowError> {
        match self.get_by_name(col) {
            Some(Value::Int(v)) => Ok(Some(*v)),
            Some(Value::UInt(v)) => Ok(Some(*v as i64)),
            Some(Value::Null) | None => Ok(None),
            Some(other) => Err(RowError::type_mismatch(col, "int", other)),
        }
    }

    /// Get a required f64 column by name.
    /// Returns 0.0 for NULL.
    pub fn float64(&self, col: &str) -> Result<f64, RowError> {
        match self.get_by_name(col) {
            Some(Value::Float(v)) => Ok(*v),
            Some(Value::Int(v)) => Ok(*v as f64),
            Some(Value::Null) => Ok(0.0),
            Some(other) => Err(RowError::type_mismatch(col, "float", other)),
            None => Err(RowError::missing_column(col)),
        }
    }

    /// Get an optional f64 column by name.
    /// Returns None for NULL.
    pub fn optional_float64(&self, col: &str) -> Result<Option<f64>, RowError> {
        match self.get_by_name(col) {
            Some(Value::Float(v)) => Ok(Some(*v)),
            Some(Value::Int(v)) => Ok(Some(*v as f64)),
            Some(Value::Null) | None => Ok(None),
            Some(other) => Err(RowError::type_mismatch(col, "float", other)),
        }
    }

    /// Get a required bool column by name.
    /// Handles both Bool and Int (0/1) representations (SQLite stores bools as integers).
    /// Returns false for NULL.
    pub fn boolean(&self, col: &str) -> Result<bool, RowError> {
        match self.get_by_name(col) {
            Some(Value::Bool(b)) => Ok(*b),
            Some(Value::Int(v)) => Ok(*v != 0),
            Some(Value::UInt(v)) => Ok(*v != 0),
            Some(Value::Null) => Ok(false),
            Some(other) => Err(RowError::type_mismatch(col, "bool", other)),
            None => Err(RowError::missing_column(col)),
        }
    }

    /// Get an optional bool column by name.
    /// Returns None for NULL.
    pub fn optional_boolean(&self, col: &str) -> Result<Option<bool>, RowError> {
        match self.get_by_name(col) {
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(Value::Int(v)) => Ok(Some(*v != 0)),
            Some(Value::UInt(v)) => Ok(Some(*v != 0)),
            Some(Value::Null) | None => Ok(None),
            Some(other) => Err(RowError::type_mismatch(col, "bool", other)),
        }
    }

    /// Get a required blob column by name.
    /// Returns empty vec for NULL.
    pub fn blob(&self, col: &str) -> Result<Vec<u8>, RowError> {
        match self.get_by_name(col) {
            Some(Value::Blob(b)) => Ok(b.clone()),
            Some(Value::Null) => Ok(Vec::new()),
            Some(other) => Err(RowError::type_mismatch(col, "blob", other)),
            None => Err(RowError::missing_column(col)),
        }
    }

    /// Get an optional blob column by name.
    /// Returns None for NULL.
    pub fn optional_blob(&self, col: &str) -> Result<Option<Vec<u8>>, RowError> {
        match self.get_by_name(col) {
            Some(Value::Blob(b)) => Ok(Some(b.clone())),
            Some(Value::Null) | None => Ok(None),
            Some(other) => Err(RowError::type_mismatch(col, "blob", other)),
        }
    }
}

/// Error type for row deserialization.
///
/// Produced by the typed `Row` accessor methods (`row.text()`, `row.int32()`, etc.)
/// and by generated `TryFrom<&Row>` implementations from `quiver-codegen`.
#[derive(Debug, Clone)]
pub struct RowError {
    pub message: String,
}

impl RowError {
    pub fn missing_column(col: &str) -> Self {
        Self {
            message: format!("column '{}' not found in row", col),
        }
    }

    pub fn type_mismatch(col: &str, expected: &str, got: &Value) -> Self {
        Self {
            message: format!("expected {} for column '{}', got {:?}", expected, col, got),
        }
    }
}

impl std::fmt::Display for RowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "row deserialization error: {}", self.message)
    }
}

impl std::error::Error for RowError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_conversions() {
        assert_eq!(Value::from(42i32), Value::Int(42));
        assert_eq!(Value::from(true), Value::Bool(true));
        assert_eq!(Value::from("hello"), Value::Text("hello".into()));
        assert_eq!(Value::from(2.72f64), Value::Float(2.72));
        assert_eq!(Value::from(None::<i32>), Value::Null);
        assert_eq!(Value::from(Some(42i32)), Value::Int(42));
    }

    #[test]
    fn value_accessors() {
        assert_eq!(Value::Int(42).as_i64(), Some(42));
        assert_eq!(Value::Int(1).as_bool(), Some(true));
        assert_eq!(Value::Int(0).as_bool(), Some(false));
        assert_eq!(Value::Text("hello".into()).as_str(), Some("hello"));
        assert_eq!(Value::Float(1.5).as_f64(), Some(1.5));
        assert!(Value::Null.is_null());
    }

    #[test]
    fn row_access() {
        let row = Row {
            columns: vec![
                Column { name: "id".into() },
                Column {
                    name: "name".into(),
                },
            ],
            values: vec![Value::Int(1), Value::Text("Alice".into())],
        };
        assert_eq!(row.get_i64(0), Some(1));
        assert_eq!(row.get_string(1), Some("Alice".into()));
        assert_eq!(row.get_by_name("name"), Some(&Value::Text("Alice".into())));
        assert_eq!(row.get_by_name("missing"), None);
    }
}
