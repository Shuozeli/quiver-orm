//! Arrow-native query execution.
//!
//! Provides `query_arrow()` which executes a query and returns an Arrow
//! `RecordBatch` directly, skipping the intermediate `Vec<Row>` for
//! callers that want columnar data.

use arrow_array::RecordBatch;
use quiver_driver_core::arrow::rows_to_record_batch;
use quiver_driver_core::{Connection, Statement};
use quiver_error::QuiverError;

/// Execute a query and return the result as an Arrow `RecordBatch`.
///
/// This is equivalent to calling `conn.query()` followed by
/// `rows_to_record_batch()`, but provided as a convenience for
/// Arrow-native workflows.
///
/// Returns an empty `RecordBatch` (zero rows, empty schema) when the
/// query produces no results.
pub async fn query_arrow(
    conn: &dyn Connection,
    stmt: &Statement,
) -> Result<RecordBatch, QuiverError> {
    let rows = conn.query(stmt).await?;
    rows_to_record_batch(&rows).map_err(|e| QuiverError::Driver(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quiver_driver_core::{Connection, Driver, Value};

    #[tokio::test]
    async fn query_arrow_empty_result() {
        let conn = quiver_driver_sqlite::SqliteDriver
            .connect(":memory:")
            .await
            .unwrap();
        conn.execute_ddl(&quiver_driver_core::DdlStatement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)".to_string(),
        ))
        .await
        .unwrap();

        let batch = query_arrow(
            &conn,
            &Statement::new("SELECT * FROM t".to_string(), vec![]),
        )
        .await
        .unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn query_arrow_with_data() {
        let conn = quiver_driver_sqlite::SqliteDriver
            .connect(":memory:")
            .await
            .unwrap();
        conn.execute_ddl(&quiver_driver_core::DdlStatement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL)".to_string(),
        ))
        .await
        .unwrap();
        conn.execute(&Statement::new(
            "INSERT INTO t (id, name, score) VALUES (?1, ?2, ?3)".to_string(),
            vec![Value::Int(1), Value::from("Alice"), Value::Float(9.5)],
        ))
        .await
        .unwrap();
        conn.execute(&Statement::new(
            "INSERT INTO t (id, name, score) VALUES (?1, ?2, ?3)".to_string(),
            vec![Value::Int(2), Value::from("Bob"), Value::Float(8.0)],
        ))
        .await
        .unwrap();

        let batch = query_arrow(
            &conn,
            &Statement::new("SELECT * FROM t".to_string(), vec![]),
        )
        .await
        .unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "score");
    }

    #[tokio::test]
    async fn query_arrow_aggregate_with_alias() {
        let conn = quiver_driver_sqlite::SqliteDriver
            .connect(":memory:")
            .await
            .unwrap();
        conn.execute_ddl(&quiver_driver_core::DdlStatement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)".to_string(),
        ))
        .await
        .unwrap();
        for i in 1..=5 {
            conn.execute(&Statement::new(
                "INSERT INTO t (id, name) VALUES (?1, ?2)".to_string(),
                vec![Value::Int(i), Value::from(format!("user{i}"))],
            ))
            .await
            .unwrap();
        }

        let q = crate::Query::table("t").aggregate().count_all().build();
        let batch = query_arrow(&conn, &q).await.unwrap();
        assert_eq!(batch.num_rows(), 1);

        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "_count");
    }
}
