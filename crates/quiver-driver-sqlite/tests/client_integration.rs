use quiver_driver_core::{Connection, DdlStatement, Driver, QuiverClient, Statement, Value};
use quiver_driver_sqlite::SqliteDriver;
use quiver_error::QuiverError;

fn ddl(sql: &str) -> DdlStatement {
    DdlStatement::new(sql.to_string())
}

fn stmt(sql: &str, params: &[Value]) -> Statement {
    Statement::new(sql.to_string(), params.to_vec())
}

#[tokio::test]
async fn client_transaction_insert_and_query() {
    let mut client = QuiverClient::new(SqliteDriver.connect(":memory:").await.unwrap());
    client
        .execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"))
        .await
        .unwrap();

    let rows = client
        .transaction(|tx| {
            Box::pin(async move {
                tx.execute(&stmt(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Int(1), Value::from("Alice")],
                ))
                .await?;
                tx.query(&stmt("SELECT * FROM t", &[])).await
            })
        })
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_i64(0), Some(1));
}

#[tokio::test]
async fn client_transaction_commit() {
    let mut client = QuiverClient::new(SqliteDriver.connect(":memory:").await.unwrap());
    client
        .execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"))
        .await
        .unwrap();

    client
        .transaction(|tx| {
            Box::pin(async move {
                tx.execute(&stmt(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Int(1), Value::from("Alice")],
                ))
                .await?;
                tx.execute(&stmt(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Int(2), Value::from("Bob")],
                ))
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

    // Verify committed data is visible in a new transaction
    let count = client
        .transaction(|tx| {
            Box::pin(async move {
                let rows = tx.query(&stmt("SELECT COUNT(*) FROM t", &[])).await?;
                Ok(rows[0].get_i64(0).unwrap_or(0))
            })
        })
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn client_transaction_rollback_on_error() {
    let mut client = QuiverClient::new(SqliteDriver.connect(":memory:").await.unwrap());
    client
        .execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"))
        .await
        .unwrap();

    let result: Result<(), QuiverError> = client
        .transaction(|tx| {
            Box::pin(async move {
                tx.execute(&stmt(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Int(1), Value::from("Alice")],
                ))
                .await?;
                Err(QuiverError::Validation("intentional error".into()))
            })
        })
        .await;
    assert!(result.is_err());

    // Verify rollback: table should be empty
    let count = client
        .transaction(|tx| {
            Box::pin(async move {
                let rows = tx.query(&stmt("SELECT COUNT(*) FROM t", &[])).await?;
                Ok(rows[0].get_i64(0).unwrap_or(-1))
            })
        })
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn client_transaction_returns_value() {
    let mut client = QuiverClient::new(SqliteDriver.connect(":memory:").await.unwrap());
    client
        .execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"))
        .await
        .unwrap();

    let count = client
        .transaction(|tx| {
            Box::pin(async move {
                tx.execute(&stmt(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Int(1), Value::from("Alice")],
                ))
                .await?;
                let rows = tx.query(&stmt("SELECT COUNT(*) FROM t", &[])).await?;
                Ok(rows[0].get_i64(0).unwrap_or(0))
            })
        })
        .await
        .unwrap();

    assert_eq!(count, 1);
}

#[tokio::test]
async fn client_transaction_query_one_and_optional() {
    let mut client = QuiverClient::new(SqliteDriver.connect(":memory:").await.unwrap());
    client
        .execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"))
        .await
        .unwrap();

    client
        .transaction(|tx| {
            Box::pin(async move {
                tx.execute(&stmt(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Int(1), Value::from("Alice")],
                ))
                .await?;

                let row = tx
                    .query_one(&stmt("SELECT * FROM t WHERE id = ?1", &[Value::Int(1)]))
                    .await?;
                assert_eq!(row.get_string(1), Some("Alice".into()));

                let none = tx
                    .query_optional(&stmt("SELECT * FROM t WHERE id = ?1", &[Value::Int(99)]))
                    .await?;
                assert!(none.is_none());

                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn client_transaction_stream() {
    use tokio_stream::StreamExt;

    let mut client = QuiverClient::new(SqliteDriver.connect(":memory:").await.unwrap());
    client
        .execute_ddl(&ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .await
        .unwrap();

    let count = client
        .transaction(|tx| {
            Box::pin(async move {
                for i in 1..=10 {
                    tx.execute(&stmt(
                        "INSERT INTO t (id, v) VALUES (?1, ?2)",
                        &[Value::Int(i), Value::from(format!("val{i}"))],
                    ))
                    .await?;
                }

                let mut stream = tx
                    .query_stream(&stmt("SELECT id, v FROM t ORDER BY id", &[]))
                    .await?;

                let mut n = 0i64;
                while let Some(row) = stream.next().await {
                    let row = row?;
                    n += 1;
                    assert_eq!(row.get_i64(0), Some(n));
                }
                Ok(n)
            })
        })
        .await
        .unwrap();

    assert_eq!(count, 10);
}
