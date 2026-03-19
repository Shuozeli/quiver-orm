# Quiver-ORM: Consumer Integration Issues

> **Resolution (2026-03-19):** The `Connection`, `Transactional`, `Transaction`,
> `Driver`, and `Pool` traits in `quiver-driver-core` have been switched from
> RPITIT (`impl Future`) to `BoxFuture` returns. This resolves Problems 1-3
> described below. Problem 4 is also resolved because `PoolGuard<C>` delegates
> to `Connection` methods which now use `BoxFuture`.

## Context

We're migrating a real gRPC server (MyCRM, ~20 service files, SQLite backend) from diesel to quiver-orm. The mechanical migration is complete -- all service files are rewritten -- but `cargo check` reveals ~148 compile errors caused by **RPITIT (return-position impl Trait in traits)** limitations in `quiver-driver-core`.

This doc explains each problem with concrete consumer code examples, so you can decide the best fix in quiver-orm.

---

## Problem 1: `Connection` trait methods not callable inside `Box::pin(async move { ... })`

**Error count:** ~47 (E0599: no method named `query`/`execute`/`query_one`/`query_optional` found)

**Where it happens:** Inside every `QuiverClient::transaction` closure.

**Consumer code:**
```rust
use quiver_driver_core::{Connection, Pool, QuiverClient, Statement, Value};

let guard = self.pool.acquire().await?;
let mut client = QuiverClient::new(guard);

let row = client.transaction(|tx| {
    Box::pin(async move {
        // ERROR: no method named `query_one` found for reference `&SqliteTransaction`
        let row = tx.query_one(&Statement::new(
            "SELECT id, data FROM employees WHERE id = ?1".into(),
            vec![Value::from(employee_id)],
        )).await?;
        Ok(row)
    })
}).await?;
```

**Why it fails:** The `Connection` trait uses RPITIT:
```rust
pub trait Connection: Send + Sync {
    fn query(&self, stmt: &Statement) -> impl Future<Output = Result<Vec<Row>, QuiverError>> + Send;
}
```

Inside `Box::pin(async move { ... })`, the compiler needs to prove that the future returned by `tx.query()` is `Send + 'a`, but RPITIT return types are opaque and the compiler can't reason about them across the `Box::pin` boundary. The trait is imported (`use quiver_driver_core::Connection`), but the methods still don't resolve.

**Suggested fix -- add inherent methods to `SqliteTransaction` and `SqliteConnection`:**

This is the least invasive fix. Add inherent `query`, `execute`, `query_one`, `query_optional` methods directly on the concrete types so they're callable without trait dispatch:

```rust
// In quiver-driver-sqlite/src/lib.rs (or wherever SqliteTransaction is defined)
impl SqliteTransaction<'_> {
    pub async fn query(&self, stmt: &Statement) -> Result<Vec<Row>, QuiverError> {
        <Self as Connection>::query(self, stmt).await
    }
    pub async fn execute(&self, stmt: &Statement) -> Result<u64, QuiverError> {
        <Self as Connection>::execute(self, stmt).await
    }
    pub async fn query_one(&self, stmt: &Statement) -> Result<Row, QuiverError> {
        <Self as Connection>::query_one(self, stmt).await
    }
    pub async fn query_optional(&self, stmt: &Statement) -> Result<Option<Row>, QuiverError> {
        <Self as Connection>::query_optional(self, stmt).await
    }
}
```

The same for `SqliteConnection` and `PoolGuard<SqliteConnection>`.

**Alternative fix -- switch `Connection` to use `BoxFuture` returns:**

This makes the trait object-safe and eliminates all RPITIT dispatch issues:
```rust
pub trait Connection: Send + Sync {
    fn execute<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<u64, QuiverError>>;
    fn query<'a>(&'a self, stmt: &'a Statement) -> BoxFuture<'a, Result<Vec<Row>, QuiverError>>;
    // ...
}
```

You already have `DynConnection` with this pattern. The downside is one heap allocation per query call. For a small app like ours (<20 users, SQLite), this is negligible.

---

## Problem 2: `Pool::acquire()` not callable on `Arc<SqlitePool>`

**Error count:** ~5 (E0599: no method named `acquire` found for `Arc<SqlitePool>`)

**Consumer code:**
```rust
use std::sync::Arc;
use quiver_driver_core::Pool;

struct MyService {
    pool: Arc<SqlitePool>,  // shared across gRPC services
}

impl MyService {
    async fn handle(&self) {
        // ERROR: no method named `acquire` found for struct `Arc<SqlitePool>`
        let guard = self.pool.acquire().await?;
    }
}
```

**Why it fails:** `Pool` trait uses RPITIT:
```rust
pub trait Pool: Send + Sync {
    fn acquire(&self) -> impl Future<Output = Result<PoolGuard<Self::Conn>, QuiverError>> + Send;
}
```

`Arc<T>` derefs to `&T`, but Rust doesn't auto-dispatch RPITIT methods through `Deref`. The compiler needs to see the concrete type.

**Suggested fix -- add an inherent method on `SqlitePool`:**

```rust
impl SqlitePool {
    pub async fn acquire(&self) -> Result<PoolGuard<SqliteConnection>, QuiverError> {
        <Self as Pool>::acquire(self).await
    }
}
```

This lets `Arc<SqlitePool>` auto-deref to `&SqlitePool` and find the inherent method.

**Alternative: consumer workaround** (we can do this on our side if you prefer):
```rust
let guard = Pool::acquire(self.pool.as_ref()).await?;
// or
let guard = (*self.pool).acquire().await?;
```

---

## Problem 3: Type inference fails for `QuiverClient::transaction` closures

**Error count:** ~74 (E0282: type annotations needed)

**Consumer code:**
```rust
// ERROR: type annotations needed
let result = client.transaction(|tx| {
    Box::pin(async move {
        tx.execute(&Statement::new("INSERT INTO ...".into(), vec![])).await?;
        Ok(())
    })
}).await?;
```

**Why it fails:** `QuiverClient::transaction` has signature:
```rust
pub async fn transaction<F, T>(&mut self, f: F) -> Result<T, QuiverError>
where
    F: for<'a> FnOnce(&'a C::Transaction<'_>) -> BoxFut<'a, Result<T, QuiverError>>,
    T: Send,
```

The compiler can't always infer `T` from the closure body, especially when:
- The closure body uses `?` (the error path and success path give different type hints)
- The `tx.query()` methods fail to resolve (Problem 1 cascades into this)

**This may resolve automatically** once Problem 1 is fixed. If `tx.query()` resolves, the compiler can infer `T` from the return type of the closure. But if it doesn't, the consumer workaround is:

```rust
let result = client.transaction(|tx| -> BoxFut<'_, Result<MyRow, QuiverError>> {
    Box::pin(async move { ... })
}).await?;
```

---

## Problem 4 (minor): `PoolGuard` doesn't implement `Connection` methods directly

**Not a compile error yet**, but it would be nice for `PoolGuard<SqliteConnection>` to have inherent `query`/`execute` methods too, so consumers can call them for non-transactional reads (e.g., session lookup in an auth interceptor):

```rust
let guard = pool.acquire().await?;
// We want this to work without QuiverClient:
let rows = guard.query(&Statement::new("SELECT ...".into(), vec![])).await?;
```

This currently works through the `Connection` trait impl on `PoolGuard<C>`, but may hit the same RPITIT issue in some contexts (e.g., inside `tokio::task::block_in_place`).

---

## Summary of Suggested Changes

| Priority | Change | Location | Impact |
|----------|--------|----------|--------|
| **P0** | Add inherent methods on `SqliteTransaction` mirroring `Connection` | `quiver-driver-sqlite` | Fixes Problem 1 (47 errors) and unblocks Problem 3 (~74 errors) |
| **P0** | Add inherent `acquire()` on `SqlitePool` | `quiver-driver-sqlite` | Fixes Problem 2 (5 errors) |
| **P1** | Add inherent methods on `SqliteConnection` and `PoolGuard<SqliteConnection>` | `quiver-driver-sqlite` / `quiver-driver-core` | Fixes Problem 4 |
| **P2** | Consider switching `Connection`/`Pool` to `BoxFuture` instead of RPITIT | `quiver-driver-core` | Eliminates all RPITIT issues permanently (but adds heap allocs) |

The P0 fixes are ~30 lines of code total and unblock all ~148 compile errors.

---

## Consumer Codebase Reference

The consumer project follows this exact pattern:

```rust
use std::sync::Arc;
use quiver_driver_core::{Connection, Pool, QuiverClient, Row, Statement, Transactional, Value};
use quiver_error::QuiverError;
use crate::db::DbPool; // type alias for SqlitePool

struct MyServiceImpl {
    pool: Arc<DbPool>,
}

// Typical RPC handler:
async fn my_rpc(&self) -> Result<Response<Proto>, Status> {
    let guard = self.pool.acquire().await           // Problem 2: Arc<SqlitePool>
        .map_err(|e| Status::internal(format!("db: {e}")))?;
    let mut client = QuiverClient::new(guard);

    let result = client.transaction(|tx| {          // Problem 3: type inference
        Box::pin(async move {
            tx.execute(&Statement::new(             // Problem 1: RPITIT dispatch
                "INSERT INTO foo (a, b) VALUES (?1, ?2)".into(),
                vec![Value::from("hello"), Value::from(42_i64)],
            )).await?;

            let rows = tx.query(&Statement::new(    // Problem 1: RPITIT dispatch
                "SELECT last_insert_rowid() AS id".into(),
                vec![],
            )).await?;
            let id = rows[0].int64("id")
                .map_err(|e| QuiverError::Driver(format!("{e}")))?;

            let row = tx.query_one(&Statement::new( // Problem 1: RPITIT dispatch
                "SELECT id, data FROM foo WHERE id = ?1".into(),
                vec![Value::from(id)],
            )).await?;

            Ok(row.text("data").map_err(|e| QuiverError::Driver(format!("{e}")))?)
        })
    }).await.map_err(|e| Status::internal(format!("{e}")))?;

    Ok(Response::new(result))
}
```

You can run `cargo check --package crm-server` from the consumer project to see the full error list.
