# Quiver ORM -- Code Quality Audit

Audit date: 2026-03-17

## Critical: Driver Crate Copy-Paste Duplication

The three driver crates are 90%+ identical code with only type names changed.
This is the single biggest quality issue -- any bug fix requires coordinated
changes in 3 places.

### Duplicated Connection Trait Impls (~500 LOC)

`execute()`, `query()`, `query_stream()`, `execute_ddl()` are near-identical:

- `crates/quiver-driver-sqlite/src/lib.rs:77-188` (SqliteConnection)
- `crates/quiver-driver-sqlite/src/lib.rs:218-328` (SqliteTransaction)
- `crates/quiver-driver-postgres/src/lib.rs:118-237` (PostgresConnection)
- `crates/quiver-driver-postgres/src/lib.rs:268-387` (PostgresTransaction)
- `crates/quiver-driver-mysql/src/lib.rs:66-182` (MysqlConnection)
- `crates/quiver-driver-mysql/src/lib.rs:213-329` (MysqlTransaction)

Only differences: PG rewrites `?` -> `$1,$2` placeholders, PG/MySQL split
multi-statement DDL on `;`.

**Fix:** Extract a generic `GenericConnection<D: Dialect>` in `quiver-driver-core`
with a `Dialect` trait for driver-specific behavior:

```rust
trait Dialect {
    fn rewrite_sql(sql: &str, param_count: usize) -> String { sql.to_string() }
    fn requires_ddl_splitting() -> bool { false }
}
```

### Duplicated Pool Implementations (3 files, textually identical)

- `crates/quiver-driver-sqlite/src/pool.rs`
- `crates/quiver-driver-postgres/src/pool.rs`
- `crates/quiver-driver-mysql/src/pool.rs`

Same `new()`, `acquire()`, `idle_count()`, `max_size()` logic.

**Fix:** Generic `Pool<C: Connection>` in `quiver-driver-core/src/pool.rs`.

### Duplicated Transaction Drop (thread spawn anti-pattern)

All 3 drivers spawn a new OS thread + Tokio runtime inside `Drop` to rollback:

- `crates/quiver-driver-sqlite/src/lib.rs:354-378`
- `crates/quiver-driver-postgres/src/lib.rs:413-437`
- `crates/quiver-driver-mysql/src/lib.rs:355-378`

```rust
impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        if !self.finished {
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread().build()...;
                rt.block_on(async { /* rollback */ });
            });
            let _ = handle.join(); // blocks dropping thread
        }
    }
}
```

This can deadlock during shutdown and is expensive.

**Fix:** Require explicit `.rollback()` (aligns with biased ORM philosophy) or
use a background task queue. Either way, extract to shared code.

### Duplicated `adbc_err()` Helper

Identical one-liner in all 3 drivers:

- `crates/quiver-driver-sqlite/src/lib.rs:391`
- `crates/quiver-driver-postgres/src/lib.rs:450`
- `crates/quiver-driver-mysql/src/lib.rs:392`

**Fix:** Move to `quiver-driver-core`.

---

## High: Inconsistent Internal Types

Three separate `BoxFuture` type aliases in the same crate (`quiver-driver-core`):

- `src/async_api.rs` -- `type BoxFuture<'a, T>`
- `src/async_client.rs` -- `pub type BoxFut<'a, T>` (different name)
- `src/pool.rs` -- `type BoxFuture<'a, T>`

**Fix:** Single canonical definition in `lib.rs`, re-export everywhere.

---

## High: Leaky Abstractions

### ADBC Re-exported Publicly

- `crates/quiver-driver-core/src/lib.rs:31` -- `pub use adbc;`
- `crates/quiver-driver-sqlite/src/lib.rs:8` -- `pub use adbc_sqlite;`
- `crates/quiver-driver-postgres/src/lib.rs:11` -- `pub use adbc_postgres;`
- `crates/quiver-driver-mysql/src/lib.rs:11` -- `pub use adbc_mysql;`

Contradicts the project's own design: users should interact through Quiver's
`Connection`/`Transaction` traits, not raw ADBC. If ADBC changes, Quiver's
public API breaks unexpectedly.

**Fix:** Remove `pub use adbc*` exports. Keep ADBC as internal dependency.

### Query Builder Internals Exported

`crates/quiver-query/src/lib.rs:34-51` exports internal builder types
(`AggregateBuilder`, `CreateBuilder`, `CreateManyBuilder`, etc.) that should
only be reachable through the fluent `Query::table()...` API.

**Fix:** Keep builder types `pub(crate)`, only export `Query` entry point.

---

## Medium: No Dialect Abstraction

PG placeholder rewriting (`crates/quiver-driver-postgres/src/lib.rs:72-112`)
and MySQL DDL splitting (`crates/quiver-driver-mysql/src/lib.rs:116-133`) are
ad-hoc. No shared trait or enum for dialect-specific behavior.

**Fix:** `Dialect` trait in `driver-core` (see Connection duplication fix above).

---

## Medium: Missing Feature Flags

All 3 drivers are always compiled. Users who only need SQLite pull in PG and
MySQL dependencies. No `[features]` section at workspace level.

**Fix:**
```toml
[features]
driver-sqlite = ["quiver-driver-sqlite"]
driver-postgres = ["quiver-driver-postgres"]
driver-mysql = ["quiver-driver-mysql"]
default = ["driver-sqlite"]
```

---

## Medium: Incomplete Code Shipped

`crates/quiver-codegen/src/gen_fbs.rs:142`:
```rust
// TODO: generate separate struct definition
let _ = fields;
"string".into() // fallback: JSON string
```

FlatBuffers Struct handling silently falls back to JSON string.

---

## Medium: Test Quality

### Weak Assertions (15+ occurrences)

Tests that only check `result.is_ok()` without verifying output:
- `crates/quiver-query/src/validate.rs:337,402,432,455,479,516`
- `crates/quiver-codegen/src/gen_rust_fbs.rs:75-79`

### Magic Index Access (20+ occurrences)

`rows[0].get_string(2)` with no indication what column 2 is:
- `crates/quiver-query/tests/sqlite_integration.rs:88,167,193,214,242`

### Copy-Paste Tests

9+ validation tests in `crates/quiver-query/src/validate.rs:340-467` follow
the exact same pattern. 5 introspection tests in
`crates/quiver-migrate/tests/sqlite_integration.rs:474-615` are near-identical.

Should use parameterized test helpers.

### Overly Long Test Functions

- `crates/quiver-migrate/tests/sqlite_integration.rs:291-395` -- 104 lines
- `crates/quiver-migrate/tests/demo_real_world.rs:28-183` -- 155 lines

---

## Low: Overly Long Source Files

- `crates/quiver-migrate/src/sql_gen.rs` -- 1468 lines
- `crates/quiver-driver-sqlite/src/lib.rs` -- 760 lines

Should be split into submodules (Connection, Transaction, helpers).

---

## Improvement Priority

| Priority | Item | Impact |
|----------|------|--------|
| P0 | Extract shared driver code (Connection, Pool, Transaction) | -1500 LOC, eliminates triple-maintenance |
| P0 | Remove thread spawn from Drop | Eliminates deadlock risk |
| P1 | Add Dialect trait | Clean extension point for new drivers |
| P1 | Remove ADBC public re-exports | Clean public API |
| P1 | Unify BoxFuture definitions | Code consistency |
| P2 | Add feature flags for drivers | Smaller binaries |
| P2 | Improve test quality (parameterize, add assertions) | Better coverage signal |
| P2 | Complete FBS struct codegen | Feature completeness |
| P3 | Split long files into submodules | Readability |
| P3 | Hide internal builder types | API surface reduction |
