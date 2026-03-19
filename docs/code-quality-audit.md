# Quiver ORM -- Code Quality Audit

Audit date: 2026-03-17. Status updated: 2026-03-19.

## Critical: Driver Crate Copy-Paste Duplication

### Duplicated Connection Trait Impls (~500 LOC) -- RESOLVED

**Status:** Fixed. All Connection/Transaction impls are now in a single generic
`AdbcConnection<D: Dialect>` and `AdbcTransaction<D>` in
`crates/quiver-driver-core/src/generic_conn.rs`. Driver crates define only a
`Dialect` impl and export type aliases (e.g., `type SqliteConnection = AdbcConnection<SqliteDialect>`).

### Duplicated Pool Implementations (3 files) -- RESOLVED

**Status:** Fixed. `DriverPool<D: Driver>` in `crates/quiver-driver-core/src/pool.rs`
provides a generic pool. Driver pool types are type aliases
(e.g., `type SqlitePool = DriverPool<SqliteDriver>`).

### Duplicated Transaction Drop (thread spawn anti-pattern) -- OPEN

The generic `AdbcTransaction<D>` in `crates/quiver-driver-core/src/generic_conn.rs`
still spawns a new OS thread + Tokio runtime inside `Drop` to rollback.
This is now in a single location rather than duplicated across 3 drivers.

**Fix:** Require explicit `.rollback()` (aligns with biased ORM philosophy) or
use a background task queue.

### Duplicated `adbc_err()` Helper -- RESOLVED

**Status:** Fixed. Centralized in `crates/quiver-driver-core/src/helpers.rs`.
Includes credential sanitization via `sanitize_connection_error()`.

---

## High: Inconsistent Internal Types -- RESOLVED

**Status:** Fixed. Single `BoxFuture` definition in `src/async_api.rs`, re-exported
from `lib.rs`. All modules use the same type.

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

## Medium: No Dialect Abstraction -- RESOLVED

**Status:** Fixed. `Dialect` trait in `crates/quiver-driver-core/src/dialect.rs`
with `rewrite_sql()` and `split_ddl()` methods. No default implementations --
new dialects get a compile error if they forget to implement these methods.
Implementations: `SqliteDialect`, `PostgresDialect`, `MysqlDialect`.

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

| Priority | Item | Status |
|----------|------|--------|
| ~~P0~~ | ~~Extract shared driver code (Connection, Pool, Transaction)~~ | DONE -- AdbcConnection<D>, DriverPool<D> |
| P0 | Remove thread spawn from Drop | OPEN -- now in single location |
| ~~P1~~ | ~~Add Dialect trait~~ | DONE -- Dialect in driver-core |
| P1 | Remove ADBC public re-exports | OPEN |
| ~~P1~~ | ~~Unify BoxFuture definitions~~ | DONE -- single definition |
| P2 | Add feature flags for drivers | OPEN |
| P2 | Improve test quality (parameterize, add assertions) | OPEN |
| P2 | Complete FBS struct codegen | OPEN |
| P3 | Split long files into submodules | OPEN |
| P3 | Hide internal builder types | OPEN |
