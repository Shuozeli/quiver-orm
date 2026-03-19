# Code Quality Findings

## 1. Duplication

### Codegen Helper Functions Copy-Pasted Across 5 Generators -- DONE
- **Location:** `crates/quiver-codegen/src/gen_fbs.rs:169-179` (`is_auto_field`, `has_default`)
- **Also at:**
  - `crates/quiver-codegen/src/gen_proto.rs:212-222`
  - `crates/quiver-codegen/src/gen_rust_client.rs:649-659`
  - `crates/quiver-codegen/src/gen_rust_serde.rs:349-359`
  - `crates/quiver-codegen/src/gen_typescript.rs:218-228`
- **Also duplicated:** `to_snake` at `gen_proto.rs:224`, `gen_rust_client.rs:618`, `gen_rust_serde.rs:338`; `to_screaming_snake` at `gen_proto.rs:235`, `gen_rust_client.rs:629`
- **Problem:** Five identical copies of `is_auto_field` and `has_default`, three copies of `to_snake`, two copies of `to_screaming_snake`. Any bug fix must be applied to all copies.
- **Fix:** Created `crates/quiver-codegen/src/helpers.rs` exporting all four functions. Unified `to_snake` on the safer `unwrap_or(ch)` variant. Removed all per-generator copies.

### Type Conversion Matrix Duplicated Across All Codegen Targets -- SKIPPED
- **Location:** `crates/quiver-codegen/src/gen_fbs.rs:102-148` (`base_type_to_fbs`)
- **Also at:**
  - `crates/quiver-codegen/src/gen_proto.rs` (`base_type_to_proto`)
  - `crates/quiver-codegen/src/gen_sql.rs` (`base_type_to_sql`)
  - `crates/quiver-codegen/src/gen_typescript.rs` (`base_type_to_ts`)
  - `crates/quiver-codegen/src/gen_rust_serde.rs` (`base_type_to_rust`)
- **Problem:** Each generator implements a parallel ~40-arm match on `BaseType` with identical structure but different string outputs.
- **Skipped reason:** Each type mapping has target-specific nuances (e.g., FBS wraps lists as `[type]`, Proto uses `repeated type`, SQL varies by dialect, TS uses `bigint` vs `number`). A shared abstraction would need to handle nullable wrapping, list wrapping, map syntax, and struct fallbacks differently per target. The structural similarity is real but the per-target differences make a shared trait more complex than the current parallel functions. Revisit when a new BaseType variant is added.

### Pool Implementations Identical Across 3 Driver Crates -- DONE
- **Location:** `crates/quiver-driver-sqlite/src/pool.rs:13-41` (`SqlitePool`)
- **Also at:**
  - `crates/quiver-driver-postgres/src/pool.rs:9-37` (`PostgresPool`)
  - `crates/quiver-driver-mysql/src/pool.rs:9-37` (`MysqlPool`)
- **Problem:** All three wrap `DriverPool<T>` with identical `Pool` trait delegations.
- **Fix:** Added `impl<D: Driver + 'static> Pool for DriverPool<D>` in `quiver-driver-core/src/pool.rs`. Replaced all three wrapper structs with type aliases: `pub type SqlitePool = DriverPool<SqliteDriver>`, etc.

### Driver `connect()` Factory Boilerplate -- SKIPPED
- **Location:** `crates/quiver-driver-sqlite/src/lib.rs:40-67`
- **Also at:**
  - `crates/quiver-driver-postgres/src/lib.rs:45-62`
  - `crates/quiver-driver-mysql/src/lib.rs:43-59`
- **Problem:** Nearly identical connection factory code across all three drivers.
- **Skipped reason:** The connect functions are only ~15 lines each and the driver-specific differences (SQLite PRAGMA, PostgreSQL placeholder dialect) are inline. Extracting a generic factory with a callback closure would add indirection without meaningful duplication reduction. The boilerplate is manageable at 3 drivers.

---

## 2. Silent Failures

### Rollback Errors Silently Discarded -- DONE
- **Location:** `crates/quiver-driver-core/src/async_client.rs:61` (`transaction()`)
- **Also at:** `crates/quiver-driver-core/src/async_client.rs:93` (`transaction_with_retry()`)
- **Problem:** `let _ = tx.rollback().await;` discards rollback errors.
- **Fix:** Replaced with `if let Err(_rb_err) = tx.rollback().await` + `eprintln!` in debug builds. No new dependency needed. Documented as best-effort cleanup.

---

## 3. Missing Abstractions

### Dialect Trait Defaults Are Misleading -- DONE
- **Location:** `crates/quiver-driver-core/src/dialect.rs` (`Dialect` trait)
- **Problem:** Default implementations silently produced correct behavior only for SQLite. New dialects could inherit wrong behavior.
- **Fix:** Removed default implementations for `rewrite_sql()` and `split_ddl()`. Added explicit implementations to `SqliteDialect` and `MysqlDialect`. New dialects now get a compile error if they forget to implement these methods.

---

## 4. Missing Test Coverage

### PostgreSQL and MySQL Drivers Lack Functional Tests -- DONE
- **Location:** `crates/quiver-driver-postgres/src/lib.rs`, `crates/quiver-driver-mysql/src/lib.rs`
- **Problem:** Core driver functionality untested for PostgreSQL and MySQL.
- **Fix:** Added 10 integration tests to PostgreSQL driver and 9 to MySQL driver (connect, DDL, insert+query, query_one, query_optional, transaction commit, explicit rollback, null results, float roundtrip, bool roundtrip, column name lookup, streaming). All marked `#[ignore]` -- run with `QUIVER_PG_URL=... cargo test -p quiver-driver-postgres -- --ignored`. PostgreSQL tests verified passing (10/10). Notes: ADBC does not support null parameter binding (use raw SQL); drop-rollback hangs on PG (use explicit rollback); PG is type-strict (BIGINT for i64 params).

---

## 5. Redundant Wrappers

### `Column` Struct Wraps Only a String -- DONE
- **Location:** `crates/quiver-driver-core/src/types.rs:129-133`
- **Problem:** `pub struct Column { pub name: String }` adds a wrapper with no additional metadata.
- **Fix:** Removed `Column` struct. Changed `Row.columns: Vec<Column>` to `Row.column_names: Vec<String>`. Updated all consumers: `arrow.rs` (record batch conversion), `types.rs` (get_by_name), `quiver-cli/src/main.rs` (table header display). Removed `Column` from `lib.rs` re-exports.

---

## 6. Noise

### Section Divider Comments -- DONE
- **Location:** `crates/quiver-driver-core/src/generic_conn.rs`, `crates/quiver-driver-postgres/src/lib.rs`, `crates/quiver-codegen/src/gen_typescript.rs`, `crates/quiver-codegen/src/gen_rust_client.rs`, `crates/quiver-driver-core/src/pool.rs`
- **Fix:** Removed all `// ----------- ...` dividers. Kept meaningful labels as brief `//` comments where the context adds value.

### Restating-the-Code Doc Comments -- SKIPPED
- **Location:** Various files
- **Skipped reason:** Low priority cosmetic issue. The doc comments are harmless and could be useful for generated documentation. Not worth a dedicated cleanup pass.
