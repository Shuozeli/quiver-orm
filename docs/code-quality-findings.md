# Code Quality Findings

## 1. Duplication

### 1.1 SQL type-mapping functions duplicated across `quiver-codegen` and `quiver-migrate` -- FIXED
- **Location:** `crates/quiver-codegen/src/gen_sql.rs` / `crates/quiver-migrate/src/sql_gen.rs`
- **Problem:** ~200 lines of identical SQL type-mapping functions (`base_type_to_sql`, `base_type_to_sqlite`, `base_type_to_postgres`, `base_type_to_mysql`) and `SqlDialect` enum duplicated across two crates.
- **Fix applied:** Extracted all SQL type-mapping functions and `SqlDialect` enum into `crates/quiver-schema/src/sql_types.rs`. Both `quiver-codegen` and `quiver-migrate` now import from the shared module.

### 1.2 `table_name_for` / `get_table_name` duplicated 5 times across 4 crates -- FIXED
- **Location:** 6 independent implementations across `gen_sql.rs`, `gen_rust_client.rs`, `gen_rust_serde.rs`, `diff.rs`, `sql_gen.rs`, `validate.rs`
- **Problem:** Same logic (iterate model attributes, return `Map(name)` if present, else model name) duplicated with varying return types.
- **Fix applied:** Added `ModelDef::table_name() -> &str` method in `crates/quiver-schema/src/ast.rs`. All 6 callers now use this method; duplicate functions removed.

### 1.3 `column_name_for` duplicated in `gen_sql.rs` and `sql_gen.rs` -- FIXED
- **Location:** `crates/quiver-codegen/src/gen_sql.rs` / `crates/quiver-migrate/src/sql_gen.rs` / `crates/quiver-query/src/validate.rs`
- **Problem:** Identical functions for resolving field-level MAP attribute rename.
- **Fix applied:** Added `FieldDef::column_name() -> &str` method in `crates/quiver-schema/src/ast.rs`. All callers now use this method; duplicate functions removed.

### 1.4 `is_collection_type` wrapper near-duplicate of `is_collection_base_type` -- SKIPPED
- **Location:** `crates/quiver-codegen/src/gen_proto.rs`, `gen_rust_client.rs`, `helpers.rs`
- **Problem:** Trivial wrapper functions around `is_collection_base_type`.
- **Skipped reason:** Low impact -- wrappers are 1-2 lines each and one variant includes `Struct` in the check. Not worth the churn for cosmetic cleanup.

### 1.5 `SqlDialect` enum defined independently in two crates -- FIXED
- **Location:** `crates/quiver-codegen/src/gen_sql.rs` / `crates/quiver-migrate/src/sql_gen.rs`
- **Problem:** Two independent `SqlDialect` enums with same variants; CLI had to map between them.
- **Fix applied:** Unified as part of 1.1 fix. `SqlDialect` now defined once in `crates/quiver-schema/src/sql_types.rs` and re-exported by both crates.

## 2. Silent Failures

### 2.1 `arrow_array_value` returns `Value::Null` for unsupported Arrow types -- FIXED
- **Location:** `crates/quiver-driver-core/src/arrow.rs`
- **Problem:** Catch-all `_ => Value::Null` silently dropped data for Date32, Date64, Timestamp, Time32, Time64, Decimal128, Decimal256 types. **High priority -- silent data loss.**
- **Fix applied:** Added explicit match arms for all temporal and decimal types with proper conversion (ISO 8601 strings for temporals, decimal string formatting for Decimal128/256). Catch-all now returns `Err(QuiverError::Driver(...))`.

### 2.2 `build_column_array` returns `NullArray` for unsupported types -- FIXED
- **Location:** `crates/quiver-driver-core/src/arrow.rs`
- **Problem:** Reverse direction of 2.1 -- unsupported column types silently became null arrays.
- **Fix applied:** Catch-all now returns `Err(QuiverError::Driver(...))` instead of `NullArray`. Return type changed to `Result<Arc<dyn Array>, QuiverError>`.

### 2.3 FBS codegen silently degrades Struct types to `"string"` -- FIXED
- **Location:** `crates/quiver-codegen/src/gen_fbs.rs`
- **Problem:** `Struct(fields)` arm used `let _ = fields;` to suppress warning and fell back to `"string"`.
- **Fix applied:** Added upfront validation in `FbsGenerator::generate()` that returns `Err(QuiverError::Codegen(...))` for Struct fields. Pattern changed to `Struct(_)`.

### 2.4 Proto codegen silently degrades Struct types to `"bytes"` -- FIXED
- **Location:** `crates/quiver-codegen/src/gen_proto.rs`
- **Problem:** `Struct(_) => "bytes"` silently lost type information.
- **Fix applied:** Same approach as 2.3 -- upfront validation returns a clear error for Struct fields.

## 3. Dead / No-Op Code

### 3.1 Empty branch in `check_primary_keys` -- SKIPPED
- **Location:** `crates/quiver-schema/src/validate.rs:156-159`
- **Problem:** Empty branch body with comment about warnings.
- **Skipped reason:** Low priority. Requires a warnings infrastructure that does not exist yet.

### 3.2 `type_to_fbs` is a trivial passthrough -- SKIPPED
- **Location:** `crates/quiver-codegen/src/gen_fbs.rs`
- **Problem:** `type_to_fbs` just calls `base_type_to_fbs`, ignoring nullability.
- **Skipped reason:** Low priority / cosmetic. The indirection may be useful if nullability handling is added later.

### 3.3 Double validation in `load_schema` (CLI) -- SKIPPED
- **Location:** `crates/quiver-cli/src/main.rs`
- **Problem:** Schema validated twice per CLI invocation.
- **Skipped reason:** Low priority. Validation is fast and the duplicate call is harmless.

## 4. Suppressions

### 4.1 `#[allow(clippy::only_used_in_recursion)]` may hide a real issue -- SKIPPED
- **Location:** `crates/quiver-codegen/src/gen_typescript.rs:117`
- **Problem:** `schema` parameter is unused in non-recursive arms.
- **Skipped reason:** Low priority. Fixing requires deciding whether enum vs model distinction matters for TypeScript output.

### 4.2 `let _ = fields;` suppresses unused variable instead of proper pattern -- FIXED
- **Location:** `crates/quiver-codegen/src/gen_fbs.rs`
- **Problem:** Non-idiomatic suppression of unused variable.
- **Fix applied:** Changed to `Struct(_)` pattern as part of 2.3 fix.

## 5. Performance

### 5.1 `Row.column_names` cloned per row in `record_batch_to_rows` -- FIXED
- **Location:** `crates/quiver-driver-core/src/arrow.rs` and `crates/quiver-driver-core/src/types.rs`
- **Problem:** `column_names.clone()` created a full `Vec<String>` copy per row.
- **Fix applied:** Changed `Row.column_names` from `Vec<String>` to `Arc<Vec<String>>`. All rows in a batch now share a single allocation.

### 5.2 Parser `peek()` clones tokens including heap-allocated strings -- SKIPPED
- **Location:** `crates/quiver-schema/src/parser.rs`
- **Problem:** `peek()` clones every token, including those with heap-allocated strings.
- **Skipped reason:** Low-medium priority. Requires changing `peek()` to return `&Token` and updating all callers. Parser performance is not a bottleneck for typical schema sizes.

## 6. Stringly-Typed API

### 6.1 CLI `--target` argument is a raw string -- SKIPPED
- **Location:** `crates/quiver-cli/src/main.rs`
- **Problem:** Free-form string matched against ~15 patterns at runtime.
- **Skipped reason:** Low priority. Would be nice but does not cause bugs in practice.

## 7. Architecture Notes (Informational)

### 7.1 `AdbcTransaction::Drop` spawns a thread for async rollback
- **Location:** `crates/quiver-driver-core/src/generic_conn.rs`
- **Note:** Known limitation of async Rust (no async Drop). Current approach is correct for safety. Informational only.

### 7.2 `QuiverError` uses `String` for most error variants
- **Location:** `crates/quiver-error/src/lib.rs`
- **Note:** Acceptable for current project size. Structured error variants can be added as the ORM matures. Informational only.

## Summary

All high and medium severity findings have been addressed:

| # | Finding | Severity | Status |
|---|---------|----------|--------|
| 1.1 | SQL type-mapping duplication | Medium | FIXED |
| 1.2 | table_name_for duplication | Low | FIXED |
| 1.3 | column_name_for duplication | Low | FIXED |
| 1.4 | is_collection_type wrapper | Low | SKIPPED |
| 1.5 | SqlDialect duplication | Medium | FIXED |
| 2.1 | arrow_array_value silent null | High | FIXED |
| 2.2 | build_column_array silent null | Medium | FIXED |
| 2.3 | FBS Struct silent degradation | Medium | FIXED |
| 2.4 | Proto Struct silent degradation | Medium | FIXED |
| 3.1 | Empty branch in check_primary_keys | Low | SKIPPED |
| 3.2 | type_to_fbs passthrough | Low | SKIPPED |
| 3.3 | Double validation in CLI | Low | SKIPPED |
| 4.1 | Clippy suppression in gen_typescript | Low | SKIPPED |
| 4.2 | let _ = fields suppression | Low | FIXED |
| 5.1 | Per-row column_names clone | Medium | FIXED |
| 5.2 | Parser peek() clones | Low | SKIPPED |
| 6.1 | CLI --target raw string | Low | SKIPPED |
