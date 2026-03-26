# Code Quality Findings

*Last audit: 2026-03-26*

## Previous Findings (All Resolved)

The following were found and resolved in earlier audits:

| # | Finding | Severity | Status |
|---|---------|----------|--------|
| 1.1 | SQL type-mapping duplication across codegen/migrate | Medium | FIXED |
| 1.2 | `table_name_for` duplicated 5 times across 4 crates | Low | FIXED |
| 1.3 | `column_name_for` duplicated in gen_sql/sql_gen | Low | FIXED |
| 1.5 | `SqlDialect` enum defined independently in two crates | Medium | FIXED |
| 2.1 | `arrow_array_value` silent null for unsupported types | High | FIXED |
| 2.2 | `build_column_array` silent null for unsupported types | Medium | FIXED |
| 2.3 | FBS codegen silent Struct degradation | Medium | FIXED |
| 2.4 | Proto codegen silent Struct degradation | Medium | FIXED |
| 4.2 | `let _ = fields;` suppression in gen_fbs | Low | FIXED |
| 5.1 | Per-row `column_names` clone (now `Arc<Vec<String>>`) | Medium | FIXED |

---

## Current Findings

### HIGH Severity

*(none)*

### MEDIUM Severity

#### M1. `is_collection_type` in `gen_proto.rs` is a trivial passthrough wrapper

- **File:** `crates/quiver-codegen/src/gen_proto.rs:220-222`
- **Problem:** `fn is_collection_type(base: &BaseType) -> bool` is a 1-line function that just calls `is_collection_base_type(base)` with zero additional logic. This is dead indirection that obscures the call graph.
- **Fix:** Replace all callsites with direct `is_collection_base_type()` calls and remove the wrapper.
- **Status:** FIXED

#### M2. Double validation in `load_schema` (CLI)

- **File:** `crates/quiver-cli/src/main.rs:147-158`
- **Problem:** `quiver_schema::parse()` already calls `validate()` internally and returns the first error. Then `load_schema` calls `validate()` again, which re-runs all 6 validation passes. While harmless for correctness, it is wasteful and confusing -- it suggests the code author did not realize `parse()` already validates.
- **Fix:** Remove the redundant `validate()` call. Use `parse()` which already validates, or use `parse_unvalidated()` + manual `validate()` if multi-error reporting is desired.
- **Status:** FIXED

#### M3. Unused `_num_rows` parameter in `build_column_array`

- **File:** `crates/quiver-driver-core/src/arrow.rs:373`
- **Problem:** `_num_rows: usize` is passed but never used. The underscore prefix suppresses the warning, but the parameter is dead code that clutters the API.
- **Fix:** Remove the parameter and update the single callsite.
- **Status:** FIXED

### LOW Severity

#### L1. `is_collection_type` in `gen_rust_client.rs` has unused `_schema` parameter

- **File:** `crates/quiver-codegen/src/gen_rust_client.rs:578`
- **Problem:** `fn is_collection_type(type_expr: &TypeExpr, _schema: &Schema) -> bool` -- the `_schema` parameter is unused, indicated by the underscore prefix. Unlike the TypeScript case (which needs schema for recursion), this function only checks the base type.
- **Fix:** Remove the `_schema` parameter and update callsites.
- **Status:** FIXED

#### L2. `#[allow(clippy::only_used_in_recursion)]` in `gen_typescript.rs`

- **File:** `crates/quiver-codegen/src/gen_typescript.rs:117`
- **Problem:** The `schema` parameter in `base_type_to_ts` is passed recursively but never actually used in any arm of the match. The clippy suppression hides this.
- **Fix:** Remove the `schema` parameter since it is not used in any code path, and remove the `#[allow]`.
- **Status:** FIXED

#### L3. `gen_field_constants` in `gen_typescript.rs` has unused `_schema` parameter

- **File:** `crates/quiver-codegen/src/gen_typescript.rs:109`
- **Problem:** `fn gen_field_constants(out: &mut String, m: &ModelDef, _schema: &Schema)` -- `_schema` is never used.
- **Fix:** Remove the `_schema` parameter and update the callsite.
- **Status:** FIXED

#### L4. Empty branch in `check_primary_keys` validation

- **File:** `crates/quiver-schema/src/validate.rs:156-159`
- **Problem:** Empty `if !has_field_id && !has_model_id { }` branch with comment about warnings. This is dead code that does nothing.
- **Fix:** Remove the empty branch. Add a TODO comment if warning infrastructure is planned.
- **Status:** SKIPPED (requires warnings infrastructure)

## Summary

| # | Finding | Severity | Status |
|---|---------|----------|--------|
| M1 | `is_collection_type` passthrough in gen_proto | Medium | FIXED |
| M2 | Double validation in CLI load_schema | Medium | FIXED |
| M3 | Unused `_num_rows` param in build_column_array | Medium | FIXED |
| L1 | Unused `_schema` param in gen_rust_client | Low | FIXED |
| L2 | Clippy suppression hiding dead param in gen_typescript | Low | FIXED |
| L3 | Unused `_schema` in gen_typescript field_constants | Low | FIXED |
| L4 | Empty branch in check_primary_keys | Low | SKIPPED |
