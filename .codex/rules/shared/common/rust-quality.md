# Rust Code Quality

1. **No Clippy Bypasses:** Do not use `#[allow(clippy::...)]` to suppress clippy warnings. Fix the underlying code instead.
   - Exception: `#[allow(dead_code, unused_imports)]` is permitted ONLY on `mod common;` declarations in integration test files (`tests/*.rs`), since shared test utility modules are compiled per-test-binary and not every test uses every helper.

2. **No Unnecessary Casts:** Do not write redundant type casts (e.g., `x as i64` when `x` is already `i64`).

3. **Implement Standard Traits:** When a method has the same signature as a standard trait (e.g., `from_str`), implement the trait (`std::str::FromStr`) instead of defining an inherent method. If the error type differs from `String`, use a different method name (e.g., `parse`).

4. **Clean Imports:** Do not leave unused imports. Remove them rather than suppressing the warning.

5. **No `#[allow(dead_code)]`:** Do not suppress dead code warnings. Remove unused code instead.
