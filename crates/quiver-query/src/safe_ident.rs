//! SQL injection prevention via compile-time identifier validation.
//!
//! All table and column names in queries must be `&'static str`, ensuring they
//! are string literals known at compile time. This makes it impossible to
//! construct SQL identifiers from user input at runtime.
//!
//! # Design
//!
//! [`SafeIdent`] wraps a `&'static str` and validates it contains only safe
//! characters (alphanumeric, underscore, dot for table.column qualification).
//! The validation happens at construction time, and since the inner string is
//! `'static`, it cannot come from user input.
//!
//! ```ignore
//! // Compiles: string literal
//! let ident = SafeIdent::new("users");
//!
//! // Won't compile: String is not &'static str
//! let user_input = String::from("users; DROP TABLE users");
//! let ident = SafeIdent::new(&user_input); // ERROR: lifetime mismatch
//! ```

use std::fmt;

/// A validated SQL identifier that can only be constructed from `&'static str`.
///
/// This prevents SQL injection by ensuring identifiers are compile-time
/// constants that cannot be influenced by runtime user input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SafeIdent {
    value: &'static str,
}

impl SafeIdent {
    /// Create a new safe identifier from a static string literal.
    ///
    /// # Panics
    ///
    /// Panics if the identifier contains characters other than alphanumeric,
    /// underscore, or dot (for table.column qualification).
    pub const fn new(value: &'static str) -> Self {
        assert!(!value.is_empty(), "identifier must not be empty");

        let bytes = value.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            assert!(
                b.is_ascii_alphanumeric() || b == b'_' || b == b'.',
                "identifier contains invalid character"
            );
            i += 1;
        }

        Self { value }
    }

    /// Get the raw identifier string.
    pub const fn as_str(&self) -> &'static str {
        self.value
    }

    /// Render as a quoted SQL identifier.
    ///
    /// Handles `table.column` -> `"table"."column"`.
    pub fn to_quoted_sql(&self) -> String {
        quote_ident(self.value)
    }
}

impl fmt::Display for SafeIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.value)
    }
}

/// Quote an identifier for SQL, handling `table.column` -> `"table"."column"`.
/// Escapes embedded double quotes (`"` -> `""`) per SQL standard.
pub(crate) fn quote_ident(ident: &str) -> String {
    if let Some((table, column)) = ident.split_once('.') {
        format!(
            "\"{}\".\"{}\"",
            table.replace('"', "\"\""),
            column.replace('"', "\"\"")
        )
    } else {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }
}

/// Quote a table name for SQL.
/// Escapes embedded double quotes (`"` -> `""`) per SQL standard.
pub(crate) fn quote_table(table: &str) -> String {
    format!("\"{}\"", table.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_identifiers() {
        let _ = SafeIdent::new("users");
        let _ = SafeIdent::new("user_id");
        let _ = SafeIdent::new("User");
        let _ = SafeIdent::new("Account.name");
        let _ = SafeIdent::new("post123");
    }

    #[test]
    #[should_panic(expected = "identifier contains invalid character")]
    fn reject_semicolon() {
        let _ = SafeIdent::new("users;");
    }

    #[test]
    #[should_panic(expected = "identifier contains invalid character")]
    fn reject_quotes() {
        let _ = SafeIdent::new("users\"");
    }

    #[test]
    #[should_panic(expected = "identifier contains invalid character")]
    fn reject_space() {
        let _ = SafeIdent::new("users DROP");
    }

    #[test]
    #[should_panic(expected = "identifier contains invalid character")]
    fn reject_parenthesis() {
        let _ = SafeIdent::new("users()");
    }

    #[test]
    #[should_panic(expected = "identifier must not be empty")]
    fn reject_empty() {
        let _ = SafeIdent::new("");
    }

    #[test]
    fn quoted_sql_simple() {
        let ident = SafeIdent::new("users");
        assert_eq!(ident.to_quoted_sql(), "\"users\"");
    }

    #[test]
    fn quoted_sql_table_column() {
        let ident = SafeIdent::new("User.email");
        assert_eq!(ident.to_quoted_sql(), "\"User\".\"email\"");
    }

    #[test]
    fn static_str_only() {
        // This test documents the safety guarantee.
        // The following would NOT compile because String is not &'static str:
        //
        //   let user_input = String::from("malicious");
        //   let _ = SafeIdent::new(&user_input); // compile error!
        //
        // Only string literals (compile-time constants) can be used.
        let ident = SafeIdent::new("safe_column");
        assert_eq!(ident.as_str(), "safe_column");
    }

    #[test]
    fn const_construction() {
        // SafeIdent::new is const, so it can be used in const contexts
        const IDENT: SafeIdent = SafeIdent::new("users");
        assert_eq!(IDENT.as_str(), "users");
    }
}
