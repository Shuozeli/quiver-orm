//! Error message sanitization to prevent credential leaks.
//!
//! Database driver errors may include connection URLs containing embedded
//! passwords (e.g., `postgres://user:secret@host/db`). This module strips
//! credentials from error messages before they reach the user.

/// Redact credentials from a database error message.
///
/// Replaces `://user:password@` patterns with `://***:***@` in the error
/// string. Handles common URL schemes: postgres, postgresql, mysql, sqlite.
pub fn sanitize_connection_error(msg: &str) -> String {
    // Match patterns like "scheme://user:pass@" and redact user:pass.
    // This is a simple approach that handles the common case without pulling
    // in a URL parser dependency.
    let mut result = msg.to_string();
    for scheme in &[
        "postgres://",
        "postgresql://",
        "mysql://",
        "sqlite://",
        "host=",
    ] {
        if let Some(scheme_start) = result.find(scheme) {
            let after_scheme = scheme_start + scheme.len();
            // Look for @ which terminates the userinfo section
            if let Some(at_offset) = result[after_scheme..].find('@') {
                let at_pos = after_scheme + at_offset;
                // Replace everything between scheme and @ with redacted
                result.replace_range(after_scheme..at_pos, "***:***");
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_postgres_url() {
        let msg = "connection error: postgres://admin:s3cret@localhost:5432/mydb";
        let sanitized = sanitize_connection_error(msg);
        assert_eq!(
            sanitized,
            "connection error: postgres://***:***@localhost:5432/mydb"
        );
    }

    #[test]
    fn redacts_mysql_url() {
        let msg = "failed to connect: mysql://root:password123@db.example.com/app";
        let sanitized = sanitize_connection_error(msg);
        assert_eq!(
            sanitized,
            "failed to connect: mysql://***:***@db.example.com/app"
        );
    }

    #[test]
    fn preserves_message_without_url() {
        let msg = "column 'id' not found";
        assert_eq!(sanitize_connection_error(msg), msg);
    }

    #[test]
    fn preserves_url_without_credentials() {
        // No @ means no userinfo to redact
        let msg = "error connecting to postgres://localhost:5432/db";
        assert_eq!(sanitize_connection_error(msg), msg);
    }

    #[test]
    fn redacts_postgresql_scheme() {
        let msg = "postgresql://user:pass@host/db: connection refused";
        let sanitized = sanitize_connection_error(msg);
        assert_eq!(
            sanitized,
            "postgresql://***:***@host/db: connection refused"
        );
    }
}
