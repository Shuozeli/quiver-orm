//! Migration tracker -- records which migrations have been applied.

use quiver_driver_core::{Connection, DdlStatement, Statement, Value};
use quiver_error::QuiverError;

use crate::sql_gen::TrustedSql;
use crate::step::Migration;

const MIGRATIONS_TABLE: &str = "_quiver_migrations";

/// Tracks applied migrations in a database table.
pub struct MigrationTracker;

impl MigrationTracker {
    /// Ensure the migrations tracking table exists.
    pub async fn ensure_table(conn: &dyn Connection) -> Result<(), QuiverError> {
        let ddl = DdlStatement::new(format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" (\
             id TEXT PRIMARY KEY, \
             description TEXT NOT NULL, \
             applied_at TEXT NOT NULL, \
             checksum TEXT NOT NULL\
             )",
            MIGRATIONS_TABLE
        ));
        conn.execute_ddl(&ddl).await
    }

    /// List all applied migration IDs in order.
    pub async fn applied(conn: &dyn Connection) -> Result<Vec<String>, QuiverError> {
        Self::ensure_table(conn).await?;
        let stmt = Statement::new(
            format!("SELECT id FROM \"{}\" ORDER BY id ASC", MIGRATIONS_TABLE),
            Vec::new(),
        );
        let rows = conn.query(&stmt).await?;
        let mut ids = Vec::new();
        for row in rows {
            if let Some(Value::Text(id)) = row.values.first() {
                ids.push(id.clone());
            }
        }
        Ok(ids)
    }

    /// Check if a specific migration has been applied.
    pub async fn is_applied(
        conn: &dyn Connection,
        migration_id: &str,
    ) -> Result<bool, QuiverError> {
        Self::ensure_table(conn).await?;
        let stmt = Statement::new(
            format!("SELECT COUNT(*) FROM \"{}\" WHERE id = ?", MIGRATIONS_TABLE),
            vec![Value::Text(migration_id.to_string())],
        );
        let row = conn.query_one(&stmt).await?;
        match row.values.first() {
            Some(Value::Int(n)) => Ok(*n > 0),
            _ => Ok(false),
        }
    }

    /// Record a migration as applied.
    ///
    /// `applied_at` must be provided by the caller (no implicit system time).
    pub async fn record_applied(
        conn: &dyn Connection,
        migration: &Migration,
        applied_at: &str,
    ) -> Result<(), QuiverError> {
        Self::ensure_table(conn).await?;
        let checksum = compute_checksum(&migration.up);
        let stmt = Statement::new(
            format!(
                "INSERT INTO \"{}\" (id, description, applied_at, checksum) VALUES (?, ?, ?, ?)",
                MIGRATIONS_TABLE
            ),
            vec![
                Value::Text(migration.id.clone()),
                Value::Text(migration.description.clone()),
                Value::Text(applied_at.to_string()),
                Value::Text(checksum),
            ],
        );
        conn.execute(&stmt).await?;
        Ok(())
    }

    /// Remove a migration record (for rollback).
    pub async fn record_reverted(
        conn: &dyn Connection,
        migration_id: &str,
    ) -> Result<(), QuiverError> {
        Self::ensure_table(conn).await?;
        let stmt = Statement::new(
            format!("DELETE FROM \"{}\" WHERE id = ?", MIGRATIONS_TABLE),
            vec![Value::Text(migration_id.to_string())],
        );
        conn.execute(&stmt).await?;
        Ok(())
    }

    /// Execute a [`TrustedSql`] statement against a connection.
    ///
    /// If the statement has bind parameters, uses parameterized `execute`.
    /// Otherwise, uses `execute_ddl` for pure DDL.
    ///
    /// Multi-statement `TrustedSql` (e.g. `CREATE TABLE ...; INSERT INTO ...`)
    /// is split on `";\n"`. Sub-statements without `?` placeholders are executed
    /// as DDL; sub-statements with placeholders are executed with the
    /// corresponding slice of bind parameters.
    async fn exec_trusted(conn: &dyn Connection, trusted: &TrustedSql) -> Result<(), QuiverError> {
        if trusted.has_params() {
            let parts: Vec<&str> = trusted.sql.split(";\n").collect();
            if parts.len() == 1 {
                let stmt = Statement::new(trusted.sql.clone(), trusted.params.clone());
                conn.execute(&stmt).await?;
            } else {
                let mut param_idx = 0;
                for part in &parts {
                    let trimmed = part.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let q_count = trimmed.matches('?').count();
                    if q_count == 0 {
                        conn.execute_ddl(&DdlStatement::new(trimmed.to_string()))
                            .await?;
                    } else {
                        let end = param_idx + q_count;
                        if end > trusted.params.len() {
                            return Err(QuiverError::Migration(format!(
                                "TrustedSql has {} placeholders but only {} params (at offset {})",
                                q_count,
                                trusted.params.len().saturating_sub(param_idx),
                                param_idx,
                            )));
                        }
                        let params = trusted.params[param_idx..end].to_vec();
                        param_idx = end;
                        let stmt = Statement::new(trimmed.to_string(), params);
                        conn.execute(&stmt).await?;
                    }
                }
            }
        } else {
            for part in trusted.sql.split(";\n") {
                let trimmed = part.trim();
                if !trimmed.is_empty() {
                    conn.execute_ddl(&DdlStatement::new(trimmed.to_string()))
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Apply a migration: execute all `up` statements and record it.
    ///
    /// `applied_at` must be provided by the caller.
    pub async fn apply(
        conn: &dyn Connection,
        migration: &Migration,
        applied_at: &str,
    ) -> Result<(), QuiverError> {
        if Self::is_applied(conn, &migration.id).await? {
            return Err(QuiverError::Migration(format!(
                "Migration '{}' is already applied",
                migration.id
            )));
        }

        for trusted_sql in &migration.up {
            Self::exec_trusted(conn, trusted_sql).await?;
        }

        Self::record_applied(conn, migration, applied_at).await?;
        Ok(())
    }

    /// Rollback a migration: execute all `down` statements and remove the record.
    pub async fn rollback(conn: &dyn Connection, migration: &Migration) -> Result<(), QuiverError> {
        if !Self::is_applied(conn, &migration.id).await? {
            return Err(QuiverError::Migration(format!(
                "Migration '{}' is not applied",
                migration.id
            )));
        }

        for trusted_sql in &migration.down {
            Self::exec_trusted(conn, trusted_sql).await?;
        }

        Self::record_reverted(conn, &migration.id).await?;
        Ok(())
    }
}

/// Simple checksum of migration SQL statements for drift detection.
fn compute_checksum(statements: &[TrustedSql]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    for s in statements {
        s.sql.hash(&mut hasher);
        // Include params in checksum so different enum values produce different checksums.
        for p in &s.params {
            format!("{:?}", p).hash(&mut hasher);
        }
    }
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_deterministic() {
        let stmts = vec![TrustedSql {
            sql: "CREATE TABLE foo (id INT)".to_string(),
            params: Vec::new(),
        }];
        let c1 = compute_checksum(&stmts);
        let c2 = compute_checksum(&stmts);
        assert_eq!(c1, c2);
    }

    #[test]
    fn checksum_changes_with_content() {
        let a = compute_checksum(&[TrustedSql {
            sql: "CREATE TABLE foo (id INT)".to_string(),
            params: Vec::new(),
        }]);
        let b = compute_checksum(&[TrustedSql {
            sql: "CREATE TABLE bar (id INT)".to_string(),
            params: Vec::new(),
        }]);
        assert_ne!(a, b);
    }
}
