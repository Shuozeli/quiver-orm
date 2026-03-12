//! Nested writes -- create/update parent + children atomically in a transaction.
//!
//! Supports creating a parent with related children in one operation,
//! automatically handling FK assignment within a transaction.

use quiver_driver_core::{Connection, Statement, Transaction, Transactional, Value};
use quiver_error::QuiverError;

use crate::safe_ident::SafeIdent;

/// A child write that depends on the parent's generated ID.
#[derive(Debug, Clone)]
pub struct ChildWrite {
    /// Table to insert into.
    pub table: SafeIdent,
    /// Columns (excluding FK -- that's injected from parent ID).
    pub columns: Vec<SafeIdent>,
    /// Values per row (excluding FK).
    pub rows: Vec<Vec<Value>>,
}

impl ChildWrite {
    /// Create a new child write specification.
    pub fn new(table: &'static str) -> Self {
        Self {
            table: SafeIdent::new(table),
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Define columns for the child rows (excluding the FK column).
    pub fn columns(mut self, columns: &[&'static str]) -> Self {
        self.columns = columns.iter().map(|c| SafeIdent::new(c)).collect();
        self
    }

    /// Add a child row's values (FK will be prepended automatically).
    pub fn values(mut self, row: Vec<Value>) -> Self {
        self.rows.push(row);
        self
    }
}

/// Create a parent row and its children atomically in a transaction.
///
/// Executes the parent INSERT, retrieves the generated ID via
/// `last_insert_rowid()`, then inserts all children with the parent ID
/// set as the FK value.
///
/// Returns `(parent_id, child_rows_inserted)`.
pub async fn create_with_children<C>(
    conn: &mut C,
    parent_query: &Statement,
    fk_column: &'static str,
    children: &[ChildWrite],
) -> Result<(Value, u64), QuiverError>
where
    C: Transactional,
    for<'a> C::Transaction<'a>: Connection + Transaction,
{
    let tx = conn.begin().await?;

    tx.execute(parent_query).await?;

    let id_stmt = Statement::sql("SELECT last_insert_rowid()".to_string());
    let id_rows = tx.query(&id_stmt).await?;
    let parent_id = id_rows
        .first()
        .and_then(|r: &quiver_driver_core::Row| r.get(0).cloned())
        .unwrap_or(Value::Null);

    let fk_ident = SafeIdent::new(fk_column);
    let mut child_count = 0u64;

    for child in children {
        for row_values in &child.rows {
            let mut all_cols: Vec<String> = vec![fk_ident.to_quoted_sql()];
            all_cols.extend(child.columns.iter().map(|c| c.to_quoted_sql()));

            let mut all_vals = vec![parent_id.clone()];
            all_vals.extend(row_values.iter().cloned());

            let placeholders: Vec<String> =
                (1..=all_vals.len()).map(|i| format!("?{}", i)).collect();

            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                crate::safe_ident::quote_table(child.table.as_str()),
                all_cols.join(", "),
                placeholders.join(", "),
            );

            tx.execute(&Statement::new(sql, all_vals)).await?;
            child_count += 1;
        }
    }

    tx.commit().await?;
    Ok((parent_id, child_count))
}
