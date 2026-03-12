use quiver_driver_core::Value;

use crate::safe_ident::SafeIdent;

/// A filter condition for WHERE clauses.
#[derive(Debug, Clone)]
pub enum Filter {
    /// column = value
    Eq(SafeIdent, Value),
    /// column != value
    Neq(SafeIdent, Value),
    /// column > value
    Gt(SafeIdent, Value),
    /// column >= value
    Gte(SafeIdent, Value),
    /// column < value
    Lt(SafeIdent, Value),
    /// column <= value
    Lte(SafeIdent, Value),
    /// column IN (values...)
    In(SafeIdent, Vec<Value>),
    /// column NOT IN (values...)
    NotIn(SafeIdent, Vec<Value>),
    /// column LIKE pattern
    Like(SafeIdent, Value),
    /// column IS NULL
    IsNull(SafeIdent),
    /// column IS NOT NULL
    IsNotNull(SafeIdent),
    /// column BETWEEN low AND high
    Between(SafeIdent, Value, Value),
    /// AND of multiple filters
    And(Vec<Filter>),
    /// OR of multiple filters
    Or(Vec<Filter>),
    /// NOT filter
    Not(Box<Filter>),
    /// Raw SQL filter expression (for subqueries, EXISTS, etc.)
    Raw { sql: String },
}

impl Filter {
    pub fn eq(column: &'static str, value: impl Into<Value>) -> Self {
        Filter::Eq(SafeIdent::new(column), value.into())
    }

    pub fn neq(column: &'static str, value: impl Into<Value>) -> Self {
        Filter::Neq(SafeIdent::new(column), value.into())
    }

    pub fn gt(column: &'static str, value: impl Into<Value>) -> Self {
        Filter::Gt(SafeIdent::new(column), value.into())
    }

    pub fn gte(column: &'static str, value: impl Into<Value>) -> Self {
        Filter::Gte(SafeIdent::new(column), value.into())
    }

    pub fn lt(column: &'static str, value: impl Into<Value>) -> Self {
        Filter::Lt(SafeIdent::new(column), value.into())
    }

    pub fn lte(column: &'static str, value: impl Into<Value>) -> Self {
        Filter::Lte(SafeIdent::new(column), value.into())
    }

    pub fn is_in(column: &'static str, values: Vec<Value>) -> Self {
        Filter::In(SafeIdent::new(column), values)
    }

    pub fn not_in(column: &'static str, values: Vec<Value>) -> Self {
        Filter::NotIn(SafeIdent::new(column), values)
    }

    pub fn like(column: &'static str, pattern: impl Into<Value>) -> Self {
        Filter::Like(SafeIdent::new(column), pattern.into())
    }

    pub fn is_null(column: &'static str) -> Self {
        Filter::IsNull(SafeIdent::new(column))
    }

    pub fn is_not_null(column: &'static str) -> Self {
        Filter::IsNotNull(SafeIdent::new(column))
    }

    pub fn between(column: &'static str, low: impl Into<Value>, high: impl Into<Value>) -> Self {
        Filter::Between(SafeIdent::new(column), low.into(), high.into())
    }

    pub fn and(filters: Vec<Filter>) -> Self {
        Filter::And(filters)
    }

    pub fn or(filters: Vec<Filter>) -> Self {
        Filter::Or(filters)
    }

    pub fn negate(filter: Filter) -> Self {
        Filter::Not(Box::new(filter))
    }

    /// Create a raw SQL filter expression.
    ///
    /// Use for subqueries, EXISTS, or other SQL that cannot be expressed
    /// through the typed filter API. The SQL is included verbatim -- do not
    /// pass user input.
    pub fn raw(sql: &'static str) -> Self {
        Filter::Raw {
            sql: sql.to_string(),
        }
    }

    /// Filter using a subquery: `column IN (SELECT ...)`.
    ///
    /// Both `column` and `subquery_sql` must be compile-time literals.
    pub fn in_subquery(column: &'static str, subquery_sql: &'static str) -> Self {
        Filter::Raw {
            sql: format!("\"{}\" IN ({})", column, subquery_sql),
        }
    }

    /// Filter using an EXISTS subquery.
    ///
    /// `subquery_sql` must be a compile-time literal.
    pub fn exists(subquery_sql: &'static str) -> Self {
        Filter::Raw {
            sql: format!("EXISTS ({})", subquery_sql),
        }
    }

    /// Create a filter from a validated SafeIdent (for internal use by include system).
    pub(crate) fn in_ident(ident: SafeIdent, values: Vec<Value>) -> Self {
        Filter::In(ident, values)
    }

    /// Render to SQL with positional parameters (?N for SQLite).
    /// Returns (sql_fragment, params consumed).
    pub(crate) fn to_sql(&self, param_offset: &mut usize) -> (String, Vec<Value>) {
        match self {
            Filter::Eq(col, val) => {
                *param_offset += 1;
                (
                    format!("{} = ?{}", col.to_quoted_sql(), *param_offset),
                    vec![val.clone()],
                )
            }
            Filter::Neq(col, val) => {
                *param_offset += 1;
                (
                    format!("{} != ?{}", col.to_quoted_sql(), *param_offset),
                    vec![val.clone()],
                )
            }
            Filter::Gt(col, val) => {
                *param_offset += 1;
                (
                    format!("{} > ?{}", col.to_quoted_sql(), *param_offset),
                    vec![val.clone()],
                )
            }
            Filter::Gte(col, val) => {
                *param_offset += 1;
                (
                    format!("{} >= ?{}", col.to_quoted_sql(), *param_offset),
                    vec![val.clone()],
                )
            }
            Filter::Lt(col, val) => {
                *param_offset += 1;
                (
                    format!("{} < ?{}", col.to_quoted_sql(), *param_offset),
                    vec![val.clone()],
                )
            }
            Filter::Lte(col, val) => {
                *param_offset += 1;
                (
                    format!("{} <= ?{}", col.to_quoted_sql(), *param_offset),
                    vec![val.clone()],
                )
            }
            Filter::In(col, vals) => {
                let mut params = Vec::new();
                let placeholders: Vec<String> = vals
                    .iter()
                    .map(|v| {
                        *param_offset += 1;
                        params.push(v.clone());
                        format!("?{}", *param_offset)
                    })
                    .collect();
                (
                    format!("{} IN ({})", col.to_quoted_sql(), placeholders.join(", ")),
                    params,
                )
            }
            Filter::NotIn(col, vals) => {
                let mut params = Vec::new();
                let placeholders: Vec<String> = vals
                    .iter()
                    .map(|v| {
                        *param_offset += 1;
                        params.push(v.clone());
                        format!("?{}", *param_offset)
                    })
                    .collect();
                (
                    format!(
                        "{} NOT IN ({})",
                        col.to_quoted_sql(),
                        placeholders.join(", ")
                    ),
                    params,
                )
            }
            Filter::Like(col, val) => {
                *param_offset += 1;
                (
                    format!("{} LIKE ?{}", col.to_quoted_sql(), *param_offset),
                    vec![val.clone()],
                )
            }
            Filter::IsNull(col) => (format!("{} IS NULL", col.to_quoted_sql()), vec![]),
            Filter::IsNotNull(col) => (format!("{} IS NOT NULL", col.to_quoted_sql()), vec![]),
            Filter::Between(col, low, high) => {
                *param_offset += 1;
                let p1 = *param_offset;
                *param_offset += 1;
                let p2 = *param_offset;
                (
                    format!("{} BETWEEN ?{} AND ?{}", col.to_quoted_sql(), p1, p2),
                    vec![low.clone(), high.clone()],
                )
            }
            Filter::And(filters) => {
                let mut parts = Vec::new();
                let mut params = Vec::new();
                for f in filters {
                    let (sql, p) = f.to_sql(param_offset);
                    parts.push(sql);
                    params.extend(p);
                }
                (format!("({})", parts.join(" AND ")), params)
            }
            Filter::Or(filters) => {
                let mut parts = Vec::new();
                let mut params = Vec::new();
                for f in filters {
                    let (sql, p) = f.to_sql(param_offset);
                    parts.push(sql);
                    params.extend(p);
                }
                (format!("({})", parts.join(" OR ")), params)
            }
            Filter::Not(inner) => {
                let (sql, params) = inner.to_sql(param_offset);
                (format!("NOT ({})", sql), params)
            }
            Filter::Raw { sql } => (sql.clone(), vec![]),
        }
    }
}

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_eq_sql() {
        let f = Filter::eq("email", "alice@test.com");
        let mut offset = 0;
        let (sql, params) = f.to_sql(&mut offset);
        assert_eq!(sql, "\"email\" = ?1");
        assert_eq!(params, vec![Value::from("alice@test.com")]);
        assert_eq!(offset, 1);
    }

    #[test]
    fn filter_and_or() {
        let f = Filter::and(vec![
            Filter::eq("age", 18i32),
            Filter::or(vec![Filter::eq("role", "admin"), Filter::eq("role", "mod")]),
        ]);
        let mut offset = 0;
        let (sql, params) = f.to_sql(&mut offset);
        assert_eq!(sql, "(\"age\" = ?1 AND (\"role\" = ?2 OR \"role\" = ?3))");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn filter_in() {
        let f = Filter::is_in(
            "id",
            vec![Value::from(1i32), Value::from(2i32), Value::from(3i32)],
        );
        let mut offset = 0;
        let (sql, _params) = f.to_sql(&mut offset);
        assert_eq!(sql, "\"id\" IN (?1, ?2, ?3)");
    }

    #[test]
    fn filter_between() {
        let f = Filter::between("score", 10i32, 100i32);
        let mut offset = 0;
        let (sql, params) = f.to_sql(&mut offset);
        assert_eq!(sql, "\"score\" BETWEEN ?1 AND ?2");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn filter_not_null() {
        let f = Filter::is_not_null("email");
        let mut offset = 0;
        let (sql, params) = f.to_sql(&mut offset);
        assert_eq!(sql, "\"email\" IS NOT NULL");
        assert!(params.is_empty());
    }

    #[test]
    fn filter_not() {
        let f = Filter::negate(Filter::eq("deleted", true));
        let mut offset = 0;
        let (sql, _) = f.to_sql(&mut offset);
        assert_eq!(sql, "NOT (\"deleted\" = ?1)");
    }

    #[test]
    fn filter_like() {
        let f = Filter::like("name", "%alice%");
        let mut offset = 0;
        let (sql, _) = f.to_sql(&mut offset);
        assert_eq!(sql, "\"name\" LIKE ?1");
    }

    #[test]
    fn filter_raw() {
        let f = Filter::raw("EXISTS (SELECT 1)");
        let mut offset = 0;
        let (sql, params) = f.to_sql(&mut offset);
        assert_eq!(sql, "EXISTS (SELECT 1)");
        assert!(params.is_empty());
        assert_eq!(offset, 0); // raw filters don't consume params
    }

    #[test]
    fn filter_in_subquery() {
        let f = Filter::in_subquery("id", "SELECT user_id FROM orders");
        let mut offset = 0;
        let (sql, params) = f.to_sql(&mut offset);
        assert_eq!(sql, "\"id\" IN (SELECT user_id FROM orders)");
        assert!(params.is_empty());
    }

    #[test]
    fn filter_exists() {
        let f = Filter::exists("SELECT 1 FROM orders WHERE orders.user_id = users.id");
        let mut offset = 0;
        let (sql, params) = f.to_sql(&mut offset);
        assert_eq!(
            sql,
            "EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn filter_table_qualified_column() {
        let f = Filter::eq("Account.name", "Alice");
        let mut offset = 0;
        let (sql, _) = f.to_sql(&mut offset);
        assert_eq!(sql, "\"Account\".\"name\" = ?1");
    }
}
