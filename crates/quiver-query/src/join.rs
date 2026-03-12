//! SQL JOIN support for query builder.

use quiver_driver_core::Value;

use crate::filter::Filter;
use crate::safe_ident::{SafeIdent, quote_ident, quote_table};

/// Type of SQL JOIN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Cross,
}

impl JoinType {
    pub(crate) fn as_sql(&self) -> &'static str {
        match self {
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Cross => "CROSS JOIN",
        }
    }
}

/// A JOIN clause specification.
#[derive(Debug, Clone)]
pub struct Join {
    pub join_type: JoinType,
    pub table: SafeIdent,
    /// Optional alias for the joined table.
    pub alias: Option<SafeIdent>,
    /// ON condition: left_table.column = right_table.column pairs.
    pub on_conditions: Vec<JoinCondition>,
    /// Additional filter conditions on the joined table.
    pub filters: Vec<Filter>,
}

/// A single ON condition: left_col = right_col.
#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left_table: SafeIdent,
    pub left_column: SafeIdent,
    pub right_table: SafeIdent,
    pub right_column: SafeIdent,
}

impl Join {
    /// Create a new JOIN.
    ///
    /// Only accepts `&'static str` to prevent SQL injection.
    pub fn new(join_type: JoinType, table: &'static str) -> Self {
        Self {
            join_type,
            table: SafeIdent::new(table),
            alias: None,
            on_conditions: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Set an alias for the joined table.
    pub fn alias(mut self, alias: &'static str) -> Self {
        self.alias = Some(SafeIdent::new(alias));
        self
    }

    /// Add an ON condition: left_table.left_col = right_table.right_col.
    pub fn on(
        mut self,
        left_table: &'static str,
        left_col: &'static str,
        right_table: &'static str,
        right_col: &'static str,
    ) -> Self {
        self.on_conditions.push(JoinCondition {
            left_table: SafeIdent::new(left_table),
            left_column: SafeIdent::new(left_col),
            right_table: SafeIdent::new(right_table),
            right_column: SafeIdent::new(right_col),
        });
        self
    }

    /// Add a filter condition to this join.
    pub fn filter(mut self, f: Filter) -> Self {
        self.filters.push(f);
        self
    }

    /// Render the JOIN clause to SQL.
    pub(crate) fn to_sql(&self, param_offset: &mut usize) -> (String, Vec<Value>) {
        let table_ref = if let Some(alias) = &self.alias {
            format!(
                "{} AS {}",
                quote_table(self.table.as_str()),
                quote_table(alias.as_str())
            )
        } else {
            quote_table(self.table.as_str())
        };

        let on_parts: Vec<String> = self
            .on_conditions
            .iter()
            .map(|c| {
                format!(
                    "{}.{} = {}.{}",
                    quote_table(c.left_table.as_str()),
                    quote_ident(c.left_column.as_str()),
                    quote_table(c.right_table.as_str()),
                    quote_ident(c.right_column.as_str()),
                )
            })
            .collect();

        let mut params = Vec::new();
        let mut all_conditions = on_parts;

        for f in &self.filters {
            let (sql, p) = f.to_sql(param_offset);
            all_conditions.push(sql);
            params.extend(p);
        }

        let sql = if all_conditions.is_empty() {
            format!("{} {}", self.join_type.as_sql(), table_ref)
        } else {
            format!(
                "{} {} ON {}",
                self.join_type.as_sql(),
                table_ref,
                all_conditions.join(" AND ")
            )
        };

        (sql, params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inner_join_basic() {
        let join = Join::new(JoinType::Inner, "Post").on("User", "id", "Post", "author_id");
        let mut offset = 0;
        let (sql, params) = join.to_sql(&mut offset);
        assert_eq!(
            sql,
            "INNER JOIN \"Post\" ON \"User\".\"id\" = \"Post\".\"author_id\""
        );
        assert!(params.is_empty());
    }

    #[test]
    fn left_join_with_alias() {
        let join = Join::new(JoinType::Left, "Post")
            .alias("p")
            .on("User", "id", "p", "author_id");
        let mut offset = 0;
        let (sql, _) = join.to_sql(&mut offset);
        assert_eq!(
            sql,
            "LEFT JOIN \"Post\" AS \"p\" ON \"User\".\"id\" = \"p\".\"author_id\""
        );
    }

    #[test]
    fn join_with_filter() {
        let join = Join::new(JoinType::Inner, "Post")
            .on("User", "id", "Post", "author_id")
            .filter(Filter::eq("published", true));
        let mut offset = 0;
        let (sql, params) = join.to_sql(&mut offset);
        assert_eq!(
            sql,
            "INNER JOIN \"Post\" ON \"User\".\"id\" = \"Post\".\"author_id\" AND \"published\" = ?1"
        );
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn cross_join() {
        let join = Join::new(JoinType::Cross, "Category");
        let mut offset = 0;
        let (sql, _) = join.to_sql(&mut offset);
        assert_eq!(sql, "CROSS JOIN \"Category\"");
    }
}
