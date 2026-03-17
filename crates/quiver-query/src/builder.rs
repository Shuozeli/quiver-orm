use quiver_driver_core::{Statement, Value};

use crate::filter::{Filter, Order};
use crate::join::Join;
use crate::safe_ident::{SafeIdent, quote_table};

/// A compiled query ready for execution.
///
/// Type alias for [`Statement`] -- the query builder produces statements
/// that can be passed directly to `Connection::execute` or `Connection::query`.
pub type BuiltQuery = Statement;

/// Entry point for building queries against a table.
pub struct Query;

impl Query {
    /// Start building queries against the given table.
    ///
    /// Only accepts `&'static str` to prevent SQL injection -- table names
    /// must be compile-time string literals, not runtime user input.
    pub fn table(name: &'static str) -> TableRef {
        TableRef {
            table: SafeIdent::new(name),
        }
    }

    /// Execute raw SQL with parameterized values.
    ///
    /// The SQL string must be a compile-time literal (`&'static str`).
    pub fn raw(sql: &'static str) -> RawQueryBuilder {
        RawQueryBuilder {
            sql,
            params: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// RawQuery
// ---------------------------------------------------------------------------

/// Builder for raw SQL queries with compile-time-literal SQL.
pub struct RawQueryBuilder {
    sql: &'static str,
    params: Vec<Value>,
}

impl RawQueryBuilder {
    /// Add a bind parameter.
    pub fn param(mut self, value: Value) -> Self {
        self.params.push(value);
        self
    }

    /// Add multiple bind parameters.
    pub fn params(mut self, values: impl IntoIterator<Item = Value>) -> Self {
        self.params.extend(values);
        self
    }

    /// Build the statement.
    pub fn build(self) -> BuiltQuery {
        Statement::new(self.sql.to_string(), self.params)
    }
}

/// A reference to a specific table, from which query operations branch.
pub struct TableRef {
    table: SafeIdent,
}

impl TableRef {
    /// Build a SELECT query returning multiple rows.
    pub fn find_many(self) -> FindManyBuilder {
        FindManyBuilder {
            table: self.table,
            select: Vec::new(),
            distinct: false,
            joins: Vec::new(),
            filters: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: None,
            ctes: Vec::new(),
        }
    }

    /// Build a SELECT query returning at most one row (LIMIT 1).
    pub fn find_first(self) -> FindFirstBuilder {
        FindFirstBuilder {
            table: self.table,
            select: Vec::new(),
            distinct: false,
            joins: Vec::new(),
            filters: Vec::new(),
            order: Vec::new(),
        }
    }

    /// Build an INSERT query.
    pub fn create(self) -> CreateBuilder {
        CreateBuilder {
            table: self.table,
            columns: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Build an UPDATE query.
    pub fn update(self) -> UpdateBuilder {
        UpdateBuilder {
            table: self.table,
            sets: Vec::new(),
            set_values: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Build a DELETE query.
    pub fn delete(self) -> DeleteBuilder {
        DeleteBuilder {
            table: self.table,
            filters: Vec::new(),
        }
    }

    /// Build a bulk INSERT query.
    pub fn create_many(self) -> CreateManyBuilder {
        CreateManyBuilder {
            table: self.table,
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Build an UPDATE query that can affect multiple rows.
    pub fn update_many(self) -> UpdateBuilder {
        // update_many is the same as update -- it's the filter that determines scope
        UpdateBuilder {
            table: self.table,
            sets: Vec::new(),
            set_values: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Build a DELETE query that can affect multiple rows.
    pub fn delete_many(self) -> DeleteBuilder {
        // delete_many is the same as delete -- it's the filter that determines scope
        DeleteBuilder {
            table: self.table,
            filters: Vec::new(),
        }
    }

    /// Build an aggregation query (COUNT, SUM, AVG, MIN, MAX, GROUP BY).
    pub fn aggregate(self) -> AggregateBuilder {
        AggregateBuilder {
            table: self.table,
            aggregates: Vec::new(),
            filters: Vec::new(),
            group_by: Vec::new(),
            having: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    /// Build an INSERT ... ON CONFLICT UPDATE (upsert) query.
    pub fn upsert(self) -> UpsertBuilder {
        UpsertBuilder {
            table: self.table,
            columns: Vec::new(),
            values: Vec::new(),
            conflict_columns: Vec::new(),
            update_sets: Vec::new(),
            update_values: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// CTE (Common Table Expression)
// ---------------------------------------------------------------------------

/// A Common Table Expression (CTE) for use with `WITH` clauses.
pub struct Cte {
    /// Name of the CTE.
    pub name: &'static str,
    /// The query that defines the CTE.
    pub query: Statement,
}

impl Cte {
    /// Create a new CTE with the given name and query.
    pub fn new(name: &'static str, query: Statement) -> Self {
        Self { name, query }
    }
}

/// Prepend `WITH` clause to a SQL string if CTEs are present.
fn prepend_ctes(ctes: &[Cte], sql: &mut String, params: &mut Vec<Value>) {
    if ctes.is_empty() {
        return;
    }

    let cte_parts: Vec<String> = ctes
        .iter()
        .map(|cte| format!("\"{}\" AS ({})", cte.name, cte.query.sql))
        .collect();

    // Collect CTE params in order (before the main query params)
    let mut cte_params: Vec<Value> = Vec::new();
    for cte in ctes {
        cte_params.extend(cte.query.params.clone());
    }

    // Prepend CTE params before main query params
    cte_params.append(params);
    *params = cte_params;

    *sql = format!("WITH {} {}", cte_parts.join(", "), sql);
}

// ---------------------------------------------------------------------------
// FindMany
// ---------------------------------------------------------------------------

pub struct FindManyBuilder {
    pub(crate) table: SafeIdent,
    pub(crate) select: Vec<SafeIdent>,
    pub(crate) distinct: bool,
    pub(crate) joins: Vec<Join>,
    pub(crate) filters: Vec<Filter>,
    pub(crate) order: Vec<(SafeIdent, Order)>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: Option<u64>,
    pub(crate) ctes: Vec<Cte>,
}

impl FindManyBuilder {
    /// Select specific columns. If never called, selects all (*).
    ///
    /// Only accepts `&'static str` to prevent SQL injection.
    pub fn select(mut self, columns: &[&'static str]) -> Self {
        self.select = columns.iter().map(|c| SafeIdent::new(c)).collect();
        self
    }

    /// Return only distinct rows.
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add a JOIN clause.
    pub fn join(mut self, j: Join) -> Self {
        self.joins.push(j);
        self
    }

    /// Add a WHERE filter.
    pub fn filter(mut self, f: Filter) -> Self {
        self.filters.push(f);
        self
    }

    /// Add an ORDER BY clause.
    ///
    /// Only accepts `&'static str` to prevent SQL injection.
    pub fn order_by(mut self, column: &'static str, direction: Order) -> Self {
        self.order.push((SafeIdent::new(column), direction));
        self
    }

    /// Set the maximum number of rows to return.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Set the offset for pagination.
    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }

    /// Add a Common Table Expression (CTE) to the query.
    ///
    /// Multiple CTEs can be chained: `WITH a AS (...), b AS (...) SELECT ...`
    pub fn with_cte(mut self, name: &'static str, query: Statement) -> Self {
        self.ctes.push(Cte::new(name, query));
        self
    }

    /// Compile the query into SQL + params.
    pub fn build(self) -> BuiltQuery {
        let mut param_offset = 0;
        let mut params = Vec::new();

        let cols = if self.select.is_empty() {
            "*".to_string()
        } else {
            self.select
                .iter()
                .map(|c| c.to_quoted_sql())
                .collect::<Vec<_>>()
                .join(", ")
        };

        let select_keyword = if self.distinct {
            "SELECT DISTINCT"
        } else {
            "SELECT"
        };
        let mut sql = format!(
            "{} {} FROM {}",
            select_keyword,
            cols,
            quote_table(self.table.as_str())
        );

        for j in &self.joins {
            let (join_sql, join_params) = j.to_sql(&mut param_offset);
            sql.push_str(&format!(" {}", join_sql));
            params.extend(join_params);
        }

        if !self.filters.is_empty() {
            let where_clause = build_where(&self.filters, &mut param_offset, &mut params);
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        if !self.order.is_empty() {
            let order_clause: Vec<String> = self
                .order
                .iter()
                .map(|(col, dir)| {
                    let d = match dir {
                        Order::Asc => "ASC",
                        Order::Desc => "DESC",
                    };
                    format!("{} {}", col.to_quoted_sql(), d)
                })
                .collect();
            sql.push_str(&format!(" ORDER BY {}", order_clause.join(", ")));
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        prepend_ctes(&self.ctes, &mut sql, &mut params);

        Statement::new(sql, params)
    }
}

// ---------------------------------------------------------------------------
// FindFirst
// ---------------------------------------------------------------------------

pub struct FindFirstBuilder {
    pub(crate) table: SafeIdent,
    pub(crate) select: Vec<SafeIdent>,
    pub(crate) distinct: bool,
    pub(crate) joins: Vec<Join>,
    pub(crate) filters: Vec<Filter>,
    pub(crate) order: Vec<(SafeIdent, Order)>,
}

impl FindFirstBuilder {
    /// Select specific columns.
    pub fn select(mut self, columns: &[&'static str]) -> Self {
        self.select = columns.iter().map(|c| SafeIdent::new(c)).collect();
        self
    }

    /// Return only distinct rows.
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add a JOIN clause.
    pub fn join(mut self, j: Join) -> Self {
        self.joins.push(j);
        self
    }

    /// Add a WHERE filter.
    pub fn filter(mut self, f: Filter) -> Self {
        self.filters.push(f);
        self
    }

    /// Add an ORDER BY clause.
    pub fn order_by(mut self, column: &'static str, direction: Order) -> Self {
        self.order.push((SafeIdent::new(column), direction));
        self
    }

    /// Compile the query (always appends LIMIT 1).
    pub fn build(self) -> BuiltQuery {
        let inner = FindManyBuilder {
            table: self.table,
            select: self.select,
            distinct: self.distinct,
            joins: self.joins,
            filters: self.filters,
            order: self.order,
            limit: Some(1),
            offset: None,
            ctes: Vec::new(),
        };
        inner.build()
    }
}

// ---------------------------------------------------------------------------
// Create (INSERT)
// ---------------------------------------------------------------------------

pub struct CreateBuilder {
    pub(crate) table: SafeIdent,
    pub(crate) columns: Vec<SafeIdent>,
    pub(crate) values: Vec<Value>,
}

impl CreateBuilder {
    /// Set a column value.
    ///
    /// Only accepts `&'static str` for column name to prevent SQL injection.
    pub fn set(mut self, column: &'static str, value: impl Into<Value>) -> Self {
        self.columns.push(SafeIdent::new(column));
        self.values.push(value.into());
        self
    }

    /// Compile the INSERT query.
    pub fn build(self) -> BuiltQuery {
        let col_list: Vec<String> = self.columns.iter().map(|c| c.to_quoted_sql()).collect();
        let placeholders: Vec<String> =
            (1..=self.values.len()).map(|i| format!("?{}", i)).collect();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_table(self.table.as_str()),
            col_list.join(", "),
            placeholders.join(", ")
        );

        Statement::new(sql, self.values)
    }
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

pub struct UpdateBuilder {
    pub(crate) table: SafeIdent,
    pub(crate) sets: Vec<SafeIdent>,
    pub(crate) set_values: Vec<Value>,
    pub(crate) filters: Vec<Filter>,
}

impl UpdateBuilder {
    /// Set a column to a new value.
    ///
    /// Only accepts `&'static str` for column name to prevent SQL injection.
    pub fn set(mut self, column: &'static str, value: impl Into<Value>) -> Self {
        self.sets.push(SafeIdent::new(column));
        self.set_values.push(value.into());
        self
    }

    /// Add a WHERE filter.
    pub fn filter(mut self, f: Filter) -> Self {
        self.filters.push(f);
        self
    }

    /// Compile the UPDATE query.
    pub fn build(self) -> BuiltQuery {
        let mut param_offset = 0;
        let mut params = Vec::new();

        let set_clause: Vec<String> = self
            .sets
            .iter()
            .zip(self.set_values.iter())
            .map(|(col, val)| {
                param_offset += 1;
                params.push(val.clone());
                format!("{} = ?{}", col.to_quoted_sql(), param_offset)
            })
            .collect();

        let mut sql = format!(
            "UPDATE {} SET {}",
            quote_table(self.table.as_str()),
            set_clause.join(", ")
        );

        if !self.filters.is_empty() {
            let where_clause = build_where(&self.filters, &mut param_offset, &mut params);
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        Statement::new(sql, params)
    }
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

pub struct DeleteBuilder {
    pub(crate) table: SafeIdent,
    pub(crate) filters: Vec<Filter>,
}

impl DeleteBuilder {
    /// Add a WHERE filter.
    pub fn filter(mut self, f: Filter) -> Self {
        self.filters.push(f);
        self
    }

    /// Compile the DELETE query.
    pub fn build(self) -> BuiltQuery {
        let mut param_offset = 0;
        let mut params = Vec::new();

        let mut sql = format!("DELETE FROM {}", quote_table(self.table.as_str()));

        if !self.filters.is_empty() {
            let where_clause = build_where(&self.filters, &mut param_offset, &mut params);
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        Statement::new(sql, params)
    }
}

// ---------------------------------------------------------------------------
// Upsert (INSERT ... ON CONFLICT ... DO UPDATE)
// ---------------------------------------------------------------------------

pub struct UpsertBuilder {
    pub(crate) table: SafeIdent,
    pub(crate) columns: Vec<SafeIdent>,
    pub(crate) values: Vec<Value>,
    pub(crate) conflict_columns: Vec<SafeIdent>,
    pub(crate) update_sets: Vec<SafeIdent>,
    pub(crate) update_values: Vec<Value>,
}

impl UpsertBuilder {
    /// Set a column value for the INSERT portion.
    pub fn set(mut self, column: &'static str, value: impl Into<Value>) -> Self {
        self.columns.push(SafeIdent::new(column));
        self.values.push(value.into());
        self
    }

    /// Specify the conflict target columns (for ON CONFLICT).
    pub fn conflict_on(mut self, columns: &[&'static str]) -> Self {
        self.conflict_columns = columns.iter().map(|c| SafeIdent::new(c)).collect();
        self
    }

    /// Set a column to update on conflict.
    pub fn on_conflict_set(mut self, column: &'static str, value: impl Into<Value>) -> Self {
        self.update_sets.push(SafeIdent::new(column));
        self.update_values.push(value.into());
        self
    }

    /// Compile the upsert query.
    pub fn build(self) -> BuiltQuery {
        let mut param_offset = 0;
        let mut params = Vec::new();

        let col_list: Vec<String> = self.columns.iter().map(|c| c.to_quoted_sql()).collect();
        let placeholders: Vec<String> = self
            .values
            .iter()
            .map(|v| {
                param_offset += 1;
                params.push(v.clone());
                format!("?{}", param_offset)
            })
            .collect();

        let conflict_cols: Vec<String> = self
            .conflict_columns
            .iter()
            .map(|c| c.to_quoted_sql())
            .collect();

        let update_clause: Vec<String> = self
            .update_sets
            .iter()
            .zip(self.update_values.iter())
            .map(|(col, val)| {
                param_offset += 1;
                params.push(val.clone());
                format!("{} = ?{}", col.to_quoted_sql(), param_offset)
            })
            .collect();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            quote_table(self.table.as_str()),
            col_list.join(", "),
            placeholders.join(", "),
            conflict_cols.join(", "),
            update_clause.join(", ")
        );

        Statement::new(sql, params)
    }
}

// ---------------------------------------------------------------------------
// CreateMany (bulk INSERT)
// ---------------------------------------------------------------------------

pub struct CreateManyBuilder {
    pub(crate) table: SafeIdent,
    pub(crate) columns: Vec<SafeIdent>,
    pub(crate) rows: Vec<Vec<Value>>,
}

impl CreateManyBuilder {
    /// Define the columns for the bulk insert.
    pub fn columns(mut self, columns: &[&'static str]) -> Self {
        self.columns = columns.iter().map(|c| SafeIdent::new(c)).collect();
        self
    }

    /// Add a row of values. Must match the column count set by `columns()`.
    pub fn values(mut self, row: Vec<Value>) -> Self {
        self.rows.push(row);
        self
    }

    /// Compile the bulk INSERT query.
    pub fn build(self) -> BuiltQuery {
        let col_list: Vec<String> = self.columns.iter().map(|c| c.to_quoted_sql()).collect();

        let mut param_offset = 0;
        let mut params = Vec::new();

        let value_groups: Vec<String> = self
            .rows
            .iter()
            .map(|row| {
                let placeholders: Vec<String> = row
                    .iter()
                    .map(|v| {
                        param_offset += 1;
                        params.push(v.clone());
                        format!("?{}", param_offset)
                    })
                    .collect();
                format!("({})", placeholders.join(", "))
            })
            .collect();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            quote_table(self.table.as_str()),
            col_list.join(", "),
            value_groups.join(", ")
        );

        Statement::new(sql, params)
    }
}

// ---------------------------------------------------------------------------
// Aggregate (COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING)
// ---------------------------------------------------------------------------

/// An aggregate function applied to a column.
#[derive(Debug, Clone)]
pub enum AggregateFunc {
    Count(SafeIdent),
    CountAll,
    Sum(SafeIdent),
    Avg(SafeIdent),
    Min(SafeIdent),
    Max(SafeIdent),
}

impl AggregateFunc {
    fn to_sql(&self) -> String {
        match self {
            AggregateFunc::Count(col) => format!("COUNT({})", col.to_quoted_sql()),
            AggregateFunc::CountAll => "COUNT(*)".to_string(),
            AggregateFunc::Sum(col) => format!("SUM({})", col.to_quoted_sql()),
            AggregateFunc::Avg(col) => format!("AVG({})", col.to_quoted_sql()),
            AggregateFunc::Min(col) => format!("MIN({})", col.to_quoted_sql()),
            AggregateFunc::Max(col) => format!("MAX({})", col.to_quoted_sql()),
        }
    }

    /// Returns a deterministic alias for this aggregate function.
    ///
    /// This makes result columns accessible via `row.get_by_name("_count")` etc.
    /// instead of requiring the raw SQL expression name like `COUNT(*)`.
    pub fn alias(&self) -> String {
        match self {
            AggregateFunc::Count(col) => format!("_count_{}", col.as_str()),
            AggregateFunc::CountAll => "_count".to_string(),
            AggregateFunc::Sum(col) => format!("_sum_{}", col.as_str()),
            AggregateFunc::Avg(col) => format!("_avg_{}", col.as_str()),
            AggregateFunc::Min(col) => format!("_min_{}", col.as_str()),
            AggregateFunc::Max(col) => format!("_max_{}", col.as_str()),
        }
    }

    /// Returns the SQL expression with a deterministic alias: `COUNT(*) AS "_count"`.
    fn to_aliased_sql(&self) -> String {
        format!("{} AS \"{}\"", self.to_sql(), self.alias())
    }
}

/// Comparison operator for HAVING clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl CompareOp {
    fn as_sql(&self) -> &'static str {
        match self {
            CompareOp::Eq => "=",
            CompareOp::Neq => "!=",
            CompareOp::Gt => ">",
            CompareOp::Gte => ">=",
            CompareOp::Lt => "<",
            CompareOp::Lte => "<=",
        }
    }
}

pub struct AggregateBuilder {
    pub(crate) table: SafeIdent,
    pub(crate) aggregates: Vec<AggregateFunc>,
    pub(crate) filters: Vec<Filter>,
    pub(crate) group_by: Vec<SafeIdent>,
    pub(crate) having: Vec<(AggregateFunc, CompareOp, Value)>,
    pub(crate) order: Vec<(SafeIdent, Order)>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: Option<u64>,
}

impl AggregateBuilder {
    /// Add COUNT(column). Passing `"*"` is equivalent to [`count_all()`](Self::count_all).
    pub fn count(mut self, column: &'static str) -> Self {
        if column == "*" {
            self.aggregates.push(AggregateFunc::CountAll);
        } else {
            self.aggregates
                .push(AggregateFunc::Count(SafeIdent::new(column)));
        }
        self
    }

    /// Add COUNT(*).
    pub fn count_all(mut self) -> Self {
        self.aggregates.push(AggregateFunc::CountAll);
        self
    }

    /// Add SUM(column).
    pub fn sum(mut self, column: &'static str) -> Self {
        self.aggregates
            .push(AggregateFunc::Sum(SafeIdent::new(column)));
        self
    }

    /// Add AVG(column).
    pub fn avg(mut self, column: &'static str) -> Self {
        self.aggregates
            .push(AggregateFunc::Avg(SafeIdent::new(column)));
        self
    }

    /// Add MIN(column).
    pub fn min(mut self, column: &'static str) -> Self {
        self.aggregates
            .push(AggregateFunc::Min(SafeIdent::new(column)));
        self
    }

    /// Add MAX(column).
    pub fn max(mut self, column: &'static str) -> Self {
        self.aggregates
            .push(AggregateFunc::Max(SafeIdent::new(column)));
        self
    }

    /// Add a WHERE filter (applied before GROUP BY).
    pub fn filter(mut self, f: Filter) -> Self {
        self.filters.push(f);
        self
    }

    /// Add a GROUP BY column.
    pub fn group_by(mut self, column: &'static str) -> Self {
        self.group_by.push(SafeIdent::new(column));
        self
    }

    /// Add a HAVING condition on an aggregate result.
    ///
    /// Example: `.having(AggregateFunc::CountAll, CompareOp::Gte, 2)`
    pub fn having(mut self, agg: AggregateFunc, op: CompareOp, value: impl Into<Value>) -> Self {
        self.having.push((agg, op, value.into()));
        self
    }

    /// Add an ORDER BY clause.
    pub fn order_by(mut self, column: &'static str, direction: Order) -> Self {
        self.order.push((SafeIdent::new(column), direction));
        self
    }

    /// Set the maximum number of rows to return.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Set the offset for pagination.
    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }

    /// Compile the aggregate query.
    pub fn build(self) -> BuiltQuery {
        let mut param_offset = 0;
        let mut params = Vec::new();

        // Build SELECT list: group_by columns + aggregate functions (with aliases)
        let mut select_parts: Vec<String> =
            self.group_by.iter().map(|c| c.to_quoted_sql()).collect();
        for agg in &self.aggregates {
            select_parts.push(agg.to_aliased_sql());
        }

        let select_clause = if select_parts.is_empty() {
            "COUNT(*) AS \"_count\"".to_string()
        } else {
            select_parts.join(", ")
        };

        let mut sql = format!(
            "SELECT {} FROM {}",
            select_clause,
            quote_table(self.table.as_str())
        );

        if !self.filters.is_empty() {
            let where_clause = build_where(&self.filters, &mut param_offset, &mut params);
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        if !self.group_by.is_empty() {
            let group_clause: Vec<String> =
                self.group_by.iter().map(|c| c.to_quoted_sql()).collect();
            sql.push_str(&format!(" GROUP BY {}", group_clause.join(", ")));
        }

        if !self.having.is_empty() {
            let having_parts: Vec<String> = self
                .having
                .iter()
                .map(|(agg, op, val)| {
                    param_offset += 1;
                    params.push(val.clone());
                    format!("{} {} ?{}", agg.to_sql(), op.as_sql(), param_offset)
                })
                .collect();
            sql.push_str(&format!(" HAVING {}", having_parts.join(" AND ")));
        }

        if !self.order.is_empty() {
            let order_clause: Vec<String> = self
                .order
                .iter()
                .map(|(col, dir)| {
                    let d = match dir {
                        Order::Asc => "ASC",
                        Order::Desc => "DESC",
                    };
                    format!("{} {}", col.to_quoted_sql(), d)
                })
                .collect();
            sql.push_str(&format!(" ORDER BY {}", order_clause.join(", ")));
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        Statement::new(sql, params)
    }
}

// ---------------------------------------------------------------------------
// Internal builder (for system-generated queries like includes)
// ---------------------------------------------------------------------------

/// Build a query from validated (but non-static) strings.
///
/// This is used internally by the include/relation system where identifiers
/// come from schema definitions, not user input. Not exposed publicly.
pub(crate) fn build_find_many_internal(table: &str, filter: Option<Filter>) -> BuiltQuery {
    let mut param_offset = 0;
    let mut params = Vec::new();

    let mut sql = format!("SELECT * FROM {}", quote_table(table));

    if let Some(f) = filter {
        let (where_sql, where_params) = f.to_sql(&mut param_offset);
        sql.push_str(&format!(" WHERE {}", where_sql));
        params = where_params;
    }

    Statement::new(sql, params)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) fn build_where(
    filters: &[Filter],
    param_offset: &mut usize,
    params: &mut Vec<Value>,
) -> String {
    let parts: Vec<String> = filters
        .iter()
        .map(|f| {
            let (sql, p) = f.to_sql(param_offset);
            params.extend(p);
            sql
        })
        .collect();

    if parts.len() == 1 {
        parts.into_iter().next().unwrap()
    } else {
        parts.join(" AND ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_many_basic() {
        let q = Query::table("User").find_many().build();
        assert_eq!(q.sql, "SELECT * FROM \"User\"");
        assert!(q.params.is_empty());
    }

    #[test]
    fn find_many_with_select() {
        let q = Query::table("User")
            .find_many()
            .select(&["id", "email"])
            .build();
        assert_eq!(q.sql, "SELECT \"id\", \"email\" FROM \"User\"");
    }

    #[test]
    fn find_many_with_filter() {
        let q = Query::table("User")
            .find_many()
            .filter(Filter::eq("email", "alice@test.com"))
            .build();
        assert_eq!(q.sql, "SELECT * FROM \"User\" WHERE \"email\" = ?1");
        assert_eq!(q.params, vec![Value::from("alice@test.com")]);
    }

    #[test]
    fn find_many_with_order_limit_offset() {
        let q = Query::table("User")
            .find_many()
            .order_by("name", Order::Asc)
            .order_by("id", Order::Desc)
            .limit(10)
            .offset(20)
            .build();
        assert_eq!(
            q.sql,
            "SELECT * FROM \"User\" ORDER BY \"name\" ASC, \"id\" DESC LIMIT 10 OFFSET 20"
        );
    }

    #[test]
    fn find_many_complex_filter() {
        let q = Query::table("User")
            .find_many()
            .filter(Filter::gte("age", 18i32))
            .filter(Filter::or(vec![
                Filter::eq("role", "admin"),
                Filter::eq("role", "moderator"),
            ]))
            .build();
        assert_eq!(
            q.sql,
            "SELECT * FROM \"User\" WHERE \"age\" >= ?1 AND (\"role\" = ?2 OR \"role\" = ?3)"
        );
        assert_eq!(q.params.len(), 3);
    }

    #[test]
    fn find_first() {
        let q = Query::table("User")
            .find_first()
            .filter(Filter::eq("id", 1i32))
            .build();
        assert_eq!(q.sql, "SELECT * FROM \"User\" WHERE \"id\" = ?1 LIMIT 1");
    }

    #[test]
    fn create_basic() {
        let q = Query::table("User")
            .create()
            .set("email", "alice@test.com")
            .set("name", "Alice")
            .build();
        assert_eq!(
            q.sql,
            "INSERT INTO \"User\" (\"email\", \"name\") VALUES (?1, ?2)"
        );
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn update_basic() {
        let q = Query::table("User")
            .update()
            .set("name", "Bob")
            .filter(Filter::eq("id", 1i32))
            .build();
        assert_eq!(q.sql, "UPDATE \"User\" SET \"name\" = ?1 WHERE \"id\" = ?2");
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn update_multiple_sets() {
        let q = Query::table("User")
            .update()
            .set("name", "Bob")
            .set("email", "bob@test.com")
            .filter(Filter::eq("id", 1i32))
            .build();
        assert_eq!(
            q.sql,
            "UPDATE \"User\" SET \"name\" = ?1, \"email\" = ?2 WHERE \"id\" = ?3"
        );
    }

    #[test]
    fn delete_basic() {
        let q = Query::table("User")
            .delete()
            .filter(Filter::eq("id", 1i32))
            .build();
        assert_eq!(q.sql, "DELETE FROM \"User\" WHERE \"id\" = ?1");
    }

    #[test]
    fn delete_all() {
        let q = Query::table("User").delete().build();
        assert_eq!(q.sql, "DELETE FROM \"User\"");
    }

    #[test]
    fn find_many_with_join() {
        use crate::join::JoinType;
        let q = Query::table("User")
            .find_many()
            .select(&["User.id", "User.name", "Post.title"])
            .join(Join::new(JoinType::Inner, "Post").on("User", "id", "Post", "author_id"))
            .build();
        assert_eq!(
            q.sql,
            "SELECT \"User\".\"id\", \"User\".\"name\", \"Post\".\"title\" FROM \"User\" INNER JOIN \"Post\" ON \"User\".\"id\" = \"Post\".\"author_id\""
        );
    }

    #[test]
    fn find_many_with_left_join_and_filter() {
        use crate::join::JoinType;
        let q = Query::table("User")
            .find_many()
            .join(
                Join::new(JoinType::Left, "Post")
                    .on("User", "id", "Post", "author_id")
                    .filter(Filter::eq("published", true)),
            )
            .filter(Filter::gte("age", 18i32))
            .build();
        assert_eq!(
            q.sql,
            "SELECT * FROM \"User\" LEFT JOIN \"Post\" ON \"User\".\"id\" = \"Post\".\"author_id\" AND \"published\" = ?1 WHERE \"age\" >= ?2"
        );
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn find_many_distinct() {
        let q = Query::table("User")
            .find_many()
            .distinct()
            .select(&["role"])
            .build();
        assert_eq!(q.sql, "SELECT DISTINCT \"role\" FROM \"User\"");
    }

    #[test]
    fn find_many_distinct_all_columns() {
        let q = Query::table("User").find_many().distinct().build();
        assert_eq!(q.sql, "SELECT DISTINCT * FROM \"User\"");
    }

    #[test]
    fn create_many_basic() {
        let q = Query::table("User")
            .create_many()
            .columns(&["email", "name"])
            .values(vec![Value::from("alice@test.com"), Value::from("Alice")])
            .values(vec![Value::from("bob@test.com"), Value::from("Bob")])
            .build();
        assert_eq!(
            q.sql,
            "INSERT INTO \"User\" (\"email\", \"name\") VALUES (?1, ?2), (?3, ?4)"
        );
        assert_eq!(q.params.len(), 4);
    }

    #[test]
    fn create_many_single_row() {
        let q = Query::table("User")
            .create_many()
            .columns(&["email"])
            .values(vec![Value::from("alice@test.com")])
            .build();
        assert_eq!(q.sql, "INSERT INTO \"User\" (\"email\") VALUES (?1)");
    }

    #[test]
    fn aggregate_count_all() {
        let q = Query::table("User").aggregate().count_all().build();
        assert_eq!(q.sql, "SELECT COUNT(*) AS \"_count\" FROM \"User\"");
    }

    #[test]
    fn aggregate_count_star_delegates_to_count_all() {
        let q = Query::table("User").aggregate().count("*").build();
        assert_eq!(q.sql, "SELECT COUNT(*) AS \"_count\" FROM \"User\"");
    }

    #[test]
    fn aggregate_with_group_by() {
        let q = Query::table("User")
            .aggregate()
            .count_all()
            .group_by("role")
            .build();
        assert_eq!(
            q.sql,
            "SELECT \"role\", COUNT(*) AS \"_count\" FROM \"User\" GROUP BY \"role\""
        );
    }

    #[test]
    fn aggregate_multiple_functions() {
        let q = Query::table("User")
            .aggregate()
            .sum("score")
            .avg("score")
            .min("score")
            .max("score")
            .build();
        assert_eq!(
            q.sql,
            "SELECT SUM(\"score\") AS \"_sum_score\", AVG(\"score\") AS \"_avg_score\", MIN(\"score\") AS \"_min_score\", MAX(\"score\") AS \"_max_score\" FROM \"User\""
        );
    }

    #[test]
    fn aggregate_with_where_and_having() {
        let q = Query::table("User")
            .aggregate()
            .count_all()
            .group_by("role")
            .filter(Filter::gte("score", 10i32))
            .having(AggregateFunc::CountAll, CompareOp::Gte, 2i32)
            .build();
        assert_eq!(
            q.sql,
            "SELECT \"role\", COUNT(*) AS \"_count\" FROM \"User\" WHERE \"score\" >= ?1 GROUP BY \"role\" HAVING COUNT(*) >= ?2"
        );
    }

    #[test]
    fn aggregate_with_order_and_limit() {
        let q = Query::table("User")
            .aggregate()
            .count_all()
            .group_by("role")
            .order_by("role", Order::Asc)
            .limit(5)
            .build();
        assert_eq!(
            q.sql,
            "SELECT \"role\", COUNT(*) AS \"_count\" FROM \"User\" GROUP BY \"role\" ORDER BY \"role\" ASC LIMIT 5"
        );
    }

    #[test]
    fn aggregate_alias_names() {
        assert_eq!(AggregateFunc::CountAll.alias(), "_count");
        assert_eq!(
            AggregateFunc::Count(SafeIdent::new("id")).alias(),
            "_count_id"
        );
        assert_eq!(
            AggregateFunc::Sum(SafeIdent::new("score")).alias(),
            "_sum_score"
        );
        assert_eq!(
            AggregateFunc::Avg(SafeIdent::new("score")).alias(),
            "_avg_score"
        );
        assert_eq!(
            AggregateFunc::Min(SafeIdent::new("price")).alias(),
            "_min_price"
        );
        assert_eq!(
            AggregateFunc::Max(SafeIdent::new("price")).alias(),
            "_max_price"
        );
    }

    #[test]
    fn raw_query_basic() {
        let q = Query::raw("SELECT * FROM users WHERE id = ?")
            .param(Value::Int(1))
            .build();
        assert_eq!(q.sql, "SELECT * FROM users WHERE id = ?");
        assert_eq!(q.params, vec![Value::Int(1)]);
    }

    #[test]
    fn raw_query_no_params() {
        let q = Query::raw("SELECT 1").build();
        assert_eq!(q.sql, "SELECT 1");
        assert!(q.params.is_empty());
    }

    #[test]
    fn raw_query_multiple_params() {
        let q = Query::raw("SELECT * FROM users WHERE age > ? AND name = ?")
            .params(vec![Value::Int(18), Value::from("Alice")])
            .build();
        assert_eq!(q.sql, "SELECT * FROM users WHERE age > ? AND name = ?");
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn filter_raw_in_query() {
        let q = Query::table("User")
            .find_many()
            .filter(Filter::raw("EXISTS (SELECT 1)"))
            .build();
        assert_eq!(q.sql, "SELECT * FROM \"User\" WHERE EXISTS (SELECT 1)");
        assert!(q.params.is_empty());
    }

    #[test]
    fn cte_simple() {
        let cte_query = Statement::new(
            "SELECT id, name FROM \"Employee\" WHERE \"active\" = true".to_string(),
            vec![],
        );
        let q = Query::table("active_employees")
            .find_many()
            .with_cte("active_employees", cte_query)
            .build();
        assert_eq!(
            q.sql,
            "WITH \"active_employees\" AS (SELECT id, name FROM \"Employee\" WHERE \"active\" = true) SELECT * FROM \"active_employees\""
        );
    }

    #[test]
    fn cte_multiple() {
        let cte_a = Statement::new(
            "SELECT id, department FROM \"Employee\"".to_string(),
            vec![],
        );
        let cte_b = Statement::new(
            "SELECT department, COUNT(*) as cnt FROM \"dept_employees\" GROUP BY department"
                .to_string(),
            vec![],
        );
        let q = Query::table("dept_counts")
            .find_many()
            .with_cte("dept_employees", cte_a)
            .with_cte("dept_counts", cte_b)
            .build();
        assert_eq!(
            q.sql,
            "WITH \"dept_employees\" AS (SELECT id, department FROM \"Employee\"), \"dept_counts\" AS (SELECT department, COUNT(*) as cnt FROM \"dept_employees\" GROUP BY department) SELECT * FROM \"dept_counts\""
        );
    }

    #[test]
    fn cte_with_filter_on_main_query() {
        let cte_query = Statement::new("SELECT id, score FROM \"Student\"".to_string(), vec![]);
        let q = Query::table("top_students")
            .find_many()
            .with_cte("top_students", cte_query)
            .filter(Filter::gte("score", 90i32))
            .build();
        assert_eq!(
            q.sql,
            "WITH \"top_students\" AS (SELECT id, score FROM \"Student\") SELECT * FROM \"top_students\" WHERE \"score\" >= ?1"
        );
        assert_eq!(q.params, vec![Value::Int(90)]);
    }

    #[test]
    fn upsert_basic() {
        let q = Query::table("User")
            .upsert()
            .set("email", "alice@test.com")
            .set("name", "Alice")
            .conflict_on(&["email"])
            .on_conflict_set("name", "Alice Updated")
            .build();
        assert_eq!(
            q.sql,
            "INSERT INTO \"User\" (\"email\", \"name\") VALUES (?1, ?2) ON CONFLICT (\"email\") DO UPDATE SET \"name\" = ?3"
        );
        assert_eq!(q.params.len(), 3);
    }
}
