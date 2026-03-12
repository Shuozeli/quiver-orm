//! SQL expression types for functions (string, math, date/time, subquery).

use crate::safe_ident::SafeIdent;
use quiver_driver_core::Statement;

/// SQL expressions that can be used in SELECT, WHERE, ORDER BY.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Column reference.
    Column(SafeIdent),
    /// String function.
    StringFn(StringFn),
    /// Math function.
    MathFn(MathFn),
    /// Date/time function.
    DateFn(DateFn),
    /// Subquery (raw SQL).
    Subquery(Box<Statement>),
    /// Window function with OVER clause.
    Window { func: WindowFn, over: WindowSpec },
}

/// SQL window functions.
#[derive(Debug, Clone)]
pub enum WindowFn {
    RowNumber,
    Rank,
    DenseRank,
    Ntile(u32),
    Lag(Box<Expr>, Option<u32>),
    Lead(Box<Expr>, Option<u32>),
    Sum(Box<Expr>),
    Count(Box<Expr>),
    Avg(Box<Expr>),
    Min(Box<Expr>),
    Max(Box<Expr>),
}

/// Window specification: PARTITION BY and ORDER BY clauses.
#[derive(Debug, Clone)]
pub struct WindowSpec {
    /// Columns to partition by.
    pub partition_by: Vec<&'static str>,
    /// Columns to order by: (column_name, ascending).
    pub order_by: Vec<(&'static str, bool)>,
}

impl WindowSpec {
    /// Create an empty window spec.
    pub fn new() -> Self {
        Self {
            partition_by: Vec::new(),
            order_by: Vec::new(),
        }
    }

    /// Add a PARTITION BY column.
    pub fn partition_by(mut self, column: &'static str) -> Self {
        self.partition_by.push(column);
        self
    }

    /// Add an ORDER BY column with direction.
    pub fn order_by(mut self, column: &'static str, ascending: bool) -> Self {
        self.order_by.push((column, ascending));
        self
    }

    /// Render the OVER clause contents.
    fn to_sql(&self) -> String {
        let mut parts = Vec::new();

        if !self.partition_by.is_empty() {
            let cols: Vec<String> = self
                .partition_by
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect();
            parts.push(format!("PARTITION BY {}", cols.join(", ")));
        }

        if !self.order_by.is_empty() {
            let cols: Vec<String> = self
                .order_by
                .iter()
                .map(|(c, asc)| {
                    let dir = if *asc { "ASC" } else { "DESC" };
                    format!("\"{}\" {}", c, dir)
                })
                .collect();
            parts.push(format!("ORDER BY {}", cols.join(", ")));
        }

        parts.join(" ")
    }
}

impl Default for WindowSpec {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowFn {
    /// Render the function call (without the OVER clause).
    fn to_sql(&self) -> String {
        match self {
            WindowFn::RowNumber => "ROW_NUMBER()".to_string(),
            WindowFn::Rank => "RANK()".to_string(),
            WindowFn::DenseRank => "DENSE_RANK()".to_string(),
            WindowFn::Ntile(n) => format!("NTILE({})", n),
            WindowFn::Lag(expr, offset) => match offset {
                Some(n) => format!("LAG({}, {})", expr.to_sql(), n),
                None => format!("LAG({})", expr.to_sql()),
            },
            WindowFn::Lead(expr, offset) => match offset {
                Some(n) => format!("LEAD({}, {})", expr.to_sql(), n),
                None => format!("LEAD({})", expr.to_sql()),
            },
            WindowFn::Sum(expr) => format!("SUM({})", expr.to_sql()),
            WindowFn::Count(expr) => format!("COUNT({})", expr.to_sql()),
            WindowFn::Avg(expr) => format!("AVG({})", expr.to_sql()),
            WindowFn::Min(expr) => format!("MIN({})", expr.to_sql()),
            WindowFn::Max(expr) => format!("MAX({})", expr.to_sql()),
        }
    }
}

/// SQL string functions.
#[derive(Debug, Clone)]
pub enum StringFn {
    Upper(SafeIdent),
    Lower(SafeIdent),
    Length(SafeIdent),
    Trim(SafeIdent),
    Concat(Vec<SafeIdent>),
    Substr(SafeIdent, i64, i64),
}

/// SQL math functions.
#[derive(Debug, Clone)]
pub enum MathFn {
    Abs(SafeIdent),
    Round(SafeIdent, Option<i32>),
    Ceil(SafeIdent),
    Floor(SafeIdent),
}

/// SQL date/time functions.
#[derive(Debug, Clone)]
pub enum DateFn {
    Year(SafeIdent),
    Month(SafeIdent),
    Day(SafeIdent),
    Now,
}

impl Expr {
    /// Render this expression to a SQL fragment.
    pub fn to_sql(&self) -> String {
        match self {
            Expr::Column(ident) => format!("\"{}\"", ident),
            Expr::StringFn(f) => match f {
                StringFn::Upper(col) => format!("UPPER(\"{}\")", col),
                StringFn::Lower(col) => format!("LOWER(\"{}\")", col),
                StringFn::Length(col) => format!("LENGTH(\"{}\")", col),
                StringFn::Trim(col) => format!("TRIM(\"{}\")", col),
                StringFn::Concat(cols) => {
                    let parts: Vec<String> = cols.iter().map(|c| format!("\"{}\"", c)).collect();
                    // Use || for SQL standard concatenation
                    parts.join(" || ")
                }
                StringFn::Substr(col, start, len) => {
                    format!("SUBSTR(\"{}\", {}, {})", col, start, len)
                }
            },
            Expr::MathFn(f) => match f {
                MathFn::Abs(col) => format!("ABS(\"{}\")", col),
                MathFn::Round(col, decimals) => match decimals {
                    Some(d) => format!("ROUND(\"{}\", {})", col, d),
                    None => format!("ROUND(\"{}\")", col),
                },
                MathFn::Ceil(col) => format!("CEIL(\"{}\")", col),
                MathFn::Floor(col) => format!("FLOOR(\"{}\")", col),
            },
            Expr::DateFn(f) => match f {
                DateFn::Year(col) => format!("EXTRACT(YEAR FROM \"{}\")", col),
                DateFn::Month(col) => format!("EXTRACT(MONTH FROM \"{}\")", col),
                DateFn::Day(col) => format!("EXTRACT(DAY FROM \"{}\")", col),
                DateFn::Now => "NOW()".to_string(),
            },
            Expr::Subquery(stmt) => format!("({})", stmt.sql),
            Expr::Window { func, over } => {
                format!("{} OVER ({})", func.to_sql(), over.to_sql())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expr_column() {
        let e = Expr::Column(SafeIdent::new("name"));
        assert_eq!(e.to_sql(), "\"name\"");
    }

    #[test]
    fn expr_upper() {
        let e = Expr::StringFn(StringFn::Upper(SafeIdent::new("name")));
        assert_eq!(e.to_sql(), "UPPER(\"name\")");
    }

    #[test]
    fn expr_lower() {
        let e = Expr::StringFn(StringFn::Lower(SafeIdent::new("name")));
        assert_eq!(e.to_sql(), "LOWER(\"name\")");
    }

    #[test]
    fn expr_length() {
        let e = Expr::StringFn(StringFn::Length(SafeIdent::new("name")));
        assert_eq!(e.to_sql(), "LENGTH(\"name\")");
    }

    #[test]
    fn expr_trim() {
        let e = Expr::StringFn(StringFn::Trim(SafeIdent::new("name")));
        assert_eq!(e.to_sql(), "TRIM(\"name\")");
    }

    #[test]
    fn expr_concat() {
        let e = Expr::StringFn(StringFn::Concat(vec![
            SafeIdent::new("first_name"),
            SafeIdent::new("last_name"),
        ]));
        assert_eq!(e.to_sql(), "\"first_name\" || \"last_name\"");
    }

    #[test]
    fn expr_substr() {
        let e = Expr::StringFn(StringFn::Substr(SafeIdent::new("name"), 1, 3));
        assert_eq!(e.to_sql(), "SUBSTR(\"name\", 1, 3)");
    }

    #[test]
    fn expr_math_abs() {
        let e = Expr::MathFn(MathFn::Abs(SafeIdent::new("balance")));
        assert_eq!(e.to_sql(), "ABS(\"balance\")");
    }

    #[test]
    fn expr_math_round_no_decimals() {
        let e = Expr::MathFn(MathFn::Round(SafeIdent::new("price"), None));
        assert_eq!(e.to_sql(), "ROUND(\"price\")");
    }

    #[test]
    fn expr_math_round_with_decimals() {
        let e = Expr::MathFn(MathFn::Round(SafeIdent::new("price"), Some(2)));
        assert_eq!(e.to_sql(), "ROUND(\"price\", 2)");
    }

    #[test]
    fn expr_math_ceil() {
        let e = Expr::MathFn(MathFn::Ceil(SafeIdent::new("score")));
        assert_eq!(e.to_sql(), "CEIL(\"score\")");
    }

    #[test]
    fn expr_math_floor() {
        let e = Expr::MathFn(MathFn::Floor(SafeIdent::new("score")));
        assert_eq!(e.to_sql(), "FLOOR(\"score\")");
    }

    #[test]
    fn expr_date_year() {
        let e = Expr::DateFn(DateFn::Year(SafeIdent::new("created_at")));
        assert_eq!(e.to_sql(), "EXTRACT(YEAR FROM \"created_at\")");
    }

    #[test]
    fn expr_date_month() {
        let e = Expr::DateFn(DateFn::Month(SafeIdent::new("created_at")));
        assert_eq!(e.to_sql(), "EXTRACT(MONTH FROM \"created_at\")");
    }

    #[test]
    fn expr_date_day() {
        let e = Expr::DateFn(DateFn::Day(SafeIdent::new("created_at")));
        assert_eq!(e.to_sql(), "EXTRACT(DAY FROM \"created_at\")");
    }

    #[test]
    fn expr_date_now() {
        let e = Expr::DateFn(DateFn::Now);
        assert_eq!(e.to_sql(), "NOW()");
    }

    #[test]
    fn expr_subquery() {
        let stmt = quiver_driver_core::Statement::new(
            "SELECT id FROM orders WHERE total > 100".to_string(),
            vec![],
        );
        let e = Expr::Subquery(Box::new(stmt));
        assert_eq!(e.to_sql(), "(SELECT id FROM orders WHERE total > 100)");
    }

    #[test]
    fn window_row_number_with_partition_and_order() {
        let e = Expr::Window {
            func: WindowFn::RowNumber,
            over: WindowSpec::new()
                .partition_by("department")
                .order_by("salary", false),
        };
        assert_eq!(
            e.to_sql(),
            "ROW_NUMBER() OVER (PARTITION BY \"department\" ORDER BY \"salary\" DESC)"
        );
    }

    #[test]
    fn window_rank() {
        let e = Expr::Window {
            func: WindowFn::Rank,
            over: WindowSpec::new().order_by("score", false),
        };
        assert_eq!(e.to_sql(), "RANK() OVER (ORDER BY \"score\" DESC)");
    }

    #[test]
    fn window_sum_aggregate() {
        let e = Expr::Window {
            func: WindowFn::Sum(Box::new(Expr::Column(SafeIdent::new("amount")))),
            over: WindowSpec::new()
                .partition_by("category")
                .order_by("created_at", true),
        };
        assert_eq!(
            e.to_sql(),
            "SUM(\"amount\") OVER (PARTITION BY \"category\" ORDER BY \"created_at\" ASC)"
        );
    }

    #[test]
    fn window_lag_with_offset() {
        let e = Expr::Window {
            func: WindowFn::Lag(Box::new(Expr::Column(SafeIdent::new("price"))), Some(2)),
            over: WindowSpec::new().order_by("date", true),
        };
        assert_eq!(e.to_sql(), "LAG(\"price\", 2) OVER (ORDER BY \"date\" ASC)");
    }

    #[test]
    fn window_lead_no_offset() {
        let e = Expr::Window {
            func: WindowFn::Lead(Box::new(Expr::Column(SafeIdent::new("value"))), None),
            over: WindowSpec::new()
                .partition_by("group_id")
                .order_by("seq", true),
        };
        assert_eq!(
            e.to_sql(),
            "LEAD(\"value\") OVER (PARTITION BY \"group_id\" ORDER BY \"seq\" ASC)"
        );
    }

    #[test]
    fn window_dense_rank() {
        let e = Expr::Window {
            func: WindowFn::DenseRank,
            over: WindowSpec::new().order_by("score", false),
        };
        assert_eq!(e.to_sql(), "DENSE_RANK() OVER (ORDER BY \"score\" DESC)");
    }

    #[test]
    fn window_ntile() {
        let e = Expr::Window {
            func: WindowFn::Ntile(4),
            over: WindowSpec::new().order_by("revenue", false),
        };
        assert_eq!(e.to_sql(), "NTILE(4) OVER (ORDER BY \"revenue\" DESC)");
    }
}
