//! Type-safe query builder for Quiver ORM.
//!
//! Generates parameterized SQL queries from a builder API. Works with any
//! `quiver-driver-core::Connection` implementation.
//!
//! # SQL Injection Prevention
//!
//! All table and column names must be `&'static str` -- compile-time string
//! literals that cannot be constructed from runtime user input. This is enforced
//! by [`SafeIdent`], which validates characters and requires a `'static` lifetime.
//! Values are always parameterized (never interpolated into SQL).
//!
//! ```ignore
//! // OK: string literals are &'static str
//! let q = Query::table("User").find_many().filter(Filter::eq("email", user_email)).build();
//!
//! // WON'T COMPILE: String is not &'static str
//! let table = format!("{}; DROP TABLE users", user_input);
//! let q = Query::table(&table).find_many().build(); // compile error!
//! ```

pub mod arrow;
mod builder;
pub mod expr;
mod filter;
pub mod join;
pub mod nested;
pub mod paginate;
pub mod relation;
pub mod safe_ident;
pub mod stream;
pub mod validate;

pub use arrow::query_arrow;
pub use builder::{
    AggregateBuilder, AggregateFunc, BuiltQuery, CompareOp, CreateBuilder, CreateManyBuilder, Cte,
    DeleteBuilder, FindFirstBuilder, FindManyBuilder, Query, RawQueryBuilder, UpdateBuilder,
    UpsertBuilder,
};
pub use expr::{WindowFn, WindowSpec};
pub use filter::{Filter, Order};
pub use join::{Join, JoinCondition, JoinType};
pub use nested::{ChildWrite, create_with_children};
pub use paginate::{
    Base64PageTokenCodec, PageRequest, PageResponse, PageTokenCodec, PaginateConfig, paginate,
    paginate_with_codec,
};
pub use relation::{Include, RelationDef, RelationType, RowWithRelations, find_with_includes};
pub use safe_ident::SafeIdent;
pub use stream::find_many_stream;
pub use validate::SchemaValidator;
