# Quiver Usage Guide

## Installation

Build from source:

```bash
cd quiver
cargo build --release
# Binary at target/release/quiver
```

## Schema Language

Quiver schemas use the `.quiver` extension. A schema defines database
configuration, codegen targets, enums, and models.

### Config Block

```
config {
    provider "postgresql"    // sqlite, postgresql, mysql
    url      "postgresql://localhost:5432/myapp"
    database "myapp"
}
```

### Generate Block

```
generate {
    flatbuffers  "./generated/fb"
    protobuf     "./generated/proto"
    rust         "./generated/rs"
    typescript   "./generated/ts"
}
```

### Enums

```
enum Role {
    User
    Admin
    Moderator
}
```

### Models

```
model User {
    // field    Type                          Attributes
    id         Int32                         @id @autoincrement
    email      Utf8                          @unique
    name       Utf8?                                              // nullable
    age        Int16?
    balance    Decimal128(10, 2)             @default(0)
    score      Float64                       @default(0.0)
    avatar     Binary?
    active     Boolean                       @default(true)
    created    Timestamp(Microsecond, UTC)   @default(now())
    tags       List<Utf8>                    @default([])
    metadata   Map<Utf8, Utf8>?
    role       Role                          @default(User)

    // Relations
    posts      Post[]                        @relation
    profile    Profile?                      @relation

    // Model-level attributes
    @@index([email])
    @@map("users")
}

model Post {
    id         Int32    @id @autoincrement
    title      Utf8
    content    LargeUtf8?
    published  Boolean  @default(false)
    authorId   Int32
    author     User     @relation(fields: [authorId], references: [id], onDelete: Cascade)

    @@index([authorId])
}
```

### Type Reference

**Integers:** Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64

**Floats:** Float16, Float32, Float64

**Decimal:** Decimal128(precision, scale), Decimal256(precision, scale)

**String:** Utf8 (up to 2GB), LargeUtf8 (up to 8EB)

**Binary:** Binary, LargeBinary, FixedSizeBinary(n)

**Boolean:** Boolean

**Temporal:** Date32, Date64, Time32(Second|Millisecond), Time64(Microsecond|Nanosecond), Timestamp(unit, timezone?)

**Nested:** List\<T\>, LargeList\<T\>, Map\<K, V\>, Struct\<{ field Type, ... }\>

**Nullability:** Append `?` to any type: `Utf8?`, `Int32?`, `List<Utf8>?`

### Referential Actions

Foreign key relations support `onDelete` and `onUpdate`:

```
author User @relation(fields: [authorId], references: [id], onDelete: Cascade, onUpdate: Restrict)
```

Actions: `Cascade`, `Restrict`, `SetNull`, `SetDefault`, `NoAction`

---

## CLI Commands

### Parse

Validate a schema file and print summary:

```bash
quiver parse schema.quiver
```

### Generate

Generate code from a schema. Available targets:

| Target | Alias | Output | Description |
|--------|-------|--------|-------------|
| rust-client | client | client.rs | Type-safe query helpers (field constants, filters, CreateData/UpdateData) |
| rust-serde | rust | models.rs | Data structs with Serialize/Deserialize + TryFrom\<&Row\> |
| flatbuffers | fbs | schema.fbs | FlatBuffers schema |
| protobuf | proto | schema.proto | Protobuf schema |
| rust-fbs | -- | schema.fbs + schema_fbs.rs | FlatBuffers + Rust bindings |
| rust-proto | -- | schema.proto + per-module .rs | Protobuf + Rust bindings |
| typescript | ts | models.ts | TypeScript interfaces + enums |
| sql-sqlite | sqlite, sql | schema.sql | SQLite DDL |
| sql-postgres | postgres | schema.sql | PostgreSQL DDL |
| sql-mysql | mysql | schema.sql | MySQL DDL |

The `rust-serde` target also generates `TryFrom<&Row>` implementations for
each model, enabling automatic deserialization from query results:

```rust
// Generated: TryFrom<&Row> for User, TryFrom<&Row> for Post, etc.
let rows = tx.query(&user::find_many().build()).await?;
let users: Vec<User> = rows.iter().map(User::try_from).collect::<Result<_, _>>()?;
```

For enums, `Display` and `FromStr` implementations are also generated, so
enum fields stored as text in the database are automatically parsed during
row deserialization.

```bash
# Type-safe Rust client (recommended for queries)
quiver generate schema.quiver -t rust-client -o generated/

# TypeScript types for frontend
quiver generate schema.quiver -t typescript -o generated/

# SQL DDL for migrations
quiver generate schema.quiver -t sql-postgres -o generated/
```

### Migrate

Manage database schema migrations:

```bash
# Create a migration from schema changes
quiver migrate create schema.quiver add_user_tags

# Apply all pending migrations
quiver migrate apply schema.quiver

# Show migration status
quiver migrate status schema.quiver

# Rollback the last applied migration
quiver migrate rollback schema.quiver
```

Migrations are stored in a `migrations/` directory (configurable with `-d`).
Each migration contains:
- `up.json` -- forward migration SQL statements
- `down.json` -- rollback SQL statements
- `schema.quiver` -- schema snapshot at this point

### Database

Direct database management:

```bash
# Push schema changes directly (no migration files)
quiver db push schema.quiver

# Pull database schema into a .quiver file
quiver db pull schema.quiver -o introspected.quiver

# Execute raw SQL
quiver db execute schema.quiver "SELECT * FROM users WHERE active = true"
```

---

## Query Builder

Generate a type-safe Rust client from your schema:

```bash
quiver generate schema.quiver -t rust-client -o src/generated/
```

This generates `client.rs` with typed modules for each model. All examples
below assume this generated client. Using it ensures field names are
compile-time constants -- typos become compile errors, not runtime bugs.

### Setup

```rust
mod generated;
use generated::{user, post};
use quiver_driver_core::{Driver, QuiverClient, Value};
use quiver_driver_sqlite::SqliteDriver;

let mut client = QuiverClient::new(SqliteDriver.connect(":memory:").await?);
```

### Find Many

```rust
let rows = client.transaction(|tx| Box::pin(async move {
    let query = user::find_many()
        .select(&[user::fields::ID, user::fields::EMAIL, user::fields::NAME])
        .filter(user::filter::active_is_true())
        .filter(user::filter::balance_gte(100.0))
        .order_by(user::order::created_desc())
        .limit(10)
        .offset(20)
        .build();

    tx.query(&query).await
})).await?;
```

### Find First

```rust
let row = tx.query_one(&user::find_first()
    .filter(user::filter::email_eq("alice@example.com"))
    .build()).await?;
```

### Create

```rust
// Using CreateData struct (all required fields enforced by compiler):
let data = user::CreateData {
    email: "alice@example.com".into(),
    name: Some("Alice".into()),
    balance: None, // uses schema default
};
client.transaction(|tx| Box::pin(async move {
    tx.execute(&data.to_query()).await
})).await?;

// Or using the builder directly with field constants:
tx.execute(&user::create()
    .set(user::fields::EMAIL, "alice@example.com")
    .set(user::fields::NAME, "Alice")
    .set(user::fields::ACTIVE, true)
    .build()).await?;
```

### Create Many (Batch)

```rust
let query = user::create_many()
    .columns(&[user::fields::EMAIL, user::fields::NAME])
    .values(vec![
        vec!["alice@example.com".into(), "Alice".into()],
        vec!["bob@example.com".into(), "Bob".into()],
    ])
    .build();
```

### Update

```rust
// Using UpdateData struct (all fields optional):
let data = user::UpdateData {
    name: Some("Alice Smith".into()),
    active: Some(false),
    ..Default::default()
};
tx.execute(&data.to_query(user::filter::id_eq(1))).await?;

// Or using the builder:
let query = user::update()
    .set(user::fields::NAME, "Alice Smith")
    .set(user::fields::ACTIVE, false)
    .filter(user::filter::id_eq(1))
    .build();
```

### Delete

```rust
let query = user::delete()
    .filter(user::filter::id_eq(1))
    .build();
```

### Upsert

```rust
let query = user::upsert()
    .conflict_columns(&[user::fields::EMAIL])
    .set(user::fields::EMAIL, "alice@example.com")
    .set(user::fields::NAME, "Alice")
    .update_set(user::fields::NAME, "Alice Updated")
    .build();
```

### Aggregation

```rust
let query = post::aggregate()
    .count_all()
    .sum(post::fields::VIEWS)
    .avg(post::fields::VIEWS)
    .min(post::fields::VIEWS)
    .max(post::fields::VIEWS)
    .group_by(post::fields::AUTHOR_ID)
    .having(Filter::gte("count", Value::Int(5)))
    .build();
```

### DISTINCT

```rust
let query = user::find_many()
    .select(&[user::fields::ROLE])
    .distinct()
    .build();
```

### Filters

The generated `filter` module provides per-field typed filters. You can also
use the raw `Filter` constructors with field constants:

```rust
use quiver_query::Filter;

// Generated typed filters (preferred):
user::filter::email_eq("alice@example.com")
user::filter::email_like("%alice%")
user::filter::email_contains("alice")      // wraps with %...%
user::filter::score_gt(100)
user::filter::score_between(10, 100)
user::filter::active_is_true()
user::filter::bio_is_null()

// Raw Filter with field constants:
Filter::eq(user::fields::EMAIL, "alice@example.com")
Filter::is_in(user::fields::ROLE, vec!["Admin".into(), "Moderator".into()])

// Logical combinators:
Filter::and(vec![user::filter::active_is_true(), user::filter::score_gte(100)])
Filter::or(vec![user::filter::email_like("%@corp.com"), user::filter::role_eq("Admin")])
Filter::not(user::filter::active_is_true())

// Raw (compile-time literal only):
Filter::raw("EXISTS (SELECT 1 FROM other_table WHERE other_table.id = t.id)")
```

### JOINs

```rust
use quiver_query::{Join, JoinType};

let query = post::find_many()
    .join(Join::new(JoinType::Inner, "users")
        .on("posts", post::fields::AUTHOR_ID, "users", user::fields::ID))
    .filter(post::filter::published_is_true())
    .build();
```

Join types: `Inner`, `Left`, `Right`, `Full`

### Relations (Include)

Load related records via the generated relation definitions:

```rust
use quiver_query::find_with_includes;

let results = client.transaction(|tx| Box::pin(async move {
    // Generated relation + include helpers
    let include = user::relations::include_posts();

    find_with_includes(tx, &main_query, &[include]).await
})).await?;
```

### Nested Writes

Create parent and children atomically. Note: `create_with_children` manages
its own transaction internally -- do not wrap it in another `transaction()` call:

```rust
use quiver_query::{ChildWrite, create_with_children};

let parent = user::create()
    .set(user::fields::EMAIL, "alice@example.com")
    .set(user::fields::NAME, "Alice")
    .build();

let child = ChildWrite::new("Post")
    .columns(&[post::fields::TITLE, post::fields::PUBLISHED])
    .values(vec![Value::from("First Post"), Value::from(true)]);

let (parent_id, affected) = create_with_children(
    &mut client_conn,  // requires &mut impl Transactional
    &parent,
    post::fields::AUTHOR_ID,
    &[child],
).await?;
```

### Pagination (AIP-132)

Cursor-based pagination with Base64 page tokens:

```rust
use quiver_query::{PageRequest, PaginateConfig, paginate};

let page = client.transaction(|tx| Box::pin(async move {
    let base_query = user::find_many()
        .filter(user::filter::active_is_true())
        .order_by(user::order::id_asc())
        .build();

    let request = PageRequest::first_page(10);
    let config = PaginateConfig {
        default_page_size: 10,
        max_page_size: 100,
        include_total_size: false,
    };

    paginate(tx, &base_query, None, &request, &config).await
})).await?;

println!("Results: {:?}", page.items);
println!("Next page token: {:?}", page.next_page_token);

// Next page:
// let request = PageRequest::next(10, &page.next_page_token);
```

### Window Functions

```rust
use quiver_query::expr::{Expr, WindowFn, WindowSpec};

let expr = Expr::Window {
    func: WindowFn::RowNumber,
    over: WindowSpec::new()
        .partition_by(user::fields::DEPARTMENT)
        .order_by(user::fields::SALARY, false),
};
// ROW_NUMBER() OVER (PARTITION BY "department" ORDER BY "salary" DESC)
```

### CTEs (Common Table Expressions)

```rust
use quiver_query::Cte;

let cte = Cte {
    name: "active_users",
    query: user::find_many()
        .filter(user::filter::active_is_true())
        .build(),
};

let query = Query::table("active_users")
    .find_many()
    .with_cte(cte)
    .build();
```

### Raw Queries

For queries that cannot be expressed with the builder:

```rust
use quiver_query::Query;

let query = Query::raw("SELECT * FROM users WHERE id = ?")
    .param(Value::Int(42))
    .build();
```

### Schema Validation

Validate queries against a schema at build time:

```rust
use quiver_query::SchemaValidator;

let validator = SchemaValidator::new()
    .table("User", &[user::fields::ID, user::fields::EMAIL, user::fields::NAME, user::fields::ACTIVE]);

// Returns Ok(()) or Err with details about invalid tables/columns
validator.validate_find_many(&builder)?;
```

---

## Database Drivers

### SQLite

```rust
use quiver_driver_sqlite::SqliteDriver;
use quiver_driver_core::{Driver, QuiverClient};

let mut client = QuiverClient::new(SqliteDriver.connect(":memory:").await?);
// or
let mut client = QuiverClient::new(SqliteDriver.connect("path/to/database.db").await?);
```

### PostgreSQL

```rust
use quiver_driver_postgres::PostgresDriver;
use quiver_driver_core::{Driver, QuiverClient};

let mut client = QuiverClient::new(
    PostgresDriver.connect("postgresql://user:pass@localhost:5432/mydb").await?
);
```

The PostgreSQL driver automatically rewrites `?` placeholders to `$1, $2, ...`.

### MySQL

```rust
use quiver_driver_mysql::MysqlDriver;
use quiver_driver_core::{Driver, QuiverClient};

let mut client = QuiverClient::new(
    MysqlDriver.connect("mysql://user:pass@localhost:3306/mydb").await?
);
```

### Transactions

Quiver enforces that all data operations happen within a transaction.
Use `QuiverClient` which only exposes transactional access:

```rust
use quiver_driver_core::{Driver, QuiverClient};

let mut client = QuiverClient::new(driver.connect(url).await?);

// All queries and mutations must be inside a transaction
client.transaction(|tx| Box::pin(async move {
    tx.execute(&insert_query).await?;
    tx.execute(&update_query).await?;
    Ok(())
    // Commits on Ok, rolls back on Err
})).await?;
```

For operations that may fail due to contention, use retry:

```rust
use quiver_driver_core::RetryPolicy;

client.transaction_with_retry(RetryPolicy::default(), |tx| Box::pin(async move {
    tx.execute(&upsert_query).await?;
    Ok(())
    // Retries on serialization failures, deadlocks, lock timeouts
})).await?;
```

Transactions auto-rollback on drop if not committed.

### ADBC Access

Each driver re-exports its ADBC crate for Arrow-native access:

```rust
// SQLite ADBC
use quiver_driver_sqlite::adbc_sqlite;

// PostgreSQL ADBC
use quiver_driver_postgres::adbc_postgres;

// MySQL ADBC
use quiver_driver_mysql::adbc_mysql;
```

ADBC drivers return data as Arrow `RecordBatch` rather than row-by-row `Value`
types, which is more efficient for analytics workloads and bulk operations.

#### TLS Connections (ADBC)

PostgreSQL and FlightSQL ADBC drivers support optional TLS. Enable the `tls`
feature on the relevant ADBC crate in your `Cargo.toml`:

```toml
# PostgreSQL with TLS (uses native-tls)
adbc-postgres = { git = "https://github.com/Shuozeli/arrow-adbc-rs.git", features = ["tls"] }

# FlightSQL with TLS (uses tonic native roots)
adbc-flightsql = { git = "https://github.com/Shuozeli/arrow-adbc-rs.git", features = ["tls"] }
```

#### Bulk Ingest (ADBC)

ADBC drivers support high-throughput bulk loading via `Statement::bind` +
`Statement::execute_update`:

- **PostgreSQL**: Uses the `COPY FROM STDIN` protocol for maximum throughput
- **SQLite**: Uses transactions with column-oriented batch insertion
- **MySQL**: Uses transactional batch `INSERT`

#### SQL Safety (ADBC)

ADBC queries use the `TrustedSql` type to prevent SQL injection. Construct
trusted SQL via the `trusted_sql!` macro:

```rust
use adbc::sql::trusted_sql;

let sql = trusted_sql!("SELECT * FROM users WHERE active = true");
```

Runtime strings cannot be converted to `TrustedSql` -- the `from_raw()`
constructor is sealed to the `adbc` crate.
