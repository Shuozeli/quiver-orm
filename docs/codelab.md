# Quiver Codelab

A hands-on walkthrough building a blog application with Quiver. By the end
you will have: a schema, generated code, a running database, and working
queries.

## Prerequisites

- Rust 1.85+ (edition 2024)
- SQLite (for this codelab; PostgreSQL/MySQL work too)

## Step 1: Create Your Schema

Create `blog.quiver`:

```
config {
    provider "sqlite"
    url      "blog.db"
}

enum PostStatus {
    Draft
    Published
    Archived
}

model Author {
    id       Int32    PRIMARY KEY AUTOINCREMENT
    email    Utf8     UNIQUE
    name     Utf8
    bio      Utf8?

    INDEX (email)
    MAP "authors"
}

model Post {
    id        Int32       PRIMARY KEY AUTOINCREMENT
    title     Utf8
    content   LargeUtf8?
    status    PostStatus  DEFAULT Draft
    views     UInt32      DEFAULT 0
    authorId  Int32

    FOREIGN KEY (authorId) REFERENCES Author (id) ON DELETE CASCADE
    INDEX (authorId)
    INDEX (status)
    MAP "posts"
}

model Tag {
    id     Int32  PRIMARY KEY AUTOINCREMENT
    name   Utf8   UNIQUE
    postId Int32

    FOREIGN KEY (postId) REFERENCES Post (id)
    MAP "tags"
}
```

## Step 2: Validate Your Schema

```bash
quiver parse blog.quiver
```

Expected output:

```
Schema parsed successfully.
  Enums:  1
  Models: 3
    Author (5 fields)
    Post (8 fields)
    Tag (4 fields)
```

## Step 3: Generate Code

Generate typed Rust client, TypeScript types, and SQL DDL:

```bash
# Type-safe Rust client (query helpers, filters, CreateData/UpdateData)
quiver generate blog.quiver -t rust-client -o src/generated/

# TypeScript interfaces
quiver generate blog.quiver -t typescript -o generated/

# SQL schema for SQLite
quiver generate blog.quiver -t sql-sqlite -o generated/

# Rust serde structs (data models only)
quiver generate blog.quiver -t rust-serde -o generated/
```

Inspect the generated TypeScript (`generated/models.ts`):

```typescript
export enum PostStatus {
  Draft = "Draft",
  Published = "Published",
  Archived = "Archived",
}

export interface Author {
  id: number;
  email: string;
  name: string;
  bio: string | null;
}

export interface AuthorCreateInput {
  email: string;
  name: string;
  bio?: string | null;
}
// ... more types
```

Inspect the generated SQL (`generated/schema.sql`):

```sql
CREATE TABLE IF NOT EXISTS "authors" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "email" TEXT NOT NULL UNIQUE,
  "name" TEXT NOT NULL,
  "bio" TEXT
);

CREATE TABLE IF NOT EXISTS "posts" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "title" TEXT NOT NULL,
  "content" TEXT,
  "status" TEXT NOT NULL DEFAULT 'Draft',
  "views" INTEGER NOT NULL DEFAULT 0,
  "authorId" INTEGER NOT NULL,
  FOREIGN KEY ("authorId") REFERENCES "authors"("id") ON DELETE CASCADE
);
-- ...
```

## Step 4: Push Schema to Database

```bash
quiver db push blog.quiver
```

This creates the SQLite database file `blog.db` with all tables.

## Step 5: Write Queries in Rust

Add dependencies to your `Cargo.toml`:

```toml
[dependencies]
quiver-query = { git = "https://github.com/Shuozeli/quiver-orm.git", branch = "main" }
quiver-driver-core = { git = "https://github.com/Shuozeli/quiver-orm.git", branch = "main" }
quiver-driver-sqlite = { git = "https://github.com/Shuozeli/quiver-orm.git", branch = "main" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Include the generated client in your project:

```rust
// src/generated/mod.rs
#[allow(dead_code)]
mod client;
pub use client::*;
```

Write your application using the generated typed modules:

```rust
mod generated;
use generated::{author, post};
use quiver_driver_core::{Driver, QuiverClient, Value};
use quiver_driver_sqlite::SqliteDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect and wrap in QuiverClient (enforces transactional access)
    let mut client = QuiverClient::new(SqliteDriver.connect("blog.db").await?);

    // --- Create author and posts in a transaction ---
    client.transaction(|tx| Box::pin(async move {
        // Typed create -- required fields enforced by compiler
        tx.execute(&author::create()
            .set(author::fields::EMAIL, "alice@example.com")
            .set(author::fields::NAME, "Alice")
            .set(author::fields::BIO, "Rust enthusiast")
            .build()).await?;

        // Get the author's ID
        let found = tx.query_one(&author::find_first()
            .filter(author::filter::email_eq("alice@example.com"))
            .build()).await?;
        let author_id = found.get_i64(0).expect("expected id");

        // Create posts using typed field constants
        tx.execute(&post::create()
            .set(post::fields::TITLE, "Getting Started with Quiver")
            .set(post::fields::CONTENT, "Quiver is an Arrow-native ORM...")
            .set(post::fields::STATUS, "Published")
            .set(post::fields::AUTHOR_ID, Value::Int(author_id))
            .build()).await?;

        tx.execute(&post::create()
            .set(post::fields::TITLE, "Advanced Queries")
            .set(post::fields::CONTENT, "Window functions, CTEs, and more...")
            .set(post::fields::STATUS, "Draft")
            .set(post::fields::AUTHOR_ID, Value::Int(author_id))
            .build()).await?;

        Ok(())
    })).await?;

    // --- Query and update in a separate transaction ---
    let affected = client.transaction(|tx| Box::pin(async move {
        // Typed filters -- no risk of misspelling field names
        let rows = tx.query(&post::find_many()
            .select(&[post::fields::ID, post::fields::TITLE, post::fields::STATUS, post::fields::VIEWS])
            .filter(post::filter::status_eq("Published"))
            .order_by(post::order::id_desc())
            .build()).await?;

        for row in &rows {
            println!("Post: {:?}", row.values);
        }

        // Aggregate: count posts by status
        let counts = tx.query(&post::aggregate()
            .count_all()
            .group_by(post::fields::STATUS)
            .build()).await?;

        for row in &counts {
            println!("Status count: {:?}", row.values);
        }

        // Update: publish the draft
        let affected = tx.execute(&post::update()
            .set(post::fields::STATUS, "Published")
            .filter(post::filter::status_eq("Draft"))
            .build()).await?;

        Ok(affected)
    })).await?;

    println!("Published {} post(s)", affected);
    Ok(())
}
```

## Step 6: Use Migrations

Instead of `db push`, use migrations for production workflows:

```bash
# Create initial migration
quiver migrate create blog.quiver init

# Apply it
quiver migrate apply blog.quiver

# Check status
quiver migrate status blog.quiver
```

Now add a field to your schema (add `featured Boolean DEFAULT false` to Post),
then create a new migration:

```bash
quiver migrate create blog.quiver add_featured_flag
quiver migrate apply blog.quiver
```

The migration engine diffs the current schema against the last snapshot and
generates the appropriate ALTER TABLE statement.

## Step 7: Introspect an Existing Database

If you have an existing database, pull its schema:

```bash
quiver db pull blog.quiver -o introspected.quiver
```

This connects to the database, reads the table/column metadata, and generates
a `.quiver` schema file.

## Step 8: Use JOINs and Relations

```rust
use quiver_query::{Join, JoinType, find_with_includes};

// JOIN posts with authors (inside a transaction)
let query = post::find_many()
    .select(&[post::fields::TITLE, post::fields::STATUS])
    .join(Join::new(JoinType::Inner, "authors")
        .on("posts", post::fields::AUTHOR_ID, "authors", author::fields::ID))
    .filter(post::filter::status_eq("Published"))
    .build();

// Or use generated relation includes:
let results = client.transaction(|tx| Box::pin(async move {
    let query = author::find_many().build();
    let include = author::relations::include_posts();
    find_with_includes(tx, &query, &[include]).await
})).await?;
```

## Step 9: Pagination

```rust
use quiver_query::{PageRequest, PaginateConfig, paginate};

let config = PaginateConfig {
    default_page_size: 10,
    max_page_size: 100,
    include_total_size: false,
};

let page1 = client.transaction(|tx| Box::pin(async move {
    let base_query = post::find_many()
        .order_by(post::order::id_asc())
        .build();

    let request = PageRequest::first_page(10);
    paginate(tx, &base_query, None, &request, &config).await
})).await?;

// Next page (using token from previous response)
if !page1.next_page_token.is_empty() {
    let token = page1.next_page_token.clone();
    let page2 = client.transaction(|tx| Box::pin(async move {
        let base_query = post::find_many()
            .order_by(post::order::id_asc())
            .build();

        let request = PageRequest::next(10, &token);
        paginate(tx, &base_query, None, &request, &config).await
    })).await?;
}
```

## Step 10: Generate for Multiple Targets

Quiver generates from one schema to many targets:

```bash
# Type-safe Rust client (query helpers, filters, CreateData/UpdateData)
quiver generate blog.quiver -t rust-client -o backend/src/generated/

# Rust data structs (Serialize/Deserialize)
quiver generate blog.quiver -t rust-serde -o backend/generated/

# For your TypeScript frontend
quiver generate blog.quiver -t typescript -o frontend/src/generated/

# For your FlatBuffers wire format
quiver generate blog.quiver -t rust-fbs -o backend/generated/

# For your Protobuf interchange
quiver generate blog.quiver -t rust-proto -o backend/generated/

# Database DDL
quiver generate blog.quiver -t sql-postgres -o migrations/
```

All targets are generated from the same `.quiver` source of truth.

---

## Summary

What you built:

1. A `.quiver` schema with 3 models, an enum, relations, and indexes
2. Generated TypeScript types, SQL DDL, and Rust structs
3. Created and applied database migrations
4. Wrote type-safe queries with SQL injection prevention
5. Used `QuiverClient` with transactions, aggregations, JOINs, and pagination

## Next Steps

- Read the [usage guide](usage.md) for complete API reference
- Look at `crates/quiver-e2e/src/lib.rs` for more query examples
- Look at `crates/quiver-query/tests/sqlite_integration.rs` for integration test patterns
