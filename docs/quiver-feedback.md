<!-- agent-updated: 2026-03-13T04:45:00Z -->

# Quiver ORM Feedback

Evaluation based on hands-on experimentation with quiver-orm across two versions:
- **v1** (commit `895ba9f`): sync-only drivers, no pooling
- **v2** (commit `41cb0b3`): async-first drivers, connection pooling, `QuiverClient`

Tested against a simplified issue tracker schema (Component, Issue, Comment) using
the SQLite driver with in-memory databases.

## Experiment Setup

- Repo: https://github.com/Shuozeli/quiver-orm.git
- Crates used: `quiver-schema`, `quiver-codegen`, `quiver-query`, `quiver-driver-core`, `quiver-driver-sqlite`
- Experiment code: `docs/experiment-src/`
- Schema: 3 models (Component, Issue, Comment) with relations, defaults, nullable fields
- Runtime: `tokio` (async)

### Test Schema

```
config {
    provider "sqlite"
    database "experiment"
}

model Component {
    id                      Int32      @id @autoincrement
    name                    Utf8
    description             Utf8       @default("")
    parentId                Int32?
    expandedAccessEnabled   Boolean    @default(true)
    editableCommentsEnabled Boolean    @default(false)
    createdAt               Utf8       @default(now())
    updatedAt               Utf8       @default(now())
}

model Issue {
    id            Int32    @id @autoincrement
    title         Utf8
    description   Utf8     @default("")
    status        Utf8     @default("NEW")
    priority      Utf8     @default("P2")
    componentId   Int32
    component     Component @relation(fields: [componentId], references: [id])
    assignee      Utf8     @default("")
    reporter      Utf8     @default("")
    createdAt     Utf8     @default(now())

    @@index([componentId])
}

model Comment {
    id            Int32    @id @autoincrement
    issueId       Int32
    issue         Issue    @relation(fields: [issueId], references: [id])
    author        Utf8
    body          Utf8
    hidden        Boolean  @default(false)
    createdAt     Utf8     @default(now())

    @@index([issueId])
}
```

## What Works Well

### Schema Parsing & Validation

```rust
let schema_src = include_str!("../schema.quiver");
let schema = parse(schema_src).expect("parse schema");
validate(&schema).expect("validate schema");
```

### DDL Generation

```rust
let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).expect("generate DDL");
```

Generated output:

```sql
CREATE TABLE IF NOT EXISTS "Component" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL,
  "description" TEXT NOT NULL DEFAULT '',
  "parentId" INTEGER,
  "expandedAccessEnabled" INTEGER NOT NULL DEFAULT 1,
  "editableCommentsEnabled" INTEGER NOT NULL DEFAULT 0,
  "createdAt" TEXT NOT NULL DEFAULT (datetime('now')),
  "updatedAt" TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS "Issue" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "title" TEXT NOT NULL,
  -- ... (foreign keys, indexes generated correctly)
);
```

### Query Builder -- CRUD Examples

#### Create

```rust
let stmt = Query::table("Component")
    .create()
    .set("name", Value::Text("Backend".into()))
    .set("description", Value::Text("Backend services".into()))
    .build();
// SQL: INSERT INTO "Component" ("name", "description") VALUES (?1, ?2)
let affected = tx.execute(&stmt).await?;
// affected = 1
```

**vs prisma-rs:** prisma-rs returns the created row as JSON. Quiver returns only
`affected: u64`. Must run a follow-up SELECT to get the created row.

#### Read with Filters, Ordering, Projection

```rust
let stmt = Query::table("Issue")
    .find_many()
    .select(&["id", "title", "status", "priority", "assignee"])
    .filter(Filter::eq("status", Value::Text("NEW".into())))
    .order_by("id", Order::Desc)
    .limit(10)
    .build();
// SQL: SELECT "id", "title", "status", "priority", "assignee"
//      FROM "Issue" WHERE "status" = ?1 ORDER BY "id" DESC LIMIT 10
let rows = tx.query(&stmt).await?;
```

**vs prisma-rs:** prisma-rs returns `serde_json::Value` that deserializes to typed structs.
Quiver returns `Vec<Row>` with positional `values: Vec<Value>` -- must map by index.

#### Find One (no FindUnique)

```rust
let stmt = Query::table("Issue")
    .find_first()
    .filter(Filter::eq("id", Value::Int(1)))
    .build();
// SQL: SELECT * FROM "Issue" WHERE "id" = ?1 LIMIT 1
let row = tx.query_one(&stmt).await?;      // Err if no rows
let row = tx.query_optional(&stmt).await?;  // Option<Row>
```

#### Update

```rust
let stmt = Query::table("Issue")
    .update()
    .set("status", Value::Text("IN_PROGRESS".into()))
    .filter(Filter::eq("id", Value::Int(1)))
    .build();
// SQL: UPDATE "Issue" SET "status" = ?1 WHERE "id" = ?2
let updated = tx.execute(&stmt).await?;
// updated = 1 (affected rows, not the row itself)
```

#### Delete

```rust
let stmt = Query::table("Issue")
    .delete()
    .filter(Filter::eq("id", Value::Int(2)))
    .build();
// SQL: DELETE FROM "Issue" WHERE "id" = ?1
let deleted = tx.execute(&stmt).await?;
```

### Complex Filters (AND + OR Composition)

```rust
let stmt = Query::table("Issue")
    .find_many()
    .filter(Filter::and(vec![
        Filter::eq("status", Value::Text("IN_PROGRESS".into())),
        Filter::or(vec![
            Filter::eq("priority", Value::Text("P0".into())),
            Filter::eq("priority", Value::Text("P1".into())),
        ]),
    ]))
    .build();
// SQL: SELECT * FROM "Issue"
//      WHERE ("status" = ?1 AND ("priority" = ?2 OR "priority" = ?3))
```

**vs prisma-rs:** Quiver `Filter::and`/`Filter::or` are type-safe enums. prisma-rs uses
`json!()` macros with no compile-time validation.

### Aggregates

```rust
let stmt = Query::table("Issue")
    .aggregate()
    .count("id")      // count("*") panics -- see Bugs section
    .group_by("status")
    .build();
// SQL: SELECT "status", COUNT("id") AS "_count_id" FROM "Issue" GROUP BY "status"
let rows = tx.query(&stmt).await?;
```

Note: aggregate column is now aliased as `_count_id` (changed from v1).

### Async Transactions (NEW in v2)

```rust
// All traits are now async -- no spawn_blocking needed
let mut conn = SqliteDriver.connect(":memory:").await?;

// Commit flow
let tx = conn.begin().await?;
tx.execute(&stmt).await?;
tx.commit().await?;

// Rollback flow
let tx = conn.begin().await?;
tx.execute(&stmt).await?;
tx.rollback().await?;
// rows from this tx do NOT persist
```

**vs v1:** v1 was sync-only, requiring `tokio::task::spawn_blocking` wrappers.
v2 is natively async with `Send + Sync` connections.

### QuiverClient -- Biased Transactional API (NEW in v2)

Enforces that all data operations happen within an explicit transaction.
Only DDL can run outside a transaction.

```rust
use quiver_driver_core::QuiverClient;

let conn = SqliteDriver.connect(":memory:").await?;
let mut client = QuiverClient::new(conn);

// DDL works outside transaction
client.execute_ddl(&DdlStatement::new("CREATE TABLE ...".into())).await?;

// All data ops MUST go through transaction closure
let count = client.transaction(|tx| {
    Box::pin(async move {
        let stmt = Query::table("Component")
            .create()
            .set("name", Value::Text("via QuiverClient".into()))
            .build();
        tx.execute(&stmt).await?;

        let find = Query::table("Component").find_many().build();
        let rows = tx.query(&find).await?;
        Ok(rows.len())
    })
}).await?;
// count = 1
```

Auto-commit on `Ok`, auto-rollback on `Err`. This aligns with
the myissuetracker rule: "All database interactions must be within a transaction."

### Transaction Retry with Backoff (NEW in v2)

```rust
use quiver_driver_core::{QuiverClient, RetryPolicy};

let result = client.transaction_with_retry(
    RetryPolicy::default(),  // 3 retries, 100ms initial, 5s max
    |tx| {
        Box::pin(async move {
            // ... operations that might hit "database is locked"
            Ok(())
        })
    },
).await?;
```

Retries on `QuiverError::is_retryable()` -- covers "database is locked",
"serialization failure", "deadlock detected".

### Connection Pool (NEW in v2)

```rust
use quiver_driver_core::pool::{Pool, PoolConfig};
use quiver_driver_sqlite::SqlitePool;

let pool = SqlitePool::new(PoolConfig::new(":memory:", 4)).await?;
// max=4, idle=0 (connections created lazily)

let guard = pool.acquire().await?;
// guard implements Connection -- use directly
guard.execute(&stmt).await?;
let rows = guard.query(&find).await?;

drop(guard);
// connection returned to pool automatically
```

### SQL Injection Prevention

`SafeIdent` enforcing `&'static str` for table/column names:

```rust
// OK: string literals are &'static str
let q = Query::table("User").find_many().filter(Filter::eq("email", val)).build();

// WON'T COMPILE: String is not &'static str
let table = format!("{}; DROP TABLE users", user_input);
let q = Query::table(&table).find_many().build(); // compile error!
```

### Raw Queries

```rust
let stmt = Query::raw("SELECT COUNT(*) as cnt FROM Issue WHERE status = ?")
    .param(Value::Text("IN_PROGRESS".into()))
    .build();
let row = tx.query_one(&stmt).await?;
```

## Bugs Found

### BUG: `count("*")` panics at runtime

- **Severity:** Medium
- **Status:** Still present in v2
- **Reproduction:**
  ```rust
  let stmt = Query::table("Issue")
      .aggregate()
      .count("*")   // panics here
      .build();
  ```
- **Error:** `SafeIdent` rejects `*` (only allows `[a-zA-Z0-9_]`).
  Panic occurs inside `SafeIdent::new()` which calls `unwrap()`.
- **Workaround:** `.count("id")` instead of `.count("*")`
- **Recommendation:** Add `.count_all()` method or whitelist `*` in aggregate context.
  At minimum, return `Result::Err` instead of panicking.

### BUG: Create/Update do not return the modified row

- **Severity:** Medium
- **Status:** Still present in v2
- **Description:** `tx.execute(&create_stmt)` returns `u64` (affected rows).
  Every Create/Update needs a follow-up SELECT to get the result.
- **Impact:** 2x database roundtrips per write.
- **Recommendation:** Add `RETURNING *` clause option (PostgreSQL + SQLite 3.35+ support it).

### BUG: `.gitmodules` deleted but submodule entry remains

- **Severity:** High (blocks git-based dependency resolution)
- **Status:** New in v2 (commit `41cb0b3`)
- **Description:** The `.gitmodules` file was removed but `arrow-adbc-rs` still appears
  as a submodule entry in the git tree. Cargo fails with:
  ```
  no URL configured for submodule 'arrow-adbc-rs'
  ```
- **Impact:** Cannot use `git = "https://github.com/Shuozeli/quiver-orm.git"` in `Cargo.toml`.
  Must use local path deps or vendored copies.
- **Fix:** Either restore `.gitmodules` with the URL, or fully remove the submodule entry:
  ```bash
  git rm --cached arrow-adbc-rs
  ```

## Issues Resolved in v2

The following issues from v1 feedback have been addressed:

| Issue | v1 Status | v2 Status |
|-------|-----------|-----------|
| Sync-only drivers | Blocker | RESOLVED -- all traits natively async |
| No connection pooling | Blocker | RESOLVED -- `SqlitePool` with `PoolConfig` |
| `!Send`/`!Sync` connections | Blocker | RESOLVED -- `Arc`-wrapped, `Send + Sync` |
| No transaction retry | Pain point | RESOLVED -- `QuiverClient::transaction_with_retry` |
| No enforced transactional access | Design gap | RESOLVED -- `QuiverClient` biased API |

## Remaining Limitations

### 1. No `FindUnique` operation

Only `find_first()` and `find_many()`. Use `find_first().filter(Filter::eq("id", val))`.

**Impact:** Minor -- functionally equivalent.

### 2. No `@updatedAt` auto-population at query level

Must manually set timestamp on every update:

```rust
let stmt = Query::table("Component")
    .update()
    .set("status", Value::Text("IN_PROGRESS".into()))
    .set("updatedAt", Value::Text(chrono::Utc::now().to_rfc3339()))  // manual!
    .filter(Filter::eq("id", Value::Int(1)))
    .build();
```

### 3. No JSON/serde result mapping

Results are `Vec<Row>` with positional `values: Vec<Value>`. No built-in deserialization.

```rust
// Manual mapping required for every model
fn row_to_component(row: &Row) -> Component {
    Component {
        id: match &row.values[0] { Value::Int(v) => *v, _ => 0 },
        name: match &row.values[1] { Value::Text(v) => v.clone(), _ => String::new() },
        // ... every field by position
    }
}
```

### 4. Row results use positional indexing

No `Row::get("column_name")` convenience method. Must match by index or write a helper:

```rust
fn get_value<'a>(row: &'a Row, col: &str) -> Option<&'a Value> {
    row.columns.iter()
        .position(|c| c.name == col)
        .map(|i| &row.values[i])
}
```

### 5. No cursor-based pagination

prisma-rs has `.cursor()` + `.skip(1)`. Quiver requires manual filter-based pagination:

```rust
// Must use filter instead of cursor
if let Ok(cursor_id) = page_token.parse::<i64>() {
    builder = builder.filter(Filter::gt("id", Value::Int(cursor_id)));
}
```

## Side-by-Side: Migrating component_service.rs

### CreateComponent

```rust
// BEFORE (prisma-rs)
let input = ComponentCreateInput {
    name: req.name,
    description: Some(req.description),
    ..Default::default()
};
let data_json = serde_json::to_value(&input)?;
let qb = QueryBuilder::new("Component", Operation::CreateOne).data(data_json);
let result = tx.execute(&qb).await?;
let component: PrismaComponent = serde_json::from_value(result)?;
```

```rust
// AFTER (quiver v2 with QuiverClient)
let component = client.transaction(|tx| {
    Box::pin(async move {
        let stmt = Query::table("Component")
            .create()
            .set("name", Value::Text(req.name))
            .set("description", Value::Text(req.description))
            .set("expandedAccessEnabled", Value::Bool(true))
            .set("editableCommentsEnabled", Value::Bool(false))
            .build();
        tx.execute(&stmt).await?;

        // Must query back (no RETURNING support)
        let find = Query::table("Component")
            .find_first()
            .order_by("id", Order::Desc)
            .build();
        let row = tx.query_one(&find).await?;
        Ok(row_to_component(&row))
    })
}).await?;
```

### ListComponents with Pagination

```rust
// BEFORE (prisma-rs) -- cursor-based pagination
let mut qb = QueryBuilder::new("Component", Operation::FindMany)
    .where_arg(json!({"parentId": parent_id}))
    .order_by(json!([{"name": "asc"}]))
    .take(page_size);
if !page_token.is_empty() {
    qb = qb.cursor(json!({"id": cursor_id})).skip(1);
}
let result = tx.execute(&qb).await?;
let components: Vec<PrismaComponent> = serde_json::from_value(result)?;
```

```rust
// AFTER (quiver v2) -- filter-based pagination
let components = client.transaction(|tx| {
    Box::pin(async move {
        let mut builder = Query::table("Component")
            .find_many()
            .order_by("name", Order::Asc)
            .limit(page_size as u64);
        if let Some(pid) = parent_id {
            builder = builder.filter(Filter::eq("parentId", Value::Int(pid)));
        } else {
            builder = builder.filter(Filter::is_null("parentId"));
        }
        if let Ok(cursor_id) = page_token.parse::<i64>() {
            builder = builder.filter(Filter::gt("id", Value::Int(cursor_id)));
        }
        let rows = tx.query(&builder.build()).await?;
        Ok(rows.iter().map(row_to_component).collect::<Vec<_>>())
    })
}).await?;
```

## Migration Feasibility Assessment

### v2 significantly reduces migration effort

| Effort Area | v1 Estimate | v2 Estimate | Notes |
|-------------|-------------|-------------|-------|
| Async wrapper | ~100 lines | 0 lines | Native async |
| Connection pool | ~50 lines | ~5 lines | Built-in `SqlitePool` |
| Row mapping | ~200 lines | ~200 lines | Still manual |
| Schema translation | ~1 hour | ~1 hour | Same |
| Service migration | ~1-2 days/svc | ~1 day/svc | `QuiverClient` simplifies tx handling |

### Recommended approach

Follow the phased migration in `.claude/rules/quiver-migration.md`:
1. **Phase 0:** Add quiver deps alongside prisma-rs. Fix the `.gitmodules` bug first.
2. **Phase 1:** Migrate `component_service.rs` (simplest CRUD, 4 operations)
3. **Phase 2:** Expand to remaining 7 services
4. **Phase 3:** Remove prisma-rs after all 120+ tests pass

### Risk areas

- `.gitmodules` bug blocks git-based dependency resolution
- `@updatedAt` needs manual handling in every update
- Self-referential relations (Component -> parent) -- untested
- Composite unique constraints (`@@unique([a, b])`) -- untested
- No cursor pagination -- must switch to filter-based

## Experiment Output (v2 -- Full Run)

```
=== Quiver ORM Experiment (async) ===

[OK] Schema parsed and validated
[OK] DDL generated
[OK] Tables created

[SQL] INSERT INTO "Component" ("name", "description") VALUES (?1, ?2)
[PARAMS] [Text("Backend"), Text("Backend services")]
[OK] Created component, affected: 1

[SQL] SELECT "id", "name", "description", "expandedAccessEnabled"
      FROM "Component" WHERE "name" = ?1
[OK] Found 1 rows
  Columns: ["id", "name", "description", "expandedAccessEnabled"]
  Values: [Int(1), Text("Backend"), Text("Backend services"), Int(1)]

[OK] Created issue, issue 2, comment

[SQL] SELECT "id", "title", "status", "priority", "assignee"
      FROM "Issue" WHERE "status" = ?1 ORDER BY "id" DESC LIMIT 10
[OK] Found 2 issues

[SQL] UPDATE "Issue" SET "status" = ?1 WHERE "id" = ?2
[OK] Updated 1 rows

[SQL] DELETE FROM "Issue" WHERE "id" = ?1
[OK] Deleted 1 rows

[SQL] SELECT "status", COUNT("id") AS "_count_id" FROM "Issue" GROUP BY "status"
[OK] Aggregate: 1 groups

[SQL] SELECT * FROM "Issue"
      WHERE ("status" = ?1 AND ("priority" = ?2 OR "priority" = ?3))
[OK] Complex filter: 1 results

[SQL] SELECT COUNT(*) as cnt FROM Issue WHERE status = ?
[OK] Raw query count: [Int(1)]

[OK] Transaction committed
[OK] Transaction rolled back
[OK] Rollback verified -- no phantom rows

[OK] QuiverClient: DDL outside transaction works
[OK] QuiverClient: transaction returned 1 rows

[OK] Pool created: max=4, idle=0
[OK] Pool: queried 1 rows
[OK] Pool: connection returned, idle=0

=== All experiments passed ===
```

## Recommendations for quiver-orm

1. **Fix `.gitmodules` / submodule entry** -- git deps are broken; run `git rm --cached arrow-adbc-rs` or restore `.gitmodules`
2. **Fix `count("*")` panic** -- Add `count_all()` or whitelist `*` in aggregate context
3. **Return `Result` instead of panicking** -- `SafeIdent` validation must never crash
4. **Add `Row::get(&str) -> Option<&Value>`** -- Positional indexing is error-prone
5. **Add `RETURNING` support** -- Eliminates extra SELECT roundtrip after Create/Update
6. **Update README examples** -- At least 3 API signatures in docs don't match actual code (`order`, `group_by`, `validate`)
