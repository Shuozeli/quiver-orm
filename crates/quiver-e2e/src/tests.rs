use quiver_codegen::{SqlDialect, SqlGenerator};
use quiver_schema::parse;
use quiver_schema::validate::validate;
use rusqlite::Connection;

/// Helper: parse schema, validate, generate DDL, create in-memory SQLite DB.
fn setup_db(schema_source: &str) -> (Connection, quiver_schema::Schema) {
    let schema = parse(schema_source).expect("schema parse failed");
    validate(&schema).expect("schema validation failed");

    let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).expect("DDL generation failed");

    let conn = Connection::open_in_memory().expect("failed to open SQLite");
    conn.execute_batch("PRAGMA foreign_keys = ON;")
        .expect("failed to enable foreign keys");
    conn.execute_batch(&ddl).expect("failed to execute DDL");

    (conn, schema)
}

// -----------------------------------------------------------------------
// Basic CRUD
// -----------------------------------------------------------------------

#[test]
fn create_and_read_single_row() {
    let (conn, _schema) = setup_db(
        r#"
        model User {
            id    Int32  @id @autoincrement
            email Utf8   @unique
            name  Utf8?
            active Boolean @default(true)
        }
    "#,
    );

    conn.execute(
        "INSERT INTO \"User\" (email, name) VALUES (?1, ?2)",
        rusqlite::params!["alice@example.com", "Alice"],
    )
    .unwrap();

    let (id, email, name, active): (i32, String, Option<String>, bool) = conn
        .query_row(
            "SELECT id, email, name, active FROM \"User\" WHERE email = ?1",
            rusqlite::params!["alice@example.com"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();

    assert_eq!(id, 1);
    assert_eq!(email, "alice@example.com");
    assert_eq!(name, Some("Alice".to_string()));
    assert!(active);
}

#[test]
fn autoincrement_generates_ids() {
    let (conn, _schema) = setup_db(
        r#"
        model Item {
            id   Int32 @id @autoincrement
            name Utf8
        }
    "#,
    );

    conn.execute("INSERT INTO \"Item\" (name) VALUES ('a')", [])
        .unwrap();
    conn.execute("INSERT INTO \"Item\" (name) VALUES ('b')", [])
        .unwrap();
    conn.execute("INSERT INTO \"Item\" (name) VALUES ('c')", [])
        .unwrap();

    let ids: Vec<i32> = {
        let mut stmt = conn.prepare("SELECT id FROM \"Item\" ORDER BY id").unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };

    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn nullable_field_accepts_null() {
    let (conn, _schema) = setup_db(
        r#"
        model User {
            id   Int32 @id @autoincrement
            name Utf8?
            bio  Utf8?
        }
    "#,
    );

    conn.execute(
        "INSERT INTO \"User\" (name, bio) VALUES (?1, ?2)",
        rusqlite::params!["Alice", rusqlite::types::Null],
    )
    .unwrap();

    let bio: Option<String> = conn
        .query_row("SELECT bio FROM \"User\" WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();

    assert_eq!(bio, None);
}

#[test]
fn not_null_rejects_null() {
    let (conn, _schema) = setup_db(
        r#"
        model User {
            id    Int32 @id @autoincrement
            email Utf8
        }
    "#,
    );

    let result = conn.execute(
        "INSERT INTO \"User\" (email) VALUES (?1)",
        rusqlite::params![rusqlite::types::Null],
    );

    assert!(result.is_err(), "NOT NULL column should reject NULL");
}

// -----------------------------------------------------------------------
// Unique constraints
// -----------------------------------------------------------------------

#[test]
fn unique_constraint_prevents_duplicates() {
    let (conn, _schema) = setup_db(
        r#"
        model User {
            id    Int32 @id @autoincrement
            email Utf8  @unique
        }
    "#,
    );

    conn.execute(
        "INSERT INTO \"User\" (email) VALUES ('alice@example.com')",
        [],
    )
    .unwrap();

    let result = conn.execute(
        "INSERT INTO \"User\" (email) VALUES ('alice@example.com')",
        [],
    );

    assert!(result.is_err(), "UNIQUE should prevent duplicate emails");
}

// -----------------------------------------------------------------------
// Default values
// -----------------------------------------------------------------------

#[test]
fn boolean_default_value() {
    let (conn, _schema) = setup_db(
        r#"
        model Setting {
            id      Int32   @id @autoincrement
            enabled Boolean @default(true)
            hidden  Boolean @default(false)
        }
    "#,
    );

    conn.execute("INSERT INTO \"Setting\" DEFAULT VALUES", [])
        .unwrap();

    let (enabled, hidden): (bool, bool) = conn
        .query_row(
            "SELECT enabled, hidden FROM \"Setting\" WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    assert!(enabled);
    assert!(!hidden);
}

#[test]
fn integer_default_value() {
    let (conn, _schema) = setup_db(
        r#"
        model Counter {
            id    Int32  @id @autoincrement
            count UInt32 @default(0)
            score Int16  @default(100)
        }
    "#,
    );

    conn.execute("INSERT INTO \"Counter\" DEFAULT VALUES", [])
        .unwrap();

    let (count, score): (u32, i16) = conn
        .query_row(
            "SELECT count, score FROM \"Counter\" WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    assert_eq!(count, 0);
    assert_eq!(score, 100);
}

#[test]
fn enum_default_value() {
    let (conn, _schema) = setup_db(
        r#"
        enum Status { Draft Published Archived }
        model Article {
            id     Int32  @id @autoincrement
            status Status @default(Draft)
        }
    "#,
    );

    conn.execute("INSERT INTO \"Article\" DEFAULT VALUES", [])
        .unwrap();

    let status: String = conn
        .query_row("SELECT status FROM \"Article\" WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();

    assert_eq!(status, "Draft");
}

// -----------------------------------------------------------------------
// Enum fields
// -----------------------------------------------------------------------

#[test]
fn enum_stored_as_text() {
    let (conn, _schema) = setup_db(
        r#"
        enum Role { User Admin Moderator }
        model Account {
            id   Int32 @id @autoincrement
            role Role
        }
    "#,
    );

    conn.execute(
        "INSERT INTO \"Account\" (role) VALUES (?1)",
        rusqlite::params!["Admin"],
    )
    .unwrap();

    let role: String = conn
        .query_row("SELECT role FROM \"Account\" WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();

    assert_eq!(role, "Admin");
}

// -----------------------------------------------------------------------
// Relations / Foreign keys
// -----------------------------------------------------------------------

#[test]
fn foreign_key_constraint_enforced() {
    let (conn, _schema) = setup_db(
        r#"
        model User {
            id    Int32  @id @autoincrement
            posts Post[] @relation
        }
        model Post {
            id       Int32 @id @autoincrement
            authorId Int32
            author   User  @relation(fields: [authorId], references: [id])
        }
    "#,
    );

    // Insert a user first
    conn.execute("INSERT INTO \"User\" DEFAULT VALUES", [])
        .unwrap();

    // Insert a post with valid FK
    conn.execute(
        "INSERT INTO \"Post\" (authorId) VALUES (?1)",
        rusqlite::params![1],
    )
    .unwrap();

    // Insert a post with invalid FK should fail
    let result = conn.execute(
        "INSERT INTO \"Post\" (authorId) VALUES (?1)",
        rusqlite::params![999],
    );

    assert!(
        result.is_err(),
        "FK constraint should reject nonexistent authorId"
    );
}

#[test]
fn join_across_relation() {
    let (conn, _schema) = setup_db(
        r#"
        model User {
            id    Int32  @id @autoincrement
            name  Utf8
            posts Post[] @relation
        }
        model Post {
            id       Int32 @id @autoincrement
            title    Utf8
            authorId Int32
            author   User  @relation(fields: [authorId], references: [id])
        }
    "#,
    );

    conn.execute(
        "INSERT INTO \"User\" (name) VALUES (?1)",
        rusqlite::params!["Alice"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO \"Post\" (title, authorId) VALUES (?1, ?2)",
        rusqlite::params!["First Post", 1],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO \"Post\" (title, authorId) VALUES (?1, ?2)",
        rusqlite::params!["Second Post", 1],
    )
    .unwrap();

    let posts: Vec<(String, String)> = {
        let mut stmt = conn
            .prepare(
                r#"SELECT p.title, u.name
                   FROM "Post" p
                   JOIN "User" u ON p.authorId = u.id
                   ORDER BY p.id"#,
            )
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };

    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0], ("First Post".to_string(), "Alice".to_string()));
    assert_eq!(posts[1], ("Second Post".to_string(), "Alice".to_string()));
}

// -----------------------------------------------------------------------
// Table/column mapping (@@map, @map)
// -----------------------------------------------------------------------

#[test]
fn table_and_column_mapping() {
    let (conn, _schema) = setup_db(
        r#"
        model UserProfile {
            id       Int32 @id @autoincrement
            fullName Utf8  @map("full_name")
            @@map("user_profiles")
        }
    "#,
    );

    conn.execute(
        "INSERT INTO \"user_profiles\" (full_name) VALUES (?1)",
        rusqlite::params!["Alice Smith"],
    )
    .unwrap();

    let name: String = conn
        .query_row(
            "SELECT full_name FROM \"user_profiles\" WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(name, "Alice Smith");
}

// -----------------------------------------------------------------------
// Indexes
// -----------------------------------------------------------------------

#[test]
fn index_is_created() {
    let (conn, _schema) = setup_db(
        r#"
        model User {
            id    Int32 @id @autoincrement
            email Utf8
            @@index([email])
        }
    "#,
    );

    // Verify the index exists via sqlite_master
    let count: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_User_email%'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(count, 1);
}

// -----------------------------------------------------------------------
// Numeric types
// -----------------------------------------------------------------------

#[test]
fn various_numeric_types() {
    let (conn, _schema) = setup_db(
        r#"
        model Numbers {
            id      Int32   @id @autoincrement
            small   Int8
            medium  Int32
            big     Int64
            uflag   UInt8
            price   Float64
        }
    "#,
    );

    conn.execute(
        "INSERT INTO \"Numbers\" (small, medium, big, uflag, price) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![42i8, 100_000i32, 9_000_000_000i64, 255u8, 19.99f64],
    )
    .unwrap();

    let (small, medium, big, uflag, price): (i8, i32, i64, u8, f64) = conn
        .query_row(
            "SELECT small, medium, big, uflag, price FROM \"Numbers\" WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();

    assert_eq!(small, 42);
    assert_eq!(medium, 100_000);
    assert_eq!(big, 9_000_000_000);
    assert_eq!(uflag, 255);
    assert!((price - 19.99).abs() < f64::EPSILON);
}

// -----------------------------------------------------------------------
// JSON-encoded collections
// -----------------------------------------------------------------------

#[test]
fn list_stored_as_json() {
    let (conn, _schema) = setup_db(
        r#"
        model Doc {
            id   Int32 @id @autoincrement
            tags List<Utf8> @default([])
        }
    "#,
    );

    let tags = serde_json::json!(["rust", "arrow", "quiver"]).to_string();
    conn.execute(
        "INSERT INTO \"Doc\" (tags) VALUES (?1)",
        rusqlite::params![tags],
    )
    .unwrap();

    let stored: String = conn
        .query_row("SELECT tags FROM \"Doc\" WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();

    let parsed: Vec<String> = serde_json::from_str(&stored).unwrap();
    assert_eq!(parsed, vec!["rust", "arrow", "quiver"]);
}

#[test]
fn default_empty_list() {
    let (conn, _schema) = setup_db(
        r#"
        model Doc {
            id   Int32 @id @autoincrement
            tags List<Utf8> @default([])
        }
    "#,
    );

    conn.execute("INSERT INTO \"Doc\" DEFAULT VALUES", [])
        .unwrap();

    let stored: String = conn
        .query_row("SELECT tags FROM \"Doc\" WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();

    let parsed: Vec<String> = serde_json::from_str(&stored).unwrap();
    assert!(parsed.is_empty());
}

// -----------------------------------------------------------------------
// Temporal fields
// -----------------------------------------------------------------------

#[test]
fn timestamp_default_now() {
    let (conn, _schema) = setup_db(
        r#"
        model Event {
            id      Int32 @id @autoincrement
            created Timestamp(Microsecond, UTC) @default(now())
        }
    "#,
    );

    conn.execute("INSERT INTO \"Event\" DEFAULT VALUES", [])
        .unwrap();

    let created: String = conn
        .query_row("SELECT created FROM \"Event\" WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();

    // Should be a valid datetime string (SQLite's datetime('now') format)
    assert!(
        created.contains('-'),
        "expected ISO date format, got: {created}"
    );
}

// -----------------------------------------------------------------------
// Update and delete
// -----------------------------------------------------------------------

#[test]
fn update_row() {
    let (conn, _schema) = setup_db(
        r#"
        model User {
            id    Int32 @id @autoincrement
            name  Utf8
            score Int32 @default(0)
        }
    "#,
    );

    conn.execute("INSERT INTO \"User\" (name) VALUES ('Alice')", [])
        .unwrap();

    conn.execute("UPDATE \"User\" SET score = 42 WHERE id = 1", [])
        .unwrap();

    let score: i32 = conn
        .query_row("SELECT score FROM \"User\" WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();

    assert_eq!(score, 42);
}

#[test]
fn delete_row() {
    let (conn, _schema) = setup_db(
        r#"
        model User {
            id   Int32 @id @autoincrement
            name Utf8
        }
    "#,
    );

    conn.execute("INSERT INTO \"User\" (name) VALUES ('Alice')", [])
        .unwrap();
    conn.execute("INSERT INTO \"User\" (name) VALUES ('Bob')", [])
        .unwrap();

    conn.execute("DELETE FROM \"User\" WHERE name = 'Alice'", [])
        .unwrap();

    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM \"User\"", [], |row| row.get(0))
        .unwrap();

    assert_eq!(count, 1);
}

// -----------------------------------------------------------------------
// Transactions
// -----------------------------------------------------------------------

#[test]
fn transaction_commit() {
    let (mut conn, _schema) = setup_db(
        r#"
        model User {
            id   Int32 @id @autoincrement
            name Utf8
        }
    "#,
    );

    {
        let tx = conn.transaction().unwrap();
        tx.execute("INSERT INTO \"User\" (name) VALUES ('Alice')", [])
            .unwrap();
        tx.execute("INSERT INTO \"User\" (name) VALUES ('Bob')", [])
            .unwrap();
        tx.commit().unwrap();
    }

    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM \"User\"", [], |row| row.get(0))
        .unwrap();

    assert_eq!(count, 2);
}

#[test]
fn transaction_rollback() {
    let (mut conn, _schema) = setup_db(
        r#"
        model User {
            id   Int32 @id @autoincrement
            name Utf8
        }
    "#,
    );

    conn.execute("INSERT INTO \"User\" (name) VALUES ('Alice')", [])
        .unwrap();

    {
        let tx = conn.transaction().unwrap();
        tx.execute("INSERT INTO \"User\" (name) VALUES ('Bob')", [])
            .unwrap();
        tx.execute("INSERT INTO \"User\" (name) VALUES ('Charlie')", [])
            .unwrap();
        // Drop without commit = rollback
    }

    let count: i32 = conn
        .query_row("SELECT COUNT(*) FROM \"User\"", [], |row| row.get(0))
        .unwrap();

    assert_eq!(count, 1, "rollback should undo Bob and Charlie inserts");
}

// -----------------------------------------------------------------------
// Full schema (multi-model with relations, enums, indexes)
// -----------------------------------------------------------------------

#[test]
fn full_schema_e2e() {
    let (conn, _schema) = setup_db(
        r#"
        enum Role { User Admin Moderator }

        model User {
            id       Int32   @id @autoincrement
            email    Utf8    @unique
            name     Utf8?
            active   Boolean @default(true)
            role     Role    @default(User)
            posts    Post[]  @relation

            @@index([email])
        }

        model Post {
            id        Int32    @id @autoincrement
            title     Utf8
            content   LargeUtf8?
            published Boolean  @default(false)
            views     UInt32   @default(0)
            authorId  Int32
            author    User     @relation(fields: [authorId], references: [id])

            @@index([authorId])
        }
    "#,
    );

    // Create users
    conn.execute(
        "INSERT INTO \"User\" (email, name, role) VALUES (?1, ?2, ?3)",
        rusqlite::params!["alice@test.com", "Alice", "Admin"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO \"User\" (email, name) VALUES (?1, ?2)",
        rusqlite::params!["bob@test.com", "Bob"],
    )
    .unwrap();

    // Create posts
    conn.execute(
        "INSERT INTO \"Post\" (title, content, published, authorId) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params!["Hello World", "My first post", true, 1],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO \"Post\" (title, authorId) VALUES (?1, ?2)",
        rusqlite::params!["Draft Post", 1],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO \"Post\" (title, published, authorId) VALUES (?1, ?2, ?3)",
        rusqlite::params!["Bob's Post", true, 2],
    )
    .unwrap();

    // Verify defaults
    let (role, active): (String, bool) = conn
        .query_row(
            "SELECT role, active FROM \"User\" WHERE email = 'bob@test.com'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(role, "User");
    assert!(active);

    let (published, views): (bool, u32) = conn
        .query_row(
            "SELECT published, views FROM \"Post\" WHERE title = 'Draft Post'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(!published);
    assert_eq!(views, 0);

    // Count posts per user via join
    let alice_posts: i32 = conn
        .query_row(
            r#"SELECT COUNT(*) FROM "Post" p
               JOIN "User" u ON p.authorId = u.id
               WHERE u.email = 'alice@test.com'"#,
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(alice_posts, 2);

    // Published posts only
    let published_count: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM \"Post\" WHERE published = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(published_count, 2);

    // Verify indexes exist
    let idx_count: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        idx_count >= 2,
        "expected at least 2 indexes, got {idx_count}"
    );
}

// -----------------------------------------------------------------------
// Decimal type (stored as TEXT in SQLite)
// -----------------------------------------------------------------------

#[test]
fn decimal_stored_as_text() {
    let (conn, _schema) = setup_db(
        r#"
        model Product {
            id    Int32 @id @autoincrement
            price Decimal128(10, 2) @default(0)
        }
    "#,
    );

    conn.execute(
        "INSERT INTO \"Product\" (price) VALUES (?1)",
        rusqlite::params!["99.95"],
    )
    .unwrap();

    let price: String = conn
        .query_row("SELECT price FROM \"Product\" WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();

    assert_eq!(price, "99.95");
}

// -----------------------------------------------------------------------
// Binary type
// -----------------------------------------------------------------------

#[test]
fn binary_stored_as_blob() {
    let (conn, _schema) = setup_db(
        r#"
        model File {
            id   Int32  @id @autoincrement
            data Binary
        }
    "#,
    );

    let bytes: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
    conn.execute(
        "INSERT INTO \"File\" (data) VALUES (?1)",
        rusqlite::params![bytes],
    )
    .unwrap();

    let stored: Vec<u8> = conn
        .query_row("SELECT data FROM \"File\" WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();

    assert_eq!(stored, vec![0xDE, 0xAD, 0xBE, 0xEF]);
}
