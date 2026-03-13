//! Real-world demo: full migration workflow with SQLite.
//!
//! These tests demonstrate the complete lifecycle of a Quiver ORM application:
//!
//! 1. Define schema -> diff -> generate migration -> apply
//! 2. Evolve schema -> diff -> generate migration -> apply
//! 3. Insert data, query it, verify constraints
//! 4. Rollback migrations when needed
//!
//! Each test is self-contained and uses an in-memory SQLite database.

use quiver_driver_core::{Connection, Driver, Statement, Transaction, Transactional, Value};
use quiver_driver_sqlite::SqliteDriver;
use quiver_migrate::{
    Migration, MigrationSqlGenerator, MigrationTracker, SqlDialect, diff_schemas,
};
use quiver_schema::parse;

async fn mem_db() -> quiver_driver_sqlite::SqliteConnection {
    SqliteDriver.connect(":memory:").await.unwrap()
}

// ---------------------------------------------------------------------------
// Demo 1: Blog platform -- initial schema + evolve with enum + index
// ---------------------------------------------------------------------------

#[tokio::test]
async fn demo_blog_platform_schema_evolution() {
    let conn = mem_db().await;

    // --- V1: bare-bones blog schema ---
    let v1 = parse(
        r#"
        model Author {
            id    Int32 PRIMARY KEY AUTOINCREMENT
            name  Utf8
            email Utf8  UNIQUE
        }
        model Post {
            id       Int32 PRIMARY KEY AUTOINCREMENT
            title    Utf8
            body     Utf8?
            authorId Int32
            FOREIGN KEY (authorId) REFERENCES Author (id)
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &v1);
    let up = MigrationSqlGenerator::generate_up(&steps, &v1, SqlDialect::Sqlite).unwrap();
    let down = MigrationSqlGenerator::generate_down(&steps, &v1, SqlDialect::Sqlite).unwrap();

    let m1 = Migration {
        id: "001_initial_blog".to_string(),
        description: "Create Author and Post tables".to_string(),
        up,
        down,
    };
    MigrationTracker::apply(&conn, &m1, "2026-03-01T00:00:00Z")
        .await
        .unwrap();

    // Insert some data
    conn.execute(&Statement::new(
        "INSERT INTO \"Author\" (name, email) VALUES (?, ?)".to_string(),
        vec![
            Value::Text("Alice".into()),
            Value::Text("alice@blog.com".into()),
        ],
    ))
    .await
    .unwrap();

    conn.execute(&Statement::new(
        "INSERT INTO \"Post\" (title, body, authorId) VALUES (?, ?, ?)".to_string(),
        vec![
            Value::Text("Hello World".into()),
            Value::Text("My first post".into()),
            Value::Int(1),
        ],
    ))
    .await
    .unwrap();

    // Verify FK constraint works
    let bad_fk = conn
        .execute(&Statement::new(
            "INSERT INTO \"Post\" (title, authorId) VALUES (?, ?)".to_string(),
            vec![Value::Text("Orphan".into()), Value::Int(999)],
        ))
        .await;
    assert!(
        bad_fk.is_err(),
        "FK constraint should reject invalid authorId"
    );

    // --- V2: add enum, new fields, index ---
    let v2 = parse(
        r#"
        enum PostStatus { Draft Published Archived }
        model Author {
            id    Int32 PRIMARY KEY AUTOINCREMENT
            name  Utf8
            email Utf8  UNIQUE
        }
        model Post {
            id       Int32      PRIMARY KEY AUTOINCREMENT
            title    Utf8
            body     Utf8?
            status   PostStatus DEFAULT Draft
            authorId Int32
            FOREIGN KEY (authorId) REFERENCES Author (id)
            INDEX (authorId)
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(Some(&v1), &v2);
    let up = MigrationSqlGenerator::generate_up(&steps, &v2, SqlDialect::Sqlite).unwrap();
    let down = MigrationSqlGenerator::generate_down(&steps, &v2, SqlDialect::Sqlite).unwrap();

    // Should have: CreateEnum + AddField(status) + CreateIndex
    assert_eq!(up.len(), 3);

    let m2 = Migration {
        id: "002_add_status_and_index".to_string(),
        description: "Add PostStatus enum, status field, and authorId index".to_string(),
        up,
        down,
    };
    MigrationTracker::apply(&conn, &m2, "2026-03-02T00:00:00Z")
        .await
        .unwrap();

    // Insert with new status field (uses default)
    conn.execute(&Statement::new(
        "INSERT INTO \"Post\" (title, authorId) VALUES (?, ?)".to_string(),
        vec![Value::Text("Draft Post".into()), Value::Int(1)],
    ))
    .await
    .unwrap();

    // Query posts -- the original post has NULL status (added column), new post has 'Draft'
    let rows = conn
        .query(&Statement::sql(
            "SELECT title, status FROM \"Post\" ORDER BY id".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get_string(0), Some("Hello World".into()));
    // Old row gets DEFAULT value on the new column
    assert_eq!(rows[1].get_string(0), Some("Draft Post".into()));
    assert_eq!(rows[1].get_string(1), Some("Draft".into()));

    // Verify index was created
    let index_rows = conn
        .query(&Statement::sql(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'Post' AND name LIKE 'idx_%'"
                .to_string(),
        ))
        .await
        .unwrap();
    assert!(!index_rows.is_empty(), "authorId index should exist");

    // Verify enum values table
    let enum_rows = conn
        .query(&Statement::sql(
            "SELECT value FROM \"_enum_PostStatus\" ORDER BY value".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(enum_rows.len(), 3);

    // Verify migration history
    let applied = MigrationTracker::applied(&conn).await.unwrap();
    assert_eq!(
        applied,
        vec!["001_initial_blog", "002_add_status_and_index"]
    );
}

// ---------------------------------------------------------------------------
// Demo 2: E-commerce -- multi-step migration with alter field
// ---------------------------------------------------------------------------

#[tokio::test]
async fn demo_ecommerce_type_evolution() {
    let conn = mem_db().await;

    // --- V1: product with integer price (cents) ---
    let v1 = parse(
        r#"
        model Product {
            id    Int32 PRIMARY KEY AUTOINCREMENT
            name  Utf8
            price Int32
            sku   Utf8  UNIQUE
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &v1);
    let m1 = Migration {
        id: "001_create_products".to_string(),
        description: "Create Product table with integer price".to_string(),
        up: MigrationSqlGenerator::generate_up(&steps, &v1, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&steps, &v1, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m1, "2026-03-01T00:00:00Z")
        .await
        .unwrap();

    // Seed products
    for (name, price, sku) in &[
        ("Widget", 999, "WID-001"),
        ("Gadget", 2499, "GAD-001"),
        ("Gizmo", 14999, "GIZ-001"),
    ] {
        conn.execute(&Statement::new(
            "INSERT INTO \"Product\" (name, price, sku) VALUES (?, ?, ?)".to_string(),
            vec![
                Value::Text(name.to_string()),
                Value::Int(*price),
                Value::Text(sku.to_string()),
            ],
        ))
        .await
        .unwrap();
    }

    // --- V2: change price from Int32 to Float64 (decimal dollars) ---
    let v2 = parse(
        r#"
        model Product {
            id    Int32   PRIMARY KEY AUTOINCREMENT
            name  Utf8
            price Float64
            sku   Utf8    UNIQUE
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(Some(&v1), &v2);
    assert_eq!(steps.len(), 1, "should be exactly one AlterField step");

    let m2 = Migration {
        id: "002_price_to_float".to_string(),
        description: "Change price from Int32 to Float64".to_string(),
        up: MigrationSqlGenerator::generate_up(&steps, &v2, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&steps, &v2, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m2, "2026-03-05T00:00:00Z")
        .await
        .unwrap();

    // Verify data was preserved through the table rebuild
    let rows = conn
        .query(&Statement::sql(
            "SELECT name, price, sku FROM \"Product\" ORDER BY id".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get_string(0), Some("Widget".into()));
    // SQLite coerces INT -> REAL during the table rebuild
    assert_eq!(rows[0].get_string(2), Some("WID-001".into()));
    assert_eq!(rows[2].get_string(0), Some("Gizmo".into()));

    // Verify we can insert float values now
    conn.execute(&Statement::new(
        "INSERT INTO \"Product\" (name, price, sku) VALUES (?, ?, ?)".to_string(),
        vec![
            Value::Text("Doohickey".into()),
            Value::Float(12.99),
            Value::Text("DOO-001".into()),
        ],
    ))
    .await
    .unwrap();

    let row = conn
        .query_one(&Statement::new(
            "SELECT price FROM \"Product\" WHERE sku = ?".to_string(),
            vec![Value::Text("DOO-001".into())],
        ))
        .await
        .unwrap();
    let price = row.get_f64(0).unwrap();
    assert!((price - 12.99).abs() < f64::EPSILON);

    // Verify column type changed to REAL
    let info = conn
        .query(&Statement::sql(
            "PRAGMA table_info(\"Product\")".to_string(),
        ))
        .await
        .unwrap();
    let price_col = info
        .iter()
        .find(|r| r.values[1] == Value::Text("price".into()))
        .unwrap();
    assert_eq!(price_col.values[2], Value::Text("REAL".into()));
}

// ---------------------------------------------------------------------------
// Demo 3: SaaS multi-tenant -- transactions + rollback
// ---------------------------------------------------------------------------

#[tokio::test]
async fn demo_saas_tenants_with_rollback() {
    let mut conn = mem_db().await;

    // --- V1: tenant + user tables ---
    let v1 = parse(
        r#"
        model Tenant {
            id   Int32 PRIMARY KEY AUTOINCREMENT
            name Utf8  UNIQUE
        }
        model User {
            id       Int32 PRIMARY KEY AUTOINCREMENT
            email    Utf8  UNIQUE
            tenantId Int32
            FOREIGN KEY (tenantId) REFERENCES Tenant (id)
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &v1);
    let m1 = Migration {
        id: "001_multi_tenant".to_string(),
        description: "Create Tenant and User tables".to_string(),
        up: MigrationSqlGenerator::generate_up(&steps, &v1, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&steps, &v1, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m1, "2026-03-01T00:00:00Z")
        .await
        .unwrap();

    // Create tenants
    conn.execute(&Statement::new(
        "INSERT INTO \"Tenant\" (name) VALUES (?)".to_string(),
        vec![Value::Text("Acme Corp".into())],
    ))
    .await
    .unwrap();
    conn.execute(&Statement::new(
        "INSERT INTO \"Tenant\" (name) VALUES (?)".to_string(),
        vec![Value::Text("Globex Inc".into())],
    ))
    .await
    .unwrap();

    // Transaction: add users to Acme
    {
        let tx = conn.begin().await.unwrap();
        tx.execute(&Statement::new(
            "INSERT INTO \"User\" (email, tenantId) VALUES (?, ?)".to_string(),
            vec![Value::Text("alice@acme.com".into()), Value::Int(1)],
        ))
        .await
        .unwrap();
        tx.execute(&Statement::new(
            "INSERT INTO \"User\" (email, tenantId) VALUES (?, ?)".to_string(),
            vec![Value::Text("bob@acme.com".into()), Value::Int(1)],
        ))
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    // Failed transaction: should rollback
    {
        let tx = conn.begin().await.unwrap();
        tx.execute(&Statement::new(
            "INSERT INTO \"User\" (email, tenantId) VALUES (?, ?)".to_string(),
            vec![Value::Text("charlie@globex.com".into()), Value::Int(2)],
        ))
        .await
        .unwrap();
        // Drop without commit -> automatic rollback
    }

    // Verify: only 2 users (the committed ones)
    let users = conn
        .query(&Statement::sql("SELECT COUNT(*) FROM \"User\"".to_string()))
        .await
        .unwrap();
    assert_eq!(users[0].get_i64(0), Some(2));

    // --- V2: add subscription tier enum ---
    let v2 = parse(
        r#"
        enum SubscriptionTier { Free Pro Enterprise }
        model Tenant {
            id   Int32            PRIMARY KEY AUTOINCREMENT
            name Utf8             UNIQUE
            tier SubscriptionTier DEFAULT Free
        }
        model User {
            id       Int32 PRIMARY KEY AUTOINCREMENT
            email    Utf8  UNIQUE
            tenantId Int32
            FOREIGN KEY (tenantId) REFERENCES Tenant (id)
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(Some(&v1), &v2);
    let m2 = Migration {
        id: "002_add_subscription_tier".to_string(),
        description: "Add SubscriptionTier enum and tier field to Tenant".to_string(),
        up: MigrationSqlGenerator::generate_up(&steps, &v2, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&steps, &v2, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m2, "2026-03-05T00:00:00Z")
        .await
        .unwrap();

    // Upgrade Acme to Enterprise
    conn.execute(&Statement::new(
        "UPDATE \"Tenant\" SET tier = ? WHERE name = ?".to_string(),
        vec![
            Value::Text("Enterprise".into()),
            Value::Text("Acme Corp".into()),
        ],
    ))
    .await
    .unwrap();

    // Join query: list users with their tenant tier
    let rows = conn
        .query(&Statement::sql(
            "SELECT u.email, t.name, t.tier FROM \"User\" u \
             JOIN \"Tenant\" t ON u.tenantId = t.id \
             ORDER BY u.email"
                .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get_string(0), Some("alice@acme.com".into()));
    assert_eq!(rows[0].get_string(1), Some("Acme Corp".into()));
    assert_eq!(rows[0].get_string(2), Some("Enterprise".into()));

    // Rollback m2 to verify rollback works
    MigrationTracker::rollback(&conn, &m2).await.unwrap();

    // Tier column should be gone
    let result = conn
        .query(&Statement::sql("SELECT tier FROM \"Tenant\"".to_string()))
        .await;
    assert!(
        result.is_err(),
        "tier column should not exist after rollback"
    );

    // But original data should still be there
    let tenants = conn
        .query(&Statement::sql(
            "SELECT name FROM \"Tenant\" ORDER BY name".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(tenants.len(), 2);
    assert_eq!(tenants[0].get_string(0), Some("Acme Corp".into()));

    // Verify migration history
    let applied = MigrationTracker::applied(&conn).await.unwrap();
    assert_eq!(applied, vec!["001_multi_tenant"]);
}

// ---------------------------------------------------------------------------
// Demo 4: Task tracker -- enum evolution (add values)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn demo_task_tracker_enum_evolution() {
    let conn = mem_db().await;

    // --- V1: basic task tracker ---
    let v1 = parse(
        r#"
        enum Priority { Low Medium High }
        model Task {
            id       Int32    PRIMARY KEY AUTOINCREMENT
            title    Utf8
            priority Priority DEFAULT Medium
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &v1);
    let m1 = Migration {
        id: "001_create_tasks".to_string(),
        description: "Create Task table with Priority enum".to_string(),
        up: MigrationSqlGenerator::generate_up(&steps, &v1, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&steps, &v1, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m1, "2026-03-01T00:00:00Z")
        .await
        .unwrap();

    // Seed tasks
    for (title, priority) in &[
        ("Fix login bug", "High"),
        ("Update docs", "Low"),
        ("Refactor auth", "Medium"),
    ] {
        conn.execute(&Statement::new(
            "INSERT INTO \"Task\" (title, priority) VALUES (?, ?)".to_string(),
            vec![
                Value::Text(title.to_string()),
                Value::Text(priority.to_string()),
            ],
        ))
        .await
        .unwrap();
    }

    // --- V2: add Critical and Backlog priority levels ---
    let v2 = parse(
        r#"
        enum Priority { Backlog Low Medium High Critical }
        model Task {
            id       Int32    PRIMARY KEY AUTOINCREMENT
            title    Utf8
            priority Priority DEFAULT Medium
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(Some(&v1), &v2);
    // Should have: AddEnumValue(Backlog) + AddEnumValue(Critical)
    assert_eq!(steps.len(), 2);

    let m2 = Migration {
        id: "002_add_priority_levels".to_string(),
        description: "Add Backlog and Critical priority values".to_string(),
        up: MigrationSqlGenerator::generate_up(&steps, &v2, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&steps, &v2, SqlDialect::Sqlite).unwrap(),
    };

    // Verify the generated SQL uses bind parameters (not interpolation)
    for stmt in &m2.up {
        assert!(stmt.has_params(), "enum DML should use bind parameters");
    }

    MigrationTracker::apply(&conn, &m2, "2026-03-05T00:00:00Z")
        .await
        .unwrap();

    // Use the new enum values
    conn.execute(&Statement::new(
        "INSERT INTO \"Task\" (title, priority) VALUES (?, ?)".to_string(),
        vec![
            Value::Text("Production outage".into()),
            Value::Text("Critical".into()),
        ],
    ))
    .await
    .unwrap();

    conn.execute(&Statement::new(
        "INSERT INTO \"Task\" (title, priority) VALUES (?, ?)".to_string(),
        vec![
            Value::Text("Nice-to-have feature".into()),
            Value::Text("Backlog".into()),
        ],
    ))
    .await
    .unwrap();

    // Verify all enum values exist
    let enum_vals = conn
        .query(&Statement::sql(
            "SELECT value FROM \"_enum_Priority\" ORDER BY value".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(enum_vals.len(), 5);

    // Query by priority
    let critical = conn
        .query(&Statement::new(
            "SELECT title FROM \"Task\" WHERE priority = ?".to_string(),
            vec![Value::Text("Critical".into())],
        ))
        .await
        .unwrap();
    assert_eq!(critical.len(), 1);
    assert_eq!(critical[0].get_string(0), Some("Production outage".into()));
}

// ---------------------------------------------------------------------------
// Demo 5: Analytics dashboard -- composite schema with multiple relations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn demo_analytics_dashboard() {
    let conn = mem_db().await;

    let schema = parse(
        r#"
        enum EventType { PageView Click Purchase }
        model Campaign {
            id    Int32 PRIMARY KEY AUTOINCREMENT
            name  Utf8  UNIQUE
            budget Int32 DEFAULT 0
        }
        model Event {
            id         Int32     PRIMARY KEY AUTOINCREMENT
            eventType  EventType
            campaignId Int32
            value      Int32     DEFAULT 0
            FOREIGN KEY (campaignId) REFERENCES Campaign (id)
            INDEX (campaignId)
            INDEX (eventType)
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &schema);
    let m = Migration {
        id: "001_analytics".to_string(),
        description: "Create analytics schema".to_string(),
        up: MigrationSqlGenerator::generate_up(&steps, &schema, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&steps, &schema, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m, "2026-03-01T00:00:00Z")
        .await
        .unwrap();

    // Create campaigns
    conn.execute(&Statement::new(
        "INSERT INTO \"Campaign\" (name, budget) VALUES (?, ?)".to_string(),
        vec![Value::Text("Summer Sale".into()), Value::Int(10000)],
    ))
    .await
    .unwrap();
    conn.execute(&Statement::new(
        "INSERT INTO \"Campaign\" (name, budget) VALUES (?, ?)".to_string(),
        vec![Value::Text("Winter Promo".into()), Value::Int(5000)],
    ))
    .await
    .unwrap();

    // Generate events
    let events = vec![
        ("PageView", 1, 0),
        ("PageView", 1, 0),
        ("Click", 1, 0),
        ("Purchase", 1, 49),
        ("PageView", 2, 0),
        ("Click", 2, 0),
        ("Purchase", 2, 29),
        ("Purchase", 2, 99),
    ];
    for (event_type, campaign_id, value) in &events {
        conn.execute(&Statement::new(
            "INSERT INTO \"Event\" (eventType, campaignId, value) VALUES (?, ?, ?)".to_string(),
            vec![
                Value::Text(event_type.to_string()),
                Value::Int(*campaign_id),
                Value::Int(*value),
            ],
        ))
        .await
        .unwrap();
    }

    // Aggregate: total revenue per campaign
    let revenue = conn
        .query(&Statement::sql(
            "SELECT c.name, SUM(e.value) as total_revenue \
             FROM \"Event\" e \
             JOIN \"Campaign\" c ON e.campaignId = c.id \
             WHERE e.eventType = 'Purchase' \
             GROUP BY c.name \
             ORDER BY total_revenue DESC"
                .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(revenue.len(), 2);
    assert_eq!(revenue[0].get_string(0), Some("Winter Promo".into()));
    assert_eq!(revenue[0].get_i64(1), Some(128)); // 29 + 99
    assert_eq!(revenue[1].get_string(0), Some("Summer Sale".into()));
    assert_eq!(revenue[1].get_i64(1), Some(49));

    // Aggregate: conversion funnel per campaign
    let funnel = conn
        .query(&Statement::sql(
            "SELECT c.name, e.eventType, COUNT(*) as cnt \
             FROM \"Event\" e \
             JOIN \"Campaign\" c ON e.campaignId = c.id \
             GROUP BY c.name, e.eventType \
             ORDER BY c.name, e.eventType"
                .to_string(),
        ))
        .await
        .unwrap();
    // Summer Sale: Click=1, PageView=2, Purchase=1
    // Winter Promo: Click=1, PageView=1, Purchase=2
    assert_eq!(funnel.len(), 6);

    // Verify indexes were created
    let indexes = conn
        .query(&Statement::sql(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'Event' AND name LIKE 'idx_%'"
                .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(indexes.len(), 2, "should have 2 indexes on Event");
}

// ---------------------------------------------------------------------------
// Demo 6: Migration history inspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn demo_migration_history_audit() {
    let conn = mem_db().await;

    // Apply three migrations
    let v1 = parse("model User { id Int32 PRIMARY KEY }").unwrap();
    let s1 = diff_schemas(None, &v1);
    let m1 = Migration {
        id: "20260301_001_users".to_string(),
        description: "Create User table".to_string(),
        up: MigrationSqlGenerator::generate_up(&s1, &v1, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&s1, &v1, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m1, "2026-03-01T10:00:00Z")
        .await
        .unwrap();

    let v2 = parse(
        r#"
        model User { id Int32 PRIMARY KEY }
        model Post { id Int32 PRIMARY KEY }
    "#,
    )
    .unwrap();
    let s2 = diff_schemas(Some(&v1), &v2);
    let m2 = Migration {
        id: "20260305_002_posts".to_string(),
        description: "Create Post table".to_string(),
        up: MigrationSqlGenerator::generate_up(&s2, &v2, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&s2, &v2, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m2, "2026-03-05T14:30:00Z")
        .await
        .unwrap();

    let v3 = parse(
        r#"
        enum Tag { Tech Science Art }
        model User { id Int32 PRIMARY KEY }
        model Post { id Int32 PRIMARY KEY }
    "#,
    )
    .unwrap();
    let s3 = diff_schemas(Some(&v2), &v3);
    let m3 = Migration {
        id: "20260310_003_tags".to_string(),
        description: "Create Tag enum".to_string(),
        up: MigrationSqlGenerator::generate_up(&s3, &v3, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&s3, &v3, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m3, "2026-03-10T09:15:00Z")
        .await
        .unwrap();

    // Audit: list all applied migrations
    let applied = MigrationTracker::applied(&conn).await.unwrap();
    assert_eq!(applied.len(), 3);
    assert_eq!(applied[0], "20260301_001_users");
    assert_eq!(applied[1], "20260305_002_posts");
    assert_eq!(applied[2], "20260310_003_tags");

    // Check individual migration status
    assert!(
        MigrationTracker::is_applied(&conn, "20260305_002_posts")
            .await
            .unwrap()
    );
    assert!(
        !MigrationTracker::is_applied(&conn, "999_nonexistent")
            .await
            .unwrap()
    );

    // Rollback last two in reverse order
    MigrationTracker::rollback(&conn, &m3).await.unwrap();
    MigrationTracker::rollback(&conn, &m2).await.unwrap();

    let remaining = MigrationTracker::applied(&conn).await.unwrap();
    assert_eq!(remaining, vec!["20260301_001_users"]);

    // Re-apply to verify forward migration still works after rollback
    MigrationTracker::apply(&conn, &m2, "2026-03-11T00:00:00Z")
        .await
        .unwrap();
    let final_state = MigrationTracker::applied(&conn).await.unwrap();
    assert_eq!(
        final_state,
        vec!["20260301_001_users", "20260305_002_posts"]
    );
}

// ---------------------------------------------------------------------------
// Demo 7: Security -- bind parameters prevent SQL injection in enum values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn demo_sql_injection_prevented() {
    let conn = mem_db().await;

    // Schema with an enum that has normal values
    let v1 = parse(
        r#"
        enum Status { Active Inactive }
        model Item { id Int32 PRIMARY KEY }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &v1);
    let m1 = Migration {
        id: "001_init".to_string(),
        description: "Initial schema".to_string(),
        up: MigrationSqlGenerator::generate_up(&steps, &v1, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&steps, &v1, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m1, "2026-03-01T00:00:00Z")
        .await
        .unwrap();

    // Now add an enum value -- even if it contained SQL metacharacters,
    // it would be safe because it goes through bind parameters
    let v2 = parse(
        r#"
        enum Status { Active Inactive Pending }
        model Item { id Int32 PRIMARY KEY }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(Some(&v1), &v2);
    let up = MigrationSqlGenerator::generate_up(&steps, &v2, SqlDialect::Sqlite).unwrap();

    // Verify the generated SQL uses ? placeholders, not string interpolation
    assert_eq!(up.len(), 1);
    assert!(
        up[0].sql.contains('?'),
        "enum value INSERT must use bind parameter"
    );
    assert!(
        !up[0].sql.contains("'Pending'"),
        "enum value must NOT be interpolated into SQL"
    );
    assert_eq!(up[0].params, vec![Value::Text("Pending".into())]);

    // Apply it to verify it works end-to-end
    let m2 = Migration {
        id: "002_add_pending".to_string(),
        description: "Add Pending status".to_string(),
        up,
        down: MigrationSqlGenerator::generate_down(&steps, &v2, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m2, "2026-03-02T00:00:00Z")
        .await
        .unwrap();

    // Verify the value was inserted correctly
    let rows = conn
        .query(&Statement::sql(
            "SELECT value FROM \"_enum_Status\" ORDER BY value".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].get_string(0), Some("Pending".into()));
}

// ---------------------------------------------------------------------------
// Demo 8: Column mapping with MAP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn demo_custom_table_mapping() {
    let conn = mem_db().await;

    let schema = parse(
        r#"
        model UserProfile {
            id       Int32 PRIMARY KEY AUTOINCREMENT
            fullName Utf8  MAP "full_name"
            MAP "user_profiles"
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &schema);
    let m = Migration {
        id: "001_mapped".to_string(),
        description: "Create mapped table".to_string(),
        up: MigrationSqlGenerator::generate_up(&steps, &schema, SqlDialect::Sqlite).unwrap(),
        down: MigrationSqlGenerator::generate_down(&steps, &schema, SqlDialect::Sqlite).unwrap(),
    };
    MigrationTracker::apply(&conn, &m, "2026-03-01T00:00:00Z")
        .await
        .unwrap();

    // The actual SQL table name should be "user_profiles", not "UserProfile"
    conn.execute(&Statement::new(
        "INSERT INTO \"user_profiles\" (full_name) VALUES (?)".to_string(),
        vec![Value::Text("Alice Smith".into())],
    ))
    .await
    .unwrap();

    let row = conn
        .query_one(&Statement::sql(
            "SELECT full_name FROM \"user_profiles\" WHERE id = 1".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(row.get_string(0), Some("Alice Smith".into()));

    // Verify the table is named "user_profiles" in SQLite
    let tables = conn
        .query(&Statement::sql(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'user_profiles'"
                .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(tables.len(), 1);
}
