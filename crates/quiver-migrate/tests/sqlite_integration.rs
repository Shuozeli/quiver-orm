//! Integration tests for quiver-migrate with SQLite.

use quiver_driver_core::{Connection, DdlStatement, Driver, Statement, Value};
use quiver_driver_sqlite::SqliteDriver;
use quiver_migrate::{
    Migration, MigrationSqlGenerator, MigrationTracker, SqlDialect, diff_schemas, introspect,
    schema_to_quiver,
};
use quiver_schema::parse;

async fn open_memory_db() -> quiver_driver_sqlite::SqliteConnection {
    SqliteDriver.connect(":memory:").await.unwrap()
}

#[tokio::test]
async fn apply_create_model_migration() {
    let conn = open_memory_db().await;
    let schema = parse(
        r#"
        model User {
            id    Int32 PRIMARY KEY AUTOINCREMENT
            name  Utf8
            email Utf8  UNIQUE
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &schema);
    let up = MigrationSqlGenerator::generate_up(&steps, &schema, SqlDialect::Sqlite).unwrap();
    let down = MigrationSqlGenerator::generate_down(&steps, &schema, SqlDialect::Sqlite).unwrap();

    let migration = Migration {
        id: "20260310_001_create_user".to_string(),
        description: "Create User table".to_string(),
        up,
        down,
    };

    MigrationTracker::apply(&conn, &migration, "2026-03-10T00:00:00Z")
        .await
        .unwrap();

    // Table should exist -- insert a row to prove it
    let ddl = DdlStatement::new(
        "INSERT INTO \"User\" (name, email) VALUES ('Alice', 'alice@example.com')".to_string(),
    );
    conn.execute_ddl(&ddl).await.unwrap();

    // Migration should be recorded
    let applied = MigrationTracker::applied(&conn).await.unwrap();
    assert_eq!(applied, vec!["20260310_001_create_user"]);
}

#[tokio::test]
async fn apply_then_rollback() {
    let conn = open_memory_db().await;
    let schema = parse(
        r#"
        model Post {
            id    Int32 PRIMARY KEY AUTOINCREMENT
            title Utf8
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &schema);
    let up = MigrationSqlGenerator::generate_up(&steps, &schema, SqlDialect::Sqlite).unwrap();
    let down = MigrationSqlGenerator::generate_down(&steps, &schema, SqlDialect::Sqlite).unwrap();

    let migration = Migration {
        id: "20260310_002_create_post".to_string(),
        description: "Create Post table".to_string(),
        up,
        down,
    };

    MigrationTracker::apply(&conn, &migration, "2026-03-10T00:00:00Z")
        .await
        .unwrap();
    assert!(
        MigrationTracker::is_applied(&conn, &migration.id)
            .await
            .unwrap()
    );

    MigrationTracker::rollback(&conn, &migration).await.unwrap();
    assert!(
        !MigrationTracker::is_applied(&conn, &migration.id)
            .await
            .unwrap()
    );

    // Table should be gone
    let result = conn
        .execute_ddl(&DdlStatement::new(
            "INSERT INTO \"Post\" (title) VALUES ('hello')".to_string(),
        ))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn duplicate_apply_fails() {
    let conn = open_memory_db().await;
    let schema = parse(
        r#"
        model Tag { id Int32 PRIMARY KEY }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &schema);
    let up = MigrationSqlGenerator::generate_up(&steps, &schema, SqlDialect::Sqlite).unwrap();
    let down = MigrationSqlGenerator::generate_down(&steps, &schema, SqlDialect::Sqlite).unwrap();

    let migration = Migration {
        id: "20260310_003_create_tag".to_string(),
        description: "Create Tag table".to_string(),
        up,
        down,
    };

    MigrationTracker::apply(&conn, &migration, "2026-03-10T00:00:00Z")
        .await
        .unwrap();
    let result = MigrationTracker::apply(&conn, &migration, "2026-03-10T00:01:00Z").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn rollback_unapplied_fails() {
    let conn = open_memory_db().await;

    let migration = Migration {
        id: "20260310_004_never_applied".to_string(),
        description: "Never applied".to_string(),
        up: vec![],
        down: vec![],
    };

    let result = MigrationTracker::rollback(&conn, &migration).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn add_field_migration() {
    let conn = open_memory_db().await;

    // Apply initial schema
    let v1 = parse(
        r#"
        model User {
            id   Int32 PRIMARY KEY AUTOINCREMENT
            name Utf8
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &v1);
    let up = MigrationSqlGenerator::generate_up(&steps, &v1, SqlDialect::Sqlite).unwrap();
    let down = MigrationSqlGenerator::generate_down(&steps, &v1, SqlDialect::Sqlite).unwrap();
    let m1 = Migration {
        id: "001_create_user".to_string(),
        description: "Create User".to_string(),
        up,
        down,
    };
    MigrationTracker::apply(&conn, &m1, "2026-03-10T00:00:00Z")
        .await
        .unwrap();

    // Now add a field
    let v2 = parse(
        r#"
        model User {
            id    Int32 PRIMARY KEY AUTOINCREMENT
            name  Utf8
            email Utf8?
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(Some(&v1), &v2);
    let up = MigrationSqlGenerator::generate_up(&steps, &v2, SqlDialect::Sqlite).unwrap();
    let down = MigrationSqlGenerator::generate_down(&steps, &v2, SqlDialect::Sqlite).unwrap();
    let m2 = Migration {
        id: "002_add_email".to_string(),
        description: "Add email to User".to_string(),
        up,
        down,
    };
    MigrationTracker::apply(&conn, &m2, "2026-03-10T00:01:00Z")
        .await
        .unwrap();

    // Insert with the new column
    conn.execute_ddl(&DdlStatement::new(
        "INSERT INTO \"User\" (name, email) VALUES ('Bob', 'bob@example.com')".to_string(),
    ))
    .await
    .unwrap();

    // Verify both migrations are recorded in order
    let applied = MigrationTracker::applied(&conn).await.unwrap();
    assert_eq!(applied, vec!["001_create_user", "002_add_email"]);
}

#[tokio::test]
async fn multi_step_migration_with_enum_and_index() {
    let conn = open_memory_db().await;

    let old = parse(
        r#"
        model User {
            id   Int32 PRIMARY KEY
            name Utf8
        }
    "#,
    )
    .unwrap();

    // First apply old schema
    let init_steps = diff_schemas(None, &old);
    let init_up =
        MigrationSqlGenerator::generate_up(&init_steps, &old, SqlDialect::Sqlite).unwrap();
    let init_down =
        MigrationSqlGenerator::generate_down(&init_steps, &old, SqlDialect::Sqlite).unwrap();
    let m1 = Migration {
        id: "001_init".to_string(),
        description: "Initial schema".to_string(),
        up: init_up,
        down: init_down,
    };
    MigrationTracker::apply(&conn, &m1, "2026-03-10T00:00:00Z")
        .await
        .unwrap();

    // Now evolve: add enum, add field with default, add index
    let new = parse(
        r#"
        enum Status { Active Inactive }
        model User {
            id     Int32  PRIMARY KEY
            name   Utf8
            email  Utf8
            status Status DEFAULT Active
            INDEX (email)
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(Some(&old), &new);
    let up = MigrationSqlGenerator::generate_up(&steps, &new, SqlDialect::Sqlite).unwrap();
    let down = MigrationSqlGenerator::generate_down(&steps, &new, SqlDialect::Sqlite).unwrap();

    // Should have 4 steps: CreateEnum, AddField(email), AddField(status), CreateIndex
    assert_eq!(up.len(), 4);

    let m2 = Migration {
        id: "002_evolve".to_string(),
        description: "Add status enum, email, and index".to_string(),
        up,
        down,
    };
    MigrationTracker::apply(&conn, &m2, "2026-03-10T00:01:00Z")
        .await
        .unwrap();

    // Insert with new columns
    conn.execute_ddl(&DdlStatement::new(
        "INSERT INTO \"User\" (id, name, email, status) VALUES (1, 'Alice', 'alice@example.com', 'Active')"
            .to_string(),
    ))
    .await
    .unwrap();

    let applied = MigrationTracker::applied(&conn).await.unwrap();
    assert_eq!(applied.len(), 2);
}

// ---------------------------------------------------------------------------
// #23: SQLite table rebuild for ALTER COLUMN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn alter_field_type_via_table_rebuild() {
    let conn = open_memory_db().await;

    // Create initial table
    let v1 = parse(
        r#"
        model Product {
            id    Int32 PRIMARY KEY AUTOINCREMENT
            name  Utf8
            price Int32
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &v1);
    let up = MigrationSqlGenerator::generate_up(&steps, &v1, SqlDialect::Sqlite).unwrap();
    for trusted_sql in &up {
        for part in trusted_sql.sql.split(";\n") {
            let trimmed = part.trim();
            if !trimmed.is_empty() {
                conn.execute_ddl(&DdlStatement::new(trimmed.to_string()))
                    .await
                    .unwrap();
            }
        }
    }

    // Insert some data
    conn.execute_ddl(&DdlStatement::new(
        "INSERT INTO \"Product\" (name, price) VALUES ('Widget', 100)".to_string(),
    ))
    .await
    .unwrap();
    conn.execute_ddl(&DdlStatement::new(
        "INSERT INTO \"Product\" (name, price) VALUES ('Gadget', 250)".to_string(),
    ))
    .await
    .unwrap();

    // Now change price from Int32 to Float64
    let v2 = parse(
        r#"
        model Product {
            id    Int32   PRIMARY KEY AUTOINCREMENT
            name  Utf8
            price Float64
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(Some(&v1), &v2);
    assert_eq!(steps.len(), 1); // AlterField

    let up = MigrationSqlGenerator::generate_up(&steps, &v2, SqlDialect::Sqlite).unwrap();
    assert_eq!(up.len(), 1);

    // The rebuild SQL should contain RENAME, CREATE, INSERT...SELECT, DROP
    let rebuild_sql = &up[0].sql;
    assert!(rebuild_sql.contains("RENAME TO"));
    assert!(rebuild_sql.contains("CREATE TABLE IF NOT EXISTS \"Product\""));
    assert!(rebuild_sql.contains("INSERT INTO \"Product\""));
    assert!(rebuild_sql.contains("SELECT"));
    assert!(rebuild_sql.contains("DROP TABLE"));

    // Execute the rebuild (split on semicolons)
    for stmt in rebuild_sql.split(";\n") {
        let trimmed = stmt.trim();
        if !trimmed.is_empty() {
            conn.execute_ddl(&DdlStatement::new(trimmed.to_string()))
                .await
                .unwrap();
        }
    }

    // Verify data was preserved
    let rows = conn
        .query(&Statement::sql(
            "SELECT name, price FROM \"Product\" ORDER BY id".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    // First row
    assert_eq!(rows[0].values[0], Value::Text("Widget".to_string()));
    // Second row
    assert_eq!(rows[1].values[0], Value::Text("Gadget".to_string()));

    // Verify new column type is REAL (Float64)
    let info = conn
        .query(&Statement::sql(
            "PRAGMA table_info(\"Product\")".to_string(),
        ))
        .await
        .unwrap();
    let price_row = info
        .iter()
        .find(|r| r.values[1] == Value::Text("price".to_string()));
    assert!(price_row.is_some());
    assert_eq!(
        price_row.unwrap().values[2],
        Value::Text("REAL".to_string())
    );
}

#[tokio::test]
async fn alter_field_nullability_via_table_rebuild() {
    let conn = open_memory_db().await;

    let v1 = parse(
        r#"
        model Item {
            id   Int32 PRIMARY KEY
            name Utf8
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &v1);
    let up = MigrationSqlGenerator::generate_up(&steps, &v1, SqlDialect::Sqlite).unwrap();
    for trusted_sql in &up {
        for part in trusted_sql.sql.split(";\n") {
            let trimmed = part.trim();
            if !trimmed.is_empty() {
                conn.execute_ddl(&DdlStatement::new(trimmed.to_string()))
                    .await
                    .unwrap();
            }
        }
    }
    conn.execute_ddl(&DdlStatement::new(
        "INSERT INTO \"Item\" (id, name) VALUES (1, 'Foo')".to_string(),
    ))
    .await
    .unwrap();

    // Change name from NOT NULL to nullable
    let v2 = parse(
        r#"
        model Item {
            id   Int32 PRIMARY KEY
            name Utf8?
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(Some(&v1), &v2);
    let up = MigrationSqlGenerator::generate_up(&steps, &v2, SqlDialect::Sqlite).unwrap();

    for stmt in up[0].sql.split(";\n") {
        let trimmed = stmt.trim();
        if !trimmed.is_empty() {
            conn.execute_ddl(&DdlStatement::new(trimmed.to_string()))
                .await
                .unwrap();
        }
    }

    // Should now accept NULL
    conn.execute_ddl(&DdlStatement::new(
        "INSERT INTO \"Item\" (id, name) VALUES (2, NULL)".to_string(),
    ))
    .await
    .unwrap();

    let rows = conn
        .query(&Statement::sql(
            "SELECT id, name FROM \"Item\" ORDER BY id".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].values[1], Value::Null);
}

// ---------------------------------------------------------------------------
// #7: Schema introspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn introspect_simple_table() {
    let conn = open_memory_db().await;
    conn.execute_ddl(&DdlStatement::new(
        "CREATE TABLE \"User\" (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE)".to_string(),
    ))
    .await
    .unwrap();

    let schema = introspect(&conn, SqlDialect::Sqlite).await.unwrap();
    assert_eq!(schema.models.len(), 1);

    let model = &schema.models[0];
    assert_eq!(model.name, "User");
    assert_eq!(model.fields.len(), 3);

    // id field should have PRIMARY KEY
    let id_field = model.fields.iter().find(|f| f.name == "id").unwrap();
    assert!(
        id_field
            .attributes
            .iter()
            .any(|a| matches!(a, quiver_schema::ast::FieldAttribute::Id))
    );
    assert!(
        id_field
            .attributes
            .iter()
            .any(|a| matches!(a, quiver_schema::ast::FieldAttribute::Autoincrement))
    );

    // email should have UNIQUE
    let email_field = model.fields.iter().find(|f| f.name == "email").unwrap();
    assert!(
        email_field
            .attributes
            .iter()
            .any(|a| matches!(a, quiver_schema::ast::FieldAttribute::Unique))
    );
    assert!(!email_field.type_expr.nullable);
}

#[tokio::test]
async fn introspect_with_foreign_key() {
    let conn = open_memory_db().await;
    conn.execute_ddl(&DdlStatement::new(
        "CREATE TABLE \"User\" (id INTEGER PRIMARY KEY, name TEXT NOT NULL)".to_string(),
    ))
    .await
    .unwrap();
    conn.execute_ddl(&DdlStatement::new(
        "CREATE TABLE \"Post\" (id INTEGER PRIMARY KEY, title TEXT NOT NULL, authorId INTEGER NOT NULL, FOREIGN KEY (\"authorId\") REFERENCES \"User\"(\"id\"))".to_string(),
    ))
    .await
    .unwrap();

    let schema = introspect(&conn, SqlDialect::Sqlite).await.unwrap();
    assert_eq!(schema.models.len(), 2);

    // Post should have a FOREIGN KEY model attribute
    let post = schema.models.iter().find(|m| m.name == "Post").unwrap();
    assert!(post.attributes.iter().any(|a| matches!(
        a,
        quiver_schema::ast::ModelAttribute::ForeignKey {
            references_model,
            ..
        } if references_model == "User"
    )));
}

#[tokio::test]
async fn introspect_enum_table() {
    let conn = open_memory_db().await;
    conn.execute_ddl(&DdlStatement::new(
        "CREATE TABLE \"_enum_Role\" (value TEXT PRIMARY KEY)".to_string(),
    ))
    .await
    .unwrap();
    conn.execute_ddl(&DdlStatement::new(
        "INSERT INTO \"_enum_Role\" (value) VALUES ('User'), ('Admin'), ('Moderator')".to_string(),
    ))
    .await
    .unwrap();

    let schema = introspect(&conn, SqlDialect::Sqlite).await.unwrap();
    assert_eq!(schema.enums.len(), 1);
    assert_eq!(schema.enums[0].name, "Role");
    assert_eq!(schema.enums[0].values.len(), 3);
    assert_eq!(schema.enums[0].values[0].name, "User");
    assert_eq!(schema.enums[0].values[1].name, "Admin");
    assert_eq!(schema.enums[0].values[2].name, "Moderator");
    // The _enum_ table should NOT appear as a model
    assert!(schema.models.is_empty());
}

#[tokio::test]
async fn introspect_with_index() {
    let conn = open_memory_db().await;
    conn.execute_ddl(&DdlStatement::new(
        "CREATE TABLE \"User\" (id INTEGER PRIMARY KEY, email TEXT NOT NULL)".to_string(),
    ))
    .await
    .unwrap();
    conn.execute_ddl(&DdlStatement::new(
        "CREATE INDEX \"idx_User_email\" ON \"User\" (\"email\")".to_string(),
    ))
    .await
    .unwrap();

    let schema = introspect(&conn, SqlDialect::Sqlite).await.unwrap();
    let model = &schema.models[0];

    // Should have INDEX (email)
    let has_index = model.attributes.iter().any(
        |a| matches!(a, quiver_schema::ast::ModelAttribute::Index(cols) if cols == &["email"]),
    );
    assert!(has_index);
}

#[tokio::test]
async fn introspect_with_defaults() {
    let conn = open_memory_db().await;
    conn.execute_ddl(&DdlStatement::new(
        "CREATE TABLE \"Config\" (id INTEGER PRIMARY KEY, active INTEGER NOT NULL DEFAULT 1, label TEXT NOT NULL DEFAULT 'default')".to_string(),
    ))
    .await
    .unwrap();

    let schema = introspect(&conn, SqlDialect::Sqlite).await.unwrap();
    let model = &schema.models[0];

    let active = model.fields.iter().find(|f| f.name == "active").unwrap();
    assert!(active.attributes.iter().any(|a| matches!(
        a,
        quiver_schema::ast::FieldAttribute::Default(quiver_schema::ast::DefaultValue::Int(1))
    )));

    let label = model.fields.iter().find(|f| f.name == "label").unwrap();
    assert!(label
        .attributes
        .iter()
        .any(|a| matches!(a, quiver_schema::ast::FieldAttribute::Default(quiver_schema::ast::DefaultValue::String(s)) if s == "default")));
}

#[tokio::test]
async fn introspect_mapped_table_generates_pascal_case() {
    let conn = open_memory_db().await;
    conn.execute_ddl(&DdlStatement::new(
        "CREATE TABLE \"user_profiles\" (id INTEGER PRIMARY KEY, bio TEXT)".to_string(),
    ))
    .await
    .unwrap();

    let schema = introspect(&conn, SqlDialect::Sqlite).await.unwrap();
    let model = &schema.models[0];

    // Model name should be PascalCase
    assert_eq!(model.name, "UserProfiles");
    // Should have MAP "user_profiles"
    assert!(
        model.attributes.iter().any(
            |a| matches!(a, quiver_schema::ast::ModelAttribute::Map(n) if n == "user_profiles")
        )
    );
}

#[tokio::test]
async fn introspect_to_quiver_output() {
    let conn = open_memory_db().await;
    conn.execute_ddl(&DdlStatement::new(
        "CREATE TABLE \"_enum_Status\" (value TEXT PRIMARY KEY)".to_string(),
    ))
    .await
    .unwrap();
    conn.execute_ddl(&DdlStatement::new(
        "INSERT INTO \"_enum_Status\" (value) VALUES ('Active'), ('Inactive')".to_string(),
    ))
    .await
    .unwrap();
    conn.execute_ddl(&DdlStatement::new(
        "CREATE TABLE \"Account\" (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'Active')".to_string(),
    ))
    .await
    .unwrap();

    let schema = introspect(&conn, SqlDialect::Sqlite).await.unwrap();
    let output = schema_to_quiver(&schema);

    assert!(output.contains("enum Status {"));
    assert!(output.contains("Active"));
    assert!(output.contains("model Account {"));
    assert!(output.contains("PRIMARY KEY"));
    assert!(output.contains("UNIQUE"));
}

#[tokio::test]
async fn introspect_roundtrip_with_migration() {
    let conn = open_memory_db().await;

    // Define a schema and apply it via migration
    let original = parse(
        r#"
        enum Role { User Admin }
        model Account {
            id    Int32 PRIMARY KEY AUTOINCREMENT
            email Utf8  UNIQUE
            name  Utf8?
            INDEX (email)
        }
    "#,
    )
    .unwrap();

    let steps = diff_schemas(None, &original);
    let up = MigrationSqlGenerator::generate_up(&steps, &original, SqlDialect::Sqlite).unwrap();

    // Use the tracker to apply (handles parameterized statements)
    let down = MigrationSqlGenerator::generate_down(&steps, &original, SqlDialect::Sqlite).unwrap();
    let migration = Migration {
        id: "001_init".to_string(),
        description: "Initial schema".to_string(),
        up,
        down,
    };
    MigrationTracker::apply(&conn, &migration, "2026-03-10T00:00:00Z")
        .await
        .unwrap();

    // Now introspect
    let introspected = introspect(&conn, SqlDialect::Sqlite).await.unwrap();

    // Should have the enum
    assert_eq!(introspected.enums.len(), 1);
    assert_eq!(introspected.enums[0].name, "Role");
    assert_eq!(introspected.enums[0].values.len(), 2);

    // Should have the model
    assert_eq!(introspected.models.len(), 1);
    let model = &introspected.models[0];
    assert_eq!(model.name, "Account");

    // Should have the index
    assert!(
        model
            .attributes
            .iter()
            .any(|a| matches!(a, quiver_schema::ast::ModelAttribute::Index(_)))
    );

    // Nullable field should be detected
    let name_field = model.fields.iter().find(|f| f.name == "name").unwrap();
    assert!(name_field.type_expr.nullable);

    // Non-nullable field
    let email_field = model.fields.iter().find(|f| f.name == "email").unwrap();
    assert!(!email_field.type_expr.nullable);
}
