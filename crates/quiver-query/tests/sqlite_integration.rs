use quiver_codegen::{SqlDialect, SqlGenerator};
use quiver_driver_core::{Connection, DdlStatement, Driver, Transaction, Transactional, Value};
use quiver_driver_sqlite::SqliteDriver;
use quiver_query::{
    AggregateFunc, ChildWrite, CompareOp, Filter, Include, Join, JoinType, Order, PageRequest,
    PaginateConfig, Query, RelationDef, RelationType, create_with_children, find_with_includes,
    paginate,
};
use quiver_schema::parse;

async fn setup() -> quiver_driver_sqlite::SqliteConnection {
    let schema = parse(
        r#"
        enum Role { User Admin Moderator }
        model Account {
            id    Int32  @id @autoincrement
            email Utf8   @unique
            name  Utf8
            role  Role   @default(User)
            score Int32  @default(0)
        }
        model Post {
            id       Int32 @id @autoincrement
            title    Utf8
            body     Utf8?
            authorId Int32
            author   Account @relation(fields: [authorId], references: [id])
        }
    "#,
    )
    .unwrap();

    let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite).unwrap();
    let driver = SqliteDriver;
    let conn = driver.connect(":memory:").await.unwrap();
    conn.execute_ddl(&DdlStatement::new(ddl)).await.unwrap();
    conn
}

#[tokio::test]
async fn create_and_find_many() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create()
        .set("email", "alice@test.com")
        .set("name", "Alice")
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Account")
        .create()
        .set("email", "bob@test.com")
        .set("name", "Bob")
        .set("role", "Admin")
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Account").find_many().build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn find_with_filter() {
    let conn = setup().await;

    for (email, name) in &[
        ("alice@test.com", "Alice"),
        ("bob@test.com", "Bob"),
        ("carol@test.com", "Carol"),
    ] {
        let q = Query::table("Account")
            .create()
            .set("email", *email)
            .set("name", *name)
            .build();
        conn.execute(&q).await.unwrap();
    }

    let q = Query::table("Account")
        .find_many()
        .filter(Filter::eq("name", "Bob"))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_string(2), Some("Bob".to_string()));
}

#[tokio::test]
async fn find_with_like() {
    let conn = setup().await;

    for name in &["Alice", "Alex", "Bob"] {
        let q = Query::table("Account")
            .create()
            .set("email", format!("{}@test.com", name.to_lowercase()))
            .set("name", *name)
            .build();
        conn.execute(&q).await.unwrap();
    }

    let q = Query::table("Account")
        .find_many()
        .filter(Filter::like("name", "Al%"))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn find_with_in_filter() {
    let conn = setup().await;

    for (email, name) in &[
        ("a@t.com", "Alice"),
        ("b@t.com", "Bob"),
        ("c@t.com", "Carol"),
    ] {
        let q = Query::table("Account")
            .create()
            .set("email", *email)
            .set("name", *name)
            .build();
        conn.execute(&q).await.unwrap();
    }

    let q = Query::table("Account")
        .find_many()
        .filter(Filter::is_in(
            "name",
            vec![Value::from("Alice"), Value::from("Carol")],
        ))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn find_with_order_and_limit() {
    let conn = setup().await;

    for (email, name, score) in &[
        ("a@t.com", "Alice", 80),
        ("b@t.com", "Bob", 90),
        ("c@t.com", "Carol", 70),
        ("d@t.com", "Dave", 95),
    ] {
        let q = Query::table("Account")
            .create()
            .set("email", *email)
            .set("name", *name)
            .set("score", Value::from(*score))
            .build();
        conn.execute(&q).await.unwrap();
    }

    let q = Query::table("Account")
        .find_many()
        .select(&["name", "score"])
        .order_by("score", Order::Desc)
        .limit(2)
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get_string(0), Some("Dave".to_string()));
    assert_eq!(rows[1].get_string(0), Some("Bob".to_string()));
}

#[tokio::test]
async fn find_with_pagination() {
    let conn = setup().await;

    for i in 1..=5 {
        let q = Query::table("Account")
            .create()
            .set("email", format!("user{}@t.com", i))
            .set("name", format!("User{}", i))
            .build();
        conn.execute(&q).await.unwrap();
    }

    // Page 2: skip 2, take 2
    let q = Query::table("Account")
        .find_many()
        .order_by("id", Order::Asc)
        .limit(2)
        .offset(2)
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get_i64(0), Some(3));
    assert_eq!(rows[1].get_i64(0), Some(4));
}

#[tokio::test]
async fn find_first() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create()
        .set("email", "alice@test.com")
        .set("name", "Alice")
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Account")
        .find_first()
        .filter(Filter::eq("email", "alice@test.com"))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_string(2), Some("Alice".to_string()));
}

#[tokio::test]
async fn update_record() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create()
        .set("email", "alice@test.com")
        .set("name", "Alice")
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Account")
        .update()
        .set("name", "Alice Updated")
        .set("score", Value::from(100i32))
        .filter(Filter::eq("email", "alice@test.com"))
        .build();
    let affected = conn.execute(&q).await.unwrap();
    assert_eq!(affected, 1);

    let q = Query::table("Account")
        .find_first()
        .filter(Filter::eq("email", "alice@test.com"))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows[0].get_string(2), Some("Alice Updated".to_string()));
    assert_eq!(rows[0].get_i64(4), Some(100));
}

#[tokio::test]
async fn delete_record() {
    let conn = setup().await;

    for name in &["Alice", "Bob", "Carol"] {
        let q = Query::table("Account")
            .create()
            .set("email", format!("{}@t.com", name.to_lowercase()))
            .set("name", *name)
            .build();
        conn.execute(&q).await.unwrap();
    }

    let q = Query::table("Account")
        .delete()
        .filter(Filter::eq("name", "Bob"))
        .build();
    let affected = conn.execute(&q).await.unwrap();
    assert_eq!(affected, 1);

    let q = Query::table("Account").find_many().build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn delete_all() {
    let conn = setup().await;

    for i in 1..=3 {
        let q = Query::table("Account")
            .create()
            .set("email", format!("u{}@t.com", i))
            .set("name", format!("U{}", i))
            .build();
        conn.execute(&q).await.unwrap();
    }

    let q = Query::table("Account").delete().build();
    let affected = conn.execute(&q).await.unwrap();
    assert_eq!(affected, 3);
}

#[tokio::test]
async fn upsert_insert_then_update() {
    let conn = setup().await;

    // First upsert: inserts
    let q = Query::table("Account")
        .upsert()
        .set("email", "alice@test.com")
        .set("name", "Alice")
        .conflict_on(&["email"])
        .on_conflict_set("name", "Alice Upserted")
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Account")
        .find_first()
        .filter(Filter::eq("email", "alice@test.com"))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows[0].get_string(2), Some("Alice".to_string()));

    // Second upsert: updates because email conflict
    let q = Query::table("Account")
        .upsert()
        .set("email", "alice@test.com")
        .set("name", "Alice")
        .conflict_on(&["email"])
        .on_conflict_set("name", "Alice Upserted")
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Account")
        .find_first()
        .filter(Filter::eq("email", "alice@test.com"))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows[0].get_string(2), Some("Alice Upserted".to_string()));
}

#[tokio::test]
async fn complex_filter_between_and_not_null() {
    let conn = setup().await;

    for (email, name, score) in &[
        ("a@t.com", "Alice", 50),
        ("b@t.com", "Bob", 75),
        ("c@t.com", "Carol", 90),
    ] {
        let q = Query::table("Account")
            .create()
            .set("email", *email)
            .set("name", *name)
            .set("score", Value::from(*score))
            .build();
        conn.execute(&q).await.unwrap();
    }

    let q = Query::table("Account")
        .find_many()
        .filter(Filter::between("score", 60i32, 80i32))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_string(2), Some("Bob".to_string()));
}

#[tokio::test]
async fn query_with_relation() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create()
        .set("email", "alice@test.com")
        .set("name", "Alice")
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Post")
        .create()
        .set("title", "Hello World")
        .set("body", "First post")
        .set("authorId", Value::from(1i32))
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Post")
        .create()
        .set("title", "Second Post")
        .set("authorId", Value::from(1i32))
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Post")
        .find_many()
        .filter(Filter::eq("authorId", Value::from(1i32)))
        .order_by("id", Order::Asc)
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get_string(1), Some("Hello World".to_string()));
}

#[tokio::test]
async fn transaction_with_query_builder() {
    let mut conn = setup().await;

    let q = Query::table("Account")
        .create()
        .set("email", "alice@test.com")
        .set("name", "Alice")
        .build();
    conn.execute(&q).await.unwrap();

    // Transaction that commits
    {
        let tx = conn.begin().await.unwrap();
        let q = Query::table("Account")
            .create()
            .set("email", "bob@test.com")
            .set("name", "Bob")
            .build();
        tx.execute(&q).await.unwrap();
        tx.commit().await.unwrap();
    }

    // Transaction that rolls back
    {
        let tx = conn.begin().await.unwrap();
        let q = Query::table("Account")
            .create()
            .set("email", "carol@test.com")
            .set("name", "Carol")
            .build();
        tx.execute(&q).await.unwrap();
        tx.rollback().await.unwrap();
    }

    let q = Query::table("Account").find_many().build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2); // Alice + Bob, Carol was rolled back
}

#[tokio::test]
async fn not_filter() {
    let conn = setup().await;

    for (email, name, role) in &[
        ("a@t.com", "Alice", "User"),
        ("b@t.com", "Bob", "Admin"),
        ("c@t.com", "Carol", "User"),
    ] {
        let q = Query::table("Account")
            .create()
            .set("email", *email)
            .set("name", *name)
            .set("role", *role)
            .build();
        conn.execute(&q).await.unwrap();
    }

    let q = Query::table("Account")
        .find_many()
        .filter(Filter::negate(Filter::eq("role", "User")))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_string(2), Some("Bob".to_string()));
}

// ---------------------------------------------------------------------------
// JOIN tests
// ---------------------------------------------------------------------------

async fn setup_with_posts() -> quiver_driver_sqlite::SqliteConnection {
    let conn = setup().await;

    // Create two users
    for (email, name) in &[("alice@t.com", "Alice"), ("bob@t.com", "Bob")] {
        let q = Query::table("Account")
            .create()
            .set("email", *email)
            .set("name", *name)
            .build();
        conn.execute(&q).await.unwrap();
    }

    // Create posts: Alice has 2 posts, Bob has 1
    for (title, author_id) in &[
        ("Alice Post 1", 1i32),
        ("Alice Post 2", 1i32),
        ("Bob Post 1", 2i32),
    ] {
        let q = Query::table("Post")
            .create()
            .set("title", *title)
            .set("authorId", Value::from(*author_id))
            .build();
        conn.execute(&q).await.unwrap();
    }

    conn
}

#[tokio::test]
async fn inner_join_posts_to_accounts() {
    let conn = setup_with_posts().await;

    let q = Query::table("Post")
        .find_many()
        .select(&["Post.title", "Account.name"])
        .join(Join::new(JoinType::Inner, "Account").on("Post", "authorId", "Account", "id"))
        .order_by("Post.id", Order::Asc)
        .build();

    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get_string(0), Some("Alice Post 1".to_string()));
    assert_eq!(rows[0].get_string(1), Some("Alice".to_string()));
    assert_eq!(rows[2].get_string(1), Some("Bob".to_string()));
}

#[tokio::test]
async fn left_join_accounts_to_posts() {
    let conn = setup().await;

    // Create users but no posts for Bob
    let q = Query::table("Account")
        .create()
        .set("email", "alice@t.com")
        .set("name", "Alice")
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Account")
        .create()
        .set("email", "bob@t.com")
        .set("name", "Bob")
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Post")
        .create()
        .set("title", "Alice Post")
        .set("authorId", Value::from(1i32))
        .build();
    conn.execute(&q).await.unwrap();

    // LEFT JOIN: Bob should appear with NULL post
    let q = Query::table("Account")
        .find_many()
        .select(&["Account.name", "Post.title"])
        .join(Join::new(JoinType::Left, "Post").on("Account", "id", "Post", "authorId"))
        .order_by("Account.id", Order::Asc)
        .build();

    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get_string(0), Some("Alice".to_string()));
    assert_eq!(rows[0].get_string(1), Some("Alice Post".to_string()));
    assert_eq!(rows[1].get_string(0), Some("Bob".to_string()));
    assert!(rows[1].get(1).unwrap().is_null());
}

#[tokio::test]
async fn join_with_where_filter() {
    let conn = setup_with_posts().await;

    let q = Query::table("Post")
        .find_many()
        .join(Join::new(JoinType::Inner, "Account").on("Post", "authorId", "Account", "id"))
        .filter(Filter::eq("Account.name", "Alice"))
        .build();

    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2); // Alice's 2 posts
}

// ---------------------------------------------------------------------------
// Include (eager loading) tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn include_one_to_many() {
    let conn = setup_with_posts().await;

    let parent_query = Query::table("Account").find_many().build();

    let posts_relation = RelationDef {
        name: "posts".to_string(),
        from_model: "Account".to_string(),
        to_model: "Post".to_string(),
        fields: vec!["authorId".to_string()],
        references: vec!["id".to_string()],
        relation_type: RelationType::OneToMany,
    };

    let results = find_with_includes(&conn, &parent_query, &[Include::new(posts_relation)])
        .await
        .unwrap();

    assert_eq!(results.len(), 2); // Alice and Bob
    // Find Alice's result
    let alice = results
        .iter()
        .find(|r| r.row.get_string(2) == Some("Alice".to_string()))
        .unwrap();
    assert_eq!(alice.relations.len(), 1);
    assert_eq!(alice.relations[0].0, "posts");
    assert_eq!(alice.relations[0].1.len(), 2); // Alice has 2 posts

    let bob = results
        .iter()
        .find(|r| r.row.get_string(2) == Some("Bob".to_string()))
        .unwrap();
    assert_eq!(bob.relations[0].1.len(), 1); // Bob has 1 post
}

#[tokio::test]
async fn include_many_to_one() {
    let conn = setup_with_posts().await;

    let parent_query = Query::table("Post").find_many().build();

    let author_relation = RelationDef {
        name: "author".to_string(),
        from_model: "Post".to_string(),
        to_model: "Account".to_string(),
        fields: vec!["authorId".to_string()],
        references: vec!["id".to_string()],
        relation_type: RelationType::ManyToOne,
    };

    let results = find_with_includes(&conn, &parent_query, &[Include::new(author_relation)])
        .await
        .unwrap();

    assert_eq!(results.len(), 3); // 3 posts

    // Each post should have exactly one author
    for post in &results {
        assert_eq!(post.relations.len(), 1);
        assert_eq!(post.relations[0].0, "author");
        assert_eq!(post.relations[0].1.len(), 1); // exactly one author
    }

    // Check Alice's posts have the right author
    let alice_post = results
        .iter()
        .find(|r| r.row.get_string(1) == Some("Alice Post 1".to_string()))
        .unwrap();
    let author = &alice_post.relations[0].1[0];
    assert_eq!(author.row.get_string(2), Some("Alice".to_string()));
}

#[tokio::test]
async fn include_empty_parent_rows() {
    let conn = setup().await;

    let parent_query = Query::table("Account").find_many().build();

    let posts_relation = RelationDef {
        name: "posts".to_string(),
        from_model: "Account".to_string(),
        to_model: "Post".to_string(),
        fields: vec!["authorId".to_string()],
        references: vec!["id".to_string()],
        relation_type: RelationType::OneToMany,
    };

    let results = find_with_includes(&conn, &parent_query, &[Include::new(posts_relation)])
        .await
        .unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn include_with_filter_on_parent() {
    let conn = setup_with_posts().await;

    let parent_query = Query::table("Account")
        .find_many()
        .filter(Filter::eq("name", "Alice"))
        .build();

    let posts_relation = RelationDef {
        name: "posts".to_string(),
        from_model: "Account".to_string(),
        to_model: "Post".to_string(),
        fields: vec!["authorId".to_string()],
        references: vec!["id".to_string()],
        relation_type: RelationType::OneToMany,
    };

    let results = find_with_includes(&conn, &parent_query, &[Include::new(posts_relation)])
        .await
        .unwrap();

    assert_eq!(results.len(), 1); // Only Alice
    assert_eq!(results[0].relations[0].1.len(), 2); // Alice has 2 posts
}

// ---------------------------------------------------------------------------
// DISTINCT tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn distinct_roles() {
    let conn = setup().await;

    for (email, name, role) in &[
        ("a@t.com", "Alice", "User"),
        ("b@t.com", "Bob", "Admin"),
        ("c@t.com", "Carol", "User"),
        ("d@t.com", "Dave", "Admin"),
    ] {
        let q = Query::table("Account")
            .create()
            .set("email", *email)
            .set("name", *name)
            .set("role", *role)
            .build();
        conn.execute(&q).await.unwrap();
    }

    let q = Query::table("Account")
        .find_many()
        .distinct()
        .select(&["role"])
        .order_by("role", Order::Asc)
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2); // Admin, User
    assert_eq!(rows[0].get_string(0), Some("Admin".to_string()));
    assert_eq!(rows[1].get_string(0), Some("User".to_string()));
}

// ---------------------------------------------------------------------------
// CreateMany (batch insert) tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_many_bulk_insert() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create_many()
        .columns(&["email", "name", "role"])
        .values(vec![
            Value::from("a@t.com"),
            Value::from("Alice"),
            Value::from("User"),
        ])
        .values(vec![
            Value::from("b@t.com"),
            Value::from("Bob"),
            Value::from("Admin"),
        ])
        .values(vec![
            Value::from("c@t.com"),
            Value::from("Carol"),
            Value::from("User"),
        ])
        .build();

    let affected = conn.execute(&q).await.unwrap();
    assert_eq!(affected, 3);

    let q = Query::table("Account").find_many().build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn update_many_by_filter() {
    let conn = setup().await;

    // Insert users
    let q = Query::table("Account")
        .create_many()
        .columns(&["email", "name", "role", "score"])
        .values(vec![
            Value::from("a@t.com"),
            Value::from("Alice"),
            Value::from("User"),
            Value::from(10i32),
        ])
        .values(vec![
            Value::from("b@t.com"),
            Value::from("Bob"),
            Value::from("User"),
            Value::from(20i32),
        ])
        .values(vec![
            Value::from("c@t.com"),
            Value::from("Carol"),
            Value::from("Admin"),
            Value::from(30i32),
        ])
        .build();
    conn.execute(&q).await.unwrap();

    // Update all Users' score to 99
    let q = Query::table("Account")
        .update_many()
        .set("score", Value::from(99i32))
        .filter(Filter::eq("role", "User"))
        .build();
    let affected = conn.execute(&q).await.unwrap();
    assert_eq!(affected, 2);

    // Verify
    let q = Query::table("Account")
        .find_many()
        .filter(Filter::eq("score", Value::from(99i32)))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn delete_many_by_filter() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create_many()
        .columns(&["email", "name", "role"])
        .values(vec![
            Value::from("a@t.com"),
            Value::from("Alice"),
            Value::from("User"),
        ])
        .values(vec![
            Value::from("b@t.com"),
            Value::from("Bob"),
            Value::from("Admin"),
        ])
        .values(vec![
            Value::from("c@t.com"),
            Value::from("Carol"),
            Value::from("User"),
        ])
        .build();
    conn.execute(&q).await.unwrap();

    // Delete all Users
    let q = Query::table("Account")
        .delete_many()
        .filter(Filter::eq("role", "User"))
        .build();
    let affected = conn.execute(&q).await.unwrap();
    assert_eq!(affected, 2);

    let q = Query::table("Account").find_many().build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_string(2), Some("Bob".to_string()));
}

// ---------------------------------------------------------------------------
// Aggregation tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aggregate_count_all() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create_many()
        .columns(&["email", "name"])
        .values(vec![Value::from("a@t.com"), Value::from("Alice")])
        .values(vec![Value::from("b@t.com"), Value::from("Bob")])
        .values(vec![Value::from("c@t.com"), Value::from("Carol")])
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Account").aggregate().count_all().build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_i64(0), Some(3));
    // Verify the alias makes the result accessible by name
    assert_eq!(rows[0].get_by_name("_count"), Some(&Value::Int(3)));
}

#[tokio::test]
async fn aggregate_sum_avg_min_max() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create_many()
        .columns(&["email", "name", "score"])
        .values(vec![
            Value::from("a@t.com"),
            Value::from("Alice"),
            Value::from(10i32),
        ])
        .values(vec![
            Value::from("b@t.com"),
            Value::from("Bob"),
            Value::from(20i32),
        ])
        .values(vec![
            Value::from("c@t.com"),
            Value::from("Carol"),
            Value::from(30i32),
        ])
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Account")
        .aggregate()
        .sum("score")
        .min("score")
        .max("score")
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_i64(0), Some(60)); // SUM
    assert_eq!(rows[0].get_i64(1), Some(10)); // MIN
    assert_eq!(rows[0].get_i64(2), Some(30)); // MAX
    // Verify aliases are accessible by name
    assert_eq!(rows[0].get_by_name("_sum_score"), Some(&Value::Int(60)));
    assert_eq!(rows[0].get_by_name("_min_score"), Some(&Value::Int(10)));
    assert_eq!(rows[0].get_by_name("_max_score"), Some(&Value::Int(30)));
}

#[tokio::test]
async fn aggregate_group_by() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create_many()
        .columns(&["email", "name", "role", "score"])
        .values(vec![
            Value::from("a@t.com"),
            Value::from("Alice"),
            Value::from("User"),
            Value::from(10i32),
        ])
        .values(vec![
            Value::from("b@t.com"),
            Value::from("Bob"),
            Value::from("Admin"),
            Value::from(20i32),
        ])
        .values(vec![
            Value::from("c@t.com"),
            Value::from("Carol"),
            Value::from("User"),
            Value::from(30i32),
        ])
        .build();
    conn.execute(&q).await.unwrap();

    let q = Query::table("Account")
        .aggregate()
        .count_all()
        .sum("score")
        .group_by("role")
        .order_by("role", Order::Asc)
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
    // Admin: count=1, sum=20
    assert_eq!(rows[0].get_string(0), Some("Admin".to_string()));
    assert_eq!(rows[0].get_i64(1), Some(1));
    assert_eq!(rows[0].get_i64(2), Some(20));
    // User: count=2, sum=40
    assert_eq!(rows[1].get_string(0), Some("User".to_string()));
    assert_eq!(rows[1].get_i64(1), Some(2));
    assert_eq!(rows[1].get_i64(2), Some(40));
    // Verify aliases are accessible by name
    assert_eq!(rows[0].get_by_name("_count"), Some(&Value::Int(1)));
    assert_eq!(rows[0].get_by_name("_sum_score"), Some(&Value::Int(20)));
}

#[tokio::test]
async fn aggregate_having() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create_many()
        .columns(&["email", "name", "role"])
        .values(vec![
            Value::from("a@t.com"),
            Value::from("Alice"),
            Value::from("User"),
        ])
        .values(vec![
            Value::from("b@t.com"),
            Value::from("Bob"),
            Value::from("Admin"),
        ])
        .values(vec![
            Value::from("c@t.com"),
            Value::from("Carol"),
            Value::from("User"),
        ])
        .values(vec![
            Value::from("d@t.com"),
            Value::from("Dave"),
            Value::from("User"),
        ])
        .build();
    conn.execute(&q).await.unwrap();

    // Only groups with count >= 2
    let q = Query::table("Account")
        .aggregate()
        .count_all()
        .group_by("role")
        .having(AggregateFunc::CountAll, CompareOp::Gte, 2i32)
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_string(0), Some("User".to_string()));
    assert_eq!(rows[0].get_i64(1), Some(3));
}

#[tokio::test]
async fn aggregate_with_where_filter() {
    let conn = setup().await;

    let q = Query::table("Account")
        .create_many()
        .columns(&["email", "name", "role", "score"])
        .values(vec![
            Value::from("a@t.com"),
            Value::from("Alice"),
            Value::from("User"),
            Value::from(10i32),
        ])
        .values(vec![
            Value::from("b@t.com"),
            Value::from("Bob"),
            Value::from("User"),
            Value::from(50i32),
        ])
        .values(vec![
            Value::from("c@t.com"),
            Value::from("Carol"),
            Value::from("Admin"),
            Value::from(90i32),
        ])
        .build();
    conn.execute(&q).await.unwrap();

    // Count users with score > 20
    let q = Query::table("Account")
        .aggregate()
        .count_all()
        .filter(Filter::gt("score", 20i32))
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows[0].get_i64(0), Some(2));
}

// ---------------------------------------------------------------------------
// Nested writes tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn nested_create_parent_with_children() {
    let mut conn = setup().await;

    let parent_query = Query::table("Account")
        .create()
        .set("email", "alice@t.com")
        .set("name", "Alice")
        .build();

    let child = ChildWrite::new("Post")
        .columns(&["title", "body"])
        .values(vec![Value::from("Post 1"), Value::from("Body 1")])
        .values(vec![Value::from("Post 2"), Value::from("Body 2")]);

    let (parent_id, child_count) =
        create_with_children(&mut conn, &parent_query, "authorId", &[child])
            .await
            .unwrap();

    assert_eq!(child_count, 2);
    // parent_id should be 1 (first insert)
    assert_eq!(parent_id, Value::from(1i64));

    // Verify posts exist with correct FK
    let q = Query::table("Post")
        .find_many()
        .order_by("id", Order::Asc)
        .build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get_string(1), Some("Post 1".to_string()));
    assert_eq!(rows[1].get_string(1), Some("Post 2".to_string()));
    // authorId should be 1 for both
    assert_eq!(rows[0].get_i64(3), Some(1));
    assert_eq!(rows[1].get_i64(3), Some(1));
}

#[tokio::test]
async fn nested_create_multiple_child_tables() {
    let mut conn = setup().await;

    let parent_query = Query::table("Account")
        .create()
        .set("email", "alice@t.com")
        .set("name", "Alice")
        .build();

    let posts = ChildWrite::new("Post")
        .columns(&["title"])
        .values(vec![Value::from("Post A")])
        .values(vec![Value::from("Post B")])
        .values(vec![Value::from("Post C")]);

    let (_, child_count) = create_with_children(&mut conn, &parent_query, "authorId", &[posts])
        .await
        .unwrap();

    assert_eq!(child_count, 3);

    let q = Query::table("Post").find_many().build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn nested_create_no_children() {
    let mut conn = setup().await;

    let parent_query = Query::table("Account")
        .create()
        .set("email", "solo@t.com")
        .set("name", "Solo")
        .build();

    let (parent_id, child_count) = create_with_children::<quiver_driver_sqlite::SqliteConnection>(
        &mut conn,
        &parent_query,
        "authorId",
        &[],
    )
    .await
    .unwrap();

    assert_eq!(child_count, 0);
    assert_eq!(parent_id, Value::from(1i64));

    let q = Query::table("Account").find_many().build();
    let rows = conn.query(&q).await.unwrap();
    assert_eq!(rows.len(), 1);
}

// ---------------------------------------------------------------------------
// Pagination (AIP-132) tests
// ---------------------------------------------------------------------------

async fn setup_many_accounts() -> quiver_driver_sqlite::SqliteConnection {
    let conn = setup().await;
    for i in 0..25 {
        let q = Query::table("Account")
            .create()
            .set("email", Value::from(format!("user{}@t.com", i)))
            .set("name", Value::from(format!("User {}", i)))
            .build();
        conn.execute(&q).await.unwrap();
    }
    conn
}

#[tokio::test]
async fn paginate_first_page() {
    let conn = setup_many_accounts().await;

    let base_query = Query::table("Account")
        .find_many()
        .order_by("id", Order::Asc)
        .build();

    let config = PaginateConfig {
        default_page_size: 10,
        max_page_size: 100,
        include_total_size: false,
    };

    let response = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::first_page(10),
        &config,
    )
    .await
    .unwrap();

    assert_eq!(response.items.len(), 10);
    assert!(!response.next_page_token.is_empty());
    assert!(response.total_size.is_none());
}

#[tokio::test]
async fn paginate_through_all_pages() {
    let conn = setup_many_accounts().await;

    let base_query = Query::table("Account")
        .find_many()
        .order_by("id", Order::Asc)
        .build();

    let config = PaginateConfig {
        default_page_size: 10,
        max_page_size: 100,
        include_total_size: false,
    };

    // Page 1: items 0-9
    let page1 = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::first_page(10),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(page1.items.len(), 10);
    assert!(!page1.next_page_token.is_empty());

    // Page 2: items 10-19
    let page2 = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::next(10, &page1.next_page_token),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(page2.items.len(), 10);
    assert!(!page2.next_page_token.is_empty());

    // Page 3: items 20-24 (last page)
    let page3 = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::next(10, &page2.next_page_token),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(page3.items.len(), 5);
    assert!(page3.next_page_token.is_empty()); // no more pages
}

#[tokio::test]
async fn paginate_with_total_size() {
    let conn = setup_many_accounts().await;

    let base_query = Query::table("Account")
        .find_many()
        .order_by("id", Order::Asc)
        .build();

    let config = PaginateConfig {
        default_page_size: 10,
        max_page_size: 100,
        include_total_size: true,
    };

    let response = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::first_page(10),
        &config,
    )
    .await
    .unwrap();

    assert_eq!(response.items.len(), 10);
    assert_eq!(response.total_size, Some(25));
}

#[tokio::test]
async fn paginate_clamps_page_size() {
    let conn = setup_many_accounts().await;

    let base_query = Query::table("Account")
        .find_many()
        .order_by("id", Order::Asc)
        .build();

    let config = PaginateConfig {
        default_page_size: 5,
        max_page_size: 10,
        include_total_size: false,
    };

    // Request page_size=50, should be clamped to max_page_size=10
    let response = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::first_page(50),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(response.items.len(), 10);

    // Request page_size=0, should use default_page_size=5
    let response = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::first_page(0),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(response.items.len(), 5);
}

#[tokio::test]
async fn paginate_single_page_no_next_token() {
    let conn = setup().await;

    // Insert only 3 accounts
    for i in 0..3 {
        let q = Query::table("Account")
            .create()
            .set("email", Value::from(format!("u{}@t.com", i)))
            .set("name", Value::from(format!("U{}", i)))
            .build();
        conn.execute(&q).await.unwrap();
    }

    let base_query = Query::table("Account")
        .find_many()
        .order_by("id", Order::Asc)
        .build();

    let config = PaginateConfig {
        default_page_size: 10,
        max_page_size: 100,
        include_total_size: false,
    };

    let response = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::first_page(10),
        &config,
    )
    .await
    .unwrap();

    assert_eq!(response.items.len(), 3);
    assert!(response.next_page_token.is_empty()); // all fit in one page
}

#[tokio::test]
async fn paginate_multi_column_order() {
    let conn = setup().await;

    // Create accounts with duplicate roles to test multi-column sort stability
    for (email, name, role, score) in &[
        ("a@t.com", "Alice", "User", 30i32),
        ("b@t.com", "Bob", "Admin", 20i32),
        ("c@t.com", "Carol", "User", 10i32),
        ("d@t.com", "Dave", "Admin", 40i32),
        ("e@t.com", "Eve", "User", 50i32),
        ("f@t.com", "Frank", "Admin", 60i32),
        ("g@t.com", "Grace", "User", 70i32),
    ] {
        let q = Query::table("Account")
            .create()
            .set("email", *email)
            .set("name", *name)
            .set("role", *role)
            .set("score", Value::from(*score))
            .build();
        conn.execute(&q).await.unwrap();
    }

    // ORDER BY role ASC, score DESC -- multi-column sort
    let base_query = Query::table("Account")
        .find_many()
        .order_by("role", Order::Asc)
        .order_by("score", Order::Desc)
        .build();

    let config = PaginateConfig {
        default_page_size: 3,
        max_page_size: 100,
        include_total_size: true,
    };

    // Page 1: first 3 rows
    let page1 = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::first_page(3),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(page1.items.len(), 3);
    assert_eq!(page1.total_size, Some(7));
    assert!(!page1.next_page_token.is_empty());
    // Admin(60), Admin(40), Admin(20) -- sorted by role ASC, score DESC
    assert_eq!(page1.items[0].get_string(2), Some("Frank".to_string()));
    assert_eq!(page1.items[1].get_string(2), Some("Dave".to_string()));
    assert_eq!(page1.items[2].get_string(2), Some("Bob".to_string()));

    // Page 2: next 3 rows
    let page2 = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::next(3, &page1.next_page_token),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(page2.items.len(), 3);
    assert!(!page2.next_page_token.is_empty());
    // User(70), User(50), User(30)
    assert_eq!(page2.items[0].get_string(2), Some("Grace".to_string()));
    assert_eq!(page2.items[1].get_string(2), Some("Eve".to_string()));
    assert_eq!(page2.items[2].get_string(2), Some("Alice".to_string()));

    // Page 3: last row
    let page3 = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::next(3, &page2.next_page_token),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(page3.items.len(), 1);
    assert!(page3.next_page_token.is_empty()); // last page
    // User(10)
    assert_eq!(page3.items[0].get_string(2), Some("Carol".to_string()));
}

#[tokio::test]
async fn paginate_with_filter_and_multi_column_order() {
    let conn = setup().await;

    for (email, name, role, score) in &[
        ("a@t.com", "Alice", "User", 10i32),
        ("b@t.com", "Bob", "User", 20i32),
        ("c@t.com", "Carol", "Admin", 30i32),
        ("d@t.com", "Dave", "User", 40i32),
        ("e@t.com", "Eve", "User", 50i32),
        ("f@t.com", "Frank", "User", 60i32),
    ] {
        let q = Query::table("Account")
            .create()
            .set("email", *email)
            .set("name", *name)
            .set("role", *role)
            .set("score", Value::from(*score))
            .build();
        conn.execute(&q).await.unwrap();
    }

    // Filtered query: only Users, ordered by score DESC
    let base_query = Query::table("Account")
        .find_many()
        .filter(Filter::eq("role", "User"))
        .order_by("score", Order::Desc)
        .build();

    let config = PaginateConfig {
        default_page_size: 2,
        max_page_size: 100,
        include_total_size: true,
    };

    // Page 1
    let page1 = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::first_page(2),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(page1.items.len(), 2);
    assert_eq!(page1.total_size, Some(5)); // 5 Users total
    assert_eq!(page1.items[0].get_string(2), Some("Frank".to_string())); // score=60
    assert_eq!(page1.items[1].get_string(2), Some("Eve".to_string())); // score=50

    // Page 2
    let page2 = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::next(2, &page1.next_page_token),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(page2.items.len(), 2);
    assert_eq!(page2.items[0].get_string(2), Some("Dave".to_string())); // score=40
    assert_eq!(page2.items[1].get_string(2), Some("Bob".to_string())); // score=20

    // Page 3 (last)
    let page3 = paginate(
        &conn,
        &base_query,
        None,
        &PageRequest::next(2, &page2.next_page_token),
        &config,
    )
    .await
    .unwrap();
    assert_eq!(page3.items.len(), 1);
    assert!(page3.next_page_token.is_empty());
    assert_eq!(page3.items[0].get_string(2), Some("Alice".to_string())); // score=10
}

#[tokio::test]
async fn paginate_with_custom_count_query() {
    let conn = setup_many_accounts().await;

    let base_query = Query::table("Account")
        .find_many()
        .order_by("id", Order::Asc)
        .build();

    // Custom count query -- sometimes you want a different count
    // (e.g., count distinct values, or count with different filter)
    let count_query = Query::table("Account").aggregate().count_all().build();

    let config = PaginateConfig {
        default_page_size: 10,
        max_page_size: 100,
        include_total_size: true,
    };

    let response = paginate(
        &conn,
        &base_query,
        Some(&count_query),
        &PageRequest::first_page(5),
        &config,
    )
    .await
    .unwrap();

    assert_eq!(response.items.len(), 5);
    assert_eq!(response.total_size, Some(25));
    assert!(!response.next_page_token.is_empty());
}
