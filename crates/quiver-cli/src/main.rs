use clap::{Parser, Subcommand};
use quiver_codegen::{
    FbsGenerator, ProtoGenerator, RustClientGenerator, RustFbsGenerator, RustProtoGenerator,
    RustSerdeGenerator, SqlDialect, SqlGenerator, TypeScriptGenerator,
};
use quiver_driver_core::{DdlStatement, Driver, DynConnection};
use quiver_error::QuiverError;
use quiver_migrate::{
    MigrationSqlGenerator, MigrationStep, MigrationTracker, TrustedSql, diff_schemas, introspect,
    schema_to_quiver,
};
use quiver_schema::Schema;
use quiver_schema::validate::validate;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "quiver", about = "Quiver: Arrow-native ORM")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Parse a .quiver schema and print the AST
    Parse {
        /// Path to the .quiver schema file
        schema: PathBuf,
    },
    /// Generate code from a .quiver schema
    Generate {
        /// Path to the .quiver schema file
        schema: PathBuf,
        /// Output directory
        #[arg(short, long, default_value = "generated")]
        output: PathBuf,
        /// Target format: flatbuffers, protobuf, rust-client, rust-fbs, rust-proto, rust-serde, typescript, sql-sqlite, sql-postgres, sql-mysql
        #[arg(short, long, default_value = "flatbuffers")]
        target: String,
    },
    /// Database migration commands
    #[command(subcommand)]
    Migrate(MigrateCommand),
    /// Database management commands
    #[command(subcommand)]
    Db(DbCommand),
}

#[derive(Subcommand)]
enum MigrateCommand {
    /// Create a new migration from schema changes
    Create {
        /// Path to the .quiver schema file
        schema: PathBuf,
        /// Migration name
        name: String,
        /// Migrations directory
        #[arg(short, long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Apply pending migrations
    Apply {
        /// Path to the .quiver schema file
        schema: PathBuf,
        /// Migrations directory
        #[arg(short, long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Show migration status
    Status {
        /// Path to the .quiver schema file
        schema: PathBuf,
        /// Migrations directory
        #[arg(short, long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Rollback the last applied migration
    Rollback {
        /// Path to the .quiver schema file
        schema: PathBuf,
        /// Migrations directory
        #[arg(short, long, default_value = "migrations")]
        dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum DbCommand {
    /// Push schema changes directly to the database (no migration files)
    Push {
        /// Path to the .quiver schema file
        schema: PathBuf,
    },
    /// Pull the current database schema into a .quiver file
    Pull {
        /// Path to the .quiver schema file
        schema: PathBuf,
        /// Output path for the generated .quiver file
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Execute raw SQL against the database
    Execute {
        /// Path to the .quiver schema file (for connection info)
        schema: PathBuf,
        /// SQL statement to execute
        sql: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Command::Parse { schema } => cmd_parse(&schema),
        Command::Generate {
            schema,
            output,
            target,
        } => cmd_generate(&schema, &output, &target),
        Command::Migrate(cmd) => match cmd {
            MigrateCommand::Create { schema, name, dir } => {
                cmd_migrate_create(&schema, &name, &dir)
            }
            MigrateCommand::Apply { schema, dir } => cmd_migrate_apply(&schema, &dir).await,
            MigrateCommand::Status { schema, dir } => cmd_migrate_status(&schema, &dir).await,
            MigrateCommand::Rollback { schema, dir } => cmd_migrate_rollback(&schema, &dir).await,
        },
        Command::Db(cmd) => match cmd {
            DbCommand::Push { schema } => cmd_db_push(&schema).await,
            DbCommand::Pull { schema, output } => cmd_db_pull(&schema, &output).await,
            DbCommand::Execute { schema, sql } => cmd_db_execute(&schema, &sql).await,
        },
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Schema loading and config resolution
// ---------------------------------------------------------------------------

fn load_schema(path: &Path) -> Result<Schema, QuiverError> {
    let source = std::fs::read_to_string(path).map_err(QuiverError::Io)?;
    let schema = quiver_schema::parse(&source)?;
    if let Err(errors) = validate(&schema) {
        let msg = errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        return Err(QuiverError::Validation(msg));
    }
    Ok(schema)
}

fn resolve_provider(schema: &Schema) -> Result<(&str, &str), QuiverError> {
    let config = schema.config.as_ref().ok_or_else(|| {
        QuiverError::Validation("schema must have a config block with 'provider' and 'url'".into())
    })?;

    let provider = config
        .entries
        .iter()
        .find(|e| e.key == "provider")
        .map(|e| e.value.as_str())
        .ok_or_else(|| {
            QuiverError::Validation("config block must have a 'provider' entry".into())
        })?;

    let url = config
        .entries
        .iter()
        .find(|e| e.key == "url")
        .map(|e| e.value.as_str())
        .ok_or_else(|| QuiverError::Validation("config block must have a 'url' entry".into()))?;

    Ok((provider, url))
}

fn migrate_dialect_for_provider(provider: &str) -> Result<quiver_migrate::SqlDialect, QuiverError> {
    match provider {
        "sqlite" => Ok(quiver_migrate::SqlDialect::Sqlite),
        "postgresql" | "postgres" => Ok(quiver_migrate::SqlDialect::Postgres),
        "mysql" => Ok(quiver_migrate::SqlDialect::Mysql),
        _ => Err(QuiverError::Validation(format!(
            "unknown provider '{provider}'"
        ))),
    }
}

/// Connect to the database and return a type-erased connection.
async fn connect(provider: &str, url: &str) -> Result<Box<dyn DynConnection>, QuiverError> {
    match provider {
        "sqlite" => {
            let conn = quiver_driver_sqlite::SqliteDriver.connect(url).await?;
            Ok(Box::new(conn))
        }
        "postgresql" | "postgres" => {
            let conn = quiver_driver_postgres::PostgresDriver.connect(url).await?;
            Ok(Box::new(conn))
        }
        "mysql" => {
            let conn = quiver_driver_mysql::MysqlDriver.connect(url).await?;
            Ok(Box::new(conn))
        }
        _ => Err(QuiverError::Validation(format!(
            "unknown provider '{provider}'"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

fn cmd_parse(schema_path: &Path) -> Result<(), QuiverError> {
    let schema = load_schema(schema_path)?;
    println!("Schema parsed successfully.");
    println!("  Enums:  {}", schema.enums.len());
    println!("  Models: {}", schema.models.len());
    for m in &schema.models {
        println!("    {} ({} fields)", m.name, m.fields.len());
    }
    Ok(())
}

fn cmd_generate(schema_path: &Path, output_dir: &Path, target: &str) -> Result<(), QuiverError> {
    let schema = load_schema(schema_path)?;

    std::fs::create_dir_all(output_dir).map_err(QuiverError::Io)?;

    match target {
        "flatbuffers" | "fbs" => {
            let namespace = "Quiver.Models";
            let fbs = FbsGenerator::generate(&schema, namespace)?;
            let out_path = output_dir.join("schema.fbs");
            std::fs::write(&out_path, &fbs).map_err(QuiverError::Io)?;
            println!("Generated {}", out_path.display());
        }
        "protobuf" | "proto" => {
            let package = "quiver.models";
            let proto = ProtoGenerator::generate(&schema, package)?;
            let out_path = output_dir.join("schema.proto");
            std::fs::write(&out_path, &proto).map_err(QuiverError::Io)?;
            println!("Generated {}", out_path.display());
        }
        "rust-fbs" => {
            let namespace = "Quiver.Models";
            let output = RustFbsGenerator::generate(&schema, namespace)?;
            let fbs_path = output_dir.join("schema.fbs");
            std::fs::write(&fbs_path, &output.fbs_schema).map_err(QuiverError::Io)?;
            println!("Generated {}", fbs_path.display());

            let rs_path = output_dir.join("schema_fbs.rs");
            std::fs::write(&rs_path, &output.rust_code).map_err(QuiverError::Io)?;
            println!("Generated {}", rs_path.display());
        }
        "rust-proto" => {
            let package = "quiver.models";
            let output = RustProtoGenerator::generate(&schema, package)?;
            let proto_path = output_dir.join("schema.proto");
            std::fs::write(&proto_path, &output.proto_schema).map_err(QuiverError::Io)?;
            println!("Generated {}", proto_path.display());

            for (module, code) in &output.rust_modules {
                let filename = format!("{}.rs", module.replace('.', "_"));
                let rs_path = output_dir.join(&filename);
                std::fs::write(&rs_path, code).map_err(QuiverError::Io)?;
                println!("Generated {}", rs_path.display());
            }
        }
        "rust-client" | "client" => {
            let code = RustClientGenerator::generate(&schema)?;
            let rs_path = output_dir.join("client.rs");
            std::fs::write(&rs_path, &code).map_err(QuiverError::Io)?;
            println!("Generated {}", rs_path.display());
        }
        "rust-serde" | "rust" => {
            let code = RustSerdeGenerator::generate(&schema)?;
            let rs_path = output_dir.join("models.rs");
            std::fs::write(&rs_path, &code).map_err(QuiverError::Io)?;
            println!("Generated {}", rs_path.display());
        }
        "typescript" | "ts" => {
            let code = TypeScriptGenerator::generate(&schema)?;
            let out_path = output_dir.join("models.ts");
            std::fs::write(&out_path, &code).map_err(QuiverError::Io)?;
            println!("Generated {}", out_path.display());
        }
        "sql" | "sqlite" | "sql-sqlite" => {
            let ddl = SqlGenerator::generate(&schema, SqlDialect::Sqlite)?;
            let out_path = output_dir.join("schema.sql");
            std::fs::write(&out_path, &ddl).map_err(QuiverError::Io)?;
            println!("Generated {}", out_path.display());
        }
        "sql-postgres" | "postgres" => {
            let ddl = SqlGenerator::generate(&schema, SqlDialect::Postgres)?;
            let out_path = output_dir.join("schema.sql");
            std::fs::write(&out_path, &ddl).map_err(QuiverError::Io)?;
            println!("Generated {}", out_path.display());
        }
        "sql-mysql" | "mysql" => {
            let ddl = SqlGenerator::generate(&schema, SqlDialect::Mysql)?;
            let out_path = output_dir.join("schema.sql");
            std::fs::write(&out_path, &ddl).map_err(QuiverError::Io)?;
            println!("Generated {}", out_path.display());
        }
        _ => {
            return Err(QuiverError::Validation(format!(
                "unknown target '{target}'. supported: flatbuffers, protobuf, rust-client, rust-fbs, rust-proto, rust-serde, typescript, sql-sqlite, sql-postgres, sql-mysql"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Migrate commands
// ---------------------------------------------------------------------------

fn cmd_migrate_create(
    schema_path: &Path,
    name: &str,
    migrations_dir: &Path,
) -> Result<(), QuiverError> {
    let schema = load_schema(schema_path)?;
    let (provider, _url) = resolve_provider(&schema)?;
    let dialect = migrate_dialect_for_provider(provider)?;

    // Load the previous schema from the last migration snapshot, if any
    let prev_schema = load_last_snapshot(migrations_dir);

    // Diff
    let steps = diff_schemas(Some(&prev_schema), &schema);
    if steps.is_empty() {
        println!("No changes detected.");
        return Ok(());
    }

    // Generate SQL
    let up_sql = MigrationSqlGenerator::generate_up(&steps, &schema, dialect)?;
    let down_sql = MigrationSqlGenerator::generate_down(&steps, &schema, dialect)?;

    // Create migration directory
    let timestamp = format!(
        "{:020}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    let migration_id = format!("{}_{}", timestamp, name);
    let migration_dir = migrations_dir.join(&migration_id);
    std::fs::create_dir_all(&migration_dir).map_err(QuiverError::Io)?;

    // Write migration files
    let up_json =
        serde_json::to_string_pretty(&up_sql).map_err(|e| QuiverError::Codegen(e.to_string()))?;
    let down_json =
        serde_json::to_string_pretty(&down_sql).map_err(|e| QuiverError::Codegen(e.to_string()))?;

    std::fs::write(migration_dir.join("up.json"), &up_json).map_err(QuiverError::Io)?;
    std::fs::write(migration_dir.join("down.json"), &down_json).map_err(QuiverError::Io)?;

    // Save schema snapshot
    let schema_source = std::fs::read_to_string(schema_path).map_err(QuiverError::Io)?;
    std::fs::write(migration_dir.join("schema.quiver"), &schema_source).map_err(QuiverError::Io)?;

    println!("Created migration: {migration_id}");
    println!(
        "  {} steps, {} up statements, {} down statements",
        steps.len(),
        up_sql.len(),
        down_sql.len()
    );

    Ok(())
}

async fn cmd_migrate_apply(schema_path: &Path, migrations_dir: &Path) -> Result<(), QuiverError> {
    let schema = load_schema(schema_path)?;
    let (provider, url) = resolve_provider(&schema)?;
    let conn = connect(provider, url).await?;

    // List migration directories sorted by name (timestamp-based)
    let mut migration_dirs = list_migration_dirs(migrations_dir)?;
    migration_dirs.sort();

    // Get applied migrations
    let applied = MigrationTracker::applied(conn.as_ref()).await?;

    let mut applied_count = 0;
    for dir_name in &migration_dirs {
        if applied.contains(dir_name) {
            continue;
        }

        let migration_dir = migrations_dir.join(dir_name);
        let up_json =
            std::fs::read_to_string(migration_dir.join("up.json")).map_err(QuiverError::Io)?;
        let up: Vec<TrustedSql> =
            serde_json::from_str(&up_json).map_err(|e| QuiverError::Migration(e.to_string()))?;

        let migration = quiver_migrate::Migration {
            id: dir_name.clone(),
            description: dir_name.clone(),
            up,
            down: Vec::new(), // down loaded only on rollback
        };

        let timestamp = chrono_now();
        MigrationTracker::apply(conn.as_ref(), &migration, &timestamp).await?;
        println!("Applied: {dir_name}");
        applied_count += 1;
    }

    if applied_count == 0 {
        println!("No pending migrations.");
    } else {
        println!("Applied {applied_count} migration(s).");
    }

    Ok(())
}

async fn cmd_migrate_status(schema_path: &Path, migrations_dir: &Path) -> Result<(), QuiverError> {
    let schema = load_schema(schema_path)?;
    let (provider, url) = resolve_provider(&schema)?;
    let conn = connect(provider, url).await?;

    let applied = MigrationTracker::applied(conn.as_ref()).await?;
    let mut migration_dirs = list_migration_dirs(migrations_dir)?;
    migration_dirs.sort();

    if migration_dirs.is_empty() {
        println!("No migrations found.");
        return Ok(());
    }

    for dir_name in &migration_dirs {
        let status = if applied.contains(dir_name) {
            "applied"
        } else {
            "pending"
        };
        println!("  [{status}] {dir_name}");
    }

    let applied_count = migration_dirs
        .iter()
        .filter(|d| applied.contains(d))
        .count();
    let pending = migration_dirs.len() - applied_count;
    println!("\n{applied_count} applied, {pending} pending");

    Ok(())
}

async fn cmd_migrate_rollback(
    schema_path: &Path,
    migrations_dir: &Path,
) -> Result<(), QuiverError> {
    let schema = load_schema(schema_path)?;
    let (provider, url) = resolve_provider(&schema)?;
    let conn = connect(provider, url).await?;

    let applied = MigrationTracker::applied(conn.as_ref()).await?;
    let last = applied
        .last()
        .ok_or_else(|| QuiverError::Migration("no migrations to rollback".into()))?;

    let migration_dir = migrations_dir.join(last);
    let down_json =
        std::fs::read_to_string(migration_dir.join("down.json")).map_err(QuiverError::Io)?;
    let down: Vec<TrustedSql> =
        serde_json::from_str(&down_json).map_err(|e| QuiverError::Migration(e.to_string()))?;

    let migration = quiver_migrate::Migration {
        id: last.clone(),
        description: last.clone(),
        up: Vec::new(),
        down,
    };

    MigrationTracker::rollback(conn.as_ref(), &migration).await?;
    println!("Rolled back: {last}");

    Ok(())
}

// ---------------------------------------------------------------------------
// Db commands
// ---------------------------------------------------------------------------

async fn cmd_db_push(schema_path: &Path) -> Result<(), QuiverError> {
    let schema = load_schema(schema_path)?;
    let (provider, url) = resolve_provider(&schema)?;
    let dialect = migrate_dialect_for_provider(provider)?;
    let conn = connect(provider, url).await?;

    // Introspect current database state
    let current = introspect(conn.as_ref(), dialect).await?;

    // Diff against desired schema
    let steps = diff_schemas(Some(&current), &schema);
    if steps.is_empty() {
        println!("Database is up to date.");
        return Ok(());
    }

    // Generate and execute migration SQL
    let statements = MigrationSqlGenerator::generate_up(&steps, &schema, dialect)?;
    for trusted in &statements {
        exec_trusted(conn.as_ref(), trusted).await?;
    }

    println!("Pushed {} change(s) to database.", steps.len());
    for step in &steps {
        println!("  {}", describe_step(step));
    }

    Ok(())
}

async fn cmd_db_pull(schema_path: &Path, output: &Path) -> Result<(), QuiverError> {
    let schema = load_schema(schema_path)?;
    let (provider, url) = resolve_provider(&schema)?;
    let dialect = migrate_dialect_for_provider(provider)?;
    let conn = connect(provider, url).await?;

    let introspected = introspect(conn.as_ref(), dialect).await?;
    let quiver_source = schema_to_quiver(&introspected);

    std::fs::write(output, &quiver_source).map_err(QuiverError::Io)?;
    println!("Pulled schema to {}", output.display());
    println!(
        "  {} model(s), {} enum(s)",
        introspected.models.len(),
        introspected.enums.len()
    );

    Ok(())
}

async fn cmd_db_execute(schema_path: &Path, sql: &str) -> Result<(), QuiverError> {
    let schema = load_schema(schema_path)?;
    let (provider, url) = resolve_provider(&schema)?;
    let conn = connect(provider, url).await?;

    // Determine if this is a query (SELECT) or a mutation
    let trimmed = sql.trim().to_uppercase();
    if trimmed.starts_with("SELECT") || trimmed.starts_with("WITH") {
        let stmt = quiver_driver_core::Statement::sql(sql.to_string());
        let rows = conn.dyn_query(&stmt).await?;

        if rows.is_empty() {
            println!("(0 rows)");
            return Ok(());
        }

        // Print column headers
        let headers: Vec<&str> = rows[0].columns.iter().map(|c| c.name.as_str()).collect();
        println!("{}", headers.join(" | "));
        println!(
            "{}",
            headers
                .iter()
                .map(|h| "-".repeat(h.len()))
                .collect::<Vec<_>>()
                .join("-+-")
        );

        // Print rows
        for row in &rows {
            let vals: Vec<String> = row.values.iter().map(format_value).collect();
            println!("{}", vals.join(" | "));
        }
        println!("({} row(s))", rows.len());
    } else {
        let stmt = quiver_driver_core::Statement::sql(sql.to_string());
        let affected = conn.dyn_execute(&stmt).await?;
        println!("{affected} row(s) affected.");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn format_value(v: &quiver_driver_core::Value) -> String {
    use quiver_driver_core::Value;
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => format!("<{} bytes>", b.len()),
    }
}

async fn exec_trusted(conn: &dyn DynConnection, trusted: &TrustedSql) -> Result<(), QuiverError> {
    if trusted.params.is_empty() {
        for part in trusted.sql.split(";\n") {
            let trimmed = part.trim();
            if !trimmed.is_empty() {
                conn.dyn_execute_ddl(&DdlStatement::new(trimmed.to_string()))
                    .await?;
            }
        }
    } else {
        let stmt = quiver_driver_core::Statement::new(trusted.sql.clone(), trusted.params.clone());
        conn.dyn_execute(&stmt).await?;
    }
    Ok(())
}

fn load_last_snapshot(migrations_dir: &Path) -> Schema {
    if let Ok(dirs) = list_migration_dirs(migrations_dir) {
        if let Some(last) = dirs.iter().max() {
            let snapshot_path = migrations_dir.join(last).join("schema.quiver");
            if let Ok(source) = std::fs::read_to_string(&snapshot_path) {
                if let Ok(schema) = quiver_schema::parse(&source) {
                    return schema;
                }
            }
        }
    }
    // Empty schema if no previous migrations
    Schema {
        config: None,
        generate: None,
        enums: Vec::new(),
        models: Vec::new(),
    }
}

fn list_migration_dirs(migrations_dir: &Path) -> Result<Vec<String>, QuiverError> {
    if !migrations_dir.exists() {
        return Ok(Vec::new());
    }
    let mut dirs = Vec::new();
    for entry in std::fs::read_dir(migrations_dir).map_err(QuiverError::Io)? {
        let entry = entry.map_err(QuiverError::Io)?;
        if entry.file_type().map_err(QuiverError::Io)?.is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                dirs.push(name.to_string());
            }
        }
    }
    Ok(dirs)
}

fn chrono_now() -> String {
    // Simple ISO 8601 timestamp without chrono dependency
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", d.as_secs())
}

fn describe_step(step: &MigrationStep) -> String {
    match step {
        MigrationStep::CreateModel { name, .. } => format!("create model {name}"),
        MigrationStep::DropModel { name } => format!("drop model {name}"),
        MigrationStep::AddField { model, field } => {
            format!("add field {}.{}", model, field.name)
        }
        MigrationStep::DropField {
            model, field_name, ..
        } => format!("drop field {model}.{field_name}"),
        MigrationStep::AlterField {
            model, new_field, ..
        } => {
            format!("alter field {}.{}", model, new_field.name)
        }
        MigrationStep::CreateIndex {
            model, index_name, ..
        } => format!("create index {index_name} on {model}"),
        MigrationStep::DropIndex { index_name } => format!("drop index {index_name}"),
        MigrationStep::CreateEnum { name, .. } => format!("create enum {name}"),
        MigrationStep::DropEnum { name } => format!("drop enum {name}"),
        MigrationStep::AddEnumValue {
            enum_name, value, ..
        } => format!("add value '{value}' to enum {enum_name}"),
        MigrationStep::RemoveEnumValue {
            enum_name, value, ..
        } => format!("remove value '{value}' from enum {enum_name}"),
    }
}
