use protoc_rs_analyzer::analyze;
use protoc_rs_codegen::generate_rust;
use quiver_error::QuiverError;
use quiver_schema::Schema;
use std::collections::HashMap;

use crate::ProtoGenerator;

/// Generates Rust types from a Quiver schema via Protobuf.
///
/// Pipeline: Quiver schema -> .proto string -> protoc-rs analyze -> Rust code
pub struct RustProtoGenerator;

impl RustProtoGenerator {
    /// Generate Rust code from a Quiver schema using the Protobuf backend.
    ///
    /// 1. Generates a .proto schema string from the Quiver AST
    /// 2. Analyzes it with protoc-rs-analyzer (parse + resolve + validate)
    /// 3. Generates Rust code with protoc-rs-codegen (prost-compatible)
    pub fn generate(schema: &Schema, package: &str) -> Result<RustProtoOutput, QuiverError> {
        // Step 1: Generate .proto schema text
        let proto_text = ProtoGenerator::generate(schema, package)?;

        // Step 2: Analyze .proto text through protoc-rs
        let fds = analyze(&proto_text)
            .map_err(|e| QuiverError::Codegen(format!("Protobuf analysis failed: {e}")))?;

        // Step 3: Generate Rust code
        let rust_modules = generate_rust(&fds)
            .map_err(|e| QuiverError::Codegen(format!("Protobuf Rust codegen failed: {e}")))?;

        Ok(RustProtoOutput {
            proto_schema: proto_text,
            rust_modules,
        })
    }
}

/// Output from Protobuf-based Rust code generation.
pub struct RustProtoOutput {
    /// The intermediate .proto schema text.
    pub proto_schema: String,
    /// Generated Rust source code, keyed by module path (e.g. "quiver.models").
    pub rust_modules: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use quiver_schema::parse;

    #[test]
    fn generate_rust_from_simple_model() {
        let schema = parse(
            r#"
            model User {
                id    Int32  PRIMARY KEY AUTOINCREMENT
                email Utf8   UNIQUE
                name  Utf8?
                age   Int16
                active Boolean DEFAULT true
            }
        "#,
        )
        .unwrap();

        let output = RustProtoGenerator::generate(&schema, "quiver.models").unwrap();

        // Should have generated both proto and Rust
        assert!(!output.proto_schema.is_empty());
        assert!(!output.rust_modules.is_empty());

        // Should have a module for our package
        let rust_code = output.rust_modules.values().next().unwrap();
        assert!(rust_code.contains("User"));
    }

    #[test]
    fn generate_rust_from_enum_model() {
        let schema = parse(
            r#"
            enum Role { User Admin Moderator }
            model Account {
                id   Int32 PRIMARY KEY
                role Role  DEFAULT Admin
            }
        "#,
        )
        .unwrap();

        let output = RustProtoGenerator::generate(&schema, "test").unwrap();
        let rust_code = output.rust_modules.values().next().unwrap();
        assert!(rust_code.contains("Role"));
        assert!(rust_code.contains("Account"));
    }

    #[test]
    fn generate_rust_with_list_field() {
        let schema = parse(
            r#"
            model Doc {
                id   Int32 PRIMARY KEY
                tags List<Utf8>
            }
        "#,
        )
        .unwrap();

        let output = RustProtoGenerator::generate(&schema, "test").unwrap();
        let rust_code = output.rust_modules.values().next().unwrap();
        assert!(rust_code.contains("Doc"));
    }

    #[test]
    fn generate_rust_with_temporal_fields() {
        let schema = parse(
            r#"
            model Event {
                id      Int32 PRIMARY KEY
                created Timestamp(Microsecond, UTC)
                day     Date32
            }
        "#,
        )
        .unwrap();

        let output = RustProtoGenerator::generate(&schema, "test").unwrap();
        let rust_code = output.rust_modules.values().next().unwrap();
        assert!(rust_code.contains("Event"));
    }

    #[test]
    fn proto_text_is_included_in_output() {
        let schema = parse(
            r#"
            model Item {
                id   Int32 PRIMARY KEY
                name Utf8
            }
        "#,
        )
        .unwrap();

        let output = RustProtoGenerator::generate(&schema, "test").unwrap();
        assert!(output.proto_schema.contains("package test;"));
        assert!(output.proto_schema.contains("message Item {"));
    }

    #[test]
    fn rust_modules_keyed_by_package() {
        let schema = parse(
            r#"
            model Foo {
                id Int32 PRIMARY KEY
            }
        "#,
        )
        .unwrap();

        let output = RustProtoGenerator::generate(&schema, "myapp.v1").unwrap();
        // protoc-rs-codegen keys modules by package name (may include .rs suffix)
        let has_key = output.rust_modules.contains_key("myapp.v1")
            || output.rust_modules.contains_key("myapp.v1.rs");
        assert!(
            has_key,
            "expected key containing 'myapp.v1', got keys: {:?}",
            output.rust_modules.keys().collect::<Vec<_>>()
        );
    }
}
