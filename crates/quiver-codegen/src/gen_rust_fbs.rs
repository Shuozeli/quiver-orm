use flatc_rs_codegen::CodeGenOptions;
use flatc_rs_compiler::compile_single;
use quiver_error::QuiverError;
use quiver_schema::Schema;

use crate::FbsGenerator;

/// Generates Rust types from a Quiver schema via FlatBuffers.
///
/// Pipeline: Quiver schema -> .fbs string -> flatc-rs compile -> Rust code
pub struct RustFbsGenerator;

impl RustFbsGenerator {
    /// Generate Rust code from a Quiver schema using the FlatBuffers backend.
    ///
    /// 1. Generates a .fbs schema string from the Quiver AST
    /// 2. Compiles it with flatc-rs-compiler (parse + analyze)
    /// 3. Generates Rust code with flatc-rs-codegen
    pub fn generate(schema: &Schema, namespace: &str) -> Result<RustFbsOutput, QuiverError> {
        // Step 1: Generate .fbs schema text
        let fbs_text = FbsGenerator::generate(schema, namespace)?;

        // Step 2: Compile .fbs text through flatc-rs
        let compilation = compile_single(&fbs_text)
            .map_err(|e| QuiverError::Codegen(format!("FlatBuffers compilation failed: {e}")))?;

        // Step 3: Generate Rust code
        let opts = CodeGenOptions {
            gen_object_api: true,
            gen_name_constants: true,
            ..CodeGenOptions::default()
        };

        let rust_code = flatc_rs_codegen::generate_rust(&compilation.schema, &opts)
            .map_err(|e| QuiverError::Codegen(format!("FlatBuffers Rust codegen failed: {e}")))?;

        Ok(RustFbsOutput {
            fbs_schema: fbs_text,
            rust_code,
        })
    }
}

/// Output from FlatBuffers-based Rust code generation.
pub struct RustFbsOutput {
    /// The intermediate .fbs schema text.
    pub fbs_schema: String,
    /// The generated Rust source code.
    pub rust_code: String,
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

        let output = RustFbsGenerator::generate(&schema, "Quiver.Models").unwrap();

        // Should have generated both FBS and Rust
        assert!(!output.fbs_schema.is_empty());
        assert!(!output.rust_code.is_empty());

        // Rust code should contain the struct
        assert!(output.rust_code.contains("User"));
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

        let output = RustFbsGenerator::generate(&schema, "Test").unwrap();
        assert!(output.rust_code.contains("Role"));
        assert!(output.rust_code.contains("Account"));
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

        let output = RustFbsGenerator::generate(&schema, "Test").unwrap();
        assert!(output.rust_code.contains("Doc"));
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

        let output = RustFbsGenerator::generate(&schema, "Test").unwrap();
        assert!(output.rust_code.contains("Event"));
    }

    #[test]
    fn fbs_text_is_included_in_output() {
        let schema = parse(
            r#"
            model Item {
                id   Int32 PRIMARY KEY
                name Utf8
            }
        "#,
        )
        .unwrap();

        let output = RustFbsGenerator::generate(&schema, "Test").unwrap();
        assert!(output.fbs_schema.contains("namespace Test;"));
        assert!(output.fbs_schema.contains("table Item {"));
    }
}
