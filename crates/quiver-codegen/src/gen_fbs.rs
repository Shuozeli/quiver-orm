use quiver_error::QuiverError;
use quiver_schema::Schema;
use quiver_schema::ast::*;

/// Generates a FlatBuffers schema (`.fbs`) from a Quiver schema.
///
/// Produces table definitions for models and enum definitions with 1:1
/// Arrow type mapping.
pub struct FbsGenerator;

impl FbsGenerator {
    pub fn generate(schema: &Schema, namespace: &str) -> Result<String, QuiverError> {
        let mut out = String::new();

        out.push_str(&format!("namespace {namespace};\n\n"));

        // Enums
        for e in &schema.enums {
            gen_enum(&mut out, e);
        }

        // Model tables
        for m in &schema.models {
            gen_table(&mut out, m);
        }

        // Input tables
        for m in &schema.models {
            gen_create_input(&mut out, m);
            gen_update_input(&mut out, m);
        }

        Ok(out)
    }
}

fn gen_enum(out: &mut String, e: &EnumDef) {
    out.push_str(&format!("enum {} : byte {{\n", e.name));
    for (i, v) in e.values.iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        out.push_str(&format!("  {} = {}", v.name, i));
    }
    out.push_str("\n}\n\n");
}

fn gen_table(out: &mut String, m: &ModelDef) {
    out.push_str(&format!("table {} {{\n", m.name));
    for f in &m.fields {
        let fbs_type = type_to_fbs(&f.type_expr);
        let required = if !f.type_expr.nullable && is_required_fbs_type(&f.type_expr.base) {
            " (required)"
        } else {
            ""
        };
        let default = field_default_fbs(f);
        out.push_str(&format!(
            "  {}: {}{}{};",
            f.name, fbs_type, default, required
        ));
        out.push('\n');
    }
    out.push_str("}\n\n");
}

fn gen_create_input(out: &mut String, m: &ModelDef) {
    out.push_str(&format!("table {}CreateInput {{\n", m.name));
    for f in &m.fields {
        if is_auto_field(f) {
            continue;
        }
        let fbs_type = type_to_fbs(&f.type_expr);
        let required = if !f.type_expr.nullable
            && !has_default(f)
            && is_required_fbs_type(&f.type_expr.base)
        {
            " (required)"
        } else {
            ""
        };
        out.push_str(&format!("  {}: {}{};", f.name, fbs_type, required));
        out.push('\n');
    }
    out.push_str("}\n\n");
}

fn gen_update_input(out: &mut String, m: &ModelDef) {
    out.push_str(&format!("table {}UpdateInput {{\n", m.name));
    for f in &m.fields {
        if is_auto_field(f) {
            continue;
        }
        // All fields optional in update (FBS table fields are optional by default)
        let fbs_type = type_to_fbs(&f.type_expr);
        out.push_str(&format!("  {}: {};", f.name, fbs_type));
        out.push('\n');
    }
    out.push_str("}\n\n");
}

fn type_to_fbs(type_expr: &TypeExpr) -> String {
    base_type_to_fbs(&type_expr.base)
}

fn base_type_to_fbs(base: &BaseType) -> String {
    match base {
        BaseType::Int8 => "int8".into(),
        BaseType::Int16 => "int16".into(),
        BaseType::Int32 => "int32".into(),
        BaseType::Int64 => "int64".into(),
        BaseType::UInt8 => "uint8".into(),
        BaseType::UInt16 => "uint16".into(),
        BaseType::UInt32 => "uint32".into(),
        BaseType::UInt64 => "uint64".into(),
        BaseType::Float16 => "float32".into(), // FBS has no float16, upcast
        BaseType::Float32 => "float32".into(),
        BaseType::Float64 => "float64".into(),
        BaseType::Decimal128 { .. } | BaseType::Decimal256 { .. } => "string".into(),
        BaseType::Utf8 | BaseType::LargeUtf8 => "string".into(),
        BaseType::Binary | BaseType::LargeBinary | BaseType::FixedSizeBinary { .. } => {
            "[ubyte]".into()
        }
        BaseType::Boolean => "bool".into(),
        BaseType::Date32 | BaseType::Date64 => "int32".into(), // days since epoch
        BaseType::Time32 { .. } => "int32".into(),
        BaseType::Time64 { .. } => "int64".into(),
        BaseType::Timestamp { .. } => "int64".into(), // epoch micros
        BaseType::List(inner) | BaseType::LargeList(inner) => {
            format!("[{}]", base_type_to_fbs(&inner.base))
        }
        BaseType::Map { key, value } => {
            // FBS has no native map; use [MapEntry] pattern
            format!(
                "[{}{}MapEntry]",
                base_type_to_fbs(&key.base),
                base_type_to_fbs(&value.base)
            )
        }
        BaseType::Struct(fields) => {
            // Inline struct -- for now emit as table name reference
            // TODO: generate separate struct definition
            let _ = fields;
            "string".into() // fallback: JSON string
        }
        BaseType::Named(name) => name.clone(),
    }
}

fn is_required_fbs_type(base: &BaseType) -> bool {
    // In FBS, scalar types (int, float, bool, enum) have implicit defaults.
    // Only string, vector, and table types need (required).
    matches!(
        base,
        BaseType::Utf8
            | BaseType::LargeUtf8
            | BaseType::Binary
            | BaseType::LargeBinary
            | BaseType::FixedSizeBinary { .. }
            | BaseType::Decimal128 { .. }
            | BaseType::Decimal256 { .. }
            | BaseType::List(_)
            | BaseType::LargeList(_)
            | BaseType::Map { .. }
            | BaseType::Struct(_)
    )
}

fn is_auto_field(f: &FieldDef) -> bool {
    f.attributes
        .iter()
        .any(|a| matches!(a, FieldAttribute::Autoincrement | FieldAttribute::Id))
}

fn has_default(f: &FieldDef) -> bool {
    f.attributes
        .iter()
        .any(|a| matches!(a, FieldAttribute::Default(_)))
}

fn field_default_fbs(f: &FieldDef) -> String {
    for attr in &f.attributes {
        if let FieldAttribute::Default(val) = attr {
            return match val {
                DefaultValue::Int(v) => format!(" = {v}"),
                DefaultValue::Float(v) => format!(" = {v}"),
                DefaultValue::Bool(v) => format!(" = {v}"),
                DefaultValue::EnumVariant(v) => format!(" = {v}"),
                _ => String::new(), // now(), uuid(), etc. can't be FBS defaults
            };
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use quiver_schema::parse;

    #[test]
    fn generate_simple_model() {
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
        let fbs = FbsGenerator::generate(&schema, "Quiver.Models").unwrap();
        assert!(fbs.contains("namespace Quiver.Models;"));
        assert!(fbs.contains("table User {"));
        assert!(fbs.contains("email: string (required);"));
        assert!(fbs.contains("name: string;"));
        assert!(fbs.contains("age: int16;"));
        assert!(fbs.contains("active: bool = true;"));
    }

    #[test]
    fn generate_enum() {
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
        let fbs = FbsGenerator::generate(&schema, "Test").unwrap();
        assert!(fbs.contains("enum Role : byte {"));
        assert!(fbs.contains("User = 0"));
        assert!(fbs.contains("Admin = 1"));
        assert!(fbs.contains("role: Role = Admin;"));
    }

    #[test]
    fn generate_temporal_fields() {
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
        let fbs = FbsGenerator::generate(&schema, "Test").unwrap();
        assert!(fbs.contains("created: int64;"));
        assert!(fbs.contains("day: int32;"));
    }

    #[test]
    fn generate_list_fields() {
        let schema = parse(
            r#"
            model Doc {
                id   Int32 PRIMARY KEY
                tags List<Utf8>
            }
        "#,
        )
        .unwrap();
        let fbs = FbsGenerator::generate(&schema, "Test").unwrap();
        assert!(fbs.contains("tags: [string];"));
    }

    #[test]
    fn generate_create_input() {
        let schema = parse(
            r#"
            model User {
                id    Int32  PRIMARY KEY AUTOINCREMENT
                email Utf8
                name  Utf8?
                active Boolean DEFAULT true
            }
        "#,
        )
        .unwrap();
        let fbs = FbsGenerator::generate(&schema, "Test").unwrap();
        assert!(fbs.contains("table UserCreateInput {"));
        // email is required, no default
        assert!(fbs.contains("email: string (required);"));
        // active has default, so no (required)
        assert!(fbs.contains("active: bool;"));
        // id excluded from CreateInput (autoincrement + @id)
        // Extract just the CreateInput block to check
        let create_block = fbs.split("table UserCreateInput").nth(1).unwrap();
        let create_block = create_block.split('}').next().unwrap();
        assert!(!create_block.contains("id:"));
    }
}
