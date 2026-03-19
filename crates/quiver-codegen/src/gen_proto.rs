use crate::helpers::{has_default, is_auto_field, to_screaming_snake, to_snake};
use quiver_error::QuiverError;
use quiver_schema::Schema;
use quiver_schema::ast::*;

/// Generates a Protocol Buffers schema (`.proto`) from a Quiver schema.
///
/// Produces message definitions for models and enum definitions with
/// proto3 syntax.
pub struct ProtoGenerator;

impl ProtoGenerator {
    pub fn generate(schema: &Schema, package: &str) -> Result<String, QuiverError> {
        let mut out = String::new();

        out.push_str("syntax = \"proto3\";\n\n");
        out.push_str(&format!("package {package};\n\n"));

        // Enums
        for e in &schema.enums {
            gen_enum(&mut out, e);
        }

        // Model messages
        for m in &schema.models {
            gen_message(&mut out, m);
        }

        // Input messages
        for m in &schema.models {
            gen_create_input(&mut out, m);
            gen_update_input(&mut out, m);
            gen_where_unique_input(&mut out, m);
        }

        Ok(out)
    }
}

fn gen_enum(out: &mut String, e: &EnumDef) {
    let upper_name = to_screaming_snake(&e.name);
    out.push_str(&format!("enum {} {{\n", e.name));
    out.push_str(&format!("  {upper_name}_UNSPECIFIED = 0;\n"));
    for (i, v) in e.values.iter().enumerate() {
        out.push_str(&format!(
            "  {}_{} = {};\n",
            upper_name,
            to_screaming_snake(&v.name),
            i + 1
        ));
    }
    out.push_str("}\n\n");
}

fn gen_message(out: &mut String, m: &ModelDef) {
    out.push_str(&format!("message {} {{\n", m.name));
    let mut field_num = 1;
    for f in &m.fields {
        let proto_type = type_to_proto(&f.type_expr);
        out.push_str(&format!(
            "  {} {} = {};\n",
            proto_type,
            to_snake(&f.name),
            field_num
        ));
        field_num += 1;
    }
    out.push_str("}\n\n");
}

fn gen_create_input(out: &mut String, m: &ModelDef) {
    out.push_str(&format!("message {}CreateInput {{\n", m.name));
    let mut field_num = 1;
    for f in &m.fields {
        if is_auto_field(f) {
            continue;
        }
        let proto_type = if is_collection_type(&f.type_expr.base) {
            // repeated/map fields can't be optional in proto3
            base_type_to_proto(&f.type_expr.base)
        } else if has_default(f) || f.type_expr.nullable {
            format!("optional {}", base_type_to_proto(&f.type_expr.base))
        } else {
            type_to_proto(&f.type_expr)
        };
        out.push_str(&format!(
            "  {} {} = {};\n",
            proto_type,
            to_snake(&f.name),
            field_num
        ));
        field_num += 1;
    }
    out.push_str("}\n\n");
}

fn gen_update_input(out: &mut String, m: &ModelDef) {
    out.push_str(&format!("message {}UpdateInput {{\n", m.name));
    let mut field_num = 1;
    for f in &m.fields {
        if is_auto_field(f) {
            continue;
        }
        if is_collection_type(&f.type_expr.base) {
            // repeated/map fields can't be optional in proto3
            let base = base_type_to_proto(&f.type_expr.base);
            out.push_str(&format!(
                "  {} {} = {};\n",
                base,
                to_snake(&f.name),
                field_num
            ));
        } else {
            let base = base_type_to_proto(&f.type_expr.base);
            out.push_str(&format!(
                "  optional {} {} = {};\n",
                base,
                to_snake(&f.name),
                field_num
            ));
        }
        field_num += 1;
    }
    out.push_str("}\n\n");
}

fn gen_where_unique_input(out: &mut String, m: &ModelDef) {
    // Collect unique key fields (those with @id or @unique)
    let unique_fields: Vec<&FieldDef> = m
        .fields
        .iter()
        .filter(|f| {
            f.attributes
                .iter()
                .any(|a| matches!(a, FieldAttribute::Id | FieldAttribute::Unique))
        })
        .collect();

    if unique_fields.is_empty() {
        return;
    }

    out.push_str(&format!("message {}WhereUniqueInput {{\n", m.name));
    if unique_fields.len() == 1 {
        let f = unique_fields[0];
        let proto_type = base_type_to_proto(&f.type_expr.base);
        out.push_str(&format!("  {} {} = 1;\n", proto_type, to_snake(&f.name)));
    } else {
        out.push_str("  oneof key {\n");
        for (i, f) in unique_fields.iter().enumerate() {
            let proto_type = base_type_to_proto(&f.type_expr.base);
            out.push_str(&format!(
                "    {} {} = {};\n",
                proto_type,
                to_snake(&f.name),
                i + 1
            ));
        }
        out.push_str("  }\n");
    }
    out.push_str("}\n\n");
}

fn type_to_proto(type_expr: &TypeExpr) -> String {
    let base = base_type_to_proto(&type_expr.base);
    if type_expr.nullable {
        format!("optional {base}")
    } else {
        base
    }
}

fn base_type_to_proto(base: &BaseType) -> String {
    match base {
        BaseType::Int8 | BaseType::Int16 | BaseType::Int32 => "int32".into(),
        BaseType::Int64 => "int64".into(),
        BaseType::UInt8 | BaseType::UInt16 | BaseType::UInt32 => "uint32".into(),
        BaseType::UInt64 => "uint64".into(),
        BaseType::Float16 | BaseType::Float32 => "float".into(),
        BaseType::Float64 => "double".into(),
        BaseType::Decimal128 { .. } | BaseType::Decimal256 { .. } => "string".into(),
        BaseType::Utf8 | BaseType::LargeUtf8 => "string".into(),
        BaseType::Binary | BaseType::LargeBinary | BaseType::FixedSizeBinary { .. } => {
            "bytes".into()
        }
        BaseType::Boolean => "bool".into(),
        BaseType::Date32 | BaseType::Date64 => "int32".into(),
        BaseType::Time32 { .. } => "int32".into(),
        BaseType::Time64 { .. } => "int64".into(),
        BaseType::Timestamp { .. } => "int64".into(), // epoch micros
        BaseType::List(inner) | BaseType::LargeList(inner) => {
            format!("repeated {}", base_type_to_proto(&inner.base))
        }
        BaseType::Map { key, value } => {
            format!(
                "map<{}, {}>",
                base_type_to_proto(&key.base),
                base_type_to_proto(&value.base)
            )
        }
        BaseType::Struct(_) => "bytes".into(), // serialize as bytes for now
        BaseType::Named(name) => name.clone(),
    }
}

fn is_collection_type(base: &BaseType) -> bool {
    matches!(
        base,
        BaseType::List(_) | BaseType::LargeList(_) | BaseType::Map { .. }
    )
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
                active Boolean DEFAULT true
            }
        "#,
        )
        .unwrap();
        let proto = ProtoGenerator::generate(&schema, "quiver.models").unwrap();
        assert!(proto.contains("syntax = \"proto3\";"));
        assert!(proto.contains("package quiver.models;"));
        assert!(proto.contains("message User {"));
        assert!(proto.contains("string email = "));
        assert!(proto.contains("optional string name = "));
        assert!(proto.contains("bool active = "));
    }

    #[test]
    fn generate_enum() {
        let schema = parse(
            r#"
            enum Role { User Admin Moderator }
            model Account {
                id   Int32 PRIMARY KEY
                role Role
            }
        "#,
        )
        .unwrap();
        let proto = ProtoGenerator::generate(&schema, "test").unwrap();
        assert!(proto.contains("enum Role {"));
        assert!(proto.contains("ROLE_UNSPECIFIED = 0;"));
        assert!(proto.contains("ROLE_USER = 1;"));
        assert!(proto.contains("ROLE_ADMIN = 2;"));
        assert!(proto.contains("ROLE_MODERATOR = 3;"));
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
        let proto = ProtoGenerator::generate(&schema, "test").unwrap();
        assert!(proto.contains("message UserCreateInput {"));
        assert!(proto.contains("string email = 1;"));
        assert!(proto.contains("optional string name = 2;")); // nullable
        assert!(proto.contains("optional bool active = 3;")); // has default
    }

    #[test]
    fn generate_update_input() {
        let schema = parse(
            r#"
            model User {
                id    Int32 PRIMARY KEY
                email Utf8
                name  Utf8?
            }
        "#,
        )
        .unwrap();
        let proto = ProtoGenerator::generate(&schema, "test").unwrap();
        assert!(proto.contains("message UserUpdateInput {"));
        assert!(proto.contains("optional string email = 1;"));
        assert!(proto.contains("optional string name = 2;"));
    }

    #[test]
    fn generate_where_unique_input() {
        let schema = parse(
            r#"
            model User {
                id    Int32 PRIMARY KEY
                email Utf8  UNIQUE
                name  Utf8
            }
        "#,
        )
        .unwrap();
        let proto = ProtoGenerator::generate(&schema, "test").unwrap();
        assert!(proto.contains("message UserWhereUniqueInput {"));
        assert!(proto.contains("oneof key {"));
        assert!(proto.contains("int32 id = 1;"));
        assert!(proto.contains("string email = 2;"));
    }

    #[test]
    fn generate_map_field() {
        let schema = parse(
            r#"
            model Doc {
                id       Int32 PRIMARY KEY
                metadata Map<Utf8, Utf8>
            }
        "#,
        )
        .unwrap();
        let proto = ProtoGenerator::generate(&schema, "test").unwrap();
        assert!(proto.contains("map<string, string> metadata = "));
    }

    #[test]
    fn generate_list_field() {
        let schema = parse(
            r#"
            model Doc {
                id   Int32 PRIMARY KEY
                tags List<Utf8>
            }
        "#,
        )
        .unwrap();
        let proto = ProtoGenerator::generate(&schema, "test").unwrap();
        assert!(proto.contains("repeated string tags = "));
    }

    #[test]
    fn generate_temporal_fields() {
        let schema = parse(
            r#"
            model Event {
                id      Int32 PRIMARY KEY
                created Timestamp(Microsecond, UTC)
                day     Date32
                time    Time64(Nanosecond)
            }
        "#,
        )
        .unwrap();
        let proto = ProtoGenerator::generate(&schema, "test").unwrap();
        assert!(proto.contains("int64 created = "));
        assert!(proto.contains("int32 day = "));
        assert!(proto.contains("int64 time = "));
    }
}
