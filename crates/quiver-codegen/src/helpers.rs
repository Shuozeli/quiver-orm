use quiver_schema::ast::{BaseType, FieldAttribute, FieldDef};

pub fn is_auto_field(f: &FieldDef) -> bool {
    f.attributes
        .iter()
        .any(|a| matches!(a, FieldAttribute::Autoincrement | FieldAttribute::Id))
}

pub fn has_default(f: &FieldDef) -> bool {
    f.attributes
        .iter()
        .any(|a| matches!(a, FieldAttribute::Default(_)))
}

pub fn to_snake(name: &str) -> String {
    let mut result = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    result
}

pub fn to_screaming_snake(name: &str) -> String {
    to_snake(name).to_uppercase()
}

/// Returns true if the base type is a collection (List, LargeList, Map).
pub fn is_collection_base_type(base: &BaseType) -> bool {
    matches!(
        base,
        BaseType::List(_) | BaseType::LargeList(_) | BaseType::Map { .. }
    )
}
