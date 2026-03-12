use crate::ast::*;
use crate::lexer::{Lexer, SpannedToken, Token};
use quiver_error::QuiverError;

/// Parse and validate a `.quiver` schema string into a Schema AST.
///
/// Runs both syntactic parsing and semantic validation. Returns the first
/// validation error if any are found.
pub fn parse(source: &str) -> Result<Schema, QuiverError> {
    let tokens = Lexer::new(source).tokenize()?;
    let mut parser = Parser::new(tokens);
    let schema = parser.parse_schema()?;

    if let Err(errors) = crate::validate::validate(&schema) {
        return Err(errors.into_iter().next().unwrap());
    }

    Ok(schema)
}

/// Parse a `.quiver` schema string without semantic validation.
///
/// Use this when you need the raw AST without validation (e.g., for tooling
/// that wants to report all errors, not just the first one).
pub fn parse_unvalidated(source: &str) -> Result<Schema, QuiverError> {
    let tokens = Lexer::new(source).tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse_schema()
}

/// Maximum nesting depth for type expressions (e.g., `List<List<...>>`).
const MAX_TYPE_DEPTH: usize = 32;

struct Parser {
    tokens: Vec<SpannedToken>,
    pos: usize,
    type_depth: usize,
}

impl Parser {
    fn new(tokens: Vec<SpannedToken>) -> Self {
        Self {
            tokens,
            pos: 0,
            type_depth: 0,
        }
    }

    fn parse_schema(&mut self) -> Result<Schema, QuiverError> {
        let mut config = None;
        let mut generate = None;
        let mut enums = Vec::new();
        let mut models = Vec::new();

        while !self.at_eof() {
            match self.peek() {
                Token::Config => {
                    config = Some(self.parse_config_block()?);
                }
                Token::Generate => {
                    generate = Some(self.parse_generate_block()?);
                }
                Token::Enum => {
                    enums.push(self.parse_enum()?);
                }
                Token::Model => {
                    models.push(self.parse_model()?);
                }
                _ => {
                    return Err(self.error("expected 'config', 'generate', 'enum', or 'model'"));
                }
            }
        }

        Ok(Schema {
            config,
            generate,
            enums,
            models,
        })
    }

    // ---- Config / Generate blocks ----

    fn parse_config_block(&mut self) -> Result<ConfigBlock, QuiverError> {
        self.expect(Token::Config)?;
        self.expect(Token::LBrace)?;
        let mut entries = Vec::new();
        while !self.check(Token::RBrace) {
            entries.push(self.parse_config_entry()?);
        }
        self.expect(Token::RBrace)?;
        Ok(ConfigBlock { entries })
    }

    fn parse_generate_block(&mut self) -> Result<GenerateBlock, QuiverError> {
        self.expect(Token::Generate)?;
        self.expect(Token::LBrace)?;
        let mut entries = Vec::new();
        while !self.check(Token::RBrace) {
            entries.push(self.parse_config_entry()?);
        }
        self.expect(Token::RBrace)?;
        Ok(GenerateBlock { entries })
    }

    fn parse_config_entry(&mut self) -> Result<ConfigEntry, QuiverError> {
        let span = self.span();
        let key = self.expect_ident()?;
        let value = self.expect_string()?;
        Ok(ConfigEntry { key, value, span })
    }

    // ---- Enum ----

    fn parse_enum(&mut self) -> Result<EnumDef, QuiverError> {
        let span = self.span();
        self.expect(Token::Enum)?;
        let name = self.expect_ident()?;
        self.expect(Token::LBrace)?;
        let mut values = Vec::new();
        while !self.check(Token::RBrace) {
            let vspan = self.span();
            let vname = self.expect_ident()?;
            values.push(EnumValue {
                name: vname,
                span: vspan,
            });
        }
        self.expect(Token::RBrace)?;
        Ok(EnumDef { name, values, span })
    }

    // ---- Model ----

    fn parse_model(&mut self) -> Result<ModelDef, QuiverError> {
        let span = self.span();
        self.expect(Token::Model)?;
        let name = self.expect_ident()?;
        self.expect(Token::LBrace)?;

        let mut fields = Vec::new();
        let mut attributes = Vec::new();

        while !self.check(Token::RBrace) {
            if self.check(Token::AtAt) {
                attributes.push(self.parse_model_attribute()?);
            } else {
                fields.push(self.parse_field()?);
            }
        }
        self.expect(Token::RBrace)?;

        Ok(ModelDef {
            name,
            fields,
            attributes,
            span,
        })
    }

    fn parse_field(&mut self) -> Result<FieldDef, QuiverError> {
        let span = self.span();
        let name = self.expect_ident()?;
        let type_expr = self.parse_type_expr()?;
        let mut attributes = Vec::new();
        while self.check(Token::At) {
            attributes.push(self.parse_field_attribute()?);
        }
        Ok(FieldDef {
            name,
            type_expr,
            attributes,
            span,
        })
    }

    // ---- Type expressions ----

    fn parse_type_expr(&mut self) -> Result<TypeExpr, QuiverError> {
        self.type_depth += 1;
        if self.type_depth > MAX_TYPE_DEPTH {
            return Err(self.error("type nesting depth exceeded (max 32 levels)"));
        }
        let result = self.parse_type_expr_inner();
        self.type_depth -= 1;
        result
    }

    fn parse_type_expr_inner(&mut self) -> Result<TypeExpr, QuiverError> {
        let span = self.span();
        let base = self.parse_base_type()?;

        // Check for [] array sugar (List<T>)
        let base = if self.check(Token::LBracket) {
            self.advance();
            self.expect(Token::RBracket)?;
            BaseType::List(Box::new(TypeExpr {
                base,
                nullable: false,
                span,
            }))
        } else {
            base
        };

        // Check for ? nullable
        let nullable = if self.check(Token::Question) {
            self.advance();
            true
        } else {
            false
        };

        Ok(TypeExpr {
            base,
            nullable,
            span,
        })
    }

    fn parse_base_type(&mut self) -> Result<BaseType, QuiverError> {
        let name = self.expect_ident()?;
        match name.as_str() {
            // Integers
            "Int8" => Ok(BaseType::Int8),
            "Int16" => Ok(BaseType::Int16),
            "Int32" => Ok(BaseType::Int32),
            "Int64" => Ok(BaseType::Int64),
            "UInt8" => Ok(BaseType::UInt8),
            "UInt16" => Ok(BaseType::UInt16),
            "UInt32" => Ok(BaseType::UInt32),
            "UInt64" => Ok(BaseType::UInt64),
            // Floats
            "Float16" => Ok(BaseType::Float16),
            "Float32" => Ok(BaseType::Float32),
            "Float64" => Ok(BaseType::Float64),
            // Decimal
            "Decimal128" => {
                let (p, s) = self.parse_decimal_params()?;
                Ok(BaseType::Decimal128 {
                    precision: p,
                    scale: s,
                })
            }
            "Decimal256" => {
                let (p, s) = self.parse_decimal_params()?;
                Ok(BaseType::Decimal256 {
                    precision: p,
                    scale: s,
                })
            }
            // String
            "Utf8" => Ok(BaseType::Utf8),
            "LargeUtf8" => Ok(BaseType::LargeUtf8),
            // Binary
            "Binary" => Ok(BaseType::Binary),
            "LargeBinary" => Ok(BaseType::LargeBinary),
            "FixedSizeBinary" => {
                self.expect(Token::LParen)?;
                let size = self.expect_int()? as i32;
                self.expect(Token::RParen)?;
                Ok(BaseType::FixedSizeBinary { size })
            }
            // Boolean
            "Boolean" => Ok(BaseType::Boolean),
            // Temporal
            "Date32" => Ok(BaseType::Date32),
            "Date64" => Ok(BaseType::Date64),
            "Time32" => {
                self.expect(Token::LParen)?;
                let unit = self.parse_time_unit()?;
                self.expect(Token::RParen)?;
                Ok(BaseType::Time32 { unit })
            }
            "Time64" => {
                self.expect(Token::LParen)?;
                let unit = self.parse_time_unit()?;
                self.expect(Token::RParen)?;
                Ok(BaseType::Time64 { unit })
            }
            "Timestamp" => {
                self.expect(Token::LParen)?;
                let unit = self.parse_time_unit()?;
                let timezone = if self.check(Token::Comma) {
                    self.advance();
                    // Accept both bare ident (UTC) and string ("America/New_York")
                    if self.check_string() {
                        Some(self.expect_string()?)
                    } else {
                        Some(self.expect_ident()?)
                    }
                } else {
                    None
                };
                self.expect(Token::RParen)?;
                Ok(BaseType::Timestamp { unit, timezone })
            }
            // Nested
            "List" => {
                self.expect(Token::LAngle)?;
                let inner = self.parse_type_expr()?;
                self.expect(Token::RAngle)?;
                Ok(BaseType::List(Box::new(inner)))
            }
            "LargeList" => {
                self.expect(Token::LAngle)?;
                let inner = self.parse_type_expr()?;
                self.expect(Token::RAngle)?;
                Ok(BaseType::LargeList(Box::new(inner)))
            }
            "Map" => {
                self.expect(Token::LAngle)?;
                let key = self.parse_type_expr()?;
                self.expect(Token::Comma)?;
                let value = self.parse_type_expr()?;
                self.expect(Token::RAngle)?;
                Ok(BaseType::Map {
                    key: Box::new(key),
                    value: Box::new(value),
                })
            }
            "Struct" => {
                self.expect(Token::LAngle)?;
                self.expect(Token::LBrace)?;
                let mut fields = Vec::new();
                while !self.check(Token::RBrace) {
                    let fname = self.expect_ident()?;
                    let ftype = self.parse_type_expr()?;
                    fields.push(StructField {
                        name: fname,
                        type_expr: ftype,
                    });
                    if !self.check(Token::RBrace) {
                        self.expect(Token::Comma)?;
                    }
                }
                self.expect(Token::RBrace)?;
                self.expect(Token::RAngle)?;
                Ok(BaseType::Struct(fields))
            }
            // Named reference (enum or relation model)
            _ => Ok(BaseType::Named(name)),
        }
    }

    fn parse_decimal_params(&mut self) -> Result<(u8, i8), QuiverError> {
        self.expect(Token::LParen)?;
        let p = self.expect_int()? as u8;
        self.expect(Token::Comma)?;
        let s = self.expect_int()? as i8;
        self.expect(Token::RParen)?;
        Ok((p, s))
    }

    fn parse_time_unit(&mut self) -> Result<TimeUnit, QuiverError> {
        let name = self.expect_ident()?;
        match name.as_str() {
            "Second" => Ok(TimeUnit::Second),
            "Millisecond" => Ok(TimeUnit::Millisecond),
            "Microsecond" => Ok(TimeUnit::Microsecond),
            "Nanosecond" => Ok(TimeUnit::Nanosecond),
            _ => Err(self.error_at_prev(&format!(
                "expected time unit (Second, Millisecond, Microsecond, Nanosecond), got '{name}'"
            ))),
        }
    }

    // ---- Attributes ----

    fn parse_field_attribute(&mut self) -> Result<FieldAttribute, QuiverError> {
        self.expect(Token::At)?;
        let name = self.expect_ident()?;
        match name.as_str() {
            "id" => Ok(FieldAttribute::Id),
            "autoincrement" => Ok(FieldAttribute::Autoincrement),
            "unique" => Ok(FieldAttribute::Unique),
            "updatedAt" => Ok(FieldAttribute::UpdatedAt),
            "ignore" => Ok(FieldAttribute::Ignore),
            "default" => {
                self.expect(Token::LParen)?;
                let val = self.parse_default_value()?;
                self.expect(Token::RParen)?;
                Ok(FieldAttribute::Default(val))
            }
            "map" => {
                self.expect(Token::LParen)?;
                let s = self.expect_string()?;
                self.expect(Token::RParen)?;
                Ok(FieldAttribute::Map(s))
            }
            "relation" => {
                if self.check(Token::LParen) {
                    self.advance();
                    let mut fields = Vec::new();
                    let mut references = Vec::new();
                    let mut on_delete = None;
                    let mut on_update = None;
                    while !self.check(Token::RParen) {
                        let key = self.expect_ident()?;
                        self.expect(Token::Colon)?;
                        match key.as_str() {
                            "fields" => fields = self.parse_ident_list()?,
                            "references" => references = self.parse_ident_list()?,
                            "onDelete" => on_delete = Some(self.parse_referential_action()?),
                            "onUpdate" => on_update = Some(self.parse_referential_action()?),
                            _ => {
                                return Err(self.error_at_prev(&format!(
                                    "unknown relation key '{key}', expected 'fields', 'references', 'onDelete', or 'onUpdate'"
                                )));
                            }
                        }
                        if !self.check(Token::RParen) {
                            self.expect(Token::Comma)?;
                        }
                    }
                    self.expect(Token::RParen)?;
                    Ok(FieldAttribute::Relation {
                        fields,
                        references,
                        on_delete,
                        on_update,
                    })
                } else {
                    // Bare @relation (back-relation side, no fields/references)
                    Ok(FieldAttribute::Relation {
                        fields: Vec::new(),
                        references: Vec::new(),
                        on_delete: None,
                        on_update: None,
                    })
                }
            }
            _ => Err(self.error_at_prev(&format!("unknown attribute '@{name}'"))),
        }
    }

    fn parse_default_value(&mut self) -> Result<DefaultValue, QuiverError> {
        match self.peek() {
            Token::IntLit(_) => {
                let v = self.expect_int()?;
                Ok(DefaultValue::Int(v))
            }
            Token::FloatLit(_) => {
                if let Token::FloatLit(v) = self.peek() {
                    self.advance();
                    Ok(DefaultValue::Float(v))
                } else {
                    Err(self.error("expected float literal"))
                }
            }
            Token::StringLit(_) => {
                let s = self.expect_string()?;
                Ok(DefaultValue::String(s))
            }
            Token::True => {
                self.advance();
                Ok(DefaultValue::Bool(true))
            }
            Token::False => {
                self.advance();
                Ok(DefaultValue::Bool(false))
            }
            Token::LBracket => {
                self.advance();
                self.expect(Token::RBracket)?;
                Ok(DefaultValue::EmptyList)
            }
            Token::LBrace => {
                self.advance();
                self.expect(Token::RBrace)?;
                Ok(DefaultValue::EmptyMap)
            }
            Token::Ident(ref name) => {
                let name = name.clone();
                self.advance();
                // Check for function call: now(), uuid(), cuid()
                if self.check(Token::LParen) {
                    self.advance();
                    self.expect(Token::RParen)?;
                    match name.as_str() {
                        "now" => Ok(DefaultValue::Now),
                        "uuid" => Ok(DefaultValue::Uuid),
                        "cuid" => Ok(DefaultValue::Cuid),
                        _ => {
                            Err(self.error_at_prev(&format!("unknown default function '{name}()'")))
                        }
                    }
                } else {
                    // Bare identifier = enum variant
                    Ok(DefaultValue::EnumVariant(name))
                }
            }
            _ => Err(self.error("expected default value")),
        }
    }

    fn parse_model_attribute(&mut self) -> Result<ModelAttribute, QuiverError> {
        self.expect(Token::AtAt)?;
        let name = self.expect_ident()?;
        match name.as_str() {
            "id" => {
                self.expect(Token::LParen)?;
                let fields = self.parse_ident_list()?;
                self.expect(Token::RParen)?;
                Ok(ModelAttribute::Id(fields))
            }
            "unique" => {
                self.expect(Token::LParen)?;
                let fields = self.parse_ident_list()?;
                self.expect(Token::RParen)?;
                Ok(ModelAttribute::Unique(fields))
            }
            "index" => {
                self.expect(Token::LParen)?;
                let fields = self.parse_ident_list()?;
                self.expect(Token::RParen)?;
                Ok(ModelAttribute::Index(fields))
            }
            "map" => {
                self.expect(Token::LParen)?;
                let s = self.expect_string()?;
                self.expect(Token::RParen)?;
                Ok(ModelAttribute::Map(s))
            }
            _ => Err(self.error_at_prev(&format!("unknown model attribute '@@{name}'"))),
        }
    }

    fn parse_referential_action(&mut self) -> Result<ReferentialAction, QuiverError> {
        let action = self.expect_ident()?;
        match action.as_str() {
            "Cascade" => Ok(ReferentialAction::Cascade),
            "Restrict" => Ok(ReferentialAction::Restrict),
            "SetNull" => Ok(ReferentialAction::SetNull),
            "SetDefault" => Ok(ReferentialAction::SetDefault),
            "NoAction" => Ok(ReferentialAction::NoAction),
            _ => Err(self.error_at_prev(&format!(
                "unknown referential action '{action}', expected Cascade, Restrict, SetNull, SetDefault, or NoAction"
            ))),
        }
    }

    fn parse_ident_list(&mut self) -> Result<Vec<String>, QuiverError> {
        self.expect(Token::LBracket)?;
        let mut items = Vec::new();
        while !self.check(Token::RBracket) {
            items.push(self.expect_ident()?);
            if !self.check(Token::RBracket) {
                self.expect(Token::Comma)?;
            }
        }
        self.expect(Token::RBracket)?;
        Ok(items)
    }

    // ---- Token helpers ----

    fn peek(&self) -> Token {
        self.tokens
            .get(self.pos)
            .map(|t| t.token.clone())
            .unwrap_or(Token::Eof)
    }

    fn span(&self) -> Span {
        self.tokens
            .get(self.pos)
            .map(|t| t.span)
            .unwrap_or_default()
    }

    fn at_eof(&self) -> bool {
        self.peek() == Token::Eof
    }

    fn check(&self, expected: Token) -> bool {
        std::mem::discriminant(&self.peek()) == std::mem::discriminant(&expected)
    }

    fn check_string(&self) -> bool {
        matches!(self.peek(), Token::StringLit(_))
    }

    fn advance(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }

    fn expect(&mut self, expected: Token) -> Result<(), QuiverError> {
        if self.check(expected.clone()) {
            self.advance();
            Ok(())
        } else {
            Err(self.error(&format!("expected {expected:?}, got {:?}", self.peek())))
        }
    }

    fn expect_ident(&mut self) -> Result<String, QuiverError> {
        match self.peek() {
            Token::Ident(name) => {
                self.advance();
                Ok(name)
            }
            other => Err(self.error(&format!("expected identifier, got {other:?}"))),
        }
    }

    fn expect_string(&mut self) -> Result<String, QuiverError> {
        match self.peek() {
            Token::StringLit(s) => {
                self.advance();
                Ok(s)
            }
            other => Err(self.error(&format!("expected string literal, got {other:?}"))),
        }
    }

    fn expect_int(&mut self) -> Result<i64, QuiverError> {
        match self.peek() {
            Token::IntLit(v) => {
                self.advance();
                Ok(v)
            }
            other => Err(self.error(&format!("expected integer literal, got {other:?}"))),
        }
    }

    fn error(&self, message: &str) -> QuiverError {
        let span = self.span();
        QuiverError::Parse {
            line: span.line,
            column: span.column,
            message: message.to_string(),
        }
    }

    fn error_at_prev(&self, message: &str) -> QuiverError {
        let span = if self.pos > 0 {
            self.tokens[self.pos - 1].span
        } else {
            self.span()
        };
        QuiverError::Parse {
            line: span.line,
            column: span.column,
            message: message.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_schema() {
        let schema = parse("").unwrap();
        assert!(schema.config.is_none());
        assert!(schema.enums.is_empty());
        assert!(schema.models.is_empty());
    }

    #[test]
    fn parse_config_block() {
        let schema = parse(
            r#"
            config {
                provider "postgresql"
                database "myapp"
            }
        "#,
        )
        .unwrap();
        let config = schema.config.unwrap();
        assert_eq!(config.entries.len(), 2);
        assert_eq!(config.entries[0].key, "provider");
        assert_eq!(config.entries[0].value, "postgresql");
        assert_eq!(config.entries[1].key, "database");
        assert_eq!(config.entries[1].value, "myapp");
    }

    #[test]
    fn parse_generate_block() {
        let schema = parse(
            r#"
            generate {
                flatbuffers "./generated/fb"
                protobuf "./generated/proto"
            }
        "#,
        )
        .unwrap();
        let generate = schema.generate.unwrap();
        assert_eq!(generate.entries.len(), 2);
        assert_eq!(generate.entries[0].key, "flatbuffers");
    }

    #[test]
    fn parse_enum() {
        let schema = parse("enum Role { User Admin Moderator }").unwrap();
        assert_eq!(schema.enums.len(), 1);
        assert_eq!(schema.enums[0].name, "Role");
        assert_eq!(schema.enums[0].values.len(), 3);
        assert_eq!(schema.enums[0].values[0].name, "User");
        assert_eq!(schema.enums[0].values[1].name, "Admin");
        assert_eq!(schema.enums[0].values[2].name, "Moderator");
    }

    #[test]
    fn parse_simple_model() {
        let schema = parse(
            r#"
            model User {
                id    Int32  @id @autoincrement
                email Utf8   @unique
                name  Utf8?
                age   Int16?
                active Boolean @default(true)
            }
        "#,
        )
        .unwrap();
        assert_eq!(schema.models.len(), 1);
        let model = &schema.models[0];
        assert_eq!(model.name, "User");
        assert_eq!(model.fields.len(), 5);

        // id field
        assert_eq!(model.fields[0].name, "id");
        assert!(!model.fields[0].type_expr.nullable);
        assert!(matches!(model.fields[0].type_expr.base, BaseType::Int32));
        assert_eq!(model.fields[0].attributes.len(), 2);
        assert!(matches!(model.fields[0].attributes[0], FieldAttribute::Id));
        assert!(matches!(
            model.fields[0].attributes[1],
            FieldAttribute::Autoincrement
        ));

        // name field (nullable)
        assert_eq!(model.fields[2].name, "name");
        assert!(model.fields[2].type_expr.nullable);
        assert!(matches!(model.fields[2].type_expr.base, BaseType::Utf8));

        // active field (default)
        assert!(matches!(
            model.fields[4].attributes[0],
            FieldAttribute::Default(DefaultValue::Bool(true))
        ));
    }

    #[test]
    fn parse_all_scalar_types() {
        let schema = parse(
            r#"
            model AllTypes {
                a Int8
                b Int16
                c Int32
                d Int64
                e UInt8
                f UInt16
                g UInt32
                h UInt64
                i Float16
                j Float32
                k Float64
                l Decimal128(10, 2)
                m Decimal256(38, 18)
                n Utf8
                o LargeUtf8
                p Binary
                q LargeBinary
                r FixedSizeBinary(16)
                s Boolean
                t Date32
                u Date64
            }
        "#,
        )
        .unwrap();
        let model = &schema.models[0];
        assert_eq!(model.fields.len(), 21);
        assert!(matches!(model.fields[0].type_expr.base, BaseType::Int8));
        assert!(matches!(
            model.fields[11].type_expr.base,
            BaseType::Decimal128 {
                precision: 10,
                scale: 2
            }
        ));
        assert!(matches!(
            model.fields[17].type_expr.base,
            BaseType::FixedSizeBinary { size: 16 }
        ));
    }

    #[test]
    fn parse_temporal_types() {
        let schema = parse(
            r#"
            model Events {
                a Time32(Second)
                b Time32(Millisecond)
                c Time64(Microsecond)
                d Time64(Nanosecond)
                e Timestamp(Microsecond, UTC)
                f Timestamp(Nanosecond, "America/New_York")
                g Timestamp(Second)
                h Date32
            }
        "#,
        )
        .unwrap();
        let model = &schema.models[0];
        assert_eq!(model.fields.len(), 8);
        assert!(matches!(
            model.fields[0].type_expr.base,
            BaseType::Time32 {
                unit: TimeUnit::Second
            }
        ));
        assert!(matches!(
            model.fields[4].type_expr.base,
            BaseType::Timestamp {
                unit: TimeUnit::Microsecond,
                ..
            }
        ));
        if let BaseType::Timestamp { timezone, .. } = &model.fields[4].type_expr.base {
            assert_eq!(timezone.as_deref(), Some("UTC"));
        }
        if let BaseType::Timestamp { timezone, .. } = &model.fields[5].type_expr.base {
            assert_eq!(timezone.as_deref(), Some("America/New_York"));
        }
        if let BaseType::Timestamp { timezone, .. } = &model.fields[6].type_expr.base {
            assert!(timezone.is_none());
        }
    }

    #[test]
    fn parse_nested_types() {
        let schema = parse(
            r#"
            model Nested {
                tags     List<Utf8>
                scores   List<Float32>
                matrix   List<List<Int32>>
                metadata Map<Utf8, Utf8>
                coords   Struct<{ lat Float64, lng Float64 }>
                aliases  Utf8[]
                optList  List<Utf8>?
            }
        "#,
        )
        .unwrap();
        let model = &schema.models[0];
        assert_eq!(model.fields.len(), 7);

        // List<Utf8>
        assert!(matches!(model.fields[0].type_expr.base, BaseType::List(_)));

        // List<List<Int32>> (nested)
        if let BaseType::List(inner) = &model.fields[2].type_expr.base {
            assert!(matches!(inner.base, BaseType::List(_)));
        } else {
            panic!("expected List");
        }

        // Map<Utf8, Utf8>
        assert!(matches!(
            model.fields[3].type_expr.base,
            BaseType::Map { .. }
        ));

        // Struct<{ lat Float64, lng Float64 }>
        if let BaseType::Struct(fields) = &model.fields[4].type_expr.base {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name, "lat");
            assert_eq!(fields[1].name, "lng");
        } else {
            panic!("expected Struct");
        }

        // Utf8[] sugar -> List<Utf8>
        assert!(matches!(model.fields[5].type_expr.base, BaseType::List(_)));

        // List<Utf8>? (nullable list)
        assert!(model.fields[6].type_expr.nullable);
        assert!(matches!(model.fields[6].type_expr.base, BaseType::List(_)));
    }

    #[test]
    fn parse_relations() {
        let schema = parse(
            r#"
            model User {
                id    Int32   @id @autoincrement
                posts Post[]  @relation
            }

            model Post {
                id       Int32  @id @autoincrement
                authorId Int32
                author   User   @relation(fields: [authorId], references: [id])

                @@index([authorId])
            }
        "#,
        )
        .unwrap();
        assert_eq!(schema.models.len(), 2);

        // User.posts is Post[] with @relation
        let user = &schema.models[0];
        assert_eq!(user.fields[1].name, "posts");
        if let BaseType::List(inner) = &user.fields[1].type_expr.base {
            assert!(matches!(inner.base, BaseType::Named(ref n) if n == "Post"));
        } else {
            panic!("expected List<Post>");
        }

        // Post.author @relation(fields: [authorId], references: [id])
        let post = &schema.models[1];
        let author_field = &post.fields[2];
        assert_eq!(author_field.name, "author");
        if let FieldAttribute::Relation {
            fields, references, ..
        } = &author_field.attributes[0]
        {
            assert_eq!(fields, &["authorId"]);
            assert_eq!(references, &["id"]);
        } else {
            panic!("expected @relation");
        }

        // @@index([authorId])
        assert_eq!(post.attributes.len(), 1);
        if let ModelAttribute::Index(fields) = &post.attributes[0] {
            assert_eq!(fields, &["authorId"]);
        } else {
            panic!("expected @@index");
        }
    }

    #[test]
    fn parse_defaults() {
        let schema = parse(
            r#"
            model Defaults {
                a Int32    @default(0)
                b Float64  @default(3.14)
                c Utf8     @default("hello")
                d Boolean  @default(false)
                e Timestamp(Microsecond, UTC) @default(now())
                f Utf8     @default(uuid())
                g List<Utf8> @default([])
                h Map<Utf8, Utf8> @default({})
            }
        "#,
        )
        .unwrap();
        let model = &schema.models[0];
        assert!(matches!(
            model.fields[0].attributes[0],
            FieldAttribute::Default(DefaultValue::Int(0))
        ));
        assert!(matches!(
            model.fields[1].attributes[0],
            FieldAttribute::Default(DefaultValue::Float(_))
        ));
        assert!(matches!(
            model.fields[4].attributes[0],
            FieldAttribute::Default(DefaultValue::Now)
        ));
        assert!(matches!(
            model.fields[5].attributes[0],
            FieldAttribute::Default(DefaultValue::Uuid)
        ));
        assert!(matches!(
            model.fields[6].attributes[0],
            FieldAttribute::Default(DefaultValue::EmptyList)
        ));
        assert!(matches!(
            model.fields[7].attributes[0],
            FieldAttribute::Default(DefaultValue::EmptyMap)
        ));
    }

    #[test]
    fn parse_model_attributes() {
        let schema = parse(
            r#"
            model Composite {
                a Int32
                b Int32
                c Utf8

                @@id([a, b])
                @@unique([b, c])
                @@index([c])
                @@map("composites")
            }
        "#,
        )
        .unwrap();
        let model = &schema.models[0];
        assert_eq!(model.attributes.len(), 4);
        assert!(matches!(&model.attributes[0], ModelAttribute::Id(f) if f == &["a", "b"]));
        assert!(matches!(&model.attributes[1], ModelAttribute::Unique(f) if f == &["b", "c"]));
        assert!(matches!(&model.attributes[2], ModelAttribute::Index(f) if f == &["c"]));
        assert!(matches!(&model.attributes[3], ModelAttribute::Map(s) if s == "composites"));
    }

    #[test]
    fn parse_map_attribute() {
        let schema = parse(
            r#"
            model User {
                id Int32 @id @map("user_id")
            }
        "#,
        )
        .unwrap();
        assert!(matches!(
            &schema.models[0].fields[0].attributes[1],
            FieldAttribute::Map(s) if s == "user_id"
        ));
    }

    #[test]
    fn parse_enum_default() {
        let schema = parse(
            r#"
            enum Role { User Admin }
            model Account {
                role Role @default(User)
            }
        "#,
        )
        .unwrap();
        assert!(matches!(
            &schema.models[0].fields[0].attributes[0],
            FieldAttribute::Default(DefaultValue::EnumVariant(s)) if s == "User"
        ));
    }

    #[test]
    fn parse_full_schema() {
        let schema = parse(
            r#"
            config {
                provider "postgresql"
                database "myapp"
            }

            generate {
                flatbuffers "./generated/fb"
                protobuf "./generated/proto"
                rust "./generated/rs"
            }

            enum Role {
                User
                Admin
                Moderator
            }

            model User {
                id       Int32                         @id @autoincrement
                email    Utf8                          @unique
                name     Utf8?
                balance  Decimal128(10, 2)             @default(0)
                active   Boolean                      @default(true)
                created  Timestamp(Microsecond, UTC)   @default(now())
                tags     List<Utf8>                    @default([])
                role     Role                          @default(User)

                posts    Post[]                        @relation
                profile  Profile?                      @relation

                @@index([email])
                @@map("users")
            }

            model Post {
                id        Int32    @id @autoincrement
                title     Utf8
                content   LargeUtf8?
                published Boolean  @default(false)
                authorId  Int32
                author    User     @relation(fields: [authorId], references: [id])

                @@index([authorId])
            }

            model Profile {
                id     Int32      @id @autoincrement
                bio    LargeUtf8?
                userId Int32      @unique
                user   User       @relation(fields: [userId], references: [id])
            }
        "#,
        )
        .unwrap();

        assert!(schema.config.is_some());
        assert!(schema.generate.is_some());
        assert_eq!(schema.enums.len(), 1);
        assert_eq!(schema.models.len(), 3);
        assert_eq!(schema.models[0].name, "User");
        assert_eq!(schema.models[0].fields.len(), 10);
        assert_eq!(schema.models[0].attributes.len(), 2);
        assert_eq!(schema.models[1].name, "Post");
        assert_eq!(schema.models[2].name, "Profile");
    }

    #[test]
    fn arrow_type_mapping() {
        let schema = parse(
            r#"
            model Types {
                a Int32
                b Timestamp(Microsecond, UTC)
                c List<Utf8>
                d Map<Utf8, Int32>
            }
        "#,
        )
        .unwrap();
        let model = &schema.models[0];

        let a_dt = model.fields[0].type_expr.base.to_arrow_data_type();
        assert_eq!(a_dt, arrow_schema::DataType::Int32);

        let b_dt = model.fields[1].type_expr.base.to_arrow_data_type();
        assert!(matches!(
            b_dt,
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _)
        ));

        let c_dt = model.fields[2].type_expr.base.to_arrow_data_type();
        assert!(matches!(c_dt, arrow_schema::DataType::List(_)));

        let d_dt = model.fields[3].type_expr.base.to_arrow_data_type();
        assert!(matches!(d_dt, arrow_schema::DataType::Map(_, _)));
    }
}
