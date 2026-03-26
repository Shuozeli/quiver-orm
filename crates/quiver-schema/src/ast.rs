/// Top-level schema AST.
#[derive(Debug, Clone)]
pub struct Schema {
    pub config: Option<ConfigBlock>,
    pub generate: Option<GenerateBlock>,
    pub enums: Vec<EnumDef>,
    pub models: Vec<ModelDef>,
}

/// `config { provider "postgresql" ... }`
#[derive(Debug, Clone)]
pub struct ConfigBlock {
    pub entries: Vec<ConfigEntry>,
}

#[derive(Debug, Clone)]
pub struct ConfigEntry {
    pub key: String,
    pub value: String,
    pub span: Span,
}

/// `generate { flatbuffers "./generated/fb" ... }`
#[derive(Debug, Clone)]
pub struct GenerateBlock {
    pub entries: Vec<ConfigEntry>,
}

/// `enum Role { User Admin Moderator }`
#[derive(Debug, Clone)]
pub struct EnumDef {
    pub name: String,
    pub values: Vec<EnumValue>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct EnumValue {
    pub name: String,
    pub span: Span,
}

/// `model User { ... }`
#[derive(Debug, Clone)]
pub struct ModelDef {
    pub name: String,
    pub fields: Vec<FieldDef>,
    pub attributes: Vec<ModelAttribute>,
    pub span: Span,
}

impl ModelDef {
    /// Return the SQL table name for this model.
    ///
    /// If a `MAP` attribute is present, returns the mapped name.
    /// Otherwise returns the model name.
    pub fn table_name(&self) -> &str {
        for attr in &self.attributes {
            if let ModelAttribute::Map(name) = attr {
                return name;
            }
        }
        &self.name
    }
}

/// A single field in a model.
#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name: String,
    pub type_expr: TypeExpr,
    pub attributes: Vec<FieldAttribute>,
    pub span: Span,
}

impl FieldDef {
    /// Return the SQL column name for this field.
    ///
    /// If a `MAP` attribute is present, returns the mapped name.
    /// Otherwise returns the field name.
    pub fn column_name(&self) -> &str {
        for attr in &self.attributes {
            if let FieldAttribute::Map(name) = attr {
                return name;
            }
        }
        &self.name
    }
}

/// Type expression with nullability.
#[derive(Debug, Clone)]
pub struct TypeExpr {
    pub base: BaseType,
    pub nullable: bool,
    pub span: Span,
}

impl PartialEq for TypeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base && self.nullable == other.nullable
    }
}

/// The base type before nullability.
#[derive(Debug, Clone, PartialEq)]
pub enum BaseType {
    // Integers
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    // Floats
    Float16,
    Float32,
    Float64,
    // Decimal
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    // String
    Utf8,
    LargeUtf8,
    // Binary
    Binary,
    LargeBinary,
    FixedSizeBinary {
        size: i32,
    },
    // Boolean
    Boolean,
    // Temporal
    Date32,
    Date64,
    Time32 {
        unit: TimeUnit,
    },
    Time64 {
        unit: TimeUnit,
    },
    Timestamp {
        unit: TimeUnit,
        timezone: Option<String>,
    },
    // Nested
    List(Box<TypeExpr>),
    LargeList(Box<TypeExpr>),
    Map {
        key: Box<TypeExpr>,
        value: Box<TypeExpr>,
    },
    Struct(Vec<StructField>),
    // Named reference (enum or relation)
    Named(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructField {
    pub name: String,
    pub type_expr: TypeExpr,
}

/// Referential action for foreign key constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
    Cascade,
    Restrict,
    SetNull,
    SetDefault,
    NoAction,
}

impl std::fmt::Display for ReferentialAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cascade => write!(f, "CASCADE"),
            Self::Restrict => write!(f, "RESTRICT"),
            Self::SetNull => write!(f, "SET NULL"),
            Self::SetDefault => write!(f, "SET DEFAULT"),
            Self::NoAction => write!(f, "NO ACTION"),
        }
    }
}

/// Field-level constraint (inline after type expression).
#[derive(Debug, Clone, PartialEq)]
pub enum FieldAttribute {
    Id,
    Autoincrement,
    Unique,
    Default(DefaultValue),
    Map(String),
}

/// Default value expression.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Now,
    Uuid,
    Cuid,
    EmptyList,
    EmptyMap,
    EnumVariant(String),
}

/// Model-level constraint (table-level declarations in model body).
#[derive(Debug, Clone)]
pub enum ModelAttribute {
    Id(Vec<String>),
    Unique(Vec<String>),
    Index(Vec<String>),
    Map(String),
    ForeignKey {
        fields: Vec<String>,
        references_model: String,
        references_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
}

/// Source position for error reporting.
#[derive(Debug, Clone, Copy, Default)]
pub struct Span {
    pub line: usize,
    pub column: usize,
}

impl BaseType {
    /// Convert to `arrow_schema::DataType`.
    pub fn to_arrow_data_type(&self) -> arrow_schema::DataType {
        match self {
            Self::Int8 => arrow_schema::DataType::Int8,
            Self::Int16 => arrow_schema::DataType::Int16,
            Self::Int32 => arrow_schema::DataType::Int32,
            Self::Int64 => arrow_schema::DataType::Int64,
            Self::UInt8 => arrow_schema::DataType::UInt8,
            Self::UInt16 => arrow_schema::DataType::UInt16,
            Self::UInt32 => arrow_schema::DataType::UInt32,
            Self::UInt64 => arrow_schema::DataType::UInt64,
            Self::Float16 => arrow_schema::DataType::Float16,
            Self::Float32 => arrow_schema::DataType::Float32,
            Self::Float64 => arrow_schema::DataType::Float64,
            Self::Decimal128 { precision, scale } => {
                arrow_schema::DataType::Decimal128(*precision, *scale)
            }
            Self::Decimal256 { precision, scale } => {
                arrow_schema::DataType::Decimal256(*precision, *scale)
            }
            Self::Utf8 => arrow_schema::DataType::Utf8,
            Self::LargeUtf8 => arrow_schema::DataType::LargeUtf8,
            Self::Binary => arrow_schema::DataType::Binary,
            Self::LargeBinary => arrow_schema::DataType::LargeBinary,
            Self::FixedSizeBinary { size } => arrow_schema::DataType::FixedSizeBinary(*size),
            Self::Boolean => arrow_schema::DataType::Boolean,
            Self::Date32 => arrow_schema::DataType::Date32,
            Self::Date64 => arrow_schema::DataType::Date64,
            Self::Time32 { unit } => arrow_schema::DataType::Time32(unit.to_arrow()),
            Self::Time64 { unit } => arrow_schema::DataType::Time64(unit.to_arrow()),
            Self::Timestamp { unit, timezone } => arrow_schema::DataType::Timestamp(
                unit.to_arrow(),
                timezone.as_ref().map(|s| s.as_str().into()),
            ),
            Self::List(inner) => arrow_schema::DataType::List(
                arrow_schema::Field::new("item", inner.base.to_arrow_data_type(), inner.nullable)
                    .into(),
            ),
            Self::LargeList(inner) => arrow_schema::DataType::LargeList(
                arrow_schema::Field::new("item", inner.base.to_arrow_data_type(), inner.nullable)
                    .into(),
            ),
            Self::Map { key, value } => {
                let entries_field = arrow_schema::Field::new(
                    "entries",
                    arrow_schema::DataType::Struct(
                        vec![
                            arrow_schema::Field::new(
                                "key",
                                key.base.to_arrow_data_type(),
                                key.nullable,
                            ),
                            arrow_schema::Field::new(
                                "value",
                                value.base.to_arrow_data_type(),
                                value.nullable,
                            ),
                        ]
                        .into(),
                    ),
                    false,
                );
                arrow_schema::DataType::Map(entries_field.into(), false)
            }
            Self::Struct(fields) => {
                let arrow_fields: Vec<arrow_schema::Field> = fields
                    .iter()
                    .map(|f| {
                        arrow_schema::Field::new(
                            &f.name,
                            f.type_expr.base.to_arrow_data_type(),
                            f.type_expr.nullable,
                        )
                    })
                    .collect();
                arrow_schema::DataType::Struct(arrow_fields.into())
            }
            Self::Named(_) => arrow_schema::DataType::Utf8, // enums serialize as strings
        }
    }
}

impl TimeUnit {
    pub fn to_arrow(&self) -> arrow_schema::TimeUnit {
        match self {
            Self::Second => arrow_schema::TimeUnit::Second,
            Self::Millisecond => arrow_schema::TimeUnit::Millisecond,
            Self::Microsecond => arrow_schema::TimeUnit::Microsecond,
            Self::Nanosecond => arrow_schema::TimeUnit::Nanosecond,
        }
    }
}
