use sqlparser::ast::DataType;

#[derive(Debug,Clone)]
pub struct Column {
    pub type_affinity: TypeAffinity,
    pub column_index: i64,
}

#[derive(Debug,Clone)]
pub enum TypeAffinity {
    Text,
    Numeric,
    Int,
    Real,
    Blob,
}

impl From<&DataType> for TypeAffinity {
    fn from(value: &DataType) -> Self {
        //https://www.sqlite.org/datatype3.html#type_affinity
        match value {
            //1. If the declared type contains the string "INT" then it is assigned INTEGER affinity.
            DataType::TinyInt(_)
            | DataType::UnsignedTinyInt(_)
            | DataType::Int2(_)
            | DataType::UnsignedInt2(_)
            | DataType::SmallInt(_)
            | DataType::UnsignedSmallInt(_)
            | DataType::MediumInt(_)
            | DataType::UnsignedMediumInt(_)
            | DataType::Int(_)
            | DataType::Int4(_)
            | DataType::Integer(_)
            | DataType::UnsignedInt(_)
            | DataType::UnsignedInt4(_)
            | DataType::UnsignedInteger(_)
            | DataType::BigInt(_)
            | DataType::UnsignedBigInt(_)
            | DataType::Int8(_)
            | DataType::UnsignedInt8(_)
            | DataType::Interval => TypeAffinity::Int,

            //2.If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity.
            //Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
            DataType::Character(_)
            | DataType::Char(_)
            | DataType::CharacterVarying(_)
            | DataType::CharVarying(_)
            | DataType::Varchar(_)
            | DataType::Nvarchar(_)
            | DataType::Text
            | DataType::Clob(_) => TypeAffinity::Text,

            //3. //If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
            DataType::Blob(_) => TypeAffinity::Blob,

            //4. If the declared type for a column contains any of the strings
            //"REAL", "FLOA", or "DOUB" then the column has REAL affinity.
            DataType::Double
            | DataType::DoublePrecision
            | DataType::Float(_)
            | DataType::Float4
            | DataType::Float8
            | DataType::Real => TypeAffinity::Real,

            //5. Otherwise, the affinity is NUMERIC.
            DataType::Numeric(_)
            | DataType::Decimal(_)
            | DataType::BigNumeric(_)
            | DataType::BigDecimal(_)
            | DataType::Dec(_)
            | DataType::Bool
            | DataType::Boolean
            | DataType::Date
            | DataType::Time(_, _)
            | DataType::Datetime(_)
            | DataType::Timestamp(_, _)
            | DataType::JSON
            | DataType::Regclass
            | DataType::String
            | DataType::Bytea
            | DataType::Custom(_, _)
            | DataType::Array(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Uuid
            | DataType::CharacterLargeObject(_)
            | DataType::CharLargeObject(_)
            | DataType::Binary(_)
            | DataType::Varbinary(_) => TypeAffinity::Numeric,
        }
    }
}
