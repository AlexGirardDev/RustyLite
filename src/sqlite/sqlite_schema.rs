use std::fmt;

#[derive(Debug)]
pub struct SqliteSchema {
    pub row_id: u64,
    pub schema_type: SchemaType,
    pub name: String,
    pub table_name: String,
    pub root_page: i64,
    pub sql: String,
}

#[derive(Debug)]
pub enum SchemaType {
    Table,
    Index,
    View,
    Trigger,
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SchemaType::Table => write!(f, "Table"),
            SchemaType::Index => write!(f, "Table"),
            SchemaType::View => write!(f, "View"),
            SchemaType::Trigger => write!(f, "Trigger"),
        }
    }
}
