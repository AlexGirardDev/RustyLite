
#[derive(Debug)]
pub struct SqliteSchema {
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
