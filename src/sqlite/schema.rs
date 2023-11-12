

use self::{table_schema::TableSchema, index_schema::IndexSchema};

pub mod index_schema;
pub mod table_schema;

#[derive(Debug)]
pub enum SqliteSchema{
    Table(TableSchema),
    Index(IndexSchema)
}
