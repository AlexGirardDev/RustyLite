use std::fmt;

use self::{table_schema::TableSchema, index_schema::IndexSchema};

pub mod index_schema;
pub mod table_schema;

pub enum SqliteSchema{
    Table(TableSchema),
    Index(IndexSchema)
}
