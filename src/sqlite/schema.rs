use std::rc::Rc;

use self::{index_schema::IndexSchema, table_schema::TableSchema};

pub mod index_schema;
pub mod table_schema;

#[derive(Debug)]
pub enum SqliteSchema {
    Table(TableSchema),
    Index(IndexSchema),
}

impl SqliteSchema {
    pub fn get_name(&self) -> Rc<str> {
        match self {
            SqliteSchema::Table(t) => t.name.clone(),
            SqliteSchema::Index(i) => i.name.clone()
        }
    }
}
