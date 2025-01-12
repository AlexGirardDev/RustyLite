use std::rc::Rc;

use crate::sqlite::column::Column;

#[derive(Debug)]
pub struct TableSchema {
    pub row_id: i64,
    pub name: Rc<str>,
    pub table_name: Rc<str>,
    pub root_page: u32,
    pub sql: String,
    pub columns: Vec<Rc<Column>>,
}
