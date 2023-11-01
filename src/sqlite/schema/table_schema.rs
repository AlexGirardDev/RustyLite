use std::{collections::HashMap, rc::Rc};

use crate::sqlite::column::Column;

pub struct TableSchema{
    pub row_id: i64,
    pub name: Rc<str>,
    pub table_name: Rc<str>,
    pub root_page: i64,
    pub sql: String,
    pub columns: Vec<Rc<Column>>,
}
