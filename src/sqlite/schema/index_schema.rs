use std::rc::Rc;

pub struct IndexSchema{
    pub row_id: i64,
    pub name: Rc<str>,
    pub table_name: Rc<str>,
    pub root_page: i64,
    pub sql: String,
}
