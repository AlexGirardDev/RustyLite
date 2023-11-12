use std::rc::Rc;

#[derive(Debug)]
pub struct IndexSchema{
    pub row_id: i64,
    pub name: Rc<str>,
    pub table_name: Rc<str>,
    pub root_page: u32,
    pub sql: String,
}
