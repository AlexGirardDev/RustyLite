use crate::sqlite::page::Page;

#[derive(Debug)]
pub struct BTree {
    pub root: Page,
}
