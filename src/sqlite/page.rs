use self::{
    index_interior::IndexInteriorPage, index_leaf::IndexLeafPage, page_header::PageHeader,
    table_interior::TableInteriorPage, table_leaf::TableLeafPage,
};

pub mod index_interior;
pub mod index_leaf;
pub mod page_header;
pub mod table_interior;
pub mod table_leaf;

#[derive(Debug)]
pub enum Page {
    Table(TablePage),
    Index(IndexPage),
}
#[derive(Debug)]
pub enum TablePage {
    Leaf(TableLeafPage),
    Interior(TableInteriorPage),
}

// #[derive(Debug)]
// pub struct Page2 {
//     pub page_number: u32,
//     pub header: PageHeader,
//     pub cell_pointers: Vec<u16>,
//     pub table_page: TablePage<'a>,
// }

//     Leaf(TableLeafPage),
//     Interior(TableInteriorPage),
// }

#[derive(Debug)]
pub enum IndexPage {
    Leaf(IndexLeafPage),
    Interior(IndexInteriorPage),
}
