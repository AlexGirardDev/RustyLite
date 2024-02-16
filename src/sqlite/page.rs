use self::{
    index_interior::IndexInteriorPage, index_leaf::IndexLeafPage,
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

#[derive(Debug, Clone)]
pub enum TablePage {
    Leaf(TableLeafPage),
    Interior(TableInteriorPage),
}

impl TablePage {
    pub fn get_row_id(&self)->i64{
        match self{
            TablePage::Leaf(l) => l.row_id,
            TablePage::Interior(i) => i.row_id,
        }

    }
    pub fn page_number(&self)->u32{
        match self{
            TablePage::Leaf(l) => l.page_number,
            TablePage::Interior(i) => i.page_number,
        }

    }
    
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

#[derive(Debug, Clone)]
pub enum IndexPage {
    Leaf(IndexLeafPage),
    Interior(IndexInteriorPage),
}
