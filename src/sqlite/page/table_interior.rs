use super::page_header::PageHeader;

#[derive(Debug)]
pub struct TableInteriorPage {
    pub page_number: i64,
    pub header:PageHeader,
    pub right_cell: u32,
    pub cell_pointers:Vec<u16>
}

#[derive(Debug)]
pub struct TableInteriorCell {
    pub page_number: i64,
    pub child_page_number: i64,
    pub row_id: i64,
}
