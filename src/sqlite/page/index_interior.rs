use super::page_header::PageHeader;


#[derive(Debug)]
pub struct IndexInteriorPage {


    pub page_number: i64,
    pub header:PageHeader,
    pub right_cell: u32,
    pub cell_pointers:Vec<u16>
}
