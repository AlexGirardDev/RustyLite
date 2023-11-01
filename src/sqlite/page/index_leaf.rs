use super::page_header::PageHeader;

#[derive(Debug)]
pub struct IndexLeafPage {
    pub page_number: i64,
    pub header:PageHeader,
    pub cell_pointers:Vec<u16>
}
