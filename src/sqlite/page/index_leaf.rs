use super::page_header::PageHeader;

#[derive(Debug)]
pub struct IndexLeafPage {
    pub page_number: u32,
    pub header:PageHeader,
    pub cell_pointers:Vec<u16>
}
