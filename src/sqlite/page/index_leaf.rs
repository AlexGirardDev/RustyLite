use crate::sqlite::record::CellValue;

use super::page_header::PageHeader;

#[derive(Debug, Clone)]
pub struct IndexLeafPage {
    pub page_number: u32,
    pub header: PageHeader,
    pub value: CellValue,
    pub cell_pointers: Vec<(u32, u16)>,
}
