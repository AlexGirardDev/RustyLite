use super::page_header::PageHeader;

#[derive(Debug, Clone)]
pub struct TableLeafPage {
    pub page_number: u32,
    pub header: PageHeader,
    pub cell_pointers: Vec<(u32, u16)>,
    // pub cells: Vec<TableLeafCell>,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    // pub row_id: i64,
    pub page_number: u32,
    pub offset: u16,
    // pub payload_size: i64,
    // pub record_header: RecordHeader,
}

impl Iterator for TableLeafCell {
    type Item = TableLeafCell;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
