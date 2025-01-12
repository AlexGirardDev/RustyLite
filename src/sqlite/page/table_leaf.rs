use super::page_header::PageHeader;

#[derive(Debug, Clone)]
pub struct TableLeafPage {
    pub page_number: u32,
    pub header: PageHeader,
    pub row_id: i64,
    pub cell_pointers: Vec<(u32, u16)>,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub page_number: u32,
    pub offset: u16,
}

impl Iterator for TableLeafCell {
    type Item = TableLeafCell;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
