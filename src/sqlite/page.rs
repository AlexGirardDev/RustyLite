use crate::sqlite::btree;

#[derive(Debug)]
pub struct Page {
    pub page_header: PageHeader,
    pub cell_array: Vec<u64>,
}

#[derive(Debug)]
pub struct PageHeader {
    pub page_type: PageType,
    pub free_block: u16,
    pub cell_count: u16,
    pub cell_content_area_offset: u16,
    pub fragmented_free_bytes: u8,
    pub right_pointer: Option<u32>
}

#[derive(Debug)]
pub enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}
