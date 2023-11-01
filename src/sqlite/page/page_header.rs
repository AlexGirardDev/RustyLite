use anyhow::{bail, Context, Ok, Result};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

#[derive(Debug)]
pub struct PageHeader {
    pub page_type: PageType,
    pub free_block: u16,
    pub cell_count: u16,
    pub cell_content_area_offset: u16,
    pub fragmented_free_bytes: u8,
}

#[derive(Debug,Clone)]
pub enum PageType {
    TableLeaf,
    TableInterior,
    IndexLeaf,
    IndexInterior,
}

