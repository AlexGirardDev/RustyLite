use std::fs::File;

use super::page_header::PageHeader;
use crate::sqlite::record::RecordHeader;
use anyhow::{bail, Context, Ok, Result};

#[derive(Debug)]
pub struct TableLeafPage {
    pub page_number: i64,
    pub header: PageHeader,
    pub cell_pointers: Vec<u16>,
    // pub cells: Vec<TableLeafCell>,
}

#[derive(Debug)]
pub struct TableLeafCell {
    pub row_id: i64,
    pub location: i64,
    pub payload_size: i64,
    pub record_header: RecordHeader,
}
