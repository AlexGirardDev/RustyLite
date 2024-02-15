use crate::sqlite::{database::Database, record::CellValue};
use anyhow::Result;

use super::page_header::PageHeader;

#[derive(Debug, Clone)]
pub struct IndexInteriorPage {
    pub page_number: u32,
    pub header: PageHeader,
    pub value: CellValue,
    // pub cell_pointers: Vec<(u32,u16)>,
    pub cells: Vec<IndexInteriorCell>,
}

impl IndexInteriorPage {
    pub fn read_cells(
        db: &Database,
        cell_pointers: Vec<(u32, u16)>,
    ) -> Result<Vec<IndexInteriorCell>> {
        cell_pointers
            .iter()
            .map(|&offset| IndexInteriorCell::read_cell(offset.0, offset.1, db))
            .collect() // This will automatically collect into Result<Vec<TableInteriorCell>, Error>
    }
}

impl IndexInteriorCell {
    pub fn read_cell(page_number: u32, offset: u16, db: &Database) -> Result<Self> {
        db.seek(page_number, offset)?;

        let left_child = db.read_u32()?;
        let record = db.read_index_record(page_number, offset + 4)?;
        let key = db.read_record_cell(&record, 0)?;
        // db.read_raw_cell()?;//payload size
        //
        Ok(IndexInteriorCell {
            left_child_page_number: left_child,
            value: key,
        })
    }
}

#[derive(Debug, Clone)]
pub struct IndexInteriorCell {
    pub left_child_page_number: u32,
    pub value: CellValue,
}
