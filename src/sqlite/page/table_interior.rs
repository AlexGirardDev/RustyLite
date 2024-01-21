use crate::sqlite::database::Database;

use super::page_header::PageHeader;
use anyhow::Result;

#[derive(Debug)]
pub struct TableInteriorPage{
    pub page_number: u32,
    pub header: PageHeader,
    pub cell_pointers: Vec<(u32,u16)>,
}

impl TableInteriorPage {
    pub fn read_cells(&self, db: &Database) -> Result<Vec<TableInteriorCell>> {
        self.cell_pointers
            .iter()
            .map(|&offset| TableInteriorCell::read_cell(offset.0, offset.1, db))
            .collect() // This will automatically collect into Result<Vec<TableInteriorCell>, Error>
    }

}

impl TableInteriorCell {
    pub fn read_cell(page_number: u32, offset: u16, db: &Database) -> Result<Self> {
        db.seek(page_number, offset)?;
        let left_child = db.read_u32()?;
        let row_id = db.read_varint()?.value;

        Ok(TableInteriorCell {
            page_number,
            offset,
            left_child_page_number: left_child,
            row_id,
        })
    }
}

// Index B-Tree Interior Cell (header 0x02):
//
// A 4-byte big-endian page number which is the left child pointer.
// A varint which is the total number of bytes of key payload, including any overflow
// The initial portion of the payload that does not spill to overflow pages.
// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.

#[derive(Debug)]
pub struct TableInteriorCell {
    page_number: u32,
    offset: u16,
    pub left_child_page_number: u32,
    pub row_id: i64,
}