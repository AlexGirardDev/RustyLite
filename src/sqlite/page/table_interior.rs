use crate::sqlite::database::Database;

use super::page_header::PageHeader;
use anyhow::Result;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct TableInteriorPage {
    pub page_number: u32,
    pub header: PageHeader,
    pub row_id: i64,
    pub right_cell: u32,
    // pub cell_pointers: Vec<(u32,u16)>,
    pub cells: Vec<TableInteriorCell>,
}

impl TableInteriorPage {
    pub fn read_cells(
        db: &Database,
        cell_pointers: &[(u32, u16)],
    ) -> Result<Vec<TableInteriorCell>> {
        let cells: Vec<TableInteriorCell> = cell_pointers
            .iter()
            .map(|&offset| TableInteriorCell::read_cell(offset.0, offset.1, db))
            .try_collect()?;

        assert_eq!(cells.len(), cell_pointers.len());
        Ok(cells.into_iter().sorted_by_key(|f| f.row_id).collect_vec()) 

    }
}

impl TableInteriorCell {
    pub fn read_cell(page_number: u32, offset: u16, db: &Database) -> Result<Self> {
        db.seek(page_number, offset)?;

        let left_child = db.read_u32()?;
        let row_id = db.read_varint()?.value;

        Ok(TableInteriorCell {
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

#[derive(Debug, Clone)]
pub struct TableInteriorCell {
    pub left_child_page_number: u32,
    pub row_id: i64,
}
