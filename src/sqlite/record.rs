use std::{default, fmt};

use sqlparser::ast::Values;

use super::database::Position;

#[derive(Debug, Default)]
pub struct Record {
    payload_size: i64,
    pub row_id: i64,
    pub record_header: RecordHeader,
    page_number: u32,
    pointer: u16,
    cell_header_size: i64,
}

#[derive(Debug, Default)]
pub struct RecordHeader {
    pub headers: Vec<CellType>,
    header_size: i64,
}

impl RecordHeader {
    pub fn new(headers: Vec<CellType>, header_size: i64) -> Self {
        Self {
            headers,
            header_size,
        }
    }
}

impl Record {
    pub fn new(
        payload_size: i64,
        row_id: i64,
        record_header: RecordHeader,
        page_number: u32,
        pointer: u16,
        cell_header_size: i64,
    ) -> Self {
        Self {
            payload_size,
            row_id,
            record_header,
            page_number,
            pointer,
            cell_header_size,
            ..Default::default()
        }
    }

    pub fn get_cell_position(&self, cell_index: usize) -> Position {
        let offset: i64 = self
            .record_header
            .headers
            .iter()
            .take(cell_index)
            .map(|f| match f {
                CellType::Null => 0 as i64,
                CellType::Float64 => 8 as i64,
                CellType::Blob(s) => *s as i64,
                CellType::String(s) => *s as i64,
                CellType::Varint(s) => *s as i64,
            })
            .sum();
        let pointer =
            self.pointer as i64 + self.cell_header_size + self.record_header.header_size + offset;
        Position::Absolute {
            page_number: self.page_number,
            pointer: pointer as u16,
        }
    }
}

impl fmt::Display for CellValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CellValue::Null => write!(f, "NULl"),
            CellValue::Int(v) => write!(f, "{v}"),
            CellValue::Float(v) => write!(f, "{v}"),
            CellValue::Blob(_) => write!(f, ""),
            CellValue::String(v) => write!(f, "{v}"),
        }
    }
}
#[derive(Debug, Clone)]
pub enum CellValue {
    Null,
    Int(i64),
    Float(f64),
    Blob(Vec<u8>),
    String(String),
}

#[derive(Debug)]
pub enum CellType {
    Null,
    Varint(u8),
    Float64,
    Blob(isize),
    String(isize),
}
