use std::fmt;

use super::database::Position;

#[derive(Debug, Default)]
pub struct Record {
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
        row_id: i64,
        record_header: RecordHeader,
        page_number: u32,
        pointer: u16,
        cell_header_size: i64,
    ) -> Self {
        Self {
            row_id,
            record_header,
            page_number,
            pointer,
            cell_header_size,
        }
    }

    pub fn get_cell_position(&self, cell_index: usize) -> Position {
        let offset: i64 = self
            .record_header
            .headers
            .iter()
            .take(cell_index)
            .map(|f| match f {
                CellType::Null => 0_i64,
                CellType::Float64 => 8_i64,
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
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum CellValue {
    Null,
    Int(i64),
    Float(f64),
    Blob(Vec<u8>),
    String(String),
}

// impl PartialEq for CellValue {
//     fn eq(&self, other: &Self) -> bool {
//         match (self, other) {
//             (Self::Int(l0), Self::Int(r0)) => l0 == r0,
//             (Self::Float(l0), Self::Float(r0)) => l0 == r0,
//             (Self::Blob(l0), Self::Blob(r0)) => l0 == r0,
//             (Self::String(l0), Self::String(r0)) => l0 == r0,
//             _ => core::mem::discriminant(self) == core::mem::discriminant(other),
//         }
//     }
// }

#[derive(Debug)]
pub enum CellType {
    Null,
    Varint(u8),
    Float64,
    Blob(isize),
    String(isize),
}
