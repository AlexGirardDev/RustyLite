use std::fmt;

#[derive(Debug)]
pub struct Record {
    pub payload_size: i64,
    pub row_id: i64,
    pub record_header: RecordHeader,
    pub values: Vec<CellValue>,
}

#[derive(Debug)]
pub struct RecordHeader {
    pub headers: Vec<CellType>,
    pub header_size: i64,
}


impl fmt::Display for CellValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CellValue::Null => write!(f,"NULl"),
            CellValue::Int(v) => write!(f,"{v}"),
            CellValue::Float(v) => write!(f,"{v}"),
            CellValue::Blob(v) => write!(f,""),
            CellValue::String(v) => write!(f,"{v}"),
        }
    }
}
#[derive(Debug,Clone)]
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
