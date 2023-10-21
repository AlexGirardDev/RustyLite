
#[derive(Debug)]
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
