use anyhow::{bail, Result};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use crate::{
    page::{Page, PageHeader, PageType},
    sqlite_schema::{SqliteSchema, SchemaType},
};

pub struct Sqlite {
    pub header: SqliteHeader,
    file: File,
}

impl Sqlite {
    pub fn new(file_path: impl Into<String>) -> Result<Self> {
        let mut file = File::open(file_path.into())?;
        let mut buffer = [0; 100];
        file.read_exact(&mut buffer)?;
        let header = SqliteHeader {
            page_size: u16::from_be_bytes([buffer[16], buffer[17]]),
            number_of_pages: u32::from_be_bytes(buffer[28..32].try_into().unwrap()),
            text_encoding: match u32::from_be_bytes(buffer[56..60].try_into().unwrap()) {
                1 => TextEncoding::Utf8,
                _ => bail!("only utf8 is supported"),
            },
        };
        Ok(Sqlite {
            header,
            file: file.into(),
        })
    }

    pub fn get_schema(&mut self) -> Result<Vec<SqliteSchema>> {
        let page = self.read_page(0)?;

        let mut schema: Vec<SqliteSchema> = Vec::new();

        for id in page.cell_array {
            self.file.seek(SeekFrom::Start(id as u64))?;
            let _payload_length = self.read_varint()?.value;
            let _row_id = self.read_varint()?.value;
            let record_headers = self.read_record_header()?;
            let schema_type = match self.read_cell(&record_headers[0])? {
                Cell::String(s) => match s.as_ref() {
                    "table" => SchemaType::Table,
                    "index" => SchemaType::Index,
                    "view" => SchemaType::View,
                    "trigger" => SchemaType::Trigger,
                    _ => todo!("bad column"),
                },
                _ => todo!(),
            };
            let Cell::String(name)=  self.read_cell(&record_headers[1])? else {todo!()};
            let Cell::String(table_name)=  self.read_cell(&record_headers[2])? else {todo!()};
            let Cell::Int8(root_page)=  self.read_cell(&record_headers[3])? else {todo!()};
            let Cell::String(sql)=  self.read_cell(&record_headers[4])? else {todo!()};

            schema.push(SqliteSchema{
                schema_type,
                name,
                table_name,
                root_page: root_page.into(),
                sql,
            });
        }

      Ok(schema) 
    }

    fn read_varint(&mut self) -> Result<Varint> {
        let mut buf = [0; 1];
        let mut more = true;
        let mut value: u64 = 0;
        let mut size = 0;
        while more {
            self.file.read_exact(&mut buf).unwrap();
            size += 1;
            let byte = buf[0] as u8;
            more = byte & 0b1000_0000 != 0;
            value <<= 7;
            value |= u64::from(0b0111_1111 & byte);
        }
        Ok(Varint{value,size})
    }


    // fn parse_varint()
    // fn varint_bytes_needed(value: u64)-> u8 {
    //
    //
    //     0;
    // }

    fn read_page(&mut self, page_number: u32) -> Result<Page> {
        let offset = match page_number {
            0 => 100,
            num => num * self.header.page_size as u32,
        };
        let mut buffer = [0; 1];
        self.file.seek(SeekFrom::Start(offset.into()))?;
        self.file.read_exact(&mut buffer).unwrap();
        let page_type = match u8::from_be_bytes(buffer) {
            2 => PageType::InteriorIndex,
            5 => PageType::InteriorTable,
            10 => PageType::LeafIndex,
            13 => PageType::LeafTable,
            _ => panic!("invalid page type"),
        };

        let mut buffer = [0; 2];

        self.file.read_exact(&mut buffer)?;
        let free_block = u16::from_be_bytes(buffer);

        self.file.read_exact(&mut buffer)?;
        let cell_count = u16::from_be_bytes(buffer);

        self.file.read_exact(&mut buffer)?;
        let cell_content_area_offset = u16::from_be_bytes(buffer);

        let mut buffer = [0; 1];
        self.file.read_exact(&mut buffer)?;
        let fragmented_free_bytes = u8::from_be_bytes(buffer);

        let mut cell_array: Vec<u8> = vec![0; cell_count as usize * 2];

        self.file.read_exact(cell_array.as_mut_slice())?;
        let ids: Vec<u64> = cell_array
            .chunks(2)
            .map(|f| u16::from_be_bytes([f[0], f[1]]).into())
            .collect();

        Ok(Page {
            page_header: PageHeader {
                page_type,
                free_block,
                cell_count,
                cell_content_area_offset,
                fragmented_free_bytes,
                right_pointer: None,
            },
            cell_array: ids,
        })
    }

    fn read_record_header(&mut self) -> Result<Vec<CellType>> {
        let Varint {mut value, size} = self.read_varint()?;

        value -= size as u64;
        let mut result: Vec<CellType> = Vec::new();
        while value > 0 {
            let varint = self.read_varint()?;
            value -= varint.size as u64;
            let wow = match varint.value {
                0 => CellType::Null,
                1 => CellType::Int8,
                2 => CellType::Int16,
                3 => CellType::Int24,
                4 => CellType::Int32,
                5 => CellType::Int48,
                6 => CellType::Int64,
                7 => CellType::Float64,
                8 => CellType::Int8,
                9 => CellType::Int8,
                10 | 11 => CellType::Null,
                code => {
                    if code % 2 == 0 {
                        CellType::Blob(((code - 12) / 2) as isize)
                    } else {
                        CellType::String(((code - 12) / 2) as isize)
                    }
                }
            };
            result.push(wow);
        }
        return Ok(result);
    }

fn read_cell(&mut self, cell_type: &CellType) -> Result<Cell> {
    return Ok(match cell_type {
        CellType::Null => Cell::Null,
        CellType::Int8 => {
            let mut buff = [0; 1];
            self.file.read_exact(&mut buff)?;
            Cell::Int8(buff[0] as i8)
        }
        CellType::Int16 => {
            let mut buff = [0; 2];
            self.file.read_exact(&mut buff).unwrap();
            Cell::Int16(i16::from_be_bytes(buff))
        }
        CellType::Int24 => {
            let mut buff = [0; 4];
            self.file.read(&mut buff[0..3]).unwrap();
            Cell::Int32(i32::from_be_bytes(buff))
        }
        CellType::Int32 => {
            let mut buff = [0; 4];
            self.file.read(&mut buff).unwrap();
            Cell::Int32(i32::from_be_bytes(buff))
        }
        CellType::Int48 => {
            let mut buff = [0; 8];
            self.file.read(&mut buff[0..6]).unwrap();
            Cell::Int64(i64::from_be_bytes(buff))
        }
        CellType::Int64 => {
            let mut buff = [0; 8];
            self.file.read(&mut buff).unwrap();
            Cell::Int64(i64::from_be_bytes(buff))
        }
        CellType::Float64 => {
            let mut buff = [0; 8];
            self.file.read(&mut buff).unwrap();
            Cell::Float(f64::from_be_bytes(buff))
        }
        CellType::Blob(len) => {
            let mut data = vec![0u8; *len as usize];
            self.file.read(&mut data).unwrap();
            Cell::Blob(data)
        }
        CellType::String(len) => {
            let mut data = vec![0u8; *len as usize];
            self.file.read(&mut data).unwrap();
            Cell::String(String::from_utf8(data).unwrap())
        }
    });
}

}

#[derive(Debug)]
pub struct SqliteHeader {
    pub page_size: u16,
    number_of_pages: u32,
    text_encoding: TextEncoding,
}

#[derive(Debug)]
pub enum TextEncoding {
    Utf8,
    Utf16le,
    Utf16be,
}

#[derive(Debug)]
enum CellType {
    Null,
    Int8,
    Int16,
    Int24,
    Int32,
    Int48,
    Int64,
    Float64,
    Blob(isize),
    String(isize),
}
#[derive(Debug)]
enum Cell {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float(f64),
    Blob(Vec<u8>),
    String(String),
}
struct Varint{
    value: u64,
    size: u8,
}
