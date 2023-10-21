use anyhow::{bail, Result};
use sqlparser::{dialect::GenericDialect, parser::Parser};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use crate::{page::{PageType, Page, PageHeader}, sqlite_cell::{CellType, CellValue}, schema::{SqliteSchema, SchemaType}};



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
        };
        Ok(Sqlite {
            header,
            file: file.into(),
        })
    }

    pub fn get_schema(&mut self) -> Result<Vec<SqliteSchema>> {
        let page = self.read_page(1)?;

        let mut schema: Vec<SqliteSchema> = Vec::new();

        for id in page.cell_array {
            self.file.seek(SeekFrom::Start(id as u64))?;
            let _payload_length = self.read_varint()?.value;
            let row_id = self.read_varint()?.value;
            let record_headers = self.read_record_header()?;
            let schema_type = match self.read_cell(&record_headers[0])? {
                CellValue::String(s) => match s.as_ref() {
                    "table" => SchemaType::Table,
                    "index" => SchemaType::Index,
                    "view" => SchemaType::View,
                    "trigger" => SchemaType::Trigger,
                    _ => todo!("bad column"),
                },
                _ => todo!(),
            };
            let CellValue::String(name)=  self.read_cell(&record_headers[1])? else {todo!()};
            let CellValue::String(table_name)=  self.read_cell(&record_headers[2])? else {todo!()};
            let CellValue::Int(root_page)=  self.read_cell(&record_headers[3])? else {todo!()};
            let CellValue::String(sql)=  self.read_cell(&record_headers[4])? else {todo!()};

            schema.push(SqliteSchema {
                row_id,
                schema_type,
                name,
                table_name,
                root_page,
                sql,
            });
        }

        Ok(schema)
    }

    pub fn count_rows(&mut self, table_name: &str) -> Result<u64> {
        let schemas = self.get_schema()?;
        let Some(schema) = schemas.iter().find(|x| x.table_name == table_name) else{
            return Ok(0);
        };
        let page = self.read_page(schema.root_page)?;

        return Ok(page.cell_array.len() as u64);
    }

    pub fn handle_sql(&mut self, sql: &str) -> Result<()> {
        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

        let ast = Parser::parse_sql(&dialect, sql)?;
        for exp in ast {
            match exp {
                sqlparser::ast::Statement::Query(query) => {
                    println!("{:?}", query.body);
                    match *query.body {
                        sqlparser::ast::SetExpr::Select(sel) => {
                            for x in sel.projection {
                                match x {
                                    sqlparser::ast::SelectItem::UnnamedExpr(exp) => {
                                        match exp {
                                            sqlparser::ast::Expr::Function(fun) => {
                                                if fun.name.0.len() == 1 {
                                                    match fun.name.0[0].value.as_ref(){
                                                        "count"=>{
                                                             
                                                        },
                                                            e=> bail!("unsported function {}",e)
                                                    }
                                                } else {
                                                    bail!(
                                                        "only single name functions are supporteed"
                                                    );
                                                }
                                            }
                                            e => bail!("{} is not currenty supported", e),
                                        };
                                    }

                                    e => bail!("{} is not currenty supported", e),
                                }
                            }
                        }
                        e => bail!("{} is not currenty supported", e),
                    }
                }

                e => bail!("{} is not currenty supported", e),
            }
        }
        Ok(())
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
        Ok(Varint { value, size })
    }

    fn read_page(&mut self, page_number: i64) -> Result<Page> {
        if page_number <= 0 {
            bail!("pages start at index 1");
        }

        let offset = match page_number {
            1 => 100,
            num => (num - 1) * self.header.page_size as i64,
        };
        let mut buffer = [0; 1];
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.read_exact(&mut buffer).unwrap();
        let page_type = match buffer[0] {
            0x0d => PageType::LeafTable,
            0x05 => PageType::InteriorTable,
            0x0a => PageType::LeafIndex,
            0x02 => PageType::InteriorIndex,
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
        let Varint { mut value, size } = self.read_varint()?;

        value -= size as u64;
        let mut result: Vec<CellType> = Vec::new();
        while value > 0 {
            let varint = self.read_varint()?;
            value -= varint.size as u64;
            let wow = match varint.value {
                0 => CellType::Null,
                1 => CellType::Varint(1),
                2 => CellType::Varint(2),
                3 => CellType::Varint(3),
                4 => CellType::Varint(4),
                5 => CellType::Varint(6),
                6 => CellType::Varint(8),
                7 => CellType::Float64,
                8 => CellType::Varint(1),
                9 => CellType::Varint(1),
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

    fn read_cell(&mut self, cell_type: &CellType) -> Result<CellValue> {
        return Ok(match cell_type {
            CellType::Null => CellValue::Null,
            CellType::Varint(size) => {
                let mut buff = [0; 8];
                let size = *size as usize;
                if size <= 0 {
                    CellValue::Int(0)
                } else {
                    self.file.read_exact(&mut buff[8 - size..8])?;
                    CellValue::Int(i64::from_be_bytes(buff))
                }
            }
            CellType::Float64 => {
                let mut buff = [0; 8];
                self.file.read(&mut buff).unwrap();
                CellValue::Float(f64::from_be_bytes(buff))
            }
            CellType::Blob(len) => {
                let mut data = vec![0u8; *len as usize];
                self.file.read(&mut data).unwrap();
                CellValue::Blob(data)
            }
            CellType::String(len) => {
                let mut data = vec![0u8; *len as usize];
                self.file.read(&mut data).unwrap();
                CellValue::String(String::from_utf8(data).unwrap())
            }
        });
    }
}

#[derive(Debug)]
pub struct SqliteHeader {
    pub page_size: u16,
    // number_of_pages: u32,
    // text_encoding: TextEncoding,
}

#[derive(Debug)]
pub enum TextEncoding {
    Utf8,
    Utf16le,
    Utf16be,
}


struct Varint {
    value: u64,
    size: u8,
}
