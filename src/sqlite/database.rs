use anyhow::{bail, Context, Result};
use itertools::Itertools;
use sqlparser::{
    ast,
    dialect::SQLiteDialect,
    parser::{Parser, ParserError},
};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    rc::Rc,
};

use crate::sqlite::page::{page_header::PageHeader, table_leaf::TableLeafPage};

use super::{
    column::Column,
    connection::DatabaseHeader,
    page::{
        index_interior::IndexInteriorPage, index_leaf::IndexLeafPage, page_header::PageType,
        table_interior::TableInteriorPage, Page,
    },
    record::{CellType, CellValue, Record, RecordHeader},
    schema::{index_schema::IndexSchema, table_schema::TableSchema, SqliteSchema},
};

pub struct Database {
    pub header: DatabaseHeader,
    file: File,
    schema: Vec<Rc<SqliteSchema>>,
}

static DIALECT: SQLiteDialect = SQLiteDialect {};

impl Database {
    pub fn new(file_path: impl Into<String>) -> Result<Database> {
        let mut file = File::open(file_path.into())?;
        let mut buffer = [0; 100];
        file.read_exact(&mut buffer)?;
        let header = DatabaseHeader {
            page_size: u16::from_be_bytes([buffer[16], buffer[17]]),
        };
        let mut db = Database {
            file,
            header,
            schema: Vec::new().into(),
        };
        let schema = db.read_schema()?;
        db.schema = schema.into_iter().map(|f| Rc::new(f)).collect_vec();
        Ok(db)
    }

    pub fn read_entire_record(&mut self, pos: Position) -> Result<Record> {
        self.seek_position(pos)?;
        let payload_size = self.read_varint()?.value;
        let row_id = self.read_varint()?.value;
        let record_header = self.read_record_header()?;
        let mut values = Vec::<CellValue>::new();
        for val in &record_header.headers {
            values.push(self.read_cell(&val)?);
        }
        Ok(Record {
            payload_size,
            row_id,
            values,
            record_header,
        })
    }

    pub fn read_record_header(&mut self) -> Result<RecordHeader> {
        let Varint { mut value, size } = self.read_varint()?;

        let header_size = value;
        value -= size as i64;
        let mut headers: Vec<CellType> = Vec::new();
        while value > 0 {
            let varint = self.read_varint()?;
            value -= varint.size as i64;
            headers.push(match varint.value {
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
            });
        }
        Ok(RecordHeader {
            headers,
            header_size,
        })
    }

    fn read_record(&mut self, start: i64) -> Result<Record> {
        self.file.seek(SeekFrom::Start(start as u64))?;
        let payload_size = self.read_varint()?.value;
        let row_id = self.read_varint()?.value;
        let record_header = self.read_record_header()?;
        let mut values = Vec::<CellValue>::new();
        for val in &record_header.headers {
            values.push(self.read_cell(&val)?);
        }
        Ok(Record {
            payload_size,
            row_id,
            values,
            record_header,
        })
    }

    fn get_location(&self, page_number: i64, offset: impl Into<i64>) -> Result<i64> {
        if page_number <= 0 {
            bail!("pages start at index 1");
        }
        let offset = offset.into();
        if offset > self.header.page_size as i64 {
            bail!("page offset can't be larger than page size");
        }

        let page_start = match page_number {
            1 => 0,
            num => (num - 1) * self.header.page_size as i64,
        };
        Ok(page_start + offset)
    }
    pub fn read_page(&mut self, page_number: i64) -> Result<Page> {
        let mut buffer = [0; 1];
        let page_start = match page_number {
            1 => 100,
            num => (num - 1) * self.header.page_size as i64,
        };

        self.file.seek(SeekFrom::Start(page_start as u64))?;
        self.file.read_exact(&mut buffer)?;
        let page_type = match buffer[0] {
            0x02 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            0x0a => PageType::IndexLeaf,
            0x0d => PageType::TableLeaf,
            _ => bail!("invalid page type "),
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

        let right_cell = match page_type {
            PageType::IndexInterior | PageType::TableInterior => {
                let mut buffer = [0; 4];
                self.file.read_exact(&mut buffer)?;
                u32::from_be_bytes(buffer)
            }
            _ => 0,
        };

        let mut cell_array: Vec<u8> = vec![0; cell_count as usize * 2];

        self.file.read_exact(cell_array.as_mut_slice())?;
        let cell_pointers: Vec<u16> = cell_array
            .chunks(2)
            .map(|f| u16::from_be_bytes([f[0], f[1]]))
            .collect();

        let header = PageHeader {
            page_type: page_type.clone(),
            free_block,
            cell_count,
            cell_content_area_offset,
            fragmented_free_bytes,
        };

        Ok(match &page_type {
            PageType::IndexInterior => Page::IndexInterior(IndexInteriorPage {
                header,
                right_cell,
                page_number,
                cell_pointers,
            }),
            PageType::TableInterior => Page::TableInterior(TableInteriorPage {
                header,
                right_cell,
                page_number,
                cell_pointers,
            }),
            PageType::IndexLeaf => Page::IndexLeaf(IndexLeafPage {
                header,
                page_number,
                cell_pointers,
            }),
            PageType::TableLeaf => Page::TableLeaf(TableLeafPage {
                header,
                page_number,
                cell_pointers,
            }),
        })
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
                CellValue::String(String::from_utf8(data)?)
            }
        });
    }

    fn read_varint(&mut self) -> Result<Varint> {
        let mut buf = [0; 1];
        let mut more = true;
        let mut value: i64 = 0;
        let mut size = 0;
        while more {
            self.file.read_exact(&mut buf).unwrap();
            size += 1;
            let byte = buf[0] as u8;
            more = byte & 0b1000_0000 != 0;
            value <<= 7;
            value |= i64::from(0b0111_1111 & byte);
        }
        Ok(Varint { value, size })
    }

    pub fn get_table_schema(&mut self, table_name: impl AsRef<str>) -> Result<Rc<SqliteSchema>> {
        let schema = self
            .schema
            .iter()
            .find(|f| match f.as_ref() {
                SqliteSchema::Table(t) => t.name.as_ref() == table_name.as_ref(),
                SqliteSchema::Index(_) => false,
            })
            .context(format!(
                "clould not find table named {}",
                table_name.as_ref()
            ))?
            .clone();
        Ok(schema)
    }

    // pub fn get_table_indices(&mut self, table_name: impl AsRef<str>) -> Result<Rc<SqliteSchema>> {
    //     let schema = self
    //         .schema
    //         .iter()
    //         .find(|f| match f.as_ref() {
    //             SqliteSchema::Table(t) => t.name.as_ref() == table_name.as_ref(),
    //             SqliteSchema::Index(_) => false,
    //         })
    //         .context(format!(indices
    //             "clould not find table named {}",
    //             table_name.as_ref()
    //         ))?
    //         .clone();
    //
    //     todo!()
    // }
    //


    pub fn get_schema(&mut self) -> Vec<Rc<SqliteSchema>> {
        self.schema.clone()
    }

    fn read_schema(&mut self) -> Result<Vec<SqliteSchema>> {
        let page = self.read_page(1)?;
        let mut schemas: Vec<SqliteSchema> = Vec::new();

        let Page::TableLeaf(page) = page else {bail!("sql schhemaa table must be a leaf page")};
        for pointer in page.cell_pointers {
            let mut record = self.read_entire_record(Position::new(1, pointer))?;
            if record.record_header.headers.len() != 5 {
                bail!("Schema table must have 5 fields");
            }

            let CellValue::String(sql)= record.values.pop().expect("array is known size") else {bail!("sql must be a string field")};
            let CellValue::Int(root_page)=  record.values.pop().expect("array is known size") else {bail!("root_page must be an int")};
            let CellValue::String(table_name)=  record.values.pop().expect("array is known size") else {bail!("table_name must be a string field")};
            let CellValue::String(name)=  record.values.pop().expect("array is known size") else {bail!("name must be a string")};
            let schema = match record.values.pop().expect("array is known size") {
                CellValue::String(s) => match s.as_ref() {
                    "table" => {
                        let ast = match Parser::parse_sql(&DIALECT, &sql) {
                            Result::Ok(ast) => ast,
                            Result::Err(err) => {
                                if let ParserError::ParserError(ref msg) = err {
                                    //sqlparser doesn't support create tables with datatypeless
                                    //columns https://github.com/sqlparser-rs/sqlparser-rs/issues/743
                                    //one of the default schema columns does this
                                    //the sqlite sequence table does this Result::Err(err) => return Err(err.into()),
                                    if msg.contains("Expected a data type name, found:") {
                                        continue;
                                    }
                                }
                                bail!(err)
                            }
                        };
                        if ast.len() != 1 {
                            bail!("table sqchema sql can only have 1 expression");
                        }
                        let ast::Statement::CreateTable { columns, .. }  = ast.get(0).expect("item is 1 item long") else {bail!("create table statement expected")};
                        let columns = columns
                            .iter()
                            .map(|f| {
                                let name = Rc::from(f.name.value.to_owned());
                                // (
                                // name,
                                Rc::new(Column {
                                    type_affinity: (&f.data_type).into(),
                                    // column_index: Some(i as i64),
                                    name,
                                })
                                // )
                            })
                            .collect();
                        SqliteSchema::Table(TableSchema {
                            row_id: record.row_id,
                            name: name.into(),
                            table_name: table_name.into(),
                            root_page,
                            sql,
                            columns,
                        })
                    }
                    "index" => SqliteSchema::Index(IndexSchema {
                        row_id: record.row_id,
                        name: name.into(),
                        table_name: table_name.into(),
                        root_page,
                        sql,
                    }),
                    "view" => bail!("views are not currenty supported"),
                    "trigger" => bail!("triggers are not currenty supported"),
                    _ => bail!("invalid schema type"),
                },
                _ => bail!("type column must be string"),
            };
            schemas.push(schema);
        }

        Ok(schemas)
    }

    fn seek_position(&mut self, pos: Position) -> Result<()> {
        match pos {
            Position::Relative => (),
            Position::Absolute {
                page_number,
                pointer,
            } => {
                self.file.seek(SeekFrom::Start(
                    self.get_location(page_number, pointer)? as u64
                ))?;
            }
        }
        Ok(())
    }
    fn seek(&mut self, page_number: i64, pointer: u16) -> Result<()> {
        self.file.seek(SeekFrom::Start(
            self.get_location(page_number, pointer)? as u64
        ))?;
        Ok(())
    }
}

pub struct Varint {
    value: i64,
    size: u8,
}
pub enum Position {
    Relative,
    Absolute { page_number: i64, pointer: u16 },
}
impl Position {
    pub fn new(page_number: i64, pointer: u16) -> Position {
        Position::Absolute {
            page_number,
            pointer,
        }
    }
}
