use anyhow::{bail, Context, Result};
use itertools::Itertools;

use sqlparser::{
    ast,
    dialect::SQLiteDialect,
    parser::{Parser, ParserError},
};
use std::{
    cell::RefCell,
    collections::HashSet,
    fs::File,
    io::{Read, Seek, SeekFrom},
    option,
    rc::Rc,
};

use crate::sqlite::page::{page_header::PageHeader, table_leaf::TableLeafPage};

use super::{
    column::Column,
    connection::DatabaseHeader,
    page::{
        index_interior::IndexInteriorPage,
        index_leaf::IndexLeafPage,
        page_header::PageType,
        table_interior::{TableInteriorCell, TableInteriorPage},
        IndexPage, Page, TablePage,
    },
    record::{CellType, CellValue, Record, RecordHeader},
    schema::{index_schema::IndexSchema, table_schema::TableSchema, SqliteSchema},
};

pub struct Database {
    pub header: DatabaseHeader,
    file: RefCell<File>,
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
            file: file.into(),
            header,
            schema: Vec::new(),
        };
        let schema = db.read_schemas()?;
        db.schema = schema.into_iter().map(Rc::new).collect_vec();

        Ok(db)
    }

    // pub fn read_entire_record(&self, pos: Position) -> Result<Record> {
    //     self.seek_position(pos)?;
    //     let payload_size = self.read_varint()?.value;
    //     let row_id = self.read_varint()?.value;
    //     let record_header = self.read_record_header(Position::Relative)?;
    //     let mut values = Vec::<CellValue>::new();
    //     for val in &record_header.headers {
    //         values.push(self.read_record_cell(Position::Relative,val)?);
    //     }
    //     Ok(Record {
    //         payload_size,
    //         row_id,
    //         values,
    //         record_header,
    //     })
    // }

    fn read_record_header(&self, pos: Position) -> Result<RecordHeader> {
        self.seek_position(pos)?;
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
        Ok(RecordHeader::new(headers, header_size))
    }

    pub fn read_cell_row_id(&self, page_number: u32, pointer: u16) -> Result<i64> {
        self.seek_position(Position::new(page_number, pointer))?;
        self.read_varint()?; //payload size;
        let row_id = self.read_varint()?;
        Ok(row_id.value)
    }
    pub fn read_index_record(&self, page_number: u32, pointer: u16) -> Result<Record> {
        self.seek_position(Position::new(page_number, pointer))?;
        let payload_size = self.read_varint()?;
        // let row_id = self.read_varint()?;
        let cell_header_size = payload_size.size; //;+ row_id.size;
        let record_header = self.read_record_header(Position::Relative)?;
        Ok(Record::new(
            0,
            record_header,
            page_number,
            pointer,
            cell_header_size as i64,
        ))
    }

    pub fn read_record(&self, page_number: u32, pointer: u16) -> Result<Record> {
        self.seek_position(Position::new(page_number, pointer))?;
        let payload_size = self.read_varint()?;
        let row_id = self.read_varint()?;
        let cell_header_size = payload_size.size + row_id.size;
        let record_header = self.read_record_header(Position::Relative)?;
        Ok(Record::new(
            row_id.value,
            record_header,
            page_number,
            pointer,
            cell_header_size as i64,
        ))
    }

    fn get_location(&self, page_number: u32, offset: u16) -> Result<i64> {
        if page_number == 0 {
            bail!("pages start at index 1");
        }
        if offset > self.header.page_size {
            bail!("page offset can't be larger than page size");
        }

        let page_start = match page_number {
            1 => 0, //100?
            num => (num - 1) * self.header.page_size as u32,
        };
        Ok((page_start + offset as u32) as i64)
    }

    pub fn read_table_page(&self, page_number: u32, row_id: Option<i64>) -> Result<TablePage> {
        match self.read_page_raw(page_number, None, row_id)? {
            Page::Table(t) => Ok(t),
            Page::Index(_) => bail!("Expectting table page but got index"),
        }
    }

    pub fn read_index_page(&self, page_number: u32, value: Option<CellValue>) -> Result<IndexPage> {
        match self.read_page_raw(page_number, value, None)? {
            Page::Table(_) => bail!("Expectting table page but got index"),
            Page::Index(i) => Ok(i),
        }
    }
    pub fn read_table_interior_cell(
        &self,
        _page_number: u32,
        _cell_pointer: i64,
    ) -> Result<TableInteriorCell> {
        todo!()
    }
    fn read_page(&self, page_number: u32) -> Result<Page> {
        self.read_page_raw(page_number, None, None)
    }

    fn read_page_raw(
        &self,
        page_number: u32,
        first_key: Option<CellValue>,
        row_id: Option<i64>,
    ) -> Result<Page> {
        let mut buffer = [0; 1];

        let offset = match page_number {
            1 => 100,
            _ => 0,
        };
        self.seek(page_number, offset)?;
        self.read_exact(&mut buffer)?;
        let page_type = match buffer[0] {
            0x02 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            0x0a => PageType::IndexLeaf,
            0x0d => PageType::TableLeaf,
            _ => bail!("invalid page type "),
        };

        // Offset	Size	Description
        // 0	1	The one-byte flag at offset 0 indicating the b-tree page type.
        // 1	2	The two-byte integer at offset 1 gives the start of the first freeblock on the page, or is zero if there are no freeblocks.
        // 3	2	The two-byte integer at offset 3 gives the number of cells on the page.
        // 5	2	The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
        // 7	1	The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell content area.
        // 8	4	The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
        //
        //
        self.seek(page_number, offset + 1)?;
        let free_block = self.read_u16()?;
        self.seek(page_number, offset + 3)?;
        let mut cell_count = self.read_u16()?;
        self.seek(page_number, offset + 5)?;
        let cell_content_area_offset = self.read_u16()?;

        let mut buffer = [0; 1];
        self.read_exact(&mut buffer)?;
        let fragmented_free_bytes = u8::from_be_bytes(buffer);

        let right_cell = match page_type {
            PageType::IndexInterior | PageType::TableInterior => self.read_u32()?,
            _ => 0,
        };

        let mut cell_array: Vec<u8> = vec![0; cell_count as usize * 2];

        // if page_number != 1 {
        //     self.seek(page_number, cell_content_area_offset)?;
        // }
        self.read_exact(cell_array.as_mut_slice())?;

        let cell_pointers: Vec<(u32, u16)> = cell_array
            .chunks(2)
            .map(|f| (page_number, u16::from_be_bytes([f[0], f[1]])))
            .collect();
        assert_eq!(cell_pointers.len(), cell_count as usize);

        let header = PageHeader {
            page_type: page_type.clone(),
            free_block,
            cell_count,
            cell_content_area_offset,
            fragmented_free_bytes,
        };

        Ok(match &page_type {
            PageType::IndexInterior => {
                let cells = IndexInteriorPage::read_cells(self, cell_pointers)?;
                Page::Index(IndexPage::Interior(IndexInteriorPage {
                    header,
                    page_number,
                    value: cells.last().unwrap().value.clone(),
                    cells,
                    right_cell,
                }))
            }
            PageType::TableInterior => {
                let mut cells = TableInteriorPage::read_cells(self, &cell_pointers)?;
                let row_id=cells.last().unwrap().row_id;

                // let page = self.read_table_page(right_cell, None)?;
                // let right_row_id = match &page {
                //     TablePage::Leaf(l) => l.row_id,
                //     TablePage::Interior(i) => i.cells.iter().last().unwrap().row_id,
                // };

                cells.push(TableInteriorCell {
                    row_id: 0,
                    left_child_page_number: right_cell,
                });

                Page::Table(TablePage::Interior(TableInteriorPage {
                    header,
                    page_number,
                    row_id: row_id,
                    cells,
                    right_cell,
                }))
            }
            PageType::IndexLeaf => Page::Index(IndexPage::Leaf(IndexLeafPage {
                header,
                page_number,
                cell_pointers,
                value: first_key.unwrap(),
            })),
            PageType::TableLeaf => Page::Table(TablePage::Leaf(TableLeafPage {
                header,
                page_number,
                row_id: row_id.unwrap_or(0),
                cell_pointers,
            })),
        })
    }

    pub fn read_raw_cell(&self, cell_type: &CellType) -> Result<CellValue> {
        return Ok(match &cell_type {
            CellType::Null => CellValue::Null,
            CellType::Varint(size) => {
                let mut buff = [0; 8];
                let size = *size as usize;
                if size == 0 {
                    CellValue::Int(0)
                } else {
                    self.read_exact(&mut buff[8 - size..8])?;
                    CellValue::Int(i64::from_be_bytes(buff))
                }
            }
            CellType::Float64 => {
                let mut buff = [0; 8];
                self.file.borrow_mut().read_exact(&mut buff).unwrap();
                CellValue::Float(f64::from_be_bytes(buff))
            }
            CellType::Blob(len) => {
                let mut data = vec![0u8; *len as usize];
                self.file.borrow_mut().read_exact(&mut data).unwrap();
                CellValue::Blob(data)
            }
            CellType::String(len) => {
                let mut data = vec![0u8; *len as usize];
                self.file.borrow_mut().read_exact(&mut data).unwrap();
                CellValue::String(String::from_utf8(data)?)
            }
        });
    }

    pub fn read_record_cell(&self, record: &Record, index: usize) -> Result<CellValue> {
        self.seek_position(record.get_cell_position(index))?;
        self.read_raw_cell(&record.record_header.headers[index])
    }

    pub fn read_varint(&self) -> Result<Varint> {
        let mut buf = [0; 1];
        let mut more = true;
        let mut value: i64 = 0;
        let mut size = 0;
        while more {
            self.read_exact(&mut buf)?;
            size += 1;
            let byte = buf[0];
            more = byte & 0b1000_0000 != 0;
            value <<= 7;
            value |= i64::from(0b0111_1111 & byte);
        }
        Ok(Varint { value, size })
    }

    pub fn get_table_indexes(&self, table_name: impl AsRef<str>) -> HashSet<String> {
        self.schema
            .iter()
            .filter_map(|f| match f.as_ref() {
                SqliteSchema::Table(_) => None,
                SqliteSchema::Index(i) => {
                    if i.parent_table.as_ref() == table_name.as_ref() {
                        Some(i.column_name.to_string())
                    } else {
                        None
                    }
                }
            })
            .collect()
    }
    pub fn get_table_schema(&self, table_name: impl AsRef<str>) -> Result<Rc<SqliteSchema>> {
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

    pub fn get_index_schema(
        &self,
        table_name: impl AsRef<str>,
        column_name: impl AsRef<str>,
    ) -> Result<Rc<SqliteSchema>> {
        let schema = self
            .schema
            .iter()
            .find(|f| match f.as_ref() {
                SqliteSchema::Index(t) => {
                    t.column_name.as_ref() == column_name.as_ref()
                        && t.parent_table.as_ref() == table_name.as_ref()
                }
                SqliteSchema::Table(_) => false,
            })
            .context(format!(
                "could not find index named {}",
                table_name.as_ref()
            ))?
            .clone();
        Ok(schema)
    }

    // pub fn get_table_indices(&self, table_name: impl AsRef<str>) -> Result<Rc<SqliteSchema>> {
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

    pub fn get_schemas(&self) -> Vec<Rc<SqliteSchema>> {
        self.schema.clone()
    }

    fn read_schemas(&self) -> Result<Vec<SqliteSchema>> {
        let page = self.read_table_page(1, None)?;
        let mut schemas: Vec<SqliteSchema> = Vec::new();
        let TablePage::Leaf(page) = page else {
            bail!("sql schhemaa table must be a leaf page")
        };
        for pointer in page.cell_pointers {
            let record = self.read_record(pointer.0, pointer.1)?;
            if record.record_header.headers.len() != 5 {
                bail!("Schema table must have 5 fields");
            }

            let CellValue::String(name) = self.read_record_cell(&record, 1)? else {
                bail!("name must be a string")
            };
            let CellValue::String(table_name) = self.read_record_cell(&record, 2)? else {
                bail!("table_name must be a string field")
            };
            let CellValue::Int(root_page) = self.read_record_cell(&record, 3)? else {
                bail!("root_page must be an int")
            };
            let CellValue::String(sql) = self.read_record_cell(&record, 4)? else {
                bail!("sql must be a string field")
            };
            let schema = match self.read_record_cell(&record, 0)? {
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
                        let ast::Statement::CreateTable { columns, .. } =
                            ast.get(0).expect("item is 1 item long")
                        else {
                            bail!("create table statement expected")
                        };
                        let columns = columns
                            .iter()
                            .map(|f| {
                                let name = Rc::from(f.name.value.to_owned());
                                Rc::new(Column {
                                    type_affinity: (&f.data_type).into(),
                                    name,
                                })
                            })
                            .collect();
                        SqliteSchema::Table(TableSchema {
                            row_id: record.row_id,
                            name: name.into(),
                            table_name: table_name.into(),
                            root_page: root_page as u32,
                            sql,
                            columns,
                        })
                    }
                    "index" => {
                        // dbg!(&schema);
                        //
                        let (_, parent_name, column_name) = name
                            .split('_')
                            .collect_tuple()
                            .expect("only single column indexes are supported");

                        SqliteSchema::Index(IndexSchema {
                            row_id: record.row_id,
                            root_page: root_page as u32,
                            sql,

                            column_name: column_name.into(),
                            parent_table: parent_name.into(),
                            name: name.into(),
                        })
                    }
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

    pub fn read_u8(&self) -> Result<u8> {
        let mut buffer = [0; 1];
        self.read_exact(&mut buffer)?;
        Ok(u8::from_be_bytes(buffer))
    }
    pub fn read_u16(&self) -> Result<u16> {
        let mut buffer = [0; 2];
        self.read_exact(&mut buffer)?;
        Ok(u16::from_be_bytes(buffer))
    }

    pub fn read_u32(&self) -> Result<u32> {
        let mut buffer = [0; 4];
        self.read_exact(&mut buffer)?;
        Ok(u32::from_be_bytes(buffer))
    }

    pub fn read_u64(&self) -> Result<u64> {
        let mut buffer = [0; 8];
        self.read_exact(&mut buffer)?;
        Ok(u64::from_be_bytes(buffer))
    }

    fn seek_position(&self, pos: Position) -> Result<()> {
        match pos {
            Position::Relative => (),
            Position::Absolute {
                page_number,
                pointer,
            } => self.seek(page_number, pointer)?,
        }
        Ok(())
    }

    pub fn seek(&self, page_number: u32, pointer: u16) -> Result<()> {
        self.file.borrow_mut().seek(SeekFrom::Start(
            self.get_location(page_number, pointer)? as u64
        ))?;
        Ok(())
    }

    fn read_exact(&self, buf: &mut [u8]) -> Result<()> {
        self.file.borrow_mut().read_exact(buf)?;
        Ok(())
    }
}
pub struct Varint {
    pub value: i64,
    pub size: u8,
}
#[derive(Debug)]
pub enum Position {
    Relative,
    Absolute { page_number: u32, pointer: u16 },
}
impl Position {
    pub fn new(page_number: u32, pointer: u16) -> Position {
        Position::Absolute {
            page_number,
            pointer,
        }
    }
}
