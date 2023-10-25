use anyhow::{bail, Context, Result};
use sqlparser::{
    ast::{self, Expr, FunctionArg, SelectItem, TableFactor},
    dialect::SQLiteDialect,
    parser::Parser,
};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    usize,
};


use crate::sqlite::{
    column::Column,
    page::{Page, PageHeader, PageType},
    record::{CellType, CellValue, Record, RecordHeader},
    row::Row,
    schema::{SchemaType, SqliteSchema},
};

pub struct Connection {
    pub header: DatabaseHeader,
    file: File,
}

static DIALECT: SQLiteDialect = SQLiteDialect {};

impl Connection {
    pub fn new(header: DatabaseHeader, file: File) -> Connection {
        Connection { header, file }
    }

    pub fn get_schema(&mut self) -> Result<Vec<SqliteSchema>> {
        let page = self.read_page(1)?;

        let mut schema: Vec<SqliteSchema> = Vec::new();

        for id in page.cell_array {
            let mut record = self.read_record(id)?;
            if record.record_header.headers.len() != 5 {
                bail!("Schema table must have 5 fields");
            }

            let CellValue::String(sql)= record.values.pop().unwrap() else {bail!("sql must be a string field")};
            let CellValue::Int(root_page)=  record.values.pop().unwrap() else {bail!("root_page must be an int")};
            let CellValue::String(table_name)=  record.values.pop().unwrap() else {bail!("table_name must be a string field")};
            let CellValue::String(name)=  record.values.pop().unwrap() else {bail!("name must be a string")};
            let schema_type = match record.values.pop().unwrap() {
                CellValue::String(s) => match s.as_ref() {
                    "table" => SchemaType::Table,
                    "index" => SchemaType::Index,
                    "view" => SchemaType::View,
                    "trigger" => SchemaType::Trigger,
                    _ => bail!("invalid schema type"),
                },
                _ => bail!("type column must be string"),
            };

            schema.push(SqliteSchema {
                row_id: record.row_id,
                schema_type,
                name,
                table_name,
                root_page,
                sql,
            });
        }

        Ok(schema)
    }

    pub fn count_rows(&mut self, table_name: &str) -> Result<i64> {
        let schemas = self.get_schema()?;
        let Some(schema) = schemas.iter().find(|x| x.table_name == table_name) else{
            return Ok(0);
        };
        let page = self.read_page(schema.root_page)?;

        return Ok(page.cell_array.len() as i64);
    }

    // pub fn execute(&mut self, sql: &str) -> Result<()> {
    //     todo!();
    // }

    pub fn query(&mut self, sql: &str) -> Result<Vec<Row>> {
        let ast = Parser::parse_sql(&DIALECT, sql)?;
        let mut rows = Vec::<Row>::new();
        for exp in ast {
            match exp {
                ast::Statement::Query(query) => match *query.body {
                    ast::SetExpr::Select(sel) => {
                        let mut columns = Vec::<String>::new();
                        for sel_item in sel.projection {
                            columns.append(&mut self.proccess_sel_item(&sel_item)?)
                        }

                        let TableFactor::Table { name,.. } = &sel.from.get(0).context("table name is required")?.relation else{
                                bail!("only selecting from tables is currently supported");
                            };
                        rows.append(&mut self.read_table(
                            &name.0.get(0).context("table name is required")?.value,
                            columns,
                        )?);
                    }
                    e => bail!("{} is not currenty supported", e),
                },

                e => bail!("{} is not currenty supported", e),
            }
        }
        Ok(rows)
    }

    fn get_table_schema(&mut self, table: &str) -> Result<SqliteSchema> {
        match self.get_schema()?.into_iter().find(|f| f.name == table) {
            Some(s) => Ok(s),
            None => bail!("could not find table"),
        }
    }

    fn get_table_columns(&mut self, scheama: &SqliteSchema) -> Result<HashMap<String, Column>> {
        match &scheama.schema_type {
            SchemaType::Table => {
                let ast = Parser::parse_sql(&DIALECT, &scheama.sql)?;
                if ast.len() != 1 {
                    bail!("table sqchema sql can only have 1 expression");
                }
                match ast.get(0).unwrap() {
                    ast::Statement::CreateTable { columns, .. } => {
                        return Ok(columns
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                (
                                    f.name.value.to_owned(),
                                    Column {
                                        type_affinity: (&f.data_type).into(),
                                        column_index: i as i64,
                                    },
                                )
                            })
                            .collect())
                    }
                    t => bail!("{} is not currently supported", t),
                }
            }
            t => bail!("{} is not currently supported", t),
        }
    }

    fn read_table(&mut self, table: &str, column_names: Vec<String>) -> Result<Vec<Row>> {
        let schema = &self.get_table_schema(table)?;

        let mut column_schema = self.get_table_columns(schema)?;
        let columns: Vec<(String, Column)> = column_names
            .into_iter()
            .map(|f| {
                let column = column_schema.remove(&f).unwrap();
                (f, column)
            })
            .collect();

        let root_page = self.read_page(schema.root_page)?;
        let mut rows = Vec::<Row>::new();
        match root_page.page_header.page_type {
            PageType::LeafTable => {
                for record_pos in root_page.cell_array {
                    let offset = ((schema.root_page-1) * self.header.page_size as i64) +record_pos;
                    let record = self.read_record(offset)?;
                    let mut cells = Vec::<CellValue>::new();
                    for c in &columns {
                        cells.push(record.values[c.1.column_index.clone() as usize].clone())
                    }

                    rows.push(Row { cells });
                }
            }
            _ => todo!("can't traverse btree yet"),
        }

        Ok(rows)
    }

    fn proccess_sel_item(&mut self, sel_item: &SelectItem) -> Result<Vec<String>> {
        let mut names = Vec::new();

        match sel_item {
            SelectItem::UnnamedExpr(exp) => {
                match exp {
                    Expr::Function(fun) => {
                        if fun.name.0.len() == 1 {
                            match fun.name.0[0].value.as_ref() {
                                "count" => {
                                    if fun.args.len() != 1 {
                                        bail!("count requires 1 arugement");
                                    }

                                    match fun.args.get(0).unwrap() {
                                        FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                            Expr::Identifier(ident),
                                        )) => {
                                            names.push(ident.value.clone());
                                        }
                                        e => bail!("unsported function {}", e),
                                    }
                                }
                                e => bail!("unsported function {}", e),
                            }
                        } else {
                            bail!("only single name functions are supporteed");
                        }
                    }
                    Expr::Identifier(ident) => {
                        names.push(ident.value.clone());
                    }
                    e => bail!("{} is not currenty supported", e),
                };
            }
            e => bail!("{} is not currenty supported", e),
        }
        Ok(names)
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
        let ids: Vec<i64> = cell_array
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

    fn read_record_header(&mut self) -> Result<RecordHeader> {
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
}

#[derive(Debug)]
pub struct DatabaseHeader {
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
    value: i64,
    size: u8,
}
