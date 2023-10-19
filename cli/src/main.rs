use anyhow::{bail, Result};
use sqlite::{page::{Page, PageHeader, PageType}, sqlite::Sqlite, sqlite_schema::SchemaType};

use std::{i8, usize};

fn main() -> Result<()> {
    // Parse arguments
    // 
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    let mut db = Sqlite::new(&args[1])?;
    match command.as_str() {
        ".dbinfo" => {
            println!("Logs from your program will appear here!");

            // let page = read_page(&file, 100);
            // let schema = get_schema(&file);

            // println!("0{:?}", page);
            // for i in 1..header.number_of_pages {
            //     file.seek(SeekFrom::Start((i * (header.page_size as u32)).into()))
            //         .unwrap();
            //     println!("{i}{:?}", Page::new(&file))
            // }

            println!("Logs from your program will appear here!");
            println!("database page size: {}", db.header.page_size);
            let schema = db.get_schema()?;
            println!("number of tables: {}", schema.iter().filter(|x|matches!(x.schema_type,SchemaType::Table)).count());
        }

        ".tables" => {
            // let page = read_page(&file, 100);
            // let schema = get_schema(&file);
            //
            // let names: Vec<String> = schema
            //     .iter()
            //     .filter(|x| matches!(x.schema_type, SchemaType::Table))
            //     .map(|x| x.name.clone())
            //     .collect();
            // print!("{}", names.join(" "));
        }

        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
// fn get_schema(mut file: &File) -> Vec<Schema> {
//     let page = read_page(file, 100);
//     let mut schema: Vec<Schema> = Vec::new();
//
//     for id in page.cell_array {
//         file.seek(SeekFrom::Start(id as u64)).unwrap();
//         let payload_length = read_varint(&file);
//         let row_id = read_varint(&file);
//         // println!("payload:{:?} - row_id:{:?}", payload_length, row_id);
//         let record_headers = read_record_header(&file);
//         let schema_type = match read_cell(file, &record_headers[0]) {
//             Cell::String(s) => match s.as_ref() {
//                 "table" => SchemaType::Table,
//                 "index" => SchemaType::Index,
//                 "view" => SchemaType::View,
//                 "trigger" => SchemaType::Trigger,
//                 _ => todo!("bad column"),
//             },
//             _ => todo!(),
//         };
//         let Cell::String(name)=  read_cell(file, &record_headers[1]) else {todo!()};
//         let Cell::String(table_name)=  read_cell(file, &record_headers[2]) else {todo!()};
//         let Cell::Int8(root_page)=  read_cell(file, &record_headers[3]) else {todo!()};
//         let Cell::String(sql)=  read_cell(file, &record_headers[4]) else {todo!()};
//
//         schema.push(Schema {
//             schema_type,
//             name,
//             table_name,
//             root_page: root_page.into(),
//             sql,
//         });
//     }
//
//     return schema;
// }
//
// fn read_varint(mut file: &File) -> (u64, u64) {
//     let mut buf = [0; 1];
//     let mut more = true;
//     let mut num: u64 = 0;
//     let mut bytes = 0;
//     while more {
//         file.read_exact(&mut buf).unwrap();
//         bytes += 1;
//         let byte = buf[0] as u8;
//         more = byte & 0b1000_0000 != 0;
//         num <<= 7;
//         num |= u64::from(0b0111_1111 & byte);
//     }
//     (num, bytes)
// }
// fn read_cell(mut file: &File, cell_type: &CellType) -> Cell {
//     return match cell_type {
//         CellType::Null => Cell::Null,
//         CellType::Int8 => {
//             let mut buff = [0; 1];
//             file.read_exact(&mut buff).unwrap();
//             return Cell::Int8(buff[0] as i8);
//         }
//         CellType::Int16 => {
//             let mut buff = [0; 2];
//             file.read_exact(&mut buff).unwrap();
//             return Cell::Int16(i16::from_be_bytes(buff));
//         }
//         CellType::Int24 => {
//             let mut buff = [0; 4];
//             file.read(&mut buff[0..3]).unwrap();
//             return Cell::Int32(i32::from_be_bytes(buff));
//         }
//         CellType::Int32 => {
//             let mut buff = [0; 4];
//             file.read(&mut buff).unwrap();
//             return Cell::Int32(i32::from_be_bytes(buff));
//         }
//         CellType::Int48 => {
//             let mut buff = [0; 8];
//             file.read(&mut buff[0..6]).unwrap();
//             return Cell::Int64(i64::from_be_bytes(buff));
//         }
//         CellType::Int64 => {
//             let mut buff = [0; 8];
//             file.read(&mut buff).unwrap();
//             return Cell::Int64(i64::from_be_bytes(buff));
//         }
//         CellType::Float64 => {
//             let mut buff = [0; 8];
//             file.read(&mut buff).unwrap();
//             return Cell::Float(f64::from_be_bytes(buff));
//         }
//         CellType::Blob(len) => {
//             let mut data = vec![0u8; *len as usize];
//             file.read(&mut data).unwrap();
//             return Cell::Blob(data);
//         }
//         CellType::String(len) => {
//             let mut data = vec![0u8; *len as usize];
//             file.read(&mut data).unwrap();
//             Cell::String(String::from_utf8(data).unwrap())
//         }
//     };
// }
//
// fn read_record_header(mut file: &File) -> Vec<CellType> {
//     let (mut header_size_left, bytes) = read_varint(&mut file);
//
//     header_size_left -= bytes;
//     let mut result: Vec<CellType> = Vec::new();
//     while header_size_left > 0 {
//         let (val, bytes) = read_varint(&mut file);
//         header_size_left -= bytes;
//         let wow = match val {
//             0 => CellType::Null,
//             1 => CellType::Int8,
//             2 => CellType::Int16,
//             3 => CellType::Int24,
//             4 => CellType::Int32,
//             5 => CellType::Int48,
//             6 => CellType::Int64,
//             7 => CellType::Float64,
//             8 => CellType::Int8,
//             9 => CellType::Int8,
//             10 | 11 => CellType::Null,
//             code => {
//                 if code % 2 == 0 {
//                     CellType::Blob(((code - 12) / 2) as isize)
//                 } else {
//                     CellType::String(((code - 12) / 2) as isize)
//                 }
//             }
//         };
//         result.push(wow);
//     }
//     result
// }
//
// #[derive(Debug)]
// enum CellType {
//     Null,
//     Int8,
//     Int16,
//     Int24,
//     Int32,
//     Int48,
//     Int64,
//     Float64,
//     Blob(isize),
//     String(isize),
// }
// #[derive(Debug)]
// enum Cell {
//     Null,
//     Int8(i8),
//     Int16(i16),
//     Int32(i32),
//     Int64(i64),
//     Float(f64),
//     Blob(Vec<u8>),
//     String(String),
// }
//
// fn read_page(mut file: &File, offset: u64) -> Page {
//     let mut buffer = [0; 1];
//     file.seek(SeekFrom::Start(offset));
//     file.read_exact(&mut buffer).unwrap();
//     let page_type = match u8::from_be_bytes(buffer) {
//         2 => PageType::InteriorIndex,
//         5 => PageType::InteriorTable,
//         10 => PageType::LeafIndex,
//         13 => PageType::LeafTable,
//         _ => panic!("invalid page type"),
//     };
//
//     let mut buffer = [0; 2];
//
//     file.read_exact(&mut buffer).unwrap();
//     let free_block = u16::from_be_bytes(buffer);
//
//     file.read_exact(&mut buffer).unwrap();
//     let cell_count = u16::from_be_bytes(buffer);
//
//     file.read_exact(&mut buffer).unwrap();
//     let cell_content_area_offset = u16::from_be_bytes(buffer);
//
//     let mut buffer = [0; 1];
//     file.read_exact(&mut buffer).unwrap();
//     let fragmented_free_bytes = u8::from_be_bytes(buffer);
//
//     let mut cell_array: Vec<u8> = vec![0; cell_count as usize * 2];
//
//     file.read_exact(cell_array.as_mut_slice());
//     let ids: Vec<u64> = cell_array
//         .chunks(2)
//         .map(|f| u16::from_be_bytes([f[0], f[1]]).into())
//         .collect();
//
//     Page {
//         page_header: PageHeader {
//             page_type,
//             free_block,
//             cell_count,
//             cell_content_area_offset,
//             fragmented_free_bytes,
//             right_pointer: None,
//         },
//         cell_array: ids,
//     }
// }
//
//
