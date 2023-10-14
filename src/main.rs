use anyhow::{bail, Result};
use std::default;
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            let page_size = u16::from_be_bytes([header[16], header[17]]);
            println!("Logs from your program will appear here!");

            let page = Page::new(file);

            println!("Logs from your program will appear here!");

            println!("database page size: {}", page_size);
            println!("number of tables: {}", page.page_header.cell_count);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

impl Page {
    fn new(mut file: File) -> Self {
        let mut buffer = [0; 1];
        file.read_exact(&mut buffer).unwrap();
        println!("{:?}", buffer);
        let page_type = match u8::from_be_bytes(buffer) {
            2 => PageType::InteriorIndex,
            5 => PageType::InteriorTable,
            10 => PageType::LeafIndex,
            13 => PageType::LeafTable,
            _ => panic!("invalid page type"),
        };

        let mut buffer = [0; 2];

        file.read_exact(&mut buffer).unwrap();
        let free_block = u16::from_be_bytes(buffer);

        file.read_exact(&mut buffer).unwrap();
        let cell_count = u16::from_be_bytes(buffer);

        file.read_exact(&mut buffer).unwrap();
        let cell_content_area = u16::from_be_bytes(buffer);

        let mut buffer = [0; 1];
        file.read_exact(&mut buffer).unwrap();
        let fragmented_free_bytes = u8::from_be_bytes(buffer);

        Page {
            page_header: PageHeader {
                page_type,
                free_block,
                cell_count,
                cell_content_area,
                fragmented_free_bytes,
                right_pointer: None,
            },
        }
    }
}

struct Page {
    page_header: PageHeader,
}

struct PageHeader {
    page_type: PageType,
    free_block: u16,
    cell_count: u16,
    cell_content_area: u16,
    fragmented_free_bytes: u8,
    right_pointer: Option<u32>,
}

enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}
