use anyhow::{bail, Result};
use sqlite::{page::{Page, PageHeader, PageType}, sqlite::Sqlite, sqlite_schema::SchemaType};

use std::{i8, usize};

pub mod sqlite;

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
            let schema = db.get_schema()?;
            println!("database page size: {}", db.header.page_size);
            println!("number of tables: {}", schema.iter().filter(|x|matches!(x.schema_type,SchemaType::Table)).count());
        }

        ".tables" => {
            let schema = db.get_schema()?;
            let names: Vec<String> = schema
                .iter()
                .filter(|x| matches!(x.schema_type, SchemaType::Table))
                .map(|x| x.name.clone())
                .collect();
            print!("{}", names.join(" "));
        }

        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
