use anyhow::{bail, Result};
use itertools::Itertools;
use prettytable::{row, Table};

use crate::sqlite::schema::SqliteSchema;

pub mod sqlite;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    let mut conn = sqlite::open(&args[1])?;
    
    match command.as_str() {
        ".dbinfo" => {
            println!("Logs from your program will appear here!");
            let schema = conn.get_schema();
            println!("database page size: {}", conn.get_header().page_size);
            println!("number of tables: {}",schema
                .clone()
                .iter()
                .filter(|x| matches!(x, SqliteSchema::Table(_)))
                .count());
        }

        ".tables" => {
            let schema = conn.get_schema();
            let names = schema
                .iter()
                .filter(|x| matches!(x, SqliteSchema::Table(_)))
                .map(|x| {
                    match x{
                        SqliteSchema::Table(t) => t.name.clone(),
                        SqliteSchema::Index(i) => i.name.clone()
                    }
                })
                .collect_vec();
            // for name in names {
            //     println!("{}",name);
            // }
            print!("{}", names.join(" "));
        }
        ".schema" => {
            // let schema = dbg!(conn.get_schema()?);
            // let mut table = Table::new();
            // table.add_row(row!["Id", "Type", "Name", "R_Page"]);
            // for sc in schema.iter() {
            //     table.add_row(row![sc.row_id, sc.schema_type, sc.name, sc.root_page]);
            // }
            // table.printstd();
        }
        ".schema" => {
            // let schema = dbg!(conn.get_schema()?);
            // let mut table = Table::new();
            // table.add_row(row!["Id", "Type", "Name", "R_Page"]);
            // for sc in schema.iter() {
            //     table.add_row(row![sc.row_id, sc.schema_type, sc.name, sc.root_page]);
            // }
            // table.printstd();
        }
        query => {
            // let result = conn.query(query)?;
            // for r in result {
            //     println!(
            //         "{}",
            //         r.cells
            //             .iter()
            //             .map(|f| f.to_string())
            //             .collect::<Vec<String>>()
            //             .join("|")
            //     );
            // }
        }
    }

    Ok(())
}
