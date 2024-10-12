use anyhow::{bail, Result};
use itertools::Itertools;

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
    let conn = sqlite::open(&args[1])?;

    match command.as_str() {
        ".dbinfo" => {
            println!("Logs from your program will appear here!");
            let schema = conn.get_schema();
            println!("database page size: {}", conn.get_header().page_size);
            let c = schema
                .iter()
                .filter(|x| matches!(x.as_ref(), SqliteSchema::Table(_)))
                .count();
            println!("number of tables: {}", c);
        }

        ".tables" => {
            let schema = conn.get_schema();
            let names = schema
                .iter()
                .filter(|x| matches!(x.as_ref(), SqliteSchema::Table(_)))
                .map(|x| match x.as_ref() {
                    SqliteSchema::Table(t) => t.name.clone(),
                    SqliteSchema::Index(i) => i.name.clone(),
                })
                .collect_vec();
            print!("{}", names.join(" "));
        }
        ".schema" => {
            for schema in conn.get_schema() {
                match schema.as_ref() {
                    SqliteSchema::Table(table) => {
                        println!("Table: {}", table.name);
                        for col in &table.columns {
                            println!("{:10} - {} ", col.type_affinity, col.name);
                        }
                    }
                    SqliteSchema::Index(index) => {
                        println!("Table: {:?}", index);
                    }
                };
            }
        }
        ".tree" => {
            for schema in conn.get_schema() {
                match schema.as_ref() {
                    SqliteSchema::Table(table) => {
                        conn.get_tree(&table.name)?.pretty_print()?;
                        println!("Table: {}", table.name);
                        for col in &table.columns {
                            println!("{:15} - {} ", col.name, col.type_affinity);
                        }
                    }
                    SqliteSchema::Index(index) => {
                        conn.get_index_tree(&index.parent_table, &index.column_name)?
                            .pretty_print()?;
                    }
                };
            }
        }
        s if s.starts_with(".page") => {
            let page_number = s.split_once(' ').unwrap().1.parse::<u32>().unwrap();
            conn.dump_page(page_number);
        }
        _query => {
            conn.execute_query(_query.trim())?;
        }
    }

    Ok(())
}
