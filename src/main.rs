use anyhow::{bail, Result};
use itertools::Itertools;

use crate::sqlite::{btree::TableBTree, schema::SqliteSchema};

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
            // for name in names {
            //     println!("{}",name);
            // }
            print!("{}", names.join(" "));
        }
        ".schema" => {
            // let tree = conn.get_tree("superheroes".into()).unwrap_or(Ok());
            let tree = conn.get_tree("superheroes".into())?;
            println!("{:?}", tree.schema);
            //
            conn.print_column("apples".into(), "name".into())?;
            // conn.print_column("superheroes".into(), "".into())?;
            // println!("test");
            // println!("{:?}",tree);
            // println!("{:?}", tree);
            // for leaf in TableBTree::get_leaf_cells(&tree.root_node) {
            //     println!("{} - {}", leaf.page_number, leaf.offset);
            // }
            // let schema = dbg!(conn.get_schema()?);
            // let mut table = Table::new();
            // table.add_row(row!["Id", "Type", "Name", "R_Page"]);
            // for sc in schema.iter() {
            //     table.add_row(row![sc.row_id, sc.schema_type, sc.name, sc.root_page]);
            // }
            // table.printstd();
        }
        _query => {
            // c
            // let wow = sql_engine::query(_query);
            let result = conn.query(_query.trim());
            println!("{:?}", result)
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
