use anyhow::{bail, Result};
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
            file.read_exact(&mut header,)?;

            let page_size = u16::from_be_bytes([header[16], header[17]]);
            println!("Logs from your program will appear here!");

            let mut b_tree_header = [0; 8];
            file.read_exact(&mut b_tree_header)?;

            let num_of_rows = u16::from_be_bytes([b_tree_header[3], b_tree_header[4]]);
            println!("Logs from your program will appear here!");

            println!("database page size: {}", page_size);
            println!("number of tables: {}", num_of_rows);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
