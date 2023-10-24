use std::{fs::File, io::Read};
use anyhow::Result;
use connection::{Connection, DatabaseHeader};

pub mod connection;
pub mod page;
pub mod schema;
pub mod column;
pub mod row;
pub mod record;

#[cfg(test)]
mod tests;

pub fn open(file_path: impl Into<String>) -> Result<Connection> {
    let mut file = File::open(file_path.into())?;
    let mut buffer = [0; 100];
    file.read_exact(&mut buffer)?;
    let header = DatabaseHeader {
        page_size: u16::from_be_bytes([buffer[16], buffer[17]]),
    };
    Ok(Connection::new(header, file))
}
