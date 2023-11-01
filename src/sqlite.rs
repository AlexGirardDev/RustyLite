use std::{fs::File, io::Read};
use anyhow::Result;
use connection::{Connection, DatabaseHeader};

pub mod connection;
pub mod page;
pub mod schema;
pub mod column;
pub mod row;
pub mod record;
pub mod database;

#[cfg(test)]
mod tests;

pub fn open(file_path: impl Into<String>) -> Result<Connection> {
    Connection::new(file_path)
}
