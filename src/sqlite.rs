
use anyhow::Result;
use connection::{Connection};

pub mod connection;
pub mod page;
pub mod schema;
pub mod column;
pub mod row;
pub mod record;
pub mod database;
pub mod sql;
pub mod btree;

#[cfg(test)]
mod tests;

pub fn open(file_path: impl Into<String>) -> Result<Connection> {
    Connection::new(file_path)
}
