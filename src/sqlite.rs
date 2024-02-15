use anyhow::Result;
use connection::Connection;

pub mod btree;
pub mod column;
pub mod connection;
pub mod database;
pub mod index_btree;
pub mod page;
pub mod record;
pub mod row;
pub mod schema;
pub mod sql;

#[cfg(test)]
mod tests;

pub fn open(file_path: impl Into<String>) -> Result<Connection> {
    Connection::new(file_path)
}
