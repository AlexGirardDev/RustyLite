use anyhow::{bail, Context, Ok, Result};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use self::{
    index_interior::IndexInteriorPage, index_leaf::IndexLeafPage, page_header::PageHeader,
    table_interior::TableInteriorPage, table_leaf::TableLeafPage,
};

pub mod index_interior;
pub mod index_leaf;
pub mod page_header;
pub mod table_interior;
pub mod table_leaf;

#[derive(Debug)]
pub enum Page {
    TableLeaf(TableLeafPage),
    TableInterior(TableInteriorPage),
    IndexLeaf(IndexLeafPage),
    IndexInterior(IndexInteriorPage),
}

