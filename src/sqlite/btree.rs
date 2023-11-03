use super::schema::SqliteSchema;

pub mod btree;

pub struct BTree {
    schema: SqliteSchema,
}

impl BTree {




}

pub struct RowReader{
    btree: BTree

}
impl Iterator for RowReader{
    type Item = ReaderRow;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct ReaderRow{

}
