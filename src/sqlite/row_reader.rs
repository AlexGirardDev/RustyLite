// use sqlparser::ast::Expr;
//
// use crate::sqlite::{connection::Connection, page::Page};
//
// pub struct TableReader<'a> {
//     root_page: Page,
//     current_id: i64,
//     connection: &'a Connection,
//     selection: Box<Expr>,
// }
//
// impl<'a> TableReader<'a> {
//     pub fn new(root_page: Page, connection: &'a Connection, selection: Box<Expr>) -> Self {
//         TableReader {
//             current_id: 0,
//             connection,
//             root_page,
//             selection
//         }
//     }
//
//     fn (&mut self,cur_id:i64){
//         let current_page = 40;
//     }
// }
// struct Condition {}
//
// impl<'a> Iterator for TableReader<'a> {
//     type Item = u64;
//
//     fn next(&mut self) -> Option<Self::Item> {
//
//         let result = match self.root_page.page_header.page_type {
//
//             PageType::InteriorTable => {
//                 let mut cells = Vec::new();
//                 for id in &page.cell_array {
//                     let cell = self.read_interior_cell(self.get_location(page_id, *id)?)?;
//                     let ids = self.read_all_ids(cell.left_child as i64)?;
//                     for i in ids {
//                         cells.push(i);
//                     }
//                 }
//
//                 cells
//             }
//             PageType::LeafTable => page
//                 .cell_array
//                 .iter()
//                 .map(|f| {
//                     let location = self.get_location(page_id, *f)?;
//                     self.read_leaf_cell_row_id(location)
//                 })
//                 .try_collect()?,
//             _ => todo!(),
//         };
//         Ok(result)
//
//         None
//     }
//
// }
// //
