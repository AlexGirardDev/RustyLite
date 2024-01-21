use sqlparser::ast::expr;


use crate::sqlite::{connection::connection, page::page};

pub struct tablereader<'a> {
    root_page: page,
    current_id: i64,
    connection: &'a connection,
    selection: box<expr>,
}

impl<'a> tablereader<'a> {
    pub fn new(root_page: page, connection: &'a connection, selection: box<expr>) -> self {
        tablereader {
            current_id: 0,
            connection,
            root_page,
            selection
        }
    }

    fn (&mut self,cur_id:i64){
        let current_page = 40;
    }
}
struct condition {}

impl<'a> iterator for tablereader<'a> {
    type item = u64;

    fn next(&mut self) -> option<self::item> {

        let result = match self.root_page.page_header.page_type {

            pagetype::interiortable => {
                let mut cells = vec::new();
                for id in &page.cell_array {
                    let cell = self.read_interior_cell(self.get_location(page_id, *id)?)?;
                    let ids = self.read_all_ids(cell.left_child as i64)?;
                    for i in ids {
                        cells.push(i);
                    }
                }

                cells
            }
            pagetype::leaftable => page
                .cell_array
                .iter()
                .map(|f| {
                    let location = self.get_location(page_id, *f)?;
                    self.read_leaf_cell_row_id(location)
                })
                .try_collect()?,
            _ => todo!(),
        };
        ok(result)
    }
}
//
