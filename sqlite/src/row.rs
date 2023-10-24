use std::{collections::HashMap, rc::Rc};
use crate::record::CellValue;
#[derive(Debug)]
pub struct Row{
    // columns: Rc<HashMap<String,String>>,
    pub cells : Vec<CellValue>
}

// impl Row{
//     pub fn new(columns : Rc<HashMap<String,String>>,cells: impl Into<Vec<CellValue>>)-> Row{
//         Row { columns, cells:cells.into()}
//     }
//
// }




