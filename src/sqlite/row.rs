use std::{collections::HashMap, rc::Rc};

use crate::sqlite::record::CellValue;
#[derive(Debug)]
pub struct Row {
    pub columns: Rc<HashMap<String, usize>>,
    pub cells: Vec<CellValue>,
}
