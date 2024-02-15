use std::{borrow::Cow, fs::File, rc::Rc, vec};

use super::{
    database::Database,
    page::{index_interior::IndexInteriorPage, index_leaf::IndexLeafPage, IndexPage},
    record::CellValue,
    schema::SqliteSchema,
};
use anyhow::{bail, Result};
use itertools::Itertools;
use ptree::{print_tree_with, write_tree_with, PrintConfig, Style, TreeItem};

#[derive(Debug, Clone)]
pub struct IndexNode {
    pub page: IndexPage,
    pub children: Vec<IndexNode>,
}

#[derive(Debug)]
pub struct IndexBTree {
    pub root_node: IndexNode,
    pub schema: Rc<SqliteSchema>,
}

impl IndexNode {
    pub fn new(page: IndexPage, db: &Database) -> Result<IndexNode> {
        Ok(match &page {
            IndexPage::Leaf(_) => IndexNode {
                page,
                children: Vec::new(),
            },
            IndexPage::Interior(i) => {
                let children = IndexBTree::get_child_pages(db, i)?;

                IndexNode { page, children }
            }
        })
    }
    pub fn get_row_ids(&self, db: &Database, value: &CellValue) -> Result<Vec<i64>> {
        match &self.page {
            IndexPage::Leaf(leaf) => self.handle_leaf_page(db, leaf, value),
            IndexPage::Interior(int) => self.handle_interior_page(db, int, value),
        }
    }

    pub fn handle_leaf_page(
        &self,
        db: &Database,
        leaf: &IndexLeafPage,
        value: &CellValue,
    ) -> Result<Vec<i64>> {
        leaf.cell_pointers
            .iter()
            .map(|(page_number, pointer)| {
                let record = db.read_index_record(*page_number, *pointer)?;
                let row_id = db.read_record_cell(&record, 1)?;
                let CellValue::Int(row_id) = row_id else { bail!("row_id must be an int {}",row_id); };
                let cell = db.read_record_cell(&record, 0)?;
                Ok((cell, row_id))
            })
            .filter_map_ok(|(cell_value, row_id)|
                { if &cell_value == value { Some(row_id) } else { None } }
            ).try_collect()
    }

    pub fn handle_interior_page(
        &self,
        db: &Database,
        int: &IndexInteriorPage,
        value: &CellValue,
    ) -> Result<Vec<i64>> {
        let start_index = &int
            .cells
            .iter()
            .find_position(|f| &f.value >= value)
            .map(|f| f.0);
        let end_index = int
            .cells
            .iter()
            .rev()
            .find_position(|f| &f.value <= value)
            .map(|f| f.0);
        if start_index.is_none() && end_index.is_none() {
            return Ok(vec![]);
        }

        let start = start_index.unwrap_or(0);
        let end = self
            .children
            .len()
            .checked_sub(end_index.unwrap_or(0))
            .unwrap_or(self.children.len());
        // dbg!(start, end, self.children());
        if start >= end {
            return Ok(vec![]);
        }
        // start, end, value, self.children.len(), &int.cells;
        int.cells[start..end]
            .iter()
            .map(|f| f.value.clone())
            .collect_vec();
        let row_ids = &self.children[start..end]
            .iter()
            .flat_map(|child| match child.get_row_ids(db, value) {
                Ok(ids) => ids.into_iter().map(Ok).collect_vec(), // Convert each id into an Ok result
                Err(e) => vec![Err(e)], // Convert the error into a single-element vector with an Err result
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(row_ids.to_owned())
    }
}

impl IndexBTree {
    pub fn new(db: &Database, schema: Rc<SqliteSchema>) -> Result<Self> {
        let SqliteSchema::Index(t_schema) = schema.as_ref() else {
            bail!("expected index schema but got table");
        };
        let root_node = IndexNode::new(db.read_index_page(t_schema.root_page, None)?, db)?;
        Ok(IndexBTree {
            root_node,
            schema: schema.clone(),
        })
    }

    pub fn get_row_ids(&self, db: &Database, value: &CellValue) -> Result<Vec<i64>> {
        self.root_node.get_row_ids(db, value)
    }

    fn get_child_pages(db: &Database, page: &IndexInteriorPage) -> Result<Vec<IndexNode>> {
        let mut result = Vec::new();
        for cell in &page.cells {
            let page =
                db.read_index_page(cell.left_child_page_number, Some(cell.value.to_owned()))?;
            let node = IndexNode::new(page, db)?;
            result.push(node);
        }
        Ok(result)
    }

    pub fn pretty_print(&self) -> Result<()> {
        let config = PrintConfig {
            leaf: Style {
                bold: true,
                ..Style::default()
            },
            branch: Style { ..Style::default() },
            ..PrintConfig::default()
        };
        let file_name = format!("trees/{}.txt", self.schema.get_name());
        let file = File::create(file_name)?;
        write_tree_with(&self.root_node, &file, &config)?;
        print_tree_with(&self.root_node, &config)?;
        Ok(())
    }
}

// pub struct IndexRowReader<'a> {
//     db: &'a Database,
//     iter: Box<dyn Iterator<Item = &'a (u32, u16)> + 'a>,
//     key: CellValue
// }
//
// impl<'a> IndexRowReader<'a> {
//     pub fn new(tree: &'a IndexBTree, db: &'a Database, key:CellValue) -> Self {
//         IndexRowReader {
//             iter: tree.root_node.cells(),
//             db,
//             key
//         }
//     }
// }
//
// impl<'a> Iterator for IndexRowReader<'a> {
//     type Item = Result<IndexRow<'a>>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         let record = self
//             .iter
//             .next()
//             .map(|(page_number, pointer)| self.db.read_index_record(*page_number, *pointer))?
//             .unwrap();
//         Some(Ok(IndexRow::new(self.db, record)))
//     }
// }
//
// pub struct IndexRow<'a> {
//     record: Record,
//     db: &'a Database,
// }
//
// impl<'a> IndexRow<'a> {
//     pub fn new(db: &'a Database, record: Record) -> Self {
//         IndexRow { record, db }
//     }
//     pub fn get_row(&self) -> Result<(i64, CellValue)> {
//         let key = self.db.read_record_cell(&self.record, 0)?;
//         let row_id = self.db.read_record_cell(&self.record, 1)?;
//
//         let CellValue::Int(row_id) = row_id else {
//             bail!("row_id must be an int {}",row_id);
//         };
//
//         Ok((row_id, key))
//     }
// }

impl TreeItem for IndexNode {
    type Child = Self;

    fn write_self<W: std::io::Write>(&self, f: &mut W, _: &ptree::Style) -> std::io::Result<()> {
        match &self.page {
            IndexPage::Leaf(leaf) => {
                write!(f, "Leaf-{}", leaf.value) // Writ
            }
            IndexPage::Interior(int) => {
                write!(f, "Interior-{}", int.value)
            }
        }
    }

    fn children(&self) -> std::borrow::Cow<[Self::Child]> {
        match &self.page {
            IndexPage::Leaf(_) => Cow::from(vec![]),
            IndexPage::Interior(_) => self.children.to_owned().into(),
        }
    }
}
