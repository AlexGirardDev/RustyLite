use std::{borrow::Cow, fs::File, rc::Rc, vec};

use super::{
    btree::TableRow,
    database::Database,
    page::{index_interior::IndexInteriorPage, table_interior::TableInteriorPage, IndexPage},
    record::{CellValue, Record},
    schema::SqliteSchema,
};
use anyhow::{anyhow, bail, Result};
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
    pub fn cells<'a>(&'a self) -> Box<dyn Iterator<Item = &(u32, u16)> + 'a> {
        match &self.page {
            IndexPage::Leaf(l) => Box::new(l.cell_pointers.iter()),
            IndexPage::Interior(_) => Box::new(self.children.iter().flat_map(|n| n.cells())),
        }
    }

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
}

impl IndexBTree {
    pub fn new(db: &Database, schema: Rc<SqliteSchema>) -> Result<Self> {
        let SqliteSchema::Index(t_schema) = schema.as_ref() else {
            bail!("expected index schema but got table");
        };
        let root_node = IndexNode::new(db.read_index_page(t_schema.root_page)?, db)?;
        Ok(IndexBTree {
            root_node,
            schema: schema.clone(),
        })
    }

    fn get_child_pages(db: &Database, page: &IndexInteriorPage) -> Result<Vec<IndexNode>> {
        let mut result = Vec::new();
        for cell in &page.cells {
            let page = db.read_index_page(cell.left_child_page_number)?;
            let node = IndexNode::new(page, db)?;
            result.push(node);
        }
        Ok(result)
    }

    pub fn row_reader<'a>(&'a self, db: &'a Database, key: CellValue) -> IndexRowReader {
        IndexRowReader::new(self, db, key)
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

pub struct IndexRowReader<'a> {
    db: &'a Database,
    iter: Box<dyn Iterator<Item = &'a (u32, u16)> + 'a>,
    key: CellValue
}

impl<'a> IndexRowReader<'a> {
    pub fn new(tree: &'a IndexBTree, db: &'a Database, key:CellValue) -> Self {
        IndexRowReader {
            iter: tree.root_node.cells(),
            db,
            key
        }
    }
}

impl<'a> Iterator for IndexRowReader<'a> {
    type Item = Result<IndexRow<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = self
            .iter
            .next()
            .map(|(page_number, pointer)| self.db.read_index_record(*page_number, *pointer))?
            .unwrap();
        Some(Ok(IndexRow::new(self.db, record)))
    }
}

pub struct IndexRow<'a> {
    record: Record,
    db: &'a Database,
}

impl<'a> IndexRow<'a> {
    pub fn new(db: &'a Database, record: Record) -> Self {
        IndexRow { record, db }
    }
    pub fn get_row(&self) -> Result<(i64, CellValue)> {
        let key = self.db.read_record_cell(&self.record, 0)?;
        let row_id = self.db.read_record_cell(&self.record, 1)?;

        let CellValue::Int(row_id) = row_id else {
            bail!("row_id must be an int {}",row_id);
        };

        Ok((row_id, key))
    }
}

impl TreeItem for IndexNode {
    type Child = Self;

    fn write_self<W: std::io::Write>(&self, f: &mut W, _: &ptree::Style) -> std::io::Result<()> {
        match &self.page {
            IndexPage::Leaf(leaf) => {
                write!(f, "Leaf-{}", leaf.page_number) // Writ
            }
            IndexPage::Interior(int) => {
                write!(f, "Interior-{}", int.page_number)
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
