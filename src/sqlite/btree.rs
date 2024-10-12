
use std::{borrow::Cow, rc::Rc, vec};

use super::{
    database::Database,
    page::{table_interior::TableInteriorPage, TablePage},
    record::{CellValue, Record},
    schema::SqliteSchema,
};
use anyhow::{anyhow, bail, Ok, Result};
use ptree::{print_tree_with,  PrintConfig, Style, TreeItem};

#[derive(Debug)]
pub struct TableBTree {
    pub root_node: TableNode,
    pub schema: Rc<SqliteSchema>,
}
#[derive(Debug, Clone)]
pub struct TableNode {
    pub page: TablePage,
    pub children: Vec<TableNode>,
}

impl TableNode {
    pub fn cells<'a>(&'a self) -> Box<dyn Iterator<Item = &(u32, u16)> + 'a> {
        match &self.page {
            TablePage::Leaf(l) => Box::new(l.cell_pointers.iter()),
            TablePage::Interior(_) => Box::new(self.children.iter().flat_map(|n| n.cells())),
        }
    }

    pub fn new(page: TablePage, db: &Database) -> Result<TableNode> {
        Ok(match &page {
            TablePage::Leaf(_) => TableNode {
                page,
                children: Vec::new(),
            },
            TablePage::Interior(i) => {
                let children = TableBTree::get_child_pages(db, i)?;
                TableNode { page, children }
            }
        })
    }
    pub fn get_row(&self, db: &Database, row_id: i64) -> Result<Record> {
        match &self.page {
            TablePage::Leaf(leaf) => {
                let mut row_ids = Vec::<i64>::new();
                for (page_number, pointer) in leaf.cell_pointers.iter() {
                    let record = db.read_record(*page_number, *pointer)?;
                    row_ids.push(record.row_id);
                    if record.row_id == row_id {
                        return Ok(record);
                    }
                }
                bail!("could not find leaf");
            }
            TablePage::Interior(_) => {
                return self
                    .children
                    .iter()
                    .find(|p| row_id <= p.page.get_row_id())
                    .expect("this should exsist")
                    .get_row(db, row_id)
            }
        }
    }
}

impl TableBTree {
    pub fn new(db: &Database, schema: Rc<SqliteSchema>) -> Result<Self> {
        let SqliteSchema::Table(t_schema) = schema.as_ref() else {
            bail!("expected table schema but got index");
        };
        let root_node = TableNode::new(db.read_table_page(t_schema.root_page, None)?, db)?;
        Ok(TableBTree {
            root_node,
            schema: schema.clone(),
        })
    }

    fn get_child_pages(db: &Database, page: &TableInteriorPage) -> Result<Vec<TableNode>> {
        let mut result = Vec::new();
        for cell in &page.cells {
            let page = db.read_table_page(cell.left_child_page_number, Some(cell.row_id))?;
            let node = TableNode::new(page, db)?;
            result.push(node);
        }
        Ok(result)
    }

    pub fn row_reader<'a>(&'a self, db: &'a Database) -> RowReader {
        RowReader::new(self, db)
    }

    pub fn get_row<'a>(&'a self, db: &'a Database, row_id: i64) -> Result<TableRow> {
        let record = self.root_node.get_row(db, row_id)?;
        Ok(TableRow::new(db, record, self.schema.clone()))
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
        print_tree_with(&self.root_node, &config)?;
        Ok(())
    }
}

pub struct RowReader<'a> {
    db: &'a Database,
    iter: Box<dyn Iterator<Item = &'a (u32, u16)> + 'a>,
    schema: Rc<SqliteSchema>,
}
impl<'a> RowReader<'a> {
    pub fn new(tree: &'a TableBTree, db: &'a Database) -> Self {
        RowReader {
            iter: tree.root_node.cells(),
            db,
            schema: tree.schema.clone(),
        }
    }
}

impl<'a> Iterator for RowReader<'a> {
    type Item = Result<TableRow<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = self
            .iter
            .next()
            .map(|(page_number, pointer)| self.db.read_record(*page_number, *pointer))?
            .unwrap();
        Some(Ok(TableRow::new(self.db, record, self.schema.clone())))
    }
}

pub struct TableRow<'a> {
    pub record: Record,
    schema: Rc<SqliteSchema>,
    db: &'a Database,
}

impl<'a> TableRow<'a> {
    pub fn new(db: &'a Database, record: Record, schema: Rc<SqliteSchema>) -> Self {
        TableRow { record, schema, db }
    }
    pub fn read_column(&self, column_name: &str) -> Result<CellValue> {
        let SqliteSchema::Table(schema) = self.schema.as_ref() else {
            unreachable!("this has to be a table schema");
        };

        if column_name == "id" {
            return Ok(CellValue::Int(self.record.row_id));
        }

        let (index, _) = schema
            .columns
            .iter()
            .enumerate()
            .find(|f| *f.1.name == *column_name)
            .ok_or(anyhow!("Invalid column name: {}", column_name))?;

        self.db.read_record_cell(&self.record, index)
    }
}

impl TreeItem for TableNode {
    type Child = Self;

    fn write_self<W: std::io::Write>(&self, f: &mut W, _: &ptree::Style) -> std::io::Result<()> {
        match &self.page {
            TablePage::Leaf(leaf) => {
                write!(f, "{}", leaf.row_id) // Writ
            }
            TablePage::Interior(int) => {
                write!(f, "{}", int.row_id)
            }
        }
    }

    fn children(&self) -> std::borrow::Cow<[Self::Child]> {
        match &self.page {
            TablePage::Leaf(_) => Cow::from(vec![]),
            TablePage::Interior(_) => self.children.to_owned().into(),
        }
    }
}
