// use std::{rc::Rc};

use std::{borrow::Cow, fs::File, rc::Rc, vec};

use super::{
    database::Database,
    page::{table_interior::TableInteriorPage, TablePage},
    record::{CellValue, Record},
    schema::SqliteSchema,
};
use anyhow::{anyhow, bail, Result};
use ptree::{
    print_tree_with, write_tree_with,
    PrintConfig, Style, TreeItem,
};

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
}

impl TableBTree {
    pub fn new(db: &Database, schema: Rc<SqliteSchema>) -> Result<Self> {
        let SqliteSchema::Table(t_schema) = schema.as_ref() else {
            bail!("expected table schema but got index");
        };
        let root_node = TableNode::new(db.read_table_page(t_schema.root_page)?, db)?;
        Ok(TableBTree {
            root_node,
            schema: schema.clone(),
        })
    }

    fn get_child_pages(db: &Database, page: &TableInteriorPage) -> Result<Vec<TableNode>> {
        let mut result = Vec::new();
        for cell in &page.cells {
            let page = db.read_table_page(cell.left_child_page_number)?;
            let node = TableNode::new(page, db)?;
            result.push(node);
        }
        Ok(result)
    }

    pub fn row_reader<'a>(&'a self, db: &'a Database) -> RowReader {
        RowReader::new(self, db)
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

impl TreeItem for TableNode {
    type Child = Self;

    fn write_self<W: std::io::Write>(
        &self,
        f: &mut W,
        _: &ptree::Style,
    ) -> std::io::Result<()> {
        match &self.page {
            TablePage::Leaf(leaf) => {
                write!(f, "Leaf-{}", leaf.page_number) // Writ
            }
            TablePage::Interior(int) => {
                write!(f, "Interior-{}", int.page_number)
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
    record: Record,
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

//     pub fn get_leaf_cells<'a>(
//         page: &'a Page2,
//         db: &'a Database,
//     ) -> Box<Iterator<Item = &TableLeafCell> + 'a> {
//         Box::new(
//             page.cell_pointers
//                 .iter()
//                 .map(|f| {
//                     match page.table_page {
//                         TablePage::Leaf(_) => vec![TableLeafCell {
//                             page_number: page.page_number,
//                             offset: *f,
//                         }]
//                         .into_iter(),
//                         TablePage::Interior(_) => {
//                             // TableLeafCell{page_number:2, offset:2}
//                             let wow =
//                                 TableInteriorCell::read_cell(page.page_number, *f, db).unwrap();
//                             let page2 = db.read_table_page(wow.left_child_page_number).unwrap();
//                             TableBTree::get_leaf_cells(page, db)
//                         }
//                     }
//                 })
//                 .flatten(),
//         )
//     }
//
//     // match (db,&pge) {
//     //     (_,TablePage::Leaf(leaf)) => {
//     //         println!("{:?}", leaf.cell_pointers);
//     //         let result = leaf.cell_pointers.iter().map(|f| {
//     //             println!("{f}");
//     //             TableLeafCell {
//     //                 page_number: leaf.page_number,
//     //                 offset: *f,
//     //             }
//     //         });
//     //         result
//     //     },
//     //     (db2,TablePage::Interior(interior)) => {
//     //         // let Some(children) = &interior.cell_pointers else {panic!("")};
//     //
//     //
//     //         let result = interior.cell_pointers.iter().map(|f| {
//     //             let wow = TableInteriorCell::read_cell(interior.page_number, *f, db2).unwrap();
//     //             let page2 = db2.read_table_page(wow.left_child_page_number).unwrap();
//     //             TableBTree::get_leaf_cells(&page2, db2)
//     //
//     //         });
//     //         result.flatten()
//     //             // TableBTree::get_leaf_cells(db.read)});
//     //
//     //
//     //         // TableBTree::get_leaf_cells(&children[0])
//     //
//     //     }
//     // }
//     // }
//
//     // pub fn values(&self) -> impl {
//     //     let wow = self.root_node.children.unwrap().iter().map(|n| n.values()).flatten();
//     //     todo!()
//     // }
// }
// // pub struct Node {
// //     pub values: Vec<i32>,
// //     pub children: Vec<Node>,
// // }
// //
// // impl Node {
// // }
// //
// //
// // fn main() {
// //     let n = Node {
// //         values: vec![1, 2, 3],
// //         children: vec![
// //             Node {
// //                 values: vec![4, 5],
// //                 children: vec![
// //                     Node {
// //                         values: vec![4, 5],
// //                         children: vec![],
// //                     },
// //                     Node {
// //                         values: vec![6, 7],
// //                         children: vec![],
// //                     },
// //                 ],
// //             },
// //             Node {
// //                 values: vec![6, 7],
// //                 children: vec![],
// //             },
// //         ],
// //     };
// //     let v: Vec<_> = n.values().collect();
// //     println!("v = {:?}", v);
// // }
// //
// // // #[derive(Debug)]
// // // pub struct TableInteriorPage {
// // //     pub page_number: i64,
// // //     pub header:PageHeader,
// // //     pub right_cell: u32,
// // //     pub cell_pointers:Vec<u16>
// // // }
// // //
// // // #[derive(Debug)]
// // // pub struct TableInteriorCell {
// // //     pub page_number: i64,
// // //     pub child_page_number: i64,
// // //     pub row_id: i64,
// // // }
// // //
// // //
// // // #[derive(Debug)]
// // // pub struct TableLeafPage {
// // //     pub page_number: i64,
// // //     pub header: PageHeader,
// // //     pub cell_pointers: Vec<u16>,
// // //     // pub cells: Vec<TableLeafCell>,
// // // }
// // //
// // // #[derive(Debug)]
// // // pub struct TableLeafCell {
// // //     pub row_id: i64,
// // //     pub location: i64,
// // //     pub payload_size: i64,
// // //     pub record_header: RecordHeader,
// // // }
