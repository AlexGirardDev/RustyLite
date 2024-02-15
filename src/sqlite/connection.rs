use anyhow::{bail, Ok, Result};
use itertools::Itertools;

use sqlparser::{
    ast::{BinaryOperator, Expr, Ident, Value},
    dialect::SQLiteDialect,
    parser::Parser,
};
use std::rc::Rc;

use crate::sqlite::{
    btree::TableRow, record::CellValue, schema::SqliteSchema, sql::sql_engine::Operator,
};

use super::{
    btree::TableBTree,
    database::Database,
    index_btree::IndexBTree,
    sql::sql_engine::{self, AggregateFunction, Expression, Object, Query},
};

static DIALECT: SQLiteDialect = SQLiteDialect {};
pub struct Connection {
    db: Database,
}

impl Connection {
    pub fn new(file_path: impl Into<String>) -> Result<Connection> {
        Ok(Connection {
            db: Database::new(file_path)?,
        })
    }

    pub fn get_schema(&self) -> Vec<Rc<SqliteSchema>> {
        self.db.get_schemas()
    }

    pub fn get_header(&self) -> &DatabaseHeader {
        &self.db.header
    }

    pub fn execute_query(&self, sql: impl AsRef<str>) -> Result<()> {
        let mut ast = Parser::parse_sql(&DIALECT, sql.as_ref())?;
        let exp: Query = match (ast.pop(), ast.pop()) {
            (Some(s), None) => (&s).try_into()?,
            _ => bail!("only a single expression is currently supported"),
        };

        let Query::Select(select) = exp;

        if select.sources.len() != 1 {
            bail!("only a single source is currently supported")
        }
        let source_name = match &select.sources[0] {
            sql_engine::Source::Table(t) => t.to_owned(),
        };

        // handle aggregate functions
        if select
            .selections
            .iter()
            .any(|f| matches!(f, sql_engine::Selection::AggFn(_)))
        {
            let tree = self.get_tree(&source_name)?;
            let agg_fn: &AggregateFunction = select
                .selections
                .iter()
                .map(|sel_item| match sel_item {
                    sql_engine::Selection::Identifier(_) => panic!("ruh roh"),
                    sql_engine::Selection::AggFn(a) => a,
                })
                .collect_vec()[0];

            match agg_fn {
                AggregateFunction::Count => {
                    let mut count = 0;
                    for row in tree.row_reader(&self.db) {
                        let row = row?;
                        if Connection::evalute_clause(&row, &select.clause)? {
                            count += 1;
                        }
                    }
                    println!("{count}");
                }
            }
            return Ok(());
        }

        let columns: Vec<String> = select
            .selections
            .iter()
            .map(|sel_item| match sel_item {
                sql_engine::Selection::Identifier(i) => Ok(i.to_owned()),
                sql_engine::Selection::AggFn(_) => {
                    bail!("can't mix agg and table values")
                }
            })
            .try_collect()?;

        let indexes = self.db.get_table_indexes(&source_name);

        let where_columns = match &select.clause {
            Some(exp) => exp.get_columns(),
            None => vec![],
        };
        if !where_columns.is_empty() && where_columns.iter().all(|f| indexes.contains(f)) {
            if where_columns.len() != 1 {
                bail!("only single column index where clauses are supported");
            }
            let clause = &select.clause.unwrap();
            let (column_name, value) = match clause {
                Expression::InfixExpression(left, Operator::Equal, right) => {
                    match (left.as_ref(), right.as_ref()) {
                        (Expression::Identifier(ident), Expression::Literal(lit)) => (ident, lit),
                        _ => bail!("invalide indxed where clause"),
                    }
                }
                _ => bail!("invalide indxed where clause"),
            };
            let index_tree = self.get_index_tree(&source_name, column_name)?;
            let tree = self.get_tree(&source_name)?;
            for row_id in index_tree.get_row_ids(&self.db, value)? {
                let row = tree.get_row(&self.db, row_id);
                let row = row?;

                let values: Vec<CellValue> =
                    columns.iter().map(|f| row.read_column(f)).try_collect()?;
                println!("{}", values.iter().map(|f| f.to_string()).join("|"));
            }
            // for row in tree.(&self.db) {
            // let row = row?;
            // if !Connection::evalute_clause(&row, &select.clause)? {
            //     continue;
            // }
            //
            // let values: Vec<CellValue> =
            //     columns.iter().map(|f| row.read_column(f)).try_collect()?;
            // println!("{}", values.iter().map(|f| f.to_string()).join("|"));
            // }
            return Ok(());
        }

        let tree = self.get_tree(&source_name)?;
        for row in tree.row_reader(&self.db) {
            let row = row?;
            if !Connection::evalute_clause(&row, &select.clause)? {
                continue;
            }

            let values: Vec<CellValue> =
                columns.iter().map(|f| row.read_column(f)).try_collect()?;
            println!("{}", values.iter().map(|f| f.to_string()).join("|"));
        }
        Ok(())
    }

    fn evalute_clause(row: &TableRow, expression: &Option<Expression>) -> Result<bool> {
        match expression {
            Some(val) => match Connection::evalute_exp(row, val)? {
                Object::Bool(b) => Ok(b),
                _ => bail!("bool expceted as result from where clause"),
            },
            None => Ok(true),
            _ => bail!("where clause must resolve to bool"),
        }
    }

    fn evalute_exp(row: &TableRow, exp: &Expression) -> Result<Object> {
        //i'm not dealing with precedence at all here,
        //this is just a hack to get where clauses mostly working for now
        match exp {
            Expression::InfixExpression(left, op, right) => {
                let l = Connection::evalute_exp(row, left)?;
                let r = Connection::evalute_exp(row, right)?;
                let result = match (l, op, r) {
                    (Object::Bool(l), Operator::Equal, Object::Bool(r)) => l == r,
                    (Object::Bool(l), Operator::NotEqual, Object::Bool(r)) => l != r,
                    (Object::Bool(l), Operator::And, Object::Bool(r)) => l && r,
                    (Object::Bool(l), Operator::Or, Object::Bool(r)) => l || r,

                    (Object::String(l), Operator::Equal, Object::String(r)) => l == r,
                    (Object::String(l), Operator::NotEqual, Object::String(r)) => l != r,
                    (l, ex, r) => bail!(
                        "invalid operation, cannot use {:?} with {:?} and {:?}",
                        ex,
                        l,
                        r
                    ),
                };

                Ok(Object::Bool(result))
            }
            Expression::Literal(l) => Ok(Object::String(l.to_string())),
            Expression::Identifier(i) => {
                let value = row.read_column(i)?;
                let val = match value {
                    CellValue::Int(i) => i.to_string(),
                    CellValue::Float(f) => f.to_string(),
                    CellValue::Blob(_) => bail!("Can't use blob field in where clause"),
                    CellValue::String(s) => s,
                    CellValue::Null => "NULL".to_string(),
                };

                Ok(Object::String(val))
            }
        }
    }

    pub fn query(&self, sql: impl AsRef<str>) -> Result<()> {
        let mut ast = Parser::parse_sql(&DIALECT, sql.as_ref())?;

        eprintln!("Query: {}", sql.as_ref());
        eprintln!("ast: {:?}", ast);
        eprintln!();

        let exp = match (ast.pop(), ast.pop()) {
            (Some(s), None) => s,
            _ => bail!("only a single expression is currently supported"),
        };
        let mut select = match exp {
            sqlparser::ast::Statement::Query(q) => match *q.body {
                sqlparser::ast::SetExpr::Select(select) => select,
                e => bail!("{} queries are not currently supported", e),
            },
            q => bail!("{} queries are not currently supported", q),
        };

        let source = match (select.from.pop(), select.from.pop()) {
            (Some(s), None) => s,
            _ => bail!("only a single source is currenly supported"),
        };

        let source_name = match source.relation {
            sqlparser::ast::TableFactor::Table { mut name, .. } => {
                match (name.0.pop(), name.0.pop()) {
                    (Some(n), None) => n.value,
                    _ => bail!("only a single expression is currently supported"),
                }
            }
            _ => bail!("currently only table sources are supported"),
        };

        let tree = self.get_tree(&source_name)?;
        // UnnamedExpr(Function(Function { name: ObjectName([Ident { value: "count", quote_style: None }]), args: [Unnamed(Wildcard)],

        let columns: Vec<String> = select
            .projection
            .iter()
            .map(|sel_item| match sel_item {
                sqlparser::ast::SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value, ..
                })) => Ok(value.to_owned()),

                _ => bail!("only field names are currently supported in selects"),
            })
            .try_collect()?;
        let where_clause = Connection::generate_clause(select.selection)?;

        for row in tree.row_reader(&self.db) {
            let row = row?;

            if !where_clause(&row)? {
                continue;
            }
            let values: Vec<CellValue> =
                columns.iter().map(|f| row.read_column(f)).try_collect()?;
            println!("{}", values.iter().map(|f| f.to_string()).join("|"));
        }
        Ok(())
    }

    fn generate_clause(selection: Option<Expr>) -> Result<SqlRowClause> {
        Ok(match selection {
            Some(Expr::BinaryOp { left, op, right }) => match (*left, *right) {
                (Expr::Identifier(l), Expr::Value(Value::SingleQuotedString(r))) => {
                    Box::new(move |row: &TableRow| {
                        let left_value = row.read_column(&l.value)?;
                        let right_value = CellValue::String(r.to_owned());
                        Ok(match op {
                            BinaryOperator::Eq => left_value == right_value,
                            BinaryOperator::NotEq => left_value != right_value,
                            _ => bail!("invalid conditoin operator"),
                        })
                    })
                }
                _ => bail!("this type of where clause is not currently supported"),
            },
            _ => Box::new(|_| Ok(true)),
        })
    }

    pub fn get_tree(&self, table_name: impl AsRef<str>) -> Result<TableBTree> {
        let schema = &self.db.get_table_schema(table_name)?;
        let wow = TableBTree::new(&self.db, schema.clone())?;
        Ok(wow)
    }

    pub fn get_index_tree(
        &self,
        table_name: impl AsRef<str>,
        column_name: impl AsRef<str>,
    ) -> Result<IndexBTree> {
        let schema = &self.db.get_index_schema(table_name, column_name)?;
        let wow = IndexBTree::new(&self.db, schema.clone())?;
        Ok(wow)
    }
    pub fn get_db(&self) -> &Database {
        &self.db
    }

    pub fn print_column(&self, table_name: String, column_name: String) -> Result<()> {
        let schema = &self.db.get_table_schema(table_name)?;
        let tree = TableBTree::new(&self.db, schema.clone())?;
        let reader = tree.row_reader(&self.db);
        for r in reader {
            let row = r.unwrap();
            let value = row.read_column(&column_name)?;
            println!("{}", value);
        }
        Ok(())
    }
    // pub fn get_row_reader<>(&'a self,tree: &'a TableBTree )->&RowReader{
    //     &tree.row_reader(&self.db)
    //
    // }
    //     let page = self.read_page()?;
    //
    //     let mut schema: Vec<SqliteSchema> = Vec::new();
    //
    //     for id in page.cell_array {
    //         let mut record = self.read_record(id)?;
    //         if record.record_header.headers.len() != 5 {
    //             bail!("Schema table must have 5 fields");
    //         }
    //
    //         let CellValue::String(sql)= record.values.pop().unwrap() else {bail!("sql must be a string field")};
    //         let CellValue::Int(root_page)=  record.values.pop().unwrap() else {bail!("root_page must be an int")};
    //         let CellValue::String(table_name)=  record.values.pop().unwrap() else {bail!("table_name must be a string field")};
    //         let CellValue::String(name)=  record.values.pop().unwrap() else {bail!("name must be a string")};
    //         let schema_type = match record.values.pop().unwrap() {
    //             CellValue::String(s) => match s.as_ref() {
    //                 "table" => SchemaType::Table,
    //                 "index" => SchemaType::Index,
    //                 "view" => SchemaType::View,
    //                 "trigger" => SchemaType::Trigger,
    //                 _ => bail!("invalid schema type"),
    //             },
    //             _ => bail!("type column must be string"),
    //         };
    //
    //         schema.push(SqliteSchema {
    //             row_id: record.row_id,
    //             schema_type,
    //             name,
    //             table_name,
    //             root_page,
    //             sql,
    //         });
    //     }
    //
    //     Ok(schema)
    // }
    //
    // pub fn count_rows(&mut self, table_name: &str) -> Result<i64> {
    //     let schemas = self.get_schema()?;
    //     let Some(schema) = schemas.iter().find(|x| x.table_name == table_name) else{
    //         return Ok(0);
    //     };
    //     let page = self.read_page(schema.root_page)?;
    //
    //     return Ok(page.cell_array.len() as i64);
    // }
    //
    // pub fn execute(&mut self, sql: &str) -> Result<()> {
    //     todo!();
    // }

    // pub fn query(&mut self, sql: &str) -> Result<Vec<Row>> {
    //     let ast = Parser::parse_sql(&DIALECT, sql)?;
    //     let mut rows = Vec::<Row>::new();
    //     for exp in ast {
    //         match exp {
    //             ast::Statement::Query(query) => match *query.body {
    //                 ast::SetExpr::Select(sel) => {
    //                     let mut columns = Vec::<SelectCell>::new();
    //                     for sel_item in sel.projection {
    //                         columns.append(&mut self.proccess_sel_item(&sel_item)?)
    //                     }
    //
    //                     let TableFactor::Table { name,.. } = &sel.from.get(0).context("table name is required")?.relation else{
    //                             bail!("only selecting from tables is currently supported");
    //                         };
    //
    //                     let row = &mut self.read_table(
    //                         &name.0.get(0).context("table name is required")?.value,
    //                         columns,
    //                     )?;
    //
    //                     match sel.selection {
    //                         Some(selection) => match selection {
    //                             Expr::BinaryOp { left, op, right } => {}
    //                             e => bail!("{} is not currently supported", e),
    //                         },
    //                         None => rows.append(row),
    //                     }
    //
    //                     rows.append(row);
    //                 }
    //                 e => bail!("{} is not currenty supported", e),
    //             },
    //
    //             e => bail!("{} is not currenty supported", e),
    //         }
    //     }
    //     Ok(rows)
    // }
    //
    // fn get_row_ids(&mut self, table: String, selection: Expr) -> Result<Vec<i64>> {
    //     todo!()
    // }
    //
    // fn sql_exp_to_cell_value(&mut self, row: Row, exp: Expr) -> Result<CellValue> {
    //     match exp {
    //         Expr::Identifier(i) => {
    //             let column = row.columns.as_ref().get(&i.value);
    //         }
    //         Expr::Value(v) => {
    //             todo!()
    //         }
    //         _ => todo!(),
    //     }
    //
    //     todo!()
    // }

    // fn get_table_schema(&mut self, table: &str) -> Result<SqliteSchema> {
    //     match self.get_schema()?.into_iter().find(|f| f.name == table) {
    //         Some(s) => Ok(s),
    //         None => bail!("could not find table"),
    //     }
    // }

    // fn get_table_columns(&mut self, scheama: &SqliteSchema) -> Result<HashMap<String, Column>> {
    //     match &scheama.schema_type {
    //         SchemaType::Table => {
    //             let ast = Parser::parse_sql(&DIALECT, &scheama.sql)?;
    //             if ast.len() != 1 {
    //                 bail!("table sqchema sql can only have 1 expression");
    //             }
    //             match ast.get(0).unwrap() {
    //                 ast::Statement::CreateTable { columns, .. } => {
    //                     return Ok(columns
    //                         .iter()
    //                         .enumerate()
    //                         .map(|(i, f)| {
    //                             (
    //                                 f.name.value.to_owned(),
    //                                 Column {
    //                                     type_affinity: (&f.data_type).into(),
    //                                     column_index: Some(i as i64),
    //                                 },
    //                             )
    //                         })
    //                         .collect())
    //                 }
    //                 t => bail!("{} is not currently supported", t),
    //             }
    //         }
    //         t => bail!("{} is not currently supported", t),
    //     }
    // }

    // fn handle_aggregate_select(
    //     &mut self,
    //     table: &str,
    //     select_columns: Vec<SelectCell>,
    // ) -> Result<Vec<Row>> {
    //     let mut cells = Vec::<CellValue>::new();
    //
    //     for col in select_columns.iter() {
    //         match col {
    //             SelectCell::NamedColumn(name) => {
    //                 bail!("cannot get column value for {} in an aggregate query", name)
    //             }
    //             SelectCell::TotalRowCount => {
    //                 cells.push(CellValue::Int(self.count_rows(table)?));
    //             }
    //         };
    //     }
    //
    //     let schema = &self.get_table_schema(table)?;
    //     Ok(vec![Row {
    //         cells,
    //         columns: Rc::new(Connection::column_hashmap(
    //             self.get_table_columns(schema)?,
    //             &select_columns,
    //         )),
    //     }])
    // }

    // fn column_hashmap(
    //     mut column_schema: HashMap<String, Column>,
    //     select_columns: &Vec<SelectCell>,
    // ) -> HashMap<String, Column> {
    //     select_columns
    //         .into_iter()
    //         .map(|f| match f {
    //             SelectCell::NamedColumn(name) => {
    //                 let column = column_schema.remove(name).unwrap();
    //                 (name.clone(), column)
    //             }
    //             SelectCell::TotalRowCount => (
    //                 String::from("count(*)"),
    //                 Column {
    //                     type_affinity: TypeAffinity::Int,
    //                 },
    //             ),
    //         })
    //         .collect()
    // }

    // fn read_table(&mut self, table: &str, select_columns: Vec<SelectCell>) -> Result<Vec<Row>> {
    //     let schema = &self.get_table_schema(table)?;
    //
    //     let column_schema = self.get_table_columns(schema)?;
    //     let aggregate_query = select_columns.iter().any(|f| match f {
    //         SelectCell::NamedColumn(_) => false,
    //         SelectCell::TotalRowCount => true,
    //     });
    //
    //     if aggregate_query {
    //         return self.handle_aggregate_select(table, select_columns);
    //     }
    //
    //     let columns = Rc::new(Connection::column_hashmap(column_schema, &select_columns));
    //     let root_page = self.read_page(schema.root_page)?;
    //     let mut rows = Vec::<Row>::new();
    //     match root_page.page_header.page_type {
    //         PageType::LeafTable => {
    //             for record_pos in root_page.cell_array {
    //                 let offset =
    //                     ((schema.root_page - 1) * self.header.page_size as i64) + record_pos;
    //                 let record = self.read_record(offset)?;
    //                 let mut cells = Vec::<CellValue>::new();
    //                 let columns_hashmap = columns.clone();
    //                 for c in &select_columns {
    //                     let column_key = match c {
    //                         SelectCell::NamedColumn(name) => name,
    //                         SelectCell::TotalRowCount => bail!(""),
    //                     }
    //                     .clone();
    //                     let value = columns_hashmap[&column_key].column_index.unwrap();
    //                     cells.push(record.values[value as usize].clone())
    //                 }
    //
    //                 rows.push(Row {
    //                     cells,
    //                     columns: columns_hashmap,
    //                 });
    //             }
    //         }
    //         _ => todo!("can't traverse btree yet"),
    //     }
    //
    //     Ok(rows)
    // }

    // fn count_rows(&mut self, table: &str) -> Result<i64> {
    //     let schema = &self.get_table_schema(table)?;
    //     let root_page = self.read_page(schema.root_page)?;
    //     let mut count: i64 = 0;
    //     match root_page.page_header.page_type {
    //         PageType::LeafTable => {
    //             count += root_page.cell_array.len() as i64;
    //         }
    //         _ => todo!("can't traverse btree yet"),
    //     }
    //     Ok(count)
    // }

    // fn proccess_sel_item(&mut self, sel_item: &SelectItem) -> Result<Vec<SelectCell>> {
    //     let mut names = Vec::new();
    //
    //     match sel_item {
    //         SelectItem::UnnamedExpr(exp) => {
    //             match exp {
    //                 Expr::Function(fun) => {
    //                     if fun.name.0.len() == 1 {
    //                         match fun.name.0[0].value.as_ref() {
    //                             "count" => {
    //                                 if fun.args.len() != 1 {
    //                                     bail!("count requires 1 arugement");
    //                                 }
    //
    //                                 match fun.args.get(0).unwrap() {
    //                                     FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
    //                                         Expr::Identifier(ident),
    //                                     )) => {
    //                                         names.push(SelectCell::NamedColumn(
    //                                             ident.value.clone().into(),
    //                                         ));
    //                                     }
    //                                     FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
    //                                         names.push(SelectCell::TotalRowCount);
    //                                     }
    //                                     e => bail!("unsported function {}--", e),
    //                                 }
    //                             }
    //                             e => bail!("unsported function {}", e),
    //                         }
    //                     } else {
    //                         bail!("only single name functions are supporteed");
    //                     }
    //                 }
    //                 Expr::Identifier(ident) => {
    //                     names.push(SelectCell::NamedColumn(ident.value.to_owned()));
    //                 }
    //                 e => bail!("{} is not currenty supported", e),
    //             };
    //         }
    //         e => bail!("{} is not currenty supported", e),
    //     }
    //     Ok(names)
    // }

    // fn read_all_ids(&mut self, page_id: i64) -> Result<Vec<i64>> {
    //     let page = self.read_page(page_id).unwrap();
    //
    //     let result = match page.page_header.page_type {
    //         PageType::InteriorTable => {
    //             let mut cells = Vec::new();
    //             for id in &page.cell_array {
    //                 let cell = self.read_interior_cell(self.get_location(page_id, *id)?)?;
    //                 let ids = self.read_all_ids(cell.left_child as i64)?;
    //                 for i in ids {
    //                     cells.push(i);
    //                 }
    //             }
    //
    //             cells
    //         }
    //         PageType::LeafTable => page
    //             .cell_array
    //             .iter()
    //             .map(|f| {
    //                 let location = self.get_location(page_id, *f)?;
    //                 self.read_leaf_cell_row_id(location)
    //             })
    //             .try_collect()?,
    //         _ => todo!(),
    //     };
    //     Ok(result)
    // }
    // fn read_interior_cell(&mut self, location: i64) -> Result<InteriorCell> {
    //     self.file.seek(SeekFrom::Start(location as u64))?;
    //     let mut buffer = [0; 4];
    //     self.file.read_exact(&mut buffer).unwrap();
    //     let left_child = u32::from_be_bytes(buffer) as usize;
    //     let row_id = self.read_varint()?.value;
    //     Ok(InteriorCell { left_child, row_id })
    // }
    // fn read_leaf_cell_row_id(&mut self, location: i64) -> Result<i64> {
    //     self.file.seek(SeekFrom::Start(location as u64))?;
    //     self.read_varint()?;
    //     Ok(self.read_varint()?.value)
    // }
}

type SqlRowClause = Box<dyn Fn(&TableRow) -> Result<bool>>;

#[derive(Debug)]
pub struct DatabaseHeader {
    pub page_size: u16,
    // number_of_pages: u32,
    // text_encoding: TextEncoding,
}

#[derive(Debug)]
pub enum TextEncoding {
    Utf8,
    Utf16le,
    Utf16be,
}

#[derive(Debug)]
pub enum SelectCell {
    NamedColumn(String),
    TotalRowCount,
}
