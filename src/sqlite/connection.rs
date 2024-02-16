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
            eprintln!("index search");
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

    pub fn dump_page(&self, page_number: u32) {
        let wow = self.db.read_table_page(page_number, None).unwrap();
        match &wow {
            crate::sqlite::page::TablePage::Leaf(leaf) => {
                for (page_number, offset) in &leaf.cell_pointers {
                    let record = self.db.read_record(*page_number, *offset).unwrap();
                    println!("{:?}",record);
                }
            }
            crate::sqlite::page::TablePage::Interior(int) => {}
        }
        println!("{:?}", wow);
    }
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
