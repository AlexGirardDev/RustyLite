use sqlparser::{dialect::SQLiteDialect, parser::Parser};

use crate::sqlite;

use super::record::CellValue;

static DIALECT: SQLiteDialect = SQLiteDialect {};

#[test]
fn sql_test() {
    let _ = sqlite::open("sample.db");
    let sql = "select name, id from apples,";

    let ast = Parser::parse_sql(&DIALECT, sql).unwrap();
    panic!("{:?}", ast);
}

#[test]
fn sort_test() {
    assert!(CellValue::String("aaa".to_string()) < CellValue::String("bbb".to_string()));
}
