use sqlparser::{dialect::SQLiteDialect, parser::Parser};

use crate::sqlite;

static DIALECT: SQLiteDialect = SQLiteDialect {};

#[test]
fn sql_test() {
    let conn = sqlite::open("sample.db");
    let sql = "select name, id from apples,";

    let ast = Parser::parse_sql(&DIALECT, sql).unwrap();
    panic!("{:?}", ast);
}
