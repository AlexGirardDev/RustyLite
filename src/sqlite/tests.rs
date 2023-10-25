use sqlparser::{
    dialect::SQLiteDialect,
    parser::Parser,
};

static DIALECT: SQLiteDialect = SQLiteDialect {};

#[test]
fn sql_test() {
    let sql = "select name, id from apples";

    let ast = Parser::parse_sql(&DIALECT, sql).unwrap();
    panic!("{:?}", ast);
}
