





// pub struct Query {
//     /// `OFFSET <N> [ { ROW | ROWS } ]`
//     pub offset: Option<Offset>,
//     /// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
//     pub fetch: Option<Fetch>,
//     /// `FOR { UPDATE | SHARE } [ OF table_name ] [ SKIP LOCKED | NOWAIT ]`
//     pub locks: Vec<LockClause>,
//
// }

pub enum Selection {
    Identifier(String),
    AggFn(AggregateFunction),
}

pub enum AggregateFunction {
    Count,
    // Sum(Identefier)
}

// pub struct Identefier{
//     pub value: String,
// }

pub enum Source {
    Table(String),
}

pub enum Expression {
    InfixExpression(Box<Expression>, Opperator, Box<Expression>),
    Null,
    Literal,
    Identifier(String),
    Not(Box<Expression>),
}

pub enum Opperator {
    Equal,
    NotEqual,
    Like,
    And,
    Or,
}

#[derive(Debug, Clone)]
pub enum CellValue {
    Null,
    Int(i64),
    Float(f64),
    Blob(Vec<u8>),
    String(String),
}
