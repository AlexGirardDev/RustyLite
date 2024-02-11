//Select count()
use anyhow::{bail, Error, Ok, Result};

use itertools::Itertools;
use sqlparser::ast;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, Ident, Select, SelectItem, SetExpr, Statement,
};

use crate::sqlite::record::CellValue;
#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
}
impl TryFrom<&Statement> for Query {
    type Error = Error;

    fn try_from(value: &Statement) -> Result<Self> {
        match value {
            Statement::Query(q) => Ok(Query::Select(q.as_ref().try_into()?)),
            s => bail!("{} is not currently supported", s),
        }
    }
}

impl TryFrom<&ast::Query> for SelectQuery {
    type Error = Error;

    fn try_from(value: &ast::Query) -> Result<Self> {
        match value.body.as_ref() {
            sqlparser::ast::SetExpr::Select(select) => Ok(SelectQuery::new(select)?),
            e => bail!("{} queries are not currently supported", e),
        }
    }
}

#[derive(Debug)]
pub struct SelectQuery {
    pub selections: Vec<Selection>,
    pub sources: Vec<Source>,
    pub clause: Option<Expression>,
}

impl SelectQuery {
    pub fn new(select: &Select) -> Result<Self> {
        let selections: Vec<Selection> = select
            .projection
            .iter()
            .map(|m| m.try_into())
            .try_collect()?;

        if select.from.len() != 1 {
            bail!("only a single source is currently supported");
        }

        let sources: Vec<Source> = select
            .from
            .iter()
            .map(|f| match &f.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    if name.0.len() != 1 {
                        bail!("only single value table names are supported");
                    }
                    Ok(Source::Table(name.0[0].value.to_owned()))
                }
                _ => bail!("Only table sources are currently supported"),
            })
            .try_collect()?;

        let clause: Option<Expression> = select
            .selection
            .as_ref()
            .map(|s| s.try_into())
            .transpose()?;

        Ok(SelectQuery {
            selections,
            sources,
            clause,
        })
    }
}

impl Query {
    pub fn new(mut ast: Vec<Statement>) -> Result<Self> {
        let exp = match (ast.pop(), ast.pop()) {
            (Some(s), None) => s,
            _ => bail!("only a single expression is currently supported"),
        };

        Ok(match exp {
            Statement::Query(q) => match *q.body {
                SetExpr::Select(select) => Query::Select(SelectQuery::new(&select)?),
                e => bail!("{} queries are not currently supported", e),
            },
            q => bail!("{} queries are not currently supported", q),
        })
    }
}

#[derive(Debug)]
pub enum Selection {
    Identifier(String),
    AggFn(AggregateFunction),
}

impl TryFrom<&SelectItem> for Selection {
    type Error = Error;

    fn try_from(value: &SelectItem) -> Result<Self> {
        Ok(match value {
            SelectItem::UnnamedExpr(Expr::Function(Function { name, .. })) => {
                if name.0.len() != 1 {
                    bail!("only single field value functions are supported");
                }
                match name.0[0].value.to_lowercase().as_ref() {
                    "count" => Selection::AggFn(AggregateFunction::Count),
                    f => bail!("{} is not a supported select function", f),
                }
            }
            SelectItem::UnnamedExpr(Expr::Identifier(Ident { value, .. })) => {
                Selection::Identifier(value.to_owned())
            }
            t => bail!("{} is not a supported selection type", t),
        })
    }
}
#[derive(Debug)]
pub enum AggregateFunction {
    Count,
    // Sum(Identefier)
}

#[derive(Debug)]
pub enum Source {
    Table(String),
}

impl TryFrom<&Expr> for Expression {
    type Error = Error;
    fn try_from(value: &Expr) -> Result<Self> {
        Ok(match value {
            Expr::Identifier(ident) => Expression::Identifier(ident.value.to_owned()),
            Expr::Value(v) => match v {
                sqlparser::ast::Value::Number(_, _) => todo!(),
                sqlparser::ast::Value::RawStringLiteral(s)
                | sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::EscapedStringLiteral(s)
                | sqlparser::ast::Value::SingleQuotedByteStringLiteral(s)
                | sqlparser::ast::Value::DoubleQuotedByteStringLiteral(s)
                | sqlparser::ast::Value::NationalStringLiteral(s)
                | sqlparser::ast::Value::HexStringLiteral(s)
                | sqlparser::ast::Value::Placeholder(s)
                | sqlparser::ast::Value::UnQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => {
                    Expression::Literal(CellValue::String(s.to_owned()))
                }
                sqlparser::ast::Value::Boolean(b) => match *b {
                    true => Expression::Literal(CellValue::Int(1)),
                    false => Expression::Literal(CellValue::Int(0)),
                },
                sqlparser::ast::Value::Null => Expression::Literal(CellValue::Null),
                e => bail!("{} is as unsupported expression type", e),
            },
            Expr::BinaryOp { left, op, right } => Expression::InfixExpression(
                Box::new(left.as_ref().try_into()?),
                op.try_into()?,
                Box::new(right.as_ref().try_into()?),
            ),
            _ => todo!(),
        })
    }
}

#[derive(Debug)]
pub enum Expression {
    InfixExpression(Box<Expression>, Operator, Box<Expression>),
    Literal(CellValue),
    Identifier(String),
}
#[derive(Debug)]
pub enum Object{
    Bool(bool),
    String(String)
}

#[derive(Debug)]
pub enum Operator {
    Equal,
    NotEqual,
    // Like,
    And,
    Or,
}

impl TryFrom<&BinaryOperator> for Operator {
    type Error = Error;

    fn try_from(value: &BinaryOperator) -> Result<Self> {
        Ok(match value {
            BinaryOperator::Eq => Operator::Equal,
            BinaryOperator::NotEq => Operator::NotEqual,
            BinaryOperator::And => Operator::And,
            BinaryOperator::Or => Operator::Or,
            o => bail!("{} is an unsupported opperator", o),
        })
    }
}
// #[derive(Debug, Clone)]
// pub enum CellValue {
//     Null,
//     Int(i64),
//     Float(f64),
//     Blob(Vec<u8>),
//     String(String),
// }
