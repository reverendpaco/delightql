use super::dialect::SqlDialect;
use crate::pipeline::sql_ast_v3::{BinaryOperator, UnaryOperator};

pub(super) fn binary_operator_to_sql(op: &BinaryOperator, dialect: SqlDialect) -> &'static str {
    match op {
        BinaryOperator::Add => "+",
        BinaryOperator::Subtract => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        BinaryOperator::Equal => "=",
        BinaryOperator::NotEqual => match dialect {
            SqlDialect::SQLite | SqlDialect::PostgreSQL => "!=",
            SqlDialect::MySQL | SqlDialect::SqlServer => "<>",
        },
        BinaryOperator::LessThan => "<",
        BinaryOperator::LessThanOrEqual => "<=",
        BinaryOperator::GreaterThan => ">",
        BinaryOperator::GreaterThanOrEqual => ">=",
        BinaryOperator::And => "AND",
        BinaryOperator::Or => "OR",
        BinaryOperator::Concatenate => match dialect {
            SqlDialect::SQLite | SqlDialect::PostgreSQL => "||",
            SqlDialect::MySQL => "CONCAT",
            SqlDialect::SqlServer => "+",
        },
        BinaryOperator::Like => "LIKE",
        BinaryOperator::NotLike => "NOT LIKE",
        BinaryOperator::Is => "IS",
        BinaryOperator::IsNot => "IS NOT",
        BinaryOperator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
        BinaryOperator::IsDistinctFrom => "IS DISTINCT FROM",
    }
}

pub fn unary_operator_to_sql(op: &UnaryOperator) -> &'static str {
    match op {
        UnaryOperator::Not => "NOT",
        UnaryOperator::Minus => "-",
        UnaryOperator::Plus => "+",
    }
}
