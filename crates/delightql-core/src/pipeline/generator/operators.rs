// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::dialect::SqlDialect;
use super::errors::GeneratorError;
use crate::pipeline::dialect_pack::DialectPack;
use crate::pipeline::sql_ast::{BinaryOperator, UnaryOperator};

/// Spell a binary operator: canonical (SQLite) token unless the dialect
/// pack carries a `dialect_render` override for this operator's render key.
/// Code positions the operator (infix, spacing); data spells it.
pub(super) fn binary_operator_to_sql<'a>(
    op: &BinaryOperator,
    dialect: SqlDialect,
    pack: &'a DialectPack,
) -> Result<&'a str, GeneratorError> {
    let (render_key, canonical) = match op {
        BinaryOperator::Add => ("op.add", "+"),
        BinaryOperator::Subtract => ("op.subtract", "-"),
        BinaryOperator::Multiply => ("op.multiply", "*"),
        BinaryOperator::Divide => ("op.divide", "/"),
        BinaryOperator::Modulo => ("op.modulo", "%"),
        BinaryOperator::Equal => ("op.equal", "="),
        BinaryOperator::NotEqual => ("op.not_equal", "!="),
        BinaryOperator::LessThan => ("op.less_than", "<"),
        BinaryOperator::LessThanOrEqual => ("op.less_than_or_equal", "<="),
        BinaryOperator::GreaterThan => ("op.greater_than", ">"),
        BinaryOperator::GreaterThanOrEqual => ("op.greater_than_or_equal", ">="),
        BinaryOperator::And => ("op.and", "AND"),
        BinaryOperator::Or => ("op.or", "OR"),
        BinaryOperator::Concatenate => ("op.concatenate", "||"),
        BinaryOperator::Like => ("op.like", "LIKE"),
        BinaryOperator::NotLike => ("op.not_like", "NOT LIKE"),
        BinaryOperator::Is => ("op.is", "IS"),
        BinaryOperator::IsNot => ("op.is_not", "IS NOT"),
        BinaryOperator::IsNotDistinctFrom => ("op.is_not_distinct_from", "IS NOT DISTINCT FROM"),
        BinaryOperator::IsDistinctFrom => ("op.is_distinct_from", "IS DISTINCT FROM"),
    };
    match pack.render(dialect.family_name(), render_key) {
        Some(rule) => rule.template().map_err(GeneratorError::Error),
        None => Ok(canonical),
    }
}

pub fn unary_operator_to_sql(op: &UnaryOperator) -> &'static str {
    match op {
        UnaryOperator::Not => "NOT",
        UnaryOperator::Minus => "-",
        UnaryOperator::Plus => "+",
    }
}
