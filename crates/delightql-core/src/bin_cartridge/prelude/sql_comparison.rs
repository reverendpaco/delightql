// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `sql_eq()` / `sql_ne()` — the target's own equality and inequality as
//! explicit prelude sigma predicates.
//!
//! Syntax: `+sql_eq(l, r)`, `+sql_ne(l, r)`, qualified
//! `+std::prelude.sql_eq(l, r)` when a nearer definition shadows the bare
//! name.
//!
//! DelightQL's `=` / `!=` are null-safe. These predicates are the engine's
//! answer instead — three-valued, UNKNOWN when either operand is null, with
//! the target's own coercions and collations — and they always lower to the
//! target's ordinary `=` and inequality operation. The selected identity
//! survives as a predicate rewrite through every SQL-AST pass, so no join
//! rewrite, optimizer or legalizer can read it back as null-safe equality:
//! the comparison node exists only inside generation, where the dialect
//! spells the operator (`<>` where the target wants it).

use crate::bin_cartridge::{
    BinEntity, EntitySignature, GeneratorContext, OutputSchema, Parameter, SqlGeneratable,
};
use crate::enums::EntityType;
use crate::error::Result;
use crate::pipeline::sql_ast::{BinaryOperator, DomainExpression};

/// The target SQL comparison an entity stands for.
#[derive(Clone, Copy)]
enum SqlComparison {
    Equal,
    NotEqual,
}

impl SqlComparison {
    fn name(self) -> &'static str {
        match self {
            SqlComparison::Equal => "sql_eq",
            SqlComparison::NotEqual => "sql_ne",
        }
    }

    fn operator(self) -> BinaryOperator {
        match self {
            SqlComparison::Equal => BinaryOperator::Equal,
            SqlComparison::NotEqual => BinaryOperator::NotEqual,
        }
    }
}

/// `sql_eq(l, r)`: target SQL `=`.
pub struct SqlEqPredicate;

/// `sql_ne(l, r)`: target SQL `<>` (or the dialect's ordinary spelling).
pub struct SqlNePredicate;

fn signature() -> EntitySignature {
    EntitySignature {
        parameters: vec![
            Parameter {
                name: "left".to_string(),
                data_type: "Any".to_string(),
                _is_optional: false,
            },
            Parameter {
                name: "right".to_string(),
                data_type: "Any".to_string(),
                _is_optional: false,
            },
        ],
        // A sigma predicate is a truth per row, never a relation.
        output_schema: OutputSchema::Void,
    }
}

fn generate(
    comparison: SqlComparison,
    args: &[DomainExpression],
    context: &GeneratorContext<'_>,
    negated: bool,
) -> Result<String> {
    let [left, right] = args else {
        return Err(crate::error::DelightQLError::validation_error(
            &format!(
                "{} expects 2 arguments, got {}",
                comparison.name(),
                args.len()
            ),
            "SqlComparison::generate_sql",
        ));
    };
    // The comparison is built as an ordinary SQL-AST node and rendered by
    // the generator, so the dialect's operator spelling applies to it
    // exactly as to any compiler-constructed comparison.
    let compared = DomainExpression::Binary {
        left: Box::new(left.clone()),
        op: comparison.operator(),
        right: Box::new(right.clone()),
    };
    let rendered = (context.render_expr)(&compared)?;
    Ok(if negated {
        format!("NOT ({rendered})")
    } else {
        rendered
    })
}

impl BinEntity for SqlEqPredicate {
    fn name(&self) -> &str {
        SqlComparison::Equal.name()
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinSigmaPredicate
    }

    fn signature(&self) -> EntitySignature {
        signature()
    }

    fn as_sql_generatable(&self) -> Option<&dyn SqlGeneratable> {
        Some(self)
    }
}

impl SqlGeneratable for SqlEqPredicate {
    fn generate_sql<'a>(
        &self,
        args: &[DomainExpression],
        context: &GeneratorContext<'a>,
        negated: bool,
    ) -> Result<String> {
        generate(SqlComparison::Equal, args, context, negated)
    }
}

impl BinEntity for SqlNePredicate {
    fn name(&self) -> &str {
        SqlComparison::NotEqual.name()
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinSigmaPredicate
    }

    fn signature(&self) -> EntitySignature {
        signature()
    }

    fn as_sql_generatable(&self) -> Option<&dyn SqlGeneratable> {
        Some(self)
    }
}

impl SqlGeneratable for SqlNePredicate {
    fn generate_sql<'a>(
        &self,
        args: &[DomainExpression],
        context: &GeneratorContext<'a>,
        negated: bool,
    ) -> Result<String> {
        generate(SqlComparison::NotEqual, args, context, negated)
    }
}
